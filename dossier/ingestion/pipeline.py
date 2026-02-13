"""
DOSSIER — Ingestion Pipeline
Orchestrates: file intake → text extraction → NER → classification → DB storage.
"""

import shutil
from pathlib import Path
from datetime import datetime

from dossier.ingestion.extractor import extract_text, file_hash
from dossier.core.ner import extract_entities, classify_document, generate_title
from dossier.db.database import get_db


PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def ingest_file(filepath: str, source: str = "", date: str = "") -> dict:
    """
    Ingest a single file into the system.

    Returns dict with:
      - success: bool
      - document_id: int (if success)
      - message: str
      - stats: dict with entity/keyword counts
    """
    filepath = Path(filepath)
    if not filepath.exists():
        return {"success": False, "message": f"File not found: {filepath}"}

    # ─── Step 1: Check for duplicates ───
    fhash = file_hash(str(filepath))
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id, filename FROM documents WHERE file_hash = ?", (fhash,)
        ).fetchone()
        if existing:
            return {
                "success": False,
                "message": f"Duplicate: already ingested as '{existing['filename']}' (id={existing['id']})",
            }

    # ─── Step 2: Extract text ───
    print(f"[INGEST] Extracting text from {filepath.name}...")
    extraction = extract_text(str(filepath))
    raw_text = extraction["text"]

    if not raw_text or len(raw_text.strip()) < 20:
        return {
            "success": False,
            "message": f"No text extracted from {filepath.name} (method: {extraction['method']})",
        }

    pages = extraction["pages"]
    print(f"[INGEST] Extracted {len(raw_text)} chars, {pages} pages via {extraction['method']}")

    # ─── Step 3: NER + Classification ───
    print("[INGEST] Running NER...")
    entities = extract_entities(raw_text)

    category = classify_document(raw_text, filepath.name)
    title = generate_title(raw_text, filepath.name)
    print(f"[INGEST] Category: {category} | Title: {title}")

    # ─── Step 4: Copy to processed directory ───
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cat_dir = PROCESSED_DIR / category
    cat_dir.mkdir(exist_ok=True)
    dest = cat_dir / filepath.name
    if dest.exists():
        stem = filepath.stem
        suffix = filepath.suffix
        dest = cat_dir / f"{stem}_{fhash[:8]}{suffix}"
    shutil.copy2(str(filepath), str(dest))

    # ─── Step 5: Store in database ───
    with get_db() as conn:
        # Insert document
        cursor = conn.execute(
            """
            INSERT INTO documents (filename, filepath, title, category, source, date, pages, file_hash, raw_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                filepath.name,
                str(dest),
                title,
                category,
                source or "Manual Upload",
                date or datetime.now().strftime("%Y-%m-%d"),
                pages,
                fhash,
                raw_text,
            ),
        )
        doc_id = cursor.lastrowid

        # Store entities
        entity_count = 0
        for etype, elist in [
            ("person", entities["people"]),
            ("place", entities["places"]),
            ("org", entities["orgs"]),
            ("date", entities["dates"]),
        ]:
            for ent in elist:
                canonical = ent["name"].lower().strip()
                # Upsert entity
                conn.execute(
                    """
                    INSERT INTO entities (name, type, canonical)
                    VALUES (?, ?, ?)
                    ON CONFLICT(canonical, type) DO NOTHING
                """,
                    (ent["name"], etype, canonical),
                )

                entity_row = conn.execute(
                    "SELECT id FROM entities WHERE canonical = ? AND type = ?", (canonical, etype)
                ).fetchone()

                if entity_row:
                    conn.execute(
                        """
                        INSERT INTO document_entities (document_id, entity_id, count)
                        VALUES (?, ?, ?)
                        ON CONFLICT(document_id, entity_id) DO UPDATE SET count = count + excluded.count
                    """,
                        (doc_id, entity_row["id"], ent["count"]),
                    )
                    entity_count += 1

        # Store keywords
        keyword_count = 0
        for kw in entities["keywords"][:50]:
            conn.execute(
                """
                INSERT INTO keywords (word, total_count, doc_count)
                VALUES (?, ?, 1)
                ON CONFLICT(word) DO UPDATE SET
                    total_count = total_count + excluded.total_count,
                    doc_count = doc_count + 1
            """,
                (kw["word"], kw["count"]),
            )

            kw_row = conn.execute(
                "SELECT id FROM keywords WHERE word = ?", (kw["word"],)
            ).fetchone()

            if kw_row:
                conn.execute(
                    """
                    INSERT INTO document_keywords (document_id, keyword_id, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(document_id, keyword_id) DO UPDATE SET count = count + excluded.count
                """,
                    (doc_id, kw_row["id"], kw["count"]),
                )
                keyword_count += 1

        # Build entity co-occurrence connections
        doc_entity_ids = [
            row["entity_id"]
            for row in conn.execute(
                "SELECT entity_id FROM document_entities WHERE document_id = ?", (doc_id,)
            ).fetchall()
        ]

        for i, eid_a in enumerate(doc_entity_ids):
            for eid_b in doc_entity_ids[i + 1 :]:
                a, b = min(eid_a, eid_b), max(eid_a, eid_b)
                conn.execute(
                    """
                    INSERT INTO entity_connections (entity_a_id, entity_b_id, weight)
                    VALUES (?, ?, 1)
                    ON CONFLICT(entity_a_id, entity_b_id) DO UPDATE SET weight = weight + 1
                """,
                    (a, b),
                )

        # ─── Step 6: Timeline extraction ───
        entity_names = [
            ent["name"]
            for etype_list in [entities["people"], entities["places"], entities["orgs"]]
            for ent in etype_list
        ]
        from dossier.forensics.timeline import TimelineExtractor, store_events

        extractor = TimelineExtractor(entity_names=entity_names)
        timeline_events = extractor.extract_events(raw_text, document_id=doc_id)
        timeline_event_ids = store_events(conn, timeline_events)

        # ─── Step 7: Entity resolution ───
        from dossier.core.resolver import EntityResolver

        resolver = EntityResolver(conn)
        resolution = resolver.resolve_all()

    stats = {
        "people": len(entities["people"]),
        "places": len(entities["places"]),
        "orgs": len(entities["orgs"]),
        "dates": len(entities["dates"]),
        "keywords": keyword_count,
        "timeline_events": len(timeline_event_ids),
        "resolved_entities": resolution.auto_merged,
        "suggested_merges": resolution.suggested,
        "text_length": len(raw_text),
        "pages": pages,
        "method": extraction["method"],
    }

    print(f"[INGEST] ✓ Document {doc_id}: {entity_count} entities, {keyword_count} keywords")
    return {
        "success": True,
        "document_id": doc_id,
        "message": "Ingested successfully",
        "stats": stats,
    }


def ingest_directory(dirpath: str, source: str = "") -> list[dict]:
    """Ingest all supported files in a directory."""
    dirpath = Path(dirpath)
    supported = {
        ".pdf",
        ".txt",
        ".md",
        ".html",
        ".htm",
        ".png",
        ".jpg",
        ".jpeg",
        ".tiff",
        ".tif",
        ".bmp",
    }
    results = []

    for f in sorted(dirpath.iterdir()):
        if f.suffix.lower() in supported and f.is_file():
            result = ingest_file(str(f), source=source)
            results.append(result)
            print(f"  {'✓' if result['success'] else '✗'} {f.name}: {result['message']}")

    return results
