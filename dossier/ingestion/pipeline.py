"""
DOSSIER — Ingestion Pipeline
Orchestrates: file intake → text extraction → NER → classification →
forensic analysis → DB storage.

Supports recursive directory scanning, ZIP extraction, and all common media types.
"""

import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

from dossier.ingestion.extractor import (
    extract_text,
    extract_zip,
    file_hash,
    SUPPORTED_EXTENSIONS,
)
from dossier.core.ner import extract_entities, classify_document, generate_title
from dossier.core.forensic_analyzer import analyze_document
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

    # ─── Step 4: Forensic Analysis ───
    print("[INGEST] Running forensic analysis...")
    forensics = analyze_document(raw_text, filepath.name)
    risk_score = forensics["risk_score"]
    if risk_score > 0:
        print(f"[INGEST] Risk score: {risk_score:.3f} | AML flags: {len(forensics['aml_flags'])}")

    # ─── Step 5: Copy to processed directory ───
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cat_dir = PROCESSED_DIR / category
    cat_dir.mkdir(exist_ok=True)
    dest = cat_dir / filepath.name
    if dest.exists():
        stem = filepath.stem
        suffix = filepath.suffix
        dest = cat_dir / f"{stem}_{fhash[:8]}{suffix}"
    shutil.copy2(str(filepath), str(dest))

    # ─── Step 6: Store in database ───
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

        # ─── Step 6b: PDF metadata extraction ───
        pdf_metadata_extracted = False
        if filepath.suffix.lower() == ".pdf":
            from dossier.forensics.provenance import (
                extract_pdf_metadata,
                store_pdf_metadata,
                _ensure_pdf_metadata_table,
            )

            _ensure_pdf_metadata_table(conn)
            pdf_meta = extract_pdf_metadata(str(filepath), document_id=doc_id)
            if pdf_meta:
                store_pdf_metadata(conn, pdf_meta)
                pdf_metadata_extracted = True

        # ─── Step 7: Store forensic results ───
        _store_forensics(conn, doc_id, forensics)

        # ─── Step 8: Timeline extraction ───
        entity_names = [
            ent["name"]
            for etype_list in [entities["people"], entities["places"], entities["orgs"]]
            for ent in etype_list
        ]
        from dossier.forensics.timeline import TimelineExtractor, store_events

        extractor = TimelineExtractor(entity_names=entity_names)
        timeline_events = extractor.extract_events(raw_text, document_id=doc_id)
        timeline_event_ids = store_events(conn, timeline_events)

        # ─── Step 9: Entity resolution ───
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
        "risk_score": risk_score,
        "aml_flags": len(forensics["aml_flags"]),
        "topics": [t["label"] for t in forensics["topics"][:3]],
        "intents": [i["label"] for i in forensics["intents"][:2]],
        "pdf_metadata_extracted": pdf_metadata_extracted,
    }

    print(
        f"[INGEST] Document {doc_id}: {entity_count} entities, {keyword_count} keywords, risk={risk_score:.3f}"
    )
    return {
        "success": True,
        "document_id": doc_id,
        "message": "Ingested successfully",
        "stats": stats,
    }


def _store_forensics(conn, doc_id: int, forensics: dict) -> None:
    """Store forensic analysis results in the database."""

    # Intents
    for intent in forensics["intents"]:
        conn.execute(
            """
            INSERT INTO document_forensics (document_id, analysis_type, label, score, evidence)
            VALUES (?, 'intent', ?, ?, ?)
            ON CONFLICT(document_id, analysis_type, label) DO UPDATE SET
                score = excluded.score, evidence = excluded.evidence
        """,
            (doc_id, intent["label"], intent["score"], json.dumps(intent["evidence"])),
        )

    # Topics
    for topic in forensics["topics"]:
        conn.execute(
            """
            INSERT INTO document_forensics (document_id, analysis_type, label, score)
            VALUES (?, 'topic', ?, ?)
            ON CONFLICT(document_id, analysis_type, label) DO UPDATE SET score = excluded.score
        """,
            (doc_id, topic["label"], topic["score"]),
        )

    # AML flags
    for flag in forensics["aml_flags"]:
        conn.execute(
            """
            INSERT INTO document_forensics (document_id, analysis_type, label, severity, evidence)
            VALUES (?, 'aml_flag', ?, ?, ?)
            ON CONFLICT(document_id, analysis_type, label) DO UPDATE SET
                severity = excluded.severity, evidence = excluded.evidence
        """,
            (doc_id, flag["flag"], flag["severity"], json.dumps(flag["evidence"])),
        )

    # Codewords
    for cw in forensics["codewords"]:
        conn.execute(
            """
            INSERT INTO document_forensics (document_id, analysis_type, label, score, evidence)
            VALUES (?, 'codeword', ?, ?, ?)
            ON CONFLICT(document_id, analysis_type, label) DO UPDATE SET
                score = excluded.score, evidence = excluded.evidence
        """,
            (doc_id, cw["word"], cw["count"], json.dumps([cw["context"]])),
        )

    # Risk score
    conn.execute(
        """
        INSERT INTO document_forensics (document_id, analysis_type, label, score)
        VALUES (?, 'risk_score', 'overall', ?)
        ON CONFLICT(document_id, analysis_type, label) DO UPDATE SET score = excluded.score
    """,
        (doc_id, forensics["risk_score"]),
    )

    # Phrases
    for phrase_data in forensics["phrases"]:
        conn.execute(
            """
            INSERT INTO phrases (phrase, doc_count, total_count)
            VALUES (?, 1, ?)
            ON CONFLICT(phrase) DO UPDATE SET
                total_count = total_count + excluded.total_count,
                doc_count = doc_count + 1
        """,
            (phrase_data["phrase"], phrase_data["count"]),
        )

        phrase_row = conn.execute(
            "SELECT id FROM phrases WHERE phrase = ?", (phrase_data["phrase"],)
        ).fetchone()

        if phrase_row:
            conn.execute(
                """
                INSERT INTO document_phrases (document_id, phrase_id, count)
                VALUES (?, ?, ?)
                ON CONFLICT(document_id, phrase_id) DO UPDATE SET count = count + excluded.count
            """,
                (doc_id, phrase_row["id"], phrase_data["count"]),
            )

    # Financial indicators
    for fi in forensics["financial_indicators"]:
        conn.execute(
            """
            INSERT INTO financial_indicators (document_id, indicator_type, value, context, risk_score)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(document_id, indicator_type, value) DO UPDATE SET
                context = excluded.context, risk_score = excluded.risk_score
        """,
            (doc_id, fi["type"], fi["value"], fi["context"], fi["risk_score"]),
        )


def ingest_directory(dirpath: str, source: str = "") -> list[dict]:
    """Ingest all supported files in a directory (non-recursive, legacy)."""
    dirpath = Path(dirpath)
    results = []

    for f in sorted(dirpath.iterdir()):
        if f.suffix.lower() in SUPPORTED_EXTENSIONS and f.is_file() and f.suffix.lower() != ".zip":
            result = ingest_file(str(f), source=source)
            results.append(result)
            print(f"  {'✓' if result['success'] else '✗'} {f.name}: {result['message']}")

    return results


def scan_path(path: str, source: str = "", recursive: bool = True) -> dict:
    """
    Recursively scan a path (file, directory, or ZIP) and ingest everything.

    Returns:
    {
        "total_files": int,
        "ingested": int,
        "failed": int,
        "skipped": int,
        "results": [dict],
    }
    """
    path = Path(path)
    results = []
    skipped = 0

    if not path.exists():
        return {
            "total_files": 0,
            "ingested": 0,
            "failed": 0,
            "skipped": 0,
            "results": [{"success": False, "message": f"Path not found: {path}"}],
        }

    files_to_ingest = []

    if path.is_file():
        if path.suffix.lower() == ".zip":
            # Extract ZIP to temp dir, then scan contents
            with tempfile.TemporaryDirectory(prefix="dossier_zip_") as tmpdir:
                print(f"[SCAN] Extracting ZIP: {path.name}...")
                extracted = extract_zip(str(path), tmpdir)
                print(f"[SCAN] Extracted {len(extracted)} files from {path.name}")
                for ef in extracted:
                    ef_path = Path(ef)
                    if ef_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                        result = ingest_file(ef, source=source or f"ZIP:{path.name}")
                        results.append(result)
                        status = "✓" if result["success"] else "✗"
                        print(f"  {status} {ef_path.name}: {result['message']}")
                    else:
                        skipped += 1

                return _summarize(results, skipped)
        else:
            files_to_ingest.append(path)
    elif path.is_dir():
        # Collect all files
        iterator = path.rglob("*") if recursive else path.iterdir()
        for f in sorted(iterator):
            if not f.is_file():
                continue
            if f.suffix.lower() == ".zip":
                # Extract and ingest ZIP contents
                with tempfile.TemporaryDirectory(prefix="dossier_zip_") as tmpdir:
                    print(f"[SCAN] Extracting ZIP: {f.name}...")
                    extracted = extract_zip(str(f), tmpdir)
                    print(f"[SCAN] Extracted {len(extracted)} files from {f.name}")
                    for ef in extracted:
                        ef_path = Path(ef)
                        if ef_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                            result = ingest_file(ef, source=source or f"ZIP:{f.name}")
                            results.append(result)
                            status = "✓" if result["success"] else "✗"
                            print(f"  {status} {ef_path.name}: {result['message']}")
                        else:
                            skipped += 1
            elif f.suffix.lower() in SUPPORTED_EXTENSIONS:
                files_to_ingest.append(f)
            else:
                skipped += 1

    # Ingest collected files
    total = len(files_to_ingest)
    for i, f in enumerate(files_to_ingest, 1):
        print(f"\n[SCAN] ({i}/{total}) {f.name}")
        result = ingest_file(str(f), source=source)
        results.append(result)
        status = "✓" if result["success"] else "✗"
        print(f"  {status} {result['message']}")

    return _summarize(results, skipped)


def ingest_mbox(filepath: str, source: str = "", limit: int = 0) -> dict:
    """
    Ingest emails from an mbox file. Each email becomes a separate document.

    Returns summary dict with ingested/failed/skipped counts.
    """
    import mailbox
    import hashlib
    import re as _re

    mbox_path = Path(filepath)
    if not mbox_path.exists():
        return {"total": 0, "ingested": 0, "failed": 0, "skipped": 0}

    mbox = mailbox.mbox(str(mbox_path))
    results = []
    skipped = 0
    total_msgs = len(mbox)

    if limit:
        total_msgs = min(total_msgs, limit)

    print(f"[MBOX] Processing {total_msgs} emails from {mbox_path.name}...")

    for i, message in enumerate(mbox):
        if limit and i >= limit:
            break

        if (i + 1) % 50 == 0:
            ingested_so_far = sum(1 for r in results if r.get("success"))
            print(f"[MBOX] ({i + 1}/{total_msgs}) {ingested_so_far} ingested...")

        # Extract email text
        parts = []
        subject = message.get("Subject", "(no subject)")
        from_addr = message.get("From", "")
        to_addr = message.get("To", "")
        cc_addr = message.get("Cc", "")
        date_str = message.get("Date", "")

        # Headers
        if from_addr:
            parts.append(f"From: {from_addr}")
        if to_addr:
            parts.append(f"To: {to_addr}")
        if cc_addr:
            parts.append(f"Cc: {cc_addr}")
        if subject:
            parts.append(f"Subject: {subject}")
        if date_str:
            parts.append(f"Date: {date_str}")
        parts.append("")

        # Body
        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        parts.append(payload.decode(charset, errors="replace"))
                elif content_type == "text/html":
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        html_text = payload.decode(charset, errors="replace")
                        clean = _re.sub(r"<[^>]+>", " ", html_text)
                        clean = _re.sub(r"\s+", " ", clean).strip()
                        parts.append(clean)
        else:
            payload = message.get_payload(decode=True)
            if payload:
                charset = message.get_content_charset() or "utf-8"
                parts.append(payload.decode(charset, errors="replace"))

        raw_text = "\n".join(parts)

        if len(raw_text.strip()) < 20:
            skipped += 1
            continue

        # Dedup by content hash
        content_hash = hashlib.sha256(raw_text.encode()).hexdigest()

        with get_db() as conn:
            existing = conn.execute(
                "SELECT id FROM documents WHERE file_hash = ?", (content_hash,)
            ).fetchone()
            if existing:
                results.append({"success": False, "message": "duplicate"})
                continue

        # Write to temp file and ingest through normal pipeline
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", prefix=f"mbox_email_{i}_", delete=False
        ) as tmp:
            tmp.write(raw_text)
            tmp_path = tmp.name

        try:
            result = ingest_file(
                tmp_path,
                source=source or f"MBOX:{mbox_path.name}",
                date=_parse_email_date(date_str),
            )
            results.append(result)
            if result["success"] and (i + 1) % 50 == 0:
                stats = result.get("stats", {})
                risk = stats.get("risk_score", 0)
                if risk > 0.3:
                    print(f"    HIGH RISK: {subject[:60]} (risk={risk:.3f})")
        except Exception as e:
            results.append({"success": False, "message": str(e)})
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    mbox.close()

    ingested = sum(1 for r in results if r.get("success"))
    failed = sum(1 for r in results if not r.get("success") and r.get("message") != "duplicate")
    dupes = sum(1 for r in results if r.get("message") == "duplicate")

    return {
        "total": total_msgs,
        "ingested": ingested,
        "failed": failed,
        "skipped": skipped + dupes,
        "results": results,
    }


def _parse_email_date(date_str: str) -> str:
    """Try to parse an email Date header into YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _summarize(results: list[dict], skipped: int) -> dict:
    """Summarize scan results."""
    ingested = sum(1 for r in results if r.get("success"))
    failed = len(results) - ingested
    return {
        "total_files": len(results) + skipped,
        "ingested": ingested,
        "failed": failed,
        "skipped": skipped,
        "results": results,
    }
