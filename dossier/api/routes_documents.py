"""DOSSIER — Document CRUD, text, notes, provenance routes."""

import re
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request

from dossier.api import utils
from dossier.db.database import get_db

router = APIRouter()


# ═══════════════════════════════════════════
# DOCUMENTS
# ═══════════════════════════════════════════


@router.get("/documents")
def list_documents(
    category: Optional[str] = None,
    flagged: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
):
    with get_db() as conn:
        sql = "SELECT id, filename, title, category, source, date, pages, flagged, ingested_at FROM documents WHERE 1=1"
        params = []
        if category:
            sql += " AND category = ?"
            params.append(category)
        if flagged is not None:
            sql += " AND flagged = ?"
            params.append(1 if flagged else 0)
        sql += " ORDER BY ingested_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            doc = dict(row)
            doc["entities"] = utils._get_doc_entities(conn, doc["id"])
            results.append(doc)

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

    return {"documents": results, "total": total}


@router.get("/documents/{doc_id}")
def get_document(doc_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")

        doc = dict(row)
        doc["entities"] = utils._get_doc_entities(conn, doc_id)

        # Get keywords for this document
        kw_rows = conn.execute(
            """
            SELECT k.word, dk.count
            FROM document_keywords dk
            JOIN keywords k ON k.id = dk.keyword_id
            WHERE dk.document_id = ?
            ORDER BY dk.count DESC
            LIMIT 30
        """,
            (doc_id,),
        ).fetchall()
        doc["keywords"] = [{"word": r["word"], "count": r["count"]} for r in kw_rows]

    return doc


@router.post("/documents/{doc_id}/flag")
def toggle_flag(doc_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT flagged FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
        new_val = 0 if row["flagged"] else 1
        conn.execute("UPDATE documents SET flagged = ? WHERE id = ?", (new_val, doc_id))
    return {"id": doc_id, "flagged": bool(new_val)}


# ═══════════════════════════════════════════
# SOURCES + DOCUMENT TEXT
# ═══════════════════════════════════════════


@router.get("/sources")
def list_sources():
    """List all document sources with counts."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT source, COUNT(*) as count, SUM(pages) as pages
            FROM documents
            WHERE source IS NOT NULL AND source != ''
            GROUP BY source ORDER BY count DESC
        """).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/documents/{doc_id}/text")
def get_document_text(doc_id: int):
    """Get full raw text of a document for reading."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, filename, title, category, source, pages, raw_text FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
    doc = dict(row)
    doc["text"] = doc.pop("raw_text") or ""
    doc["char_count"] = len(doc["text"])
    return doc


# ═══════════════════════════════════════════
# DOCUMENT NOTES
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/notes")
def get_document_notes(doc_id: int):
    """Get notes for a document."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT notes, flagged FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
    return {"document_id": doc_id, "notes": row["notes"] or "", "flagged": bool(row["flagged"])}


@router.post("/documents/{doc_id}/notes")
async def save_document_notes(doc_id: int, request: Request):
    """Save investigation notes for a document."""
    body = await request.json()
    notes_text = body.get("notes", "")
    with get_db() as conn:
        row = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
        conn.execute("UPDATE documents SET notes = ? WHERE id = ?", (notes_text, doc_id))
    return {"document_id": doc_id, "notes": notes_text, "saved": True}


# ═══════════════════════════════════════════
# SIMILAR DOCUMENTS
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/similar")
def document_similar(doc_id: int, limit: int = Query(10, ge=1, le=50)):
    """Find documents most similar to this one based on shared entities."""
    with get_db() as conn:
        row = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")

        # Jaccard-like similarity: shared entities / union of entities
        similar = conn.execute(
            """
            SELECT d.id, d.title, d.filename, d.category, d.source, d.date, d.pages,
                   COUNT(DISTINCT de2.entity_id) as shared_entities,
                   (SELECT COUNT(DISTINCT entity_id) FROM document_entities
                    WHERE document_id = d.id) as target_total
            FROM document_entities de1
            JOIN document_entities de2 ON de1.entity_id = de2.entity_id
              AND de2.document_id != ?
            JOIN documents d ON d.id = de2.document_id
            WHERE de1.document_id = ?
            GROUP BY d.id
            ORDER BY shared_entities DESC
            LIMIT ?
        """,
            (doc_id, doc_id, limit),
        ).fetchall()

        # Get source doc entity count for similarity score
        src_total = (
            conn.execute(
                "SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = ?",
                (doc_id,),
            ).fetchone()[0]
            or 1
        )

        results = []
        for r in similar:
            doc = dict(r)
            union = src_total + (doc["target_total"] or 1) - doc["shared_entities"]
            doc["similarity"] = round(doc["shared_entities"] / max(union, 1), 3)
            results.append(doc)

    return {"doc_id": doc_id, "similar": results}


# ═══════════════════════════════════════════
# CROSS-REFERENCES
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/cross-references")
def cross_references(
    doc_id: int,
    text: str = Query("", description="Selected text passage"),
    limit: int = Query(10, ge=1, le=30),
):
    """Find other documents containing the same entities/phrases from a text selection."""
    with get_db() as conn:
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        results = []

        if text.strip():
            # Extract entity names from the selected text by matching known entities
            text_lower = text.lower()
            entity_matches = conn.execute(
                """
                SELECT DISTINCT e.id, e.name, e.type
                FROM entities e
                WHERE LENGTH(e.name) >= 3 AND LOWER(e.name) != ''
                  AND ? LIKE '%' || LOWER(e.name) || '%'
                ORDER BY LENGTH(e.name) DESC
                LIMIT 20
            """,
                (text_lower,),
            ).fetchall()

            if entity_matches:
                entity_ids = [e["id"] for e in entity_matches]
                ph = ",".join("?" * len(entity_ids))
                xrefs = conn.execute(
                    f"""
                    SELECT d.id, d.title, d.filename, d.category, d.source,
                           COUNT(DISTINCT de.entity_id) as matching_entities,
                           GROUP_CONCAT(DISTINCT e.name) as matched_names
                    FROM document_entities de
                    JOIN documents d ON d.id = de.document_id
                    JOIN entities e ON e.id = de.entity_id
                    WHERE de.entity_id IN ({ph}) AND d.id != ?
                    GROUP BY d.id
                    ORDER BY matching_entities DESC
                    LIMIT ?
                """,
                    entity_ids + [doc_id, limit],
                ).fetchall()
                results = [dict(r) for r in xrefs]

            # Also try FTS if text is meaningful enough
            if len(text.strip()) >= 5 and len(results) < limit:
                fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", text.strip())[:100]
                try:
                    fts_results = conn.execute(
                        """
                        SELECT d.id, d.title, d.filename, d.category, d.source,
                               snippet(documents_fts, 1, '<mark>', '</mark>', '...', 20) as excerpt
                        FROM documents_fts
                        JOIN documents d ON d.id = documents_fts.rowid
                        WHERE documents_fts MATCH ? AND d.id != ?
                        LIMIT ?
                    """,
                        (f'"{fts_query}"', doc_id, limit),
                    ).fetchall()

                    existing_ids = {r["id"] for r in results}
                    for fr in fts_results:
                        if fr["id"] not in existing_ids:
                            results.append(
                                {
                                    **dict(fr),
                                    "matching_entities": 0,
                                    "matched_names": "",
                                    "fts_match": True,
                                }
                            )
                except Exception:
                    pass  # FTS match may fail on certain inputs

        return {
            "doc_id": doc_id,
            "cross_references": results[:limit],
            "query_text": text[:200],
        }


# ═══════════════════════════════════════════
# COMPARE DOCUMENTS
# ═══════════════════════════════════════════


@router.get("/compare-documents")
def compare_documents(
    doc_a: int = Query(...),
    doc_b: int = Query(...),
):
    """Compare two documents: shared entities, unique entities, text overlap indicators."""
    with get_db() as conn:
        a = conn.execute(
            "SELECT id, title, filename, category, source, pages, raw_text FROM documents WHERE id = ?",
            (doc_a,),
        ).fetchone()
        b = conn.execute(
            "SELECT id, title, filename, category, source, pages, raw_text FROM documents WHERE id = ?",
            (doc_b,),
        ).fetchone()
        if not a or not b:
            raise HTTPException(404, "Document not found")

        # Entities for each doc
        ents_a = conn.execute(
            """
            SELECT e.id, e.name, e.type, de.count
            FROM document_entities de JOIN entities e ON e.id = de.entity_id
            WHERE de.document_id = ? ORDER BY de.count DESC
        """,
            (doc_a,),
        ).fetchall()
        ents_b = conn.execute(
            """
            SELECT e.id, e.name, e.type, de.count
            FROM document_entities de JOIN entities e ON e.id = de.entity_id
            WHERE de.document_id = ? ORDER BY de.count DESC
        """,
            (doc_b,),
        ).fetchall()

        ids_a = {e["id"] for e in ents_a}
        ids_b = {e["id"] for e in ents_b}
        shared_ids = ids_a & ids_b

        shared = [dict(e) for e in ents_a if e["id"] in shared_ids]
        only_a = [dict(e) for e in ents_a if e["id"] not in shared_ids][:20]
        only_b = [dict(e) for e in ents_b if e["id"] not in shared_ids][:20]

        # Text excerpts (first 2000 chars each)
        text_a = (a["raw_text"] or "")[:2000]
        text_b = (b["raw_text"] or "")[:2000]

    return {
        "doc_a": {
            "id": a["id"],
            "title": a["title"] or a["filename"],
            "category": a["category"],
            "source": a["source"],
            "pages": a["pages"],
            "text_preview": text_a,
        },
        "doc_b": {
            "id": b["id"],
            "title": b["title"] or b["filename"],
            "category": b["category"],
            "source": b["source"],
            "pages": b["pages"],
            "text_preview": text_b,
        },
        "shared_entities": shared[:30],
        "only_a": only_a,
        "only_b": only_b,
        "stats": {
            "entities_a": len(ids_a),
            "entities_b": len(ids_b),
            "shared": len(shared_ids),
            "jaccard": round(len(shared_ids) / max(len(ids_a | ids_b), 1), 3),
        },
    }


# ═══════════════════════════════════════════
# TONE ANALYSIS
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/tone")
def analyze_tone(doc_id: int):
    """Analyze document tone using keyword-based markers."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, title, filename, raw_text FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        text = (doc["raw_text"] or "").lower()
        text_len = max(len(text), 1)

        # Keyword-based tone markers
        markers = {
            "threat": [
                "threat",
                "warn",
                "danger",
                "risk",
                "harm",
                "attack",
                "violent",
                "kill",
                "weapon",
            ],
            "urgency": [
                "urgent",
                "immediately",
                "asap",
                "critical",
                "emergency",
                "deadline",
                "rush",
                "time-sensitive",
            ],
            "evasion": [
                "deny",
                "refuse",
                "decline",
                "no comment",
                "plead the fifth",
                "i don't recall",
                "i don't remember",
                "cannot recall",
                "i have no",
            ],
            "legal_exposure": [
                "liability",
                "lawsuit",
                "prosecution",
                "criminal",
                "indictment",
                "guilty",
                "plaintiff",
                "defendant",
                "settlement",
                "subpoena",
            ],
            "financial": [
                "payment",
                "transfer",
                "wire",
                "account",
                "fund",
                "invest",
                "dollar",
                "million",
                "billion",
                "transaction",
            ],
            "secrecy": [
                "confidential",
                "secret",
                "classified",
                "private",
                "restricted",
                "do not distribute",
                "off the record",
                "between us",
            ],
        }

        analysis = {}
        total_score = 0
        for category, keywords in markers.items():
            hits = []
            count = 0
            for kw in keywords:
                kw_count = text.count(kw)
                if kw_count:
                    hits.append({"keyword": kw, "count": kw_count})
                    count += kw_count
            density = round(count / (text_len / 1000), 3)  # per 1000 chars
            analysis[category] = {
                "count": count,
                "density": density,
                "hits": sorted(hits, key=lambda h: -h["count"])[:5],
            }
            total_score += min(density * 10, 10)  # cap each category at 10

        overall_score = round(min(total_score / 60, 1.0), 3)  # normalize to 0-1

    return {
        "document_id": doc_id,
        "title": doc["title"] or doc["filename"],
        "overall_score": overall_score,
        "analysis": analysis,
        "text_length": len(doc["raw_text"] or ""),
    }


# ═══════════════════════════════════════════
# PROVENANCE
# ═══════════════════════════════════════════


def _ensure_provenance_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_provenance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            event_date TEXT,
            description TEXT,
            actor TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_provenance_doc ON document_provenance(document_id)"
    )


@router.get("/documents/{doc_id}/provenance")
def get_doc_provenance(doc_id: int):
    """Get provenance/chain-of-custody for a document."""
    with get_db() as conn:
        _ensure_provenance_table(conn)
        doc = conn.execute(
            "SELECT id, title, filename, category, source, date, ingested_at FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        events = conn.execute(
            """SELECT id, event_type, event_date, description, actor, created_at
               FROM document_provenance
               WHERE document_id = ?
               ORDER BY event_date, created_at""",
            (doc_id,),
        ).fetchall()

    return {"document": dict(doc), "provenance_events": [dict(e) for e in events]}


@router.post("/documents/{doc_id}/provenance")
async def add_doc_provenance(doc_id: int, request: Request):
    """Add a provenance event to a document."""
    body = await request.json()
    event_type = body.get("event_type", "")
    event_date = body.get("event_date", "")
    description = body.get("description", "")
    actor = body.get("actor", "")

    if not event_type:
        raise HTTPException(400, "event_type is required")

    with get_db() as conn:
        _ensure_provenance_table(conn)
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        conn.execute(
            """INSERT INTO document_provenance (document_id, event_type, event_date, description, actor)
               VALUES (?, ?, ?, ?, ?)""",
            (doc_id, event_type, event_date, description, actor),
        )
        conn.commit()

    return {"status": "added", "document_id": doc_id}


@router.get("/provenance-summary")
def provenance_summary():
    """Overview of document provenance tracking across the corpus."""
    with get_db() as conn:
        _ensure_provenance_table(conn)

        total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        tracked = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM document_provenance"
        ).fetchone()[0]

        event_types = conn.execute(
            """SELECT event_type, COUNT(*) as count
               FROM document_provenance
               GROUP BY event_type ORDER BY count DESC"""
        ).fetchall()

        recent = conn.execute(
            """SELECT dp.event_type, dp.event_date, dp.description, dp.actor,
                      d.title, d.id as doc_id
               FROM document_provenance dp
               JOIN documents d ON d.id = dp.document_id
               ORDER BY dp.created_at DESC LIMIT 20"""
        ).fetchall()

    return {
        "total_documents": total_docs,
        "tracked_documents": tracked,
        "untracked": total_docs - tracked,
        "event_types": [dict(e) for e in event_types],
        "recent_events": [dict(r) for r in recent],
    }
