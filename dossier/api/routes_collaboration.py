"""DOSSIER — Annotations, audit, watchlist, alerts, saved queries, notes routes."""

import re
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request

from dossier.api import utils
from dossier.db.database import get_db

router = APIRouter()


# ── Table helpers ─────────────────────────────────────────────


def _ensure_annotations_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            text TEXT NOT NULL,
            note TEXT DEFAULT '',
            color TEXT DEFAULT 'yellow',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_watchlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            entity_id INTEGER PRIMARY KEY,
            notes TEXT DEFAULT '',
            added_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_saved_queries_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS saved_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            query_text TEXT DEFAULT '',
            category TEXT DEFAULT '',
            entity_type TEXT DEFAULT '',
            source TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_keyword_alerts_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS keyword_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now')),
            is_active INTEGER DEFAULT 1
        )
    """)


def _ensure_analyst_notes_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analyst_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            note TEXT NOT NULL,
            author TEXT DEFAULT 'analyst',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_search_history_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            result_count INTEGER DEFAULT 0,
            searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _ensure_tags_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_tags (
            entity_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (entity_id, tag)
        )
    """)


def _ensure_source_ratings_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_ratings (
            source TEXT PRIMARY KEY,
            rating TEXT DEFAULT 'C',
            notes TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


# ═══════════════════════════════════════════
# DOCUMENT ANNOTATIONS
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/annotations")
def get_annotations(doc_id: int):
    """Get all annotations for a document."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        rows = conn.execute(
            "SELECT * FROM annotations WHERE document_id = ? ORDER BY start_offset",
            (doc_id,),
        ).fetchall()
    return {"document_id": doc_id, "annotations": [dict(r) for r in rows]}


@router.post("/documents/{doc_id}/annotations")
async def add_annotation(doc_id: int, request: Request):
    """Add an annotation to a document."""
    body = await request.json()
    start = body.get("start_offset")
    end = body.get("end_offset")
    text = body.get("text", "")
    note = body.get("note", "")
    color = body.get("color", "yellow")

    if start is None or end is None:
        raise HTTPException(400, "start_offset and end_offset required")

    with get_db() as conn:
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        _ensure_annotations_table(conn)
        cur = conn.execute(
            "INSERT INTO annotations (document_id, start_offset, end_offset, text, note, color) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, start, end, text, note, color),
        )
    return {"id": cur.lastrowid, "document_id": doc_id, "added": True}


@router.delete("/annotations/{annotation_id}")
def delete_annotation(annotation_id: int):
    """Delete an annotation."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        conn.execute("DELETE FROM annotations WHERE id = ?", (annotation_id,))
    return {"id": annotation_id, "deleted": True}


@router.get("/annotations/search")
def search_annotations(q: str = Query("", description="Search annotation notes")):
    """Search across all annotations."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        rows = conn.execute(
            """
            SELECT a.*, d.title as doc_title, d.filename as doc_filename
            FROM annotations a
            JOIN documents d ON d.id = a.document_id
            WHERE a.note LIKE ? OR a.text LIKE ?
            ORDER BY a.created_at DESC
            LIMIT 50
        """,
            (f"%{q}%", f"%{q}%"),
        ).fetchall()
    return {"annotations": [dict(r) for r in rows], "query": q}


# ═══════════════════════════════════════════
# AUDIT TRAIL
# ═══════════════════════════════════════════


@router.get("/audit")
def get_audit_log(
    action: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Get audit trail entries."""
    with get_db() as conn:
        utils._ensure_audit_table(conn)
        sql = "SELECT * FROM audit_log"
        params: list = []
        if action:
            sql += " WHERE action = ?"
            params.append(action)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = conn.execute(sql, params).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) FROM audit_log" + (" WHERE action = ?" if action else ""),
            [action] if action else [],
        ).fetchone()[0]
    return {"entries": [dict(r) for r in rows], "total": total}


@router.post("/audit")
async def add_audit_entry(request: Request):
    """Manually add an audit entry (for frontend-tracked actions)."""
    body = await request.json()
    action = body.get("action", "")
    if not action:
        raise HTTPException(400, "action required")
    with get_db() as conn:
        utils._log_audit(
            conn,
            action,
            body.get("target_type", ""),
            body.get("target_id", 0),
            body.get("details", ""),
        )
    return {"logged": True}


# ═══════════════════════════════════════════
# WATCHLIST
# ═══════════════════════════════════════════


@router.get("/watchlist")
def get_watchlist():
    """Get all watched entities with details."""
    with get_db() as conn:
        _ensure_watchlist_table(conn)
        rows = conn.execute("""
            SELECT w.entity_id, w.notes, w.added_at,
                   e.name, e.type,
                   COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM watchlist w
            JOIN entities e ON e.id = w.entity_id
            LEFT JOIN document_entities de ON de.entity_id = e.id
            GROUP BY w.entity_id
            ORDER BY total_mentions DESC
        """).fetchall()
    return {"watchlist": [dict(r) for r in rows]}


@router.post("/watchlist")
async def add_to_watchlist(request: Request):
    """Add an entity to the watchlist."""
    body = await request.json()
    entity_id = body.get("entity_id")
    notes = body.get("notes", "")
    if not entity_id:
        raise HTTPException(400, "entity_id required")

    with get_db() as conn:
        entity = conn.execute("SELECT id, name FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")
        _ensure_watchlist_table(conn)
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (entity_id, notes) VALUES (?, ?)",
            (entity_id, notes),
        )
    return {"entity_id": entity_id, "name": entity["name"], "added": True}


@router.delete("/watchlist/{entity_id}")
def remove_from_watchlist(entity_id: int):
    """Remove an entity from the watchlist."""
    with get_db() as conn:
        _ensure_watchlist_table(conn)
        conn.execute("DELETE FROM watchlist WHERE entity_id = ?", (entity_id,))
    return {"entity_id": entity_id, "removed": True}


# ═══════════════════════════════════════════
# SAVED QUERIES
# ═══════════════════════════════════════════


@router.get("/saved-queries")
def list_saved_queries():
    """Get all saved queries."""
    with get_db() as conn:
        _ensure_saved_queries_table(conn)
        rows = conn.execute("SELECT * FROM saved_queries ORDER BY created_at DESC").fetchall()
    return {"queries": [dict(r) for r in rows]}


@router.post("/saved-queries")
async def save_query(request: Request):
    """Save a search query."""
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "name required")
    with get_db() as conn:
        _ensure_saved_queries_table(conn)
        cur = conn.execute(
            "INSERT INTO saved_queries (name, query_text, category, entity_type, source) VALUES (?, ?, ?, ?, ?)",
            (
                name,
                body.get("query_text", ""),
                body.get("category", ""),
                body.get("entity_type", ""),
                body.get("source", ""),
            ),
        )
    return {"id": cur.lastrowid, "name": name, "saved": True}


@router.delete("/saved-queries/{query_id}")
def delete_saved_query(query_id: int):
    """Delete a saved query."""
    with get_db() as conn:
        _ensure_saved_queries_table(conn)
        conn.execute("DELETE FROM saved_queries WHERE id = ?", (query_id,))
    return {"id": query_id, "deleted": True}


# ═══════════════════════════════════════════
# KEYWORD ALERTS
# ═══════════════════════════════════════════


@router.get("/keyword-alerts")
def list_keyword_alerts():
    with get_db() as conn:
        _ensure_keyword_alerts_table(conn)
        alerts = conn.execute("SELECT * FROM keyword_alerts ORDER BY created_at DESC").fetchall()
        result = []
        for a in alerts:
            ad = dict(a)
            # Count matches across documents
            matches = conn.execute(
                """
                SELECT d.id, d.title, d.filename, d.category,
                       (LENGTH(d.raw_text) - LENGTH(REPLACE(LOWER(d.raw_text), LOWER(?), ''))) / MAX(LENGTH(?), 1) as hit_count
                FROM documents d
                WHERE LOWER(d.raw_text) LIKE '%' || LOWER(?) || '%'
                ORDER BY hit_count DESC
                LIMIT 20
            """,
                (ad["keyword"], ad["keyword"], ad["keyword"]),
            ).fetchall()
            ad["match_count"] = sum(m["hit_count"] for m in matches)
            ad["documents"] = [dict(m) for m in matches]
            result.append(ad)
    return {"alerts": result}


@router.post("/keyword-alerts")
async def create_keyword_alert(request: Request):
    body = await request.json()
    keyword = body.get("keyword", "").strip()
    if not keyword:
        raise HTTPException(400, "keyword required")
    with get_db() as conn:
        _ensure_keyword_alerts_table(conn)
        conn.execute("INSERT OR IGNORE INTO keyword_alerts (keyword) VALUES (?)", (keyword,))
        utils._log_audit(conn, "create_keyword_alert", "keyword", 0, keyword)
    return {"keyword": keyword, "created": True}


@router.delete("/keyword-alerts/{alert_id}")
def delete_keyword_alert(alert_id: int):
    with get_db() as conn:
        _ensure_keyword_alerts_table(conn)
        conn.execute("DELETE FROM keyword_alerts WHERE id = ?", (alert_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# ANALYST NOTES
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/analyst-notes")
def get_analyst_notes(doc_id: int):
    with get_db() as conn:
        _ensure_analyst_notes_table(conn)
        notes = conn.execute(
            "SELECT * FROM analyst_notes WHERE document_id = ? ORDER BY created_at DESC", (doc_id,)
        ).fetchall()
    return {"document_id": doc_id, "notes": [dict(n) for n in notes]}


@router.post("/documents/{doc_id}/analyst-notes")
async def add_analyst_note(doc_id: int, request: Request):
    body = await request.json()
    note = body.get("note", "").strip()
    author = body.get("author", "analyst").strip()
    if not note:
        raise HTTPException(400, "note required")
    with get_db() as conn:
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        _ensure_analyst_notes_table(conn)
        cur = conn.execute(
            "INSERT INTO analyst_notes (document_id, note, author) VALUES (?, ?, ?)",
            (doc_id, note, author),
        )
        utils._log_audit(conn, "add_note", "document", doc_id, note[:100])
    return {"id": cur.lastrowid, "document_id": doc_id, "note": note, "author": author}


@router.delete("/notes/{note_id}")
def delete_note(note_id: int):
    with get_db() as conn:
        _ensure_analyst_notes_table(conn)
        conn.execute("DELETE FROM analyst_notes WHERE id = ?", (note_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# SEARCH HISTORY (persistent)
# ═══════════════════════════════════════════


@router.get("/search-history")
def get_search_history(limit: int = Query(50)):
    """Get recent search history."""
    with get_db() as conn:
        _ensure_search_history_table(conn)
        history = conn.execute(
            """SELECT query, result_count, searched_at, COUNT(*) as times_searched
               FROM search_history
               GROUP BY query
               ORDER BY MAX(searched_at) DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return {"history": [dict(h) for h in history]}


@router.post("/search-history")
async def add_search_history(request: Request):
    """Record a search to history."""
    body = await request.json()
    query = body.get("query", "").strip()
    result_count = body.get("result_count", 0)
    if not query or len(query) < 2:
        return {"status": "skipped"}

    with get_db() as conn:
        _ensure_search_history_table(conn)
        conn.execute(
            "INSERT INTO search_history (query, result_count) VALUES (?, ?)",
            (query, result_count),
        )
        conn.commit()
    return {"status": "recorded"}


@router.delete("/search-history")
def clear_search_history():
    """Clear all search history."""
    with get_db() as conn:
        _ensure_search_history_table(conn)
        conn.execute("DELETE FROM search_history")
        conn.commit()
    return {"status": "cleared"}


# ═══════════════════════════════════════════
# TAG ANALYTICS
# ═══════════════════════════════════════════


@router.get("/tags/analytics")
def tag_analytics():
    """Tag usage analytics: counts, co-occurrence, entity type distribution."""
    with get_db() as conn:
        _ensure_tags_table(conn)

        # Tag counts with entity type breakdown
        tag_rows = conn.execute("""
            SELECT et.tag, e.type, COUNT(*) as count
            FROM entity_tags et
            JOIN entities e ON e.id = et.entity_id
            GROUP BY et.tag, e.type
            ORDER BY et.tag, count DESC
        """).fetchall()

        tags: dict[str, dict] = {}
        for r in tag_rows:
            tag = r["tag"]
            if tag not in tags:
                tags[tag] = {"tag": tag, "total": 0, "by_type": {}}
            tags[tag]["total"] += r["count"]
            tags[tag]["by_type"][r["type"]] = r["count"]

        # Tag co-occurrence: entities that share multiple tags
        cooccurrence = conn.execute("""
            SELECT t1.tag as tag_a, t2.tag as tag_b, COUNT(*) as shared_entities
            FROM entity_tags t1
            JOIN entity_tags t2 ON t1.entity_id = t2.entity_id AND t1.tag < t2.tag
            GROUP BY t1.tag, t2.tag
            ORDER BY shared_entities DESC
            LIMIT 50
        """).fetchall()

        # Top tagged entities
        top_tagged = conn.execute("""
            SELECT e.id, e.name, e.type, COUNT(et.tag) as tag_count,
                   GROUP_CONCAT(et.tag, ', ') as tags
            FROM entity_tags et
            JOIN entities e ON e.id = et.entity_id
            GROUP BY e.id
            ORDER BY tag_count DESC
            LIMIT 20
        """).fetchall()

    return {
        "tags": sorted(tags.values(), key=lambda t: t["total"], reverse=True),
        "cooccurrence": [dict(r) for r in cooccurrence],
        "top_tagged": [dict(r) for r in top_tagged],
    }


@router.post("/tags/bulk")
async def bulk_tag(request: Request):
    """Bulk tag entities matching a filter. Body: {tag, entity_type?, min_mentions?}"""
    body = await request.json()
    tag = body.get("tag", "").strip().lower()
    if not tag:
        raise HTTPException(400, "tag required")

    entity_type = body.get("entity_type")
    min_mentions = body.get("min_mentions", 1)

    with get_db() as conn:
        _ensure_tags_table(conn)
        sql = """
            SELECT e.id FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
        """
        params: list = []
        conditions = []
        if entity_type:
            conditions.append("e.type = ?")
            params.append(entity_type)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " GROUP BY e.id HAVING SUM(de.count) >= ?"
        params.append(min_mentions)

        entities = conn.execute(sql, params).fetchall()
        count = 0
        for row in entities:
            conn.execute(
                "INSERT OR IGNORE INTO entity_tags (entity_id, tag) VALUES (?, ?)",
                (row["id"], tag),
            )
            count += 1

    return {"tag": tag, "tagged_count": count}


# ═══════════════════════════════════════════
# BULK TAGGER (documents)
# ═══════════════════════════════════════════


@router.post("/bulk-tag")
async def bulk_tag_documents(request: Request):
    """Apply tags or category to multiple documents at once."""
    body = await request.json()
    doc_ids = body.get("doc_ids", [])
    tag = body.get("tag", "").strip()
    category = body.get("category", "").strip()

    if not doc_ids:
        raise HTTPException(400, "doc_ids required")
    if not tag and not category:
        raise HTTPException(400, "tag or category required")

    with get_db() as conn:
        updated = 0
        for doc_id in doc_ids:
            doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
            if not doc:
                continue
            if category:
                conn.execute("UPDATE documents SET category = ? WHERE id = ?", (category, doc_id))
                updated += 1
            if tag:
                # Store as document-level tag in a simple approach: append to notes
                existing = conn.execute(
                    "SELECT notes FROM documents WHERE id = ?", (doc_id,)
                ).fetchone()
                current_notes = existing["notes"] or ""
                tag_marker = f"[tag:{tag}]"
                if tag_marker not in current_notes:
                    conn.execute(
                        "UPDATE documents SET notes = ? WHERE id = ?",
                        (current_notes + f" {tag_marker}" if current_notes else tag_marker, doc_id),
                    )
                    updated += 1
        utils._log_audit(
            conn,
            "bulk_tag",
            "documents",
            len(doc_ids),
            f"tag={tag}, category={category}, count={updated}",
        )

    return {"updated": updated, "total_requested": len(doc_ids)}


@router.get("/bulk-tag-suggestions")
def bulk_tag_suggestions():
    """Get existing categories and common tags for the bulk tagger UI."""
    with get_db() as conn:
        categories = conn.execute(
            "SELECT category, COUNT(*) as count FROM documents GROUP BY category ORDER BY count DESC"
        ).fetchall()

        # Extract [tag:*] patterns from notes
        tag_rows = conn.execute("SELECT notes FROM documents WHERE notes LIKE '%[tag:%'").fetchall()
        tag_counts = {}
        for row in tag_rows:
            for m in re.finditer(r"\[tag:([^\]]+)\]", row["notes"] or ""):
                t = m.group(1)
                tag_counts[t] = tag_counts.get(t, 0) + 1

    return {
        "categories": [dict(c) for c in categories],
        "tags": [
            {"tag": t, "count": c} for t, c in sorted(tag_counts.items(), key=lambda x: -x[1])
        ],
    }


# ═══════════════════════════════════════════
# SOURCE CREDIBILITY (rate and track sources)
# ═══════════════════════════════════════════


@router.get("/source-credibility")
def source_credibility():
    """List all sources with their document counts and reliability ratings."""
    with get_db() as conn:
        _ensure_source_ratings_table(conn)
        sources = conn.execute(
            """SELECT d.source, COUNT(*) as doc_count,
                      MIN(d.date) as earliest, MAX(d.date) as latest,
                      sr.rating, sr.notes as rating_notes
               FROM documents d
               LEFT JOIN source_ratings sr ON sr.source = d.source
               WHERE d.source IS NOT NULL AND d.source != ''
               GROUP BY d.source
               ORDER BY doc_count DESC"""
        ).fetchall()

        # Cross-source entity overlap (corroboration signal)
        overlap = conn.execute(
            """SELECT d1.source as source_a, d2.source as source_b,
                      COUNT(DISTINCT de1.entity_id) as shared_entities
               FROM document_entities de1
               JOIN documents d1 ON d1.id = de1.document_id
               JOIN document_entities de2 ON de2.entity_id = de1.entity_id AND de2.document_id != de1.document_id
               JOIN documents d2 ON d2.id = de2.document_id
               WHERE d1.source < d2.source
                 AND d1.source IS NOT NULL AND d2.source IS NOT NULL
                 AND d1.source != '' AND d2.source != ''
               GROUP BY d1.source, d2.source
               HAVING shared_entities >= 3
               ORDER BY shared_entities DESC LIMIT 30"""
        ).fetchall()

    return {
        "sources": [dict(s) for s in sources],
        "cross_source_overlap": [dict(o) for o in overlap],
    }


@router.post("/source-credibility/{source}/rate")
async def rate_source(source: str, request: Request):
    """Rate a document source for credibility (A-F scale)."""
    body = await request.json()
    rating = body.get("rating", "C")
    notes = body.get("notes", "")
    if rating not in ("A", "B", "C", "D", "F"):
        raise HTTPException(400, "Rating must be A, B, C, D, or F")

    with get_db() as conn:
        _ensure_source_ratings_table(conn)
        conn.execute(
            """INSERT INTO source_ratings (source, rating, notes, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(source) DO UPDATE SET rating=excluded.rating, notes=excluded.notes, updated_at=CURRENT_TIMESTAMP""",
            (source, rating, notes),
        )
        conn.commit()

    return {"source": source, "rating": rating}
