"""DOSSIER — Entity CRUD, tags, aliases, merge, profiles routes."""

import datetime
import logging
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import HTMLResponse

from dossier.api import utils
from dossier.db.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══════════════════════════════════════════
# HELPER TABLES
# ═══════════════════════════════════════════


def _ensure_tags_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_tags (
            entity_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (entity_id, tag)
        )
    """)


def _ensure_aliases_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL,
            alias_name TEXT NOT NULL,
            UNIQUE(entity_id, alias_name)
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


# ═══════════════════════════════════════════
# ENTITIES
# ═══════════════════════════════════════════


@router.get("/entities")
def list_entities(
    type: Optional[str] = Query(None, description="person|place|org|date"),
    limit: int = Query(30, ge=1, le=200),
):
    """Top entities by total occurrence count across all documents."""
    with get_db() as conn:
        sql = """
            SELECT e.id, e.name, e.type,
                   SUM(de.count) as total_count,
                   COUNT(DISTINCT de.document_id) as doc_count
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
        """
        params = []
        if type:
            sql += " WHERE e.type = ?"
            params.append(type)
        sql += " GROUP BY e.id ORDER BY total_count DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(sql, params).fetchall()

    return {"entities": [dict(r) for r in rows]}


@router.get("/entities/search")
def search_entities(
    q: str = Query("", description="Search query"),
    limit: int = Query(10, ge=1, le=50),
):
    """Search entities by name (substring match)."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT e.id, e.name, e.type,
                   SUM(de.count) as total_count,
                   COUNT(DISTINCT de.document_id) as doc_count
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.name LIKE ?
            GROUP BY e.id ORDER BY total_count DESC LIMIT ?
        """,
            (f"%{q}%", limit),
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/entities/{entity_id}/documents")
def entity_documents(entity_id: int, limit: int = 20):
    """Get all documents containing a specific entity."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT d.id, d.title, d.category, d.date, de.count as mentions
            FROM document_entities de
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY de.count DESC
            LIMIT ?
        """,
            (entity_id, limit),
        ).fetchall()

    return {"documents": [dict(r) for r in rows]}


# ═══════════════════════════════════════════
# ENTITY PROFILES
# ═══════════════════════════════════════════


@router.get("/entities/{entity_id}/profile")
def entity_profile(entity_id: int):
    """Full entity dossier: docs, timeline, connections, co-occurring entities, risk."""
    with get_db() as conn:
        entity = conn.execute(
            "SELECT id, name, type, canonical FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")

        # Documents containing this entity
        docs = conn.execute(
            """
            SELECT d.id, d.title, d.filename, d.category, d.source, d.date,
                   d.pages, de.count as mentions
            FROM document_entities de
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY de.count DESC
        """,
            (entity_id,),
        ).fetchall()

        # Risk exposure — docs with risk scores
        risk_docs = conn.execute(
            """
            SELECT df.score, d.id, d.title
            FROM document_entities de
            JOIN document_forensics df ON df.document_id = de.document_id
              AND df.analysis_type = 'risk_score'
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY df.score DESC LIMIT 10
        """,
            (entity_id,),
        ).fetchall()

        avg_risk = 0.0
        if risk_docs:
            avg_risk = sum(r["score"] for r in risk_docs) / len(risk_docs)

        # Timeline events mentioning this entity
        timeline = conn.execute(
            """
            SELECT ev.event_date, ev.precision, ev.confidence, ev.context,
                   ev.document_id
            FROM events ev
            JOIN document_entities de ON de.document_id = ev.document_id
            WHERE de.entity_id = ? AND ev.event_date IS NOT NULL
              AND ev.confidence >= 0.5
            ORDER BY ev.event_date
            LIMIT 50
        """,
            (entity_id,),
        ).fetchall()

        # Top co-occurring entities
        cooccurring = conn.execute(
            """
            SELECT e.id, e.name, e.type, ec.weight
            FROM entity_connections ec
            JOIN entities e ON e.id = CASE
                WHEN ec.entity_a_id = ? THEN ec.entity_b_id
                ELSE ec.entity_a_id END
            WHERE (ec.entity_a_id = ? OR ec.entity_b_id = ?)
              AND ec.weight >= 1
            ORDER BY ec.weight DESC
            LIMIT 30
        """,
            (entity_id, entity_id, entity_id),
        ).fetchall()

        # Tags
        _ensure_tags_table(conn)
        tags = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ? ORDER BY tag",
            (entity_id,),
        ).fetchall()

        # Watchlist status
        _ensure_watchlist_table(conn)
        watched = (
            conn.execute("SELECT 1 FROM watchlist WHERE entity_id = ?", (entity_id,)).fetchone()
            is not None
        )

    return {
        "entity": dict(entity),
        "documents": [dict(d) for d in docs],
        "document_count": len(docs),
        "total_mentions": sum(d["mentions"] for d in docs),
        "risk_exposure": {
            "avg_risk": round(avg_risk, 3),
            "high_risk_docs": [dict(r) for r in risk_docs],
        },
        "timeline": [dict(t) for t in timeline],
        "cooccurring": [dict(c) for c in cooccurring],
        "tags": [r["tag"] for r in tags],
        "watched": watched,
    }


# ═══════════════════════════════════════════
# ENTITY TAGGING
# ═══════════════════════════════════════════


@router.get("/entities/{entity_id}/tags")
def get_entity_tags(entity_id: int):
    """Get tags for an entity."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        rows = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ? ORDER BY tag",
            (entity_id,),
        ).fetchall()
    return {"entity_id": entity_id, "tags": [r["tag"] for r in rows]}


@router.post("/entities/{entity_id}/tags")
async def add_entity_tag(entity_id: int, request: Request):
    """Add a tag to an entity."""
    body = await request.json()
    tag = body.get("tag", "").strip().lower()
    if not tag:
        raise HTTPException(400, "tag required")

    with get_db() as conn:
        entity = conn.execute("SELECT id FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")
        _ensure_tags_table(conn)
        conn.execute(
            "INSERT OR IGNORE INTO entity_tags (entity_id, tag) VALUES (?, ?)",
            (entity_id, tag),
        )
    return {"entity_id": entity_id, "tag": tag, "added": True}


@router.delete("/entities/{entity_id}/tags/{tag}")
def remove_entity_tag(entity_id: int, tag: str):
    """Remove a tag from an entity."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        conn.execute(
            "DELETE FROM entity_tags WHERE entity_id = ? AND tag = ?",
            (entity_id, tag),
        )
    return {"entity_id": entity_id, "tag": tag, "removed": True}


@router.get("/entities/by-tag")
def entities_by_tag(tag: str = Query(...)):
    """Get all entities with a specific tag."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        rows = conn.execute(
            """
            SELECT e.id, e.name, e.type, et.tag,
                   COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entity_tags et
            JOIN entities e ON e.id = et.entity_id
            LEFT JOIN document_entities de ON de.entity_id = e.id
            WHERE et.tag = ?
            GROUP BY e.id
            ORDER BY total_mentions DESC
        """,
            (tag,),
        ).fetchall()
    return {"tag": tag, "entities": [dict(r) for r in rows]}


@router.get("/tags")
def list_all_tags():
    """Get all tags with counts."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        rows = conn.execute("""
            SELECT tag, COUNT(*) as count
            FROM entity_tags GROUP BY tag ORDER BY count DESC
        """).fetchall()
    return {"tags": [dict(r) for r in rows]}


# ═══════════════════════════════════════════
# ENTITY MERGE
# ═══════════════════════════════════════════


@router.get("/entities/merge-preview")
def merge_preview(
    source_id: int = Query(...),
    target_id: int = Query(...),
):
    """Preview what merging two entities would look like."""
    with get_db() as conn:
        src = conn.execute(
            "SELECT id, name, type, canonical FROM entities WHERE id = ?", (source_id,)
        ).fetchone()
        tgt = conn.execute(
            "SELECT id, name, type, canonical FROM entities WHERE id = ?", (target_id,)
        ).fetchone()
        if not src or not tgt:
            raise HTTPException(404, "Entity not found")

        # Count docs and mentions for each
        src_stats = conn.execute(
            """
            SELECT COUNT(DISTINCT document_id) as doc_count, SUM(count) as mentions
            FROM document_entities WHERE entity_id = ?
        """,
            (source_id,),
        ).fetchone()
        tgt_stats = conn.execute(
            """
            SELECT COUNT(DISTINCT document_id) as doc_count, SUM(count) as mentions
            FROM document_entities WHERE entity_id = ?
        """,
            (target_id,),
        ).fetchone()

        # Shared documents
        shared = conn.execute(
            """
            SELECT COUNT(DISTINCT de1.document_id)
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
        """,
            (source_id, target_id),
        ).fetchone()[0]

        # Tags
        _ensure_tags_table(conn)
        src_tags = [
            r["tag"]
            for r in conn.execute(
                "SELECT tag FROM entity_tags WHERE entity_id = ?", (source_id,)
            ).fetchall()
        ]
        tgt_tags = [
            r["tag"]
            for r in conn.execute(
                "SELECT tag FROM entity_tags WHERE entity_id = ?", (target_id,)
            ).fetchall()
        ]

        # Connections
        src_conns = conn.execute(
            "SELECT COUNT(*) FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?",
            (source_id, source_id),
        ).fetchone()[0]
        tgt_conns = conn.execute(
            "SELECT COUNT(*) FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?",
            (target_id, target_id),
        ).fetchone()[0]

    return {
        "source": {
            **dict(src),
            "doc_count": src_stats["doc_count"],
            "mentions": src_stats["mentions"],
            "tags": src_tags,
            "connections": src_conns,
        },
        "target": {
            **dict(tgt),
            "doc_count": tgt_stats["doc_count"],
            "mentions": tgt_stats["mentions"],
            "tags": tgt_tags,
            "connections": tgt_conns,
        },
        "shared_documents": shared,
        "merged_tags": sorted(set(src_tags + tgt_tags)),
        "merged_doc_count": src_stats["doc_count"] + tgt_stats["doc_count"] - shared,
    }


@router.post("/entities/merge")
async def merge_entities(request: Request):
    """Merge source entity into target. Transfers docs, connections, tags, watchlist."""
    body = await request.json()
    source_id = body.get("source_id")
    target_id = body.get("target_id")
    if not source_id or not target_id or source_id == target_id:
        raise HTTPException(400, "source_id and target_id required and must differ")

    with get_db() as conn:
        src = conn.execute("SELECT id, name FROM entities WHERE id = ?", (source_id,)).fetchone()
        tgt = conn.execute("SELECT id, name FROM entities WHERE id = ?", (target_id,)).fetchone()
        if not src or not tgt:
            raise HTTPException(404, "Entity not found")

        # Transfer document_entities (merge counts for shared docs)
        shared_docs = conn.execute(
            """
            SELECT de1.document_id, de1.count as src_count, de2.count as tgt_count
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
        """,
            (source_id, target_id),
        ).fetchall()

        for sd in shared_docs:
            conn.execute(
                "UPDATE document_entities SET count = ? WHERE entity_id = ? AND document_id = ?",
                (sd["src_count"] + sd["tgt_count"], target_id, sd["document_id"]),
            )
            conn.execute(
                "DELETE FROM document_entities WHERE entity_id = ? AND document_id = ?",
                (source_id, sd["document_id"]),
            )

        # Transfer remaining non-shared document_entities
        conn.execute(
            "UPDATE document_entities SET entity_id = ? WHERE entity_id = ?",
            (target_id, source_id),
        )

        # Transfer connections (update references, skip self-loops)
        conn.execute(
            "UPDATE OR IGNORE entity_connections SET entity_a_id = ? WHERE entity_a_id = ? AND entity_b_id != ?",
            (target_id, source_id, target_id),
        )
        conn.execute(
            "UPDATE OR IGNORE entity_connections SET entity_b_id = ? WHERE entity_b_id = ? AND entity_a_id != ?",
            (target_id, source_id, target_id),
        )
        conn.execute(
            "DELETE FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?",
            (source_id, source_id),
        )

        # Transfer tags
        _ensure_tags_table(conn)
        src_tags = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ?", (source_id,)
        ).fetchall()
        for t in src_tags:
            conn.execute(
                "INSERT OR IGNORE INTO entity_tags (entity_id, tag) VALUES (?, ?)",
                (target_id, t["tag"]),
            )
        conn.execute("DELETE FROM entity_tags WHERE entity_id = ?", (source_id,))

        # Transfer watchlist
        _ensure_watchlist_table(conn)
        watched = conn.execute(
            "SELECT notes FROM watchlist WHERE entity_id = ?", (source_id,)
        ).fetchone()
        if watched:
            conn.execute(
                "INSERT OR IGNORE INTO watchlist (entity_id, notes) VALUES (?, ?)",
                (target_id, watched["notes"]),
            )
            conn.execute("DELETE FROM watchlist WHERE entity_id = ?", (source_id,))

        # Delete source entity
        conn.execute("DELETE FROM entities WHERE id = ?", (source_id,))

        # Audit
        utils._log_audit(
            conn,
            "entity_merge",
            "entity",
            target_id,
            f"Merged '{src['name']}' (#{source_id}) into '{tgt['name']}' (#{target_id})",
        )

    return {
        "merged": True,
        "source_id": source_id,
        "target_id": target_id,
        "target_name": tgt["name"],
    }


# ═══════════════════════════════════════════
# ENTITY ALIASES
# ═══════════════════════════════════════════


@router.get("/entities/{entity_id}/aliases")
def get_aliases(entity_id: int):
    with get_db() as conn:
        _ensure_aliases_table(conn)
        rows = conn.execute(
            "SELECT * FROM entity_aliases WHERE entity_id = ? ORDER BY alias_name", (entity_id,)
        ).fetchall()
    return {"entity_id": entity_id, "aliases": [dict(r) for r in rows]}


@router.post("/entities/{entity_id}/aliases")
async def add_alias(entity_id: int, request: Request):
    body = await request.json()
    alias = body.get("alias", "").strip()
    if not alias:
        raise HTTPException(400, "alias required")
    with get_db() as conn:
        entity = conn.execute("SELECT id FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")
        _ensure_aliases_table(conn)
        conn.execute(
            "INSERT OR IGNORE INTO entity_aliases (entity_id, alias_name) VALUES (?, ?)",
            (entity_id, alias),
        )
        utils._log_audit(conn, "add_alias", "entity", entity_id, alias)
    return {"entity_id": entity_id, "alias": alias, "added": True}


@router.delete("/aliases/{alias_id}")
def delete_alias(alias_id: int):
    with get_db() as conn:
        _ensure_aliases_table(conn)
        conn.execute("DELETE FROM entity_aliases WHERE id = ?", (alias_id,))
    return {"deleted": True}


@router.get("/aliases/resolve")
def resolve_alias(name: str = Query(...)):
    """Look up an alias and return the canonical entity."""
    with get_db() as conn:
        _ensure_aliases_table(conn)
        # Check aliases first
        alias_match = conn.execute(
            """
            SELECT ea.entity_id, e.name, e.type, ea.alias_name
            FROM entity_aliases ea
            JOIN entities e ON e.id = ea.entity_id
            WHERE LOWER(ea.alias_name) = LOWER(?)
        """,
            (name,),
        ).fetchone()
        if alias_match:
            return {"resolved": True, "entity": dict(alias_match), "via": "alias"}
        # Fall back to direct entity name match
        direct = conn.execute(
            "SELECT id as entity_id, name, type FROM entities WHERE LOWER(name) = LOWER(?)", (name,)
        ).fetchone()
        if direct:
            return {"resolved": True, "entity": dict(direct), "via": "direct"}
    return {"resolved": False, "query": name}


# ═══════════════════════════════════════════
# ENTITY TIMELINE
# ═══════════════════════════════════════════


@router.get("/entities/{entity_id}/timeline")
def entity_timeline(entity_id: int):
    """Get a unified chronological timeline for a single entity."""
    with get_db() as conn:
        entity = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")

        # Events linked via event_entities
        events = conn.execute(
            """SELECT e.id, e.event_date, e.context, e.precision, e.confidence,
                      e.document_id, d.title as doc_title, ee.role
               FROM events e
               JOIN event_entities ee ON ee.event_id = e.id
               LEFT JOIN documents d ON d.id = e.document_id
               WHERE ee.entity_id = ?
               ORDER BY e.event_date""",
            (entity_id,),
        ).fetchall()

        # Documents linked via document_entities
        docs = conn.execute(
            """SELECT d.id, d.title, d.filename, d.category, d.date, d.source,
                      de.count as mention_count
               FROM documents d
               JOIN document_entities de ON de.document_id = d.id
               WHERE de.entity_id = ?
               ORDER BY d.date""",
            (entity_id,),
        ).fetchall()

        # Co-occurring entities (who appears with this entity most)
        cooccurring = conn.execute(
            """SELECT e2.id, e2.name, e2.type, COUNT(DISTINCT de1.document_id) as shared_docs
               FROM document_entities de1
               JOIN document_entities de2 ON de2.document_id = de1.document_id AND de2.entity_id != de1.entity_id
               JOIN entities e2 ON e2.id = de2.entity_id
               WHERE de1.entity_id = ?
               GROUP BY e2.id ORDER BY shared_docs DESC LIMIT 15""",
            (entity_id,),
        ).fetchall()

    return {
        "entity": dict(entity),
        "events": [dict(e) for e in events],
        "documents": [dict(d) for d in docs],
        "cooccurring": [dict(c) for c in cooccurring],
    }


# ═══════════════════════════════════════════
# ENTITY DOSSIER EXPORT
# ═══════════════════════════════════════════


@router.get("/entities/{entity_id}/dossier-export")
def export_entity_dossier(entity_id: int):
    """Export full entity profile as standalone HTML."""
    with get_db() as conn:
        entity = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")

        # Documents
        docs = conn.execute(
            """
            SELECT d.id, d.title, d.filename, d.category, d.date,
                   SUM(de.count) as mentions
            FROM document_entities de
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            GROUP BY d.id
            ORDER BY mentions DESC
        """,
            (entity_id,),
        ).fetchall()

        # Co-occurring entities
        cooccurring = conn.execute(
            """
            SELECT e2.name, e2.type, COUNT(DISTINCT de1.document_id) as shared
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            JOIN entities e2 ON e2.id = de2.entity_id
            WHERE de1.entity_id = ? AND de2.entity_id != ?
            GROUP BY e2.id
            ORDER BY shared DESC
            LIMIT 30
        """,
            (entity_id, entity_id),
        ).fetchall()

        # Timeline events
        events = conn.execute(
            """
            SELECT ev.event_date, ev.context, ev.precision
            FROM events ev
            JOIN document_entities de ON de.document_id = ev.document_id
            WHERE de.entity_id = ? AND ev.event_date IS NOT NULL
            ORDER BY ev.event_date
            LIMIT 100
        """,
            (entity_id,),
        ).fetchall()

        # Aliases
        _ensure_aliases_table(conn)
        aliases = conn.execute(
            "SELECT alias_name FROM entity_aliases WHERE entity_id = ?", (entity_id,)
        ).fetchall()

        # Tags
        tags = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ?", (entity_id,)
        ).fetchall()

        # Build HTML
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
        <title>Dossier: {entity["name"]}</title>
        <style>body{{font-family:sans-serif;max-width:900px;margin:40px auto;padding:0 20px;color:#222;}}
        h1{{border-bottom:3px solid #c4473a;padding-bottom:8px;}}
        h2{{color:#c4473a;margin-top:30px;border-bottom:1px solid #ddd;padding-bottom:4px;}}
        .tag{{display:inline-block;background:#f0f0f0;padding:2px 8px;border-radius:4px;margin:2px;font-size:12px;}}
        table{{border-collapse:collapse;width:100%;margin:10px 0;}} th,td{{border:1px solid #ddd;padding:6px 10px;text-align:left;font-size:13px;}}
        th{{background:#f5f5f5;}} .meta{{color:#666;font-size:13px;}}</style></head><body>
        <h1>{entity["name"]}</h1>
        <p class="meta">Type: {entity["type"]} | Entity ID: {entity["id"]} | Generated: {now}</p>"""

        if aliases:
            html += (
                "<p><strong>Aliases:</strong> "
                + ", ".join(a["alias_name"] for a in aliases)
                + "</p>"
            )
        if tags:
            html += (
                "<p><strong>Tags:</strong> "
                + " ".join(f"<span class='tag'>{t['tag']}</span>" for t in tags)
                + "</p>"
            )

        html += f"<h2>Documents ({len(docs)})</h2><table><tr><th>Title</th><th>Category</th><th>Date</th><th>Mentions</th></tr>"
        for d in docs:
            html += f"<tr><td>{d['title'] or d['filename']}</td><td>{d['category']}</td><td>{d['date'] or '—'}</td><td>{d['mentions']}</td></tr>"
        html += "</table>"

        if events:
            html += f"<h2>Timeline ({len(events)} events)</h2><table><tr><th>Date</th><th>Context</th><th>Precision</th></tr>"
            for ev in events:
                html += f"<tr><td>{ev['event_date']}</td><td>{ev['context']}</td><td>{ev['precision'] or '—'}</td></tr>"
            html += "</table>"

        if cooccurring:
            html += f"<h2>Associated Entities ({len(cooccurring)})</h2><table><tr><th>Name</th><th>Type</th><th>Shared Docs</th></tr>"
            for c in cooccurring:
                html += f"<tr><td>{c['name']}</td><td>{c['type']}</td><td>{c['shared']}</td></tr>"
            html += "</table>"

        html += "</body></html>"

    return HTMLResponse(html)
