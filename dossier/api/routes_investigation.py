"""DOSSIER — Investigation board, case files, evidence chains, snapshots routes."""

import datetime
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from dossier.api import utils
from dossier.db.database import get_db

router = APIRouter()


# ═══════════════════════════════════════════
# TABLE HELPERS
# ═══════════════════════════════════════════


def _ensure_board_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS board_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT NOT NULL,
            ref_id INTEGER,
            title TEXT NOT NULL,
            content TEXT DEFAULT '',
            color TEXT DEFAULT '',
            x REAL DEFAULT 0,
            y REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_evidence_chains_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS evidence_chains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS evidence_chain_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            link_type TEXT NOT NULL DEFAULT 'document',
            target_id INTEGER NOT NULL,
            narrative TEXT DEFAULT '',
            FOREIGN KEY (chain_id) REFERENCES evidence_chains(id)
        )
    """)


def _ensure_snapshots_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investigation_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            snapshot_data TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_case_files_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS case_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS case_file_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_file_id INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            note TEXT DEFAULT '',
            sort_order INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now')),
            UNIQUE(case_file_id, item_type, item_id)
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


def _ensure_tags_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_tags (
            entity_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (entity_id, tag)
        )
    """)


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


# ═══════════════════════════════════════════
# INVESTIGATION BOARD
# ═══════════════════════════════════════════


@router.get("/board")
def get_board():
    """Get all investigation board items."""
    with get_db() as conn:
        _ensure_board_table(conn)
        rows = conn.execute("SELECT * FROM board_items ORDER BY created_at").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/board")
async def add_board_item(request: Request):
    """Add an item to the investigation board."""
    body = await request.json()
    title = body.get("title", "")
    if not title:
        raise HTTPException(400, "title required")

    with get_db() as conn:
        _ensure_board_table(conn)
        cursor = conn.execute(
            "INSERT INTO board_items (item_type, ref_id, title, content, color, x, y) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                body.get("item_type", "note"),
                body.get("ref_id"),
                title,
                body.get("content", ""),
                body.get("color", ""),
                body.get("x", 0),
                body.get("y", 0),
            ),
        )
        item_id = cursor.lastrowid

    return {"id": item_id, "item_type": body.get("item_type", "note"), "title": title}


@router.put("/board/{item_id}")
async def update_board_item(item_id: int, request: Request):
    """Update a board item."""
    body = await request.json()
    with get_db() as conn:
        _ensure_board_table(conn)
        row = conn.execute("SELECT id FROM board_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Board item not found")

        updates, params = [], []
        for field in ("title", "content", "color", "x", "y"):
            if field in body:
                updates.append(f"{field} = ?")
                params.append(body[field])
        if updates:
            params.append(item_id)
            conn.execute(f"UPDATE board_items SET {', '.join(updates)} WHERE id = ?", params)

    return {"id": item_id, "updated": True}


@router.delete("/board/{item_id}")
def delete_board_item(item_id: int):
    """Remove an item from the investigation board."""
    with get_db() as conn:
        _ensure_board_table(conn)
        conn.execute("DELETE FROM board_items WHERE id = ?", (item_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# EVIDENCE CHAINS
# ═══════════════════════════════════════════


@router.get("/evidence-chains")
def list_evidence_chains():
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        chains = conn.execute("SELECT * FROM evidence_chains ORDER BY updated_at DESC").fetchall()
        result = []
        for c in chains:
            links = conn.execute(
                "SELECT COUNT(*) as cnt FROM evidence_chain_links WHERE chain_id = ?", (c["id"],)
            ).fetchone()
            result.append({**dict(c), "link_count": links["cnt"]})
    return {"chains": result}


@router.get("/evidence-chains/{chain_id}")
def get_evidence_chain(chain_id: int):
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        chain = conn.execute("SELECT * FROM evidence_chains WHERE id = ?", (chain_id,)).fetchone()
        if not chain:
            raise HTTPException(404, "Chain not found")
        links = conn.execute(
            """
            SELECT ecl.*, CASE ecl.link_type
                WHEN 'document' THEN (SELECT title FROM documents WHERE id = ecl.target_id)
                WHEN 'entity' THEN (SELECT name FROM entities WHERE id = ecl.target_id)
                ELSE '' END as target_name,
                CASE ecl.link_type
                WHEN 'entity' THEN (SELECT type FROM entities WHERE id = ecl.target_id)
                ELSE '' END as target_entity_type
            FROM evidence_chain_links ecl
            WHERE ecl.chain_id = ?
            ORDER BY ecl.position
        """,
            (chain_id,),
        ).fetchall()
    return {"chain": dict(chain), "links": [dict(lnk) for lnk in links]}


@router.post("/evidence-chains")
async def create_evidence_chain(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "name required")
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        cur = conn.execute(
            "INSERT INTO evidence_chains (name, description) VALUES (?, ?)",
            (name, body.get("description", "")),
        )
        utils._log_audit(conn, "create_chain", "chain", cur.lastrowid, name)
    return {"id": cur.lastrowid, "name": name}


@router.post("/evidence-chains/{chain_id}/links")
async def add_chain_link(chain_id: int, request: Request):
    body = await request.json()
    link_type = body.get("link_type", "document")
    target_id = body.get("target_id")
    narrative = body.get("narrative", "")
    if not target_id:
        raise HTTPException(400, "target_id required")
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        max_pos = conn.execute(
            "SELECT COALESCE(MAX(position), 0) FROM evidence_chain_links WHERE chain_id = ?",
            (chain_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO evidence_chain_links (chain_id, position, link_type, target_id, narrative) VALUES (?, ?, ?, ?, ?)",
            (chain_id, max_pos + 1, link_type, target_id, narrative),
        )
        conn.execute(
            "UPDATE evidence_chains SET updated_at = datetime('now') WHERE id = ?", (chain_id,)
        )
    return {"added": True, "position": max_pos + 1}


@router.delete("/evidence-chains/{chain_id}")
def delete_evidence_chain(chain_id: int):
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        conn.execute("DELETE FROM evidence_chain_links WHERE chain_id = ?", (chain_id,))
        conn.execute("DELETE FROM evidence_chains WHERE id = ?", (chain_id,))
    return {"deleted": True}


@router.delete("/evidence-chain-links/{link_id}")
def delete_chain_link(link_id: int):
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        conn.execute("DELETE FROM evidence_chain_links WHERE id = ?", (link_id,))
    return {"deleted": True}


@router.get("/evidence-chains/{chain_id}/export")
def export_evidence_chain(chain_id: int):
    """Export evidence chain as HTML case brief."""
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        chain = conn.execute("SELECT * FROM evidence_chains WHERE id = ?", (chain_id,)).fetchone()
        if not chain:
            raise HTTPException(404, "Chain not found")
        links = conn.execute(
            """
            SELECT ecl.*, CASE ecl.link_type
                WHEN 'document' THEN (SELECT title FROM documents WHERE id = ecl.target_id)
                WHEN 'entity' THEN (SELECT name FROM entities WHERE id = ecl.target_id)
                ELSE '' END as target_name
            FROM evidence_chain_links ecl WHERE ecl.chain_id = ? ORDER BY ecl.position
        """,
            (chain_id,),
        ).fetchall()

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Case Brief: {chain["name"]}</title>
<style>body{{font-family:'Helvetica Neue',sans-serif;max-width:800px;margin:0 auto;padding:40px;color:#222;line-height:1.6;}}
h1{{color:#c4473a;border-bottom:3px solid #c4473a;padding-bottom:10px;}}
.step{{display:flex;gap:16px;margin:16px 0;padding:16px;border-left:4px solid #c4473a;background:#f9f9f9;border-radius:0 8px 8px 0;}}
.step-num{{font-size:24px;font-weight:700;color:#c4473a;min-width:40px;text-align:center;}}
.step-body h3{{margin:0 0 4px;}} .step-body p{{margin:4px 0;color:#555;font-size:14px;}}
.meta{{color:#888;font-size:11px;}}</style></head><body>
<h1>Case Brief: {chain["name"]}</h1>
<p>{chain["description"]}</p><p class="meta">Generated: {now} | {len(links)} links</p>"""
    for lnk in links:
        html += f"""<div class="step"><div class="step-num">{lnk["position"]}</div><div class="step-body">
<h3>[{lnk["link_type"].title()}] {lnk["target_name"] or f"#{lnk['target_id']}"}</h3>
<p>{lnk["narrative"] or "<em>No narrative</em>"}</p></div></div>"""
    html += "</body></html>"
    return {"html": html, "name": chain["name"]}


# ═══════════════════════════════════════════
# INVESTIGATION SNAPSHOTS
# ═══════════════════════════════════════════


@router.get("/snapshots")
def list_snapshots():
    with get_db() as conn:
        _ensure_snapshots_table(conn)
        rows = conn.execute(
            "SELECT id, name, description, created_at FROM investigation_snapshots ORDER BY created_at DESC"
        ).fetchall()
    return {"snapshots": [dict(r) for r in rows]}


@router.post("/snapshots")
async def create_snapshot(request: Request):
    """Save current investigation state as a named snapshot."""
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "name required")

    with get_db() as conn:
        _ensure_snapshots_table(conn)
        _ensure_watchlist_table(conn)
        _ensure_tags_table(conn)
        _ensure_annotations_table(conn)
        _ensure_board_table(conn)

        # Gather current state
        watchlist = [
            dict(r) for r in conn.execute("SELECT entity_id, notes FROM watchlist").fetchall()
        ]
        tags = [dict(r) for r in conn.execute("SELECT entity_id, tag FROM entity_tags").fetchall()]
        annotations = [
            dict(r)
            for r in conn.execute(
                "SELECT document_id, start_offset, end_offset, text, note, color FROM annotations"
            ).fetchall()
        ]
        board = [
            dict(r)
            for r in conn.execute(
                "SELECT id, item_type, ref_id, title, x, y FROM board_items"
            ).fetchall()
        ]
        flagged = [
            r["id"] for r in conn.execute("SELECT id FROM documents WHERE flagged = 1").fetchall()
        ]

        snapshot_data = json.dumps(
            {
                "watchlist": watchlist,
                "tags": tags,
                "annotations_count": len(annotations),
                "board_items": len(board),
                "flagged_docs": flagged,
                "filters": body.get("filters", {}),
            }
        )

        cur = conn.execute(
            "INSERT INTO investigation_snapshots (name, description, snapshot_data) VALUES (?, ?, ?)",
            (name, body.get("description", ""), snapshot_data),
        )
        utils._log_audit(conn, "create_snapshot", "snapshot", cur.lastrowid, name)

    return {"id": cur.lastrowid, "name": name}


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(snapshot_id: int):
    with get_db() as conn:
        _ensure_snapshots_table(conn)
        row = conn.execute(
            "SELECT * FROM investigation_snapshots WHERE id = ?", (snapshot_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Snapshot not found")
        result = dict(row)
        result["snapshot_data"] = json.loads(result["snapshot_data"])
    return result


@router.delete("/snapshots/{snapshot_id}")
def delete_snapshot(snapshot_id: int):
    with get_db() as conn:
        _ensure_snapshots_table(conn)
        conn.execute("DELETE FROM investigation_snapshots WHERE id = ?", (snapshot_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# CASE FILES
# ═══════════════════════════════════════════


@router.get("/case-files")
def list_case_files():
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cases = conn.execute("SELECT * FROM case_files ORDER BY updated_at DESC").fetchall()
        result = []
        for c in cases:
            cd = dict(c)
            items = conn.execute(
                "SELECT item_type, COUNT(*) as count FROM case_file_items WHERE case_file_id = ? GROUP BY item_type",
                (cd["id"],),
            ).fetchall()
            cd["item_counts"] = {i["item_type"]: i["count"] for i in items}
            cd["total_items"] = sum(i["count"] for i in items)
            result.append(cd)
    return {"case_files": result}


@router.post("/case-files")
async def create_case_file(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    desc = body.get("description", "").strip()
    if not name:
        raise HTTPException(400, "name required")
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cur = conn.execute("INSERT INTO case_files (name, description) VALUES (?, ?)", (name, desc))
        utils._log_audit(conn, "create_case_file", "case_file", cur.lastrowid, name)
    return {"id": cur.lastrowid, "name": name}


@router.get("/case-files/{case_id}")
def get_case_file(case_id: int):
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cf = conn.execute("SELECT * FROM case_files WHERE id = ?", (case_id,)).fetchone()
        if not cf:
            raise HTTPException(404, "Case file not found")
        items = conn.execute(
            "SELECT * FROM case_file_items WHERE case_file_id = ? ORDER BY sort_order, added_at",
            (case_id,),
        ).fetchall()

        enriched = []
        for item in items:
            d = dict(item)
            if d["item_type"] == "document":
                doc = conn.execute(
                    "SELECT id, title, filename, category FROM documents WHERE id = ?",
                    (d["item_id"],),
                ).fetchone()
                d["detail"] = dict(doc) if doc else None
            elif d["item_type"] == "entity":
                ent = conn.execute(
                    "SELECT id, name, type FROM entities WHERE id = ?", (d["item_id"],)
                ).fetchone()
                d["detail"] = dict(ent) if ent else None
            elif d["item_type"] == "chain":
                ch = conn.execute(
                    "SELECT id, name, description FROM evidence_chains WHERE id = ?",
                    (d["item_id"],),
                ).fetchone()
                d["detail"] = dict(ch) if ch else None
            else:
                d["detail"] = None
            enriched.append(d)

    return {"case_file": dict(cf), "items": enriched}


@router.post("/case-files/{case_id}/items")
async def add_case_file_item(case_id: int, request: Request):
    body = await request.json()
    item_type = body.get("item_type", "").strip()
    item_id = body.get("item_id", 0)
    note = body.get("note", "").strip()
    if item_type not in ("document", "entity", "chain"):
        raise HTTPException(400, "item_type must be document, entity, or chain")
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cf = conn.execute("SELECT id FROM case_files WHERE id = ?", (case_id,)).fetchone()
        if not cf:
            raise HTTPException(404, "Case file not found")
        max_order = conn.execute(
            "SELECT COALESCE(MAX(sort_order), 0) FROM case_file_items WHERE case_file_id = ?",
            (case_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO case_file_items (case_file_id, item_type, item_id, note, sort_order) VALUES (?, ?, ?, ?, ?)",
            (case_id, item_type, item_id, note, max_order + 1),
        )
        conn.execute("UPDATE case_files SET updated_at = datetime('now') WHERE id = ?", (case_id,))
    return {"added": True}


@router.delete("/case-file-items/{item_id}")
def remove_case_file_item(item_id: int):
    with get_db() as conn:
        _ensure_case_files_table(conn)
        conn.execute("DELETE FROM case_file_items WHERE id = ?", (item_id,))
    return {"deleted": True}


@router.delete("/case-files/{case_id}")
def delete_case_file(case_id: int):
    with get_db() as conn:
        _ensure_case_files_table(conn)
        conn.execute("DELETE FROM case_file_items WHERE case_file_id = ?", (case_id,))
        conn.execute("DELETE FROM case_files WHERE id = ?", (case_id,))
    return {"deleted": True}


@router.get("/case-files/{case_id}/export")
def export_case_file(case_id: int):
    """Export case file as structured HTML report."""
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cf = conn.execute("SELECT * FROM case_files WHERE id = ?", (case_id,)).fetchone()
        if not cf:
            raise HTTPException(404, "Case file not found")
        items = conn.execute(
            "SELECT * FROM case_file_items WHERE case_file_id = ? ORDER BY sort_order, added_at",
            (case_id,),
        ).fetchall()

        html = f"<h1>Case File: {cf['name']}</h1>"
        html += f"<p><em>{cf['description']}</em></p>"
        html += f"<p>Generated: {cf['created_at']}</p><hr>"

        for item in items:
            d = dict(item)
            if d["item_type"] == "document":
                doc = conn.execute(
                    "SELECT id, title, filename, category FROM documents WHERE id = ?",
                    (d["item_id"],),
                ).fetchone()
                if doc:
                    html += f"<h3>Document: {doc['title'] or doc['filename']}</h3>"
                    html += f"<p>Category: {doc['category']} | ID: {doc['id']}</p>"
            elif d["item_type"] == "entity":
                ent = conn.execute(
                    "SELECT id, name, type FROM entities WHERE id = ?", (d["item_id"],)
                ).fetchone()
                if ent:
                    html += f"<h3>Entity: {ent['name']} ({ent['type']})</h3>"
            elif d["item_type"] == "chain":
                ch = conn.execute(
                    "SELECT id, name, description FROM evidence_chains WHERE id = ?",
                    (d["item_id"],),
                ).fetchone()
                if ch:
                    html += f"<h3>Evidence Chain: {ch['name']}</h3>"
                    html += f"<p>{ch['description']}</p>"
            if d["note"]:
                html += f"<blockquote>{d['note']}</blockquote>"
            html += "<hr>"

    return HTMLResponse(html)


@router.get("/case-files/{case_id}/export/csv")
def export_case_file_csv(case_id: int):
    """Export case file items as CSV."""
    import csv
    import io

    with get_db() as conn:
        _ensure_case_files_table(conn)
        cf = conn.execute("SELECT * FROM case_files WHERE id = ?", (case_id,)).fetchone()
        if not cf:
            raise HTTPException(404, "Case file not found")
        items = conn.execute(
            "SELECT * FROM case_file_items WHERE case_file_id = ? ORDER BY sort_order, added_at",
            (case_id,),
        ).fetchall()

        rows = []
        for item in items:
            d = dict(item)
            row = {
                "item_type": d["item_type"],
                "item_id": d["item_id"],
                "note": d["note"],
                "sort_order": d["sort_order"],
                "added_at": d["added_at"],
                "detail": "",
            }
            if d["item_type"] == "document":
                doc = conn.execute(
                    "SELECT title, filename, category FROM documents WHERE id = ?",
                    (d["item_id"],),
                ).fetchone()
                if doc:
                    row["detail"] = doc["title"] or doc["filename"]
            elif d["item_type"] == "entity":
                ent = conn.execute(
                    "SELECT name, type FROM entities WHERE id = ?", (d["item_id"],)
                ).fetchone()
                if ent:
                    row["detail"] = f"{ent['name']} ({ent['type']})"
            elif d["item_type"] == "chain":
                ch = conn.execute(
                    "SELECT name FROM evidence_chains WHERE id = ?", (d["item_id"],),
                ).fetchone()
                if ch:
                    row["detail"] = ch["name"]
            rows.append(row)

    out = io.StringIO()
    if rows:
        writer = csv.DictWriter(out, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return JSONResponse(content={
        "csv": out.getvalue(),
        "count": len(rows),
        "case_file": cf["name"],
    })


# ═══════════════════════════════════════════
# INVESTIGATION STATS
# ═══════════════════════════════════════════


@router.get("/investigation-stats")
def investigation_stats():
    """Comprehensive investigation metrics dashboard."""
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        _ensure_watchlist_table(conn)
        _ensure_annotations_table(conn)
        _ensure_case_files_table(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS redactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                start_offset INTEGER NOT NULL,
                end_offset INTEGER NOT NULL,
                reason TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analyst_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                note TEXT NOT NULL,
                author TEXT DEFAULT 'analyst',
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # Core counts
        total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        total_entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        total_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        total_connections = conn.execute("SELECT COUNT(*) FROM entity_connections").fetchone()[0]

        # Entity type breakdown
        entity_types = conn.execute(
            "SELECT type, COUNT(*) as count FROM entities GROUP BY type ORDER BY count DESC"
        ).fetchall()

        # Category breakdown
        categories = conn.execute(
            "SELECT category, COUNT(*) as count FROM documents GROUP BY category ORDER BY count DESC"
        ).fetchall()

        # Source breakdown
        sources = conn.execute(
            """SELECT source, COUNT(*) as count
               FROM documents WHERE source IS NOT NULL AND source != ''
               GROUP BY source ORDER BY count DESC"""
        ).fetchall()

        # Date coverage
        date_range = conn.execute(
            """SELECT MIN(date) as earliest, MAX(date) as latest,
                      COUNT(CASE WHEN date IS NOT NULL AND date != '' THEN 1 END) as dated,
                      COUNT(CASE WHEN date IS NULL OR date = '' THEN 1 END) as undated
               FROM documents"""
        ).fetchone()

        # Total pages
        total_pages = conn.execute("SELECT COALESCE(SUM(pages), 0) FROM documents").fetchone()[0]

        # Flagged docs
        flagged = conn.execute("SELECT COUNT(*) FROM documents WHERE flagged = 1").fetchone()[0]

        # Evidence chains
        chain_count = conn.execute("SELECT COUNT(*) FROM evidence_chains").fetchone()[0]
        chain_links = conn.execute("SELECT COUNT(*) FROM evidence_chain_links").fetchone()[0]

        # Financial indicators
        fin_count = conn.execute("SELECT COUNT(*) FROM financial_indicators").fetchone()[0]
        avg_risk = conn.execute("SELECT AVG(risk_score) FROM financial_indicators").fetchone()[0]

        # Redaction count
        redaction_count = conn.execute("SELECT COUNT(*) FROM redactions").fetchone()[0]

        # Watchlist
        watchlist_count = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]

        # Annotations & notes
        annotation_count = conn.execute("SELECT COUNT(*) FROM annotations").fetchone()[0]
        analyst_note_count = conn.execute("SELECT COUNT(*) FROM analyst_notes").fetchone()[0]

        # Resolution stats
        resolved_count = conn.execute("SELECT COUNT(*) FROM entity_resolutions").fetchone()[0]

        # Most connected entities (by connection weight)
        top_connected = conn.execute(
            """SELECT e.name, e.type,
                      SUM(ec.weight) as total_weight,
                      COUNT(*) as connection_count
               FROM entities e
               JOIN entity_connections ec ON ec.entity_a_id = e.id OR ec.entity_b_id = e.id
               GROUP BY e.id
               ORDER BY total_weight DESC LIMIT 10"""
        ).fetchall()

    return {
        "core": {
            "documents": total_docs,
            "entities": total_entities,
            "events": total_events,
            "connections": total_connections,
            "pages": total_pages,
            "flagged": flagged,
        },
        "entity_types": [dict(e) for e in entity_types],
        "categories": [dict(c) for c in categories],
        "sources": [dict(s) for s in sources],
        "date_range": dict(date_range) if date_range else {},
        "analysis": {
            "evidence_chains": chain_count,
            "chain_links": chain_links,
            "financial_indicators": fin_count,
            "avg_risk_score": round(avg_risk, 2) if avg_risk else 0,
            "redactions": redaction_count,
            "watchlist": watchlist_count,
            "annotations": annotation_count,
            "analyst_notes": analyst_note_count,
            "resolved_entities": resolved_count,
        },
        "top_connected": [dict(t) for t in top_connected],
    }
