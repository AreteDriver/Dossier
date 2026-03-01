"""DOSSIER — Analytics, metrics, exports, and remaining endpoints."""

import logging
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

from dossier.db.database import get_db
from dossier.api.routes_collaboration import (
    _ensure_annotations_table,
    _ensure_analyst_notes_table,
)
from dossier.api.utils import _ensure_audit_table

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/graph/path-between")
def graph_path_between(
    source_name: str = Query(...),
    target_name: str = Query(...),
):
    """Find connection path between two named entities."""
    with get_db() as conn:
        # Find entity IDs by name
        src = conn.execute(
            "SELECT id FROM entities WHERE LOWER(name) = LOWER(?) LIMIT 1",
            (source_name,),
        ).fetchone()
        tgt = conn.execute(
            "SELECT id FROM entities WHERE LOWER(name) = LOWER(?) LIMIT 1",
            (target_name,),
        ).fetchone()
        if not src or not tgt:
            return {"error": "Entity not found", "path": [], "shared_documents": []}

    from dossier.core.graph_analysis import GraphAnalyzer

    with get_db() as conn:
        try:
            analyzer = GraphAnalyzer(conn)
            result = analyzer.find_shortest_path(src["id"], tgt["id"])
        except Exception:
            logger.exception("Graph path error")
            result = None

        # Also find shared documents
        shared = conn.execute(
            """
            SELECT DISTINCT d.id, d.title, d.filename, d.category, d.source
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            JOIN documents d ON d.id = de1.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
            ORDER BY d.title
            LIMIT 20
        """,
            (src["id"], tgt["id"]),
        ).fetchall()

    if not result:
        return {
            "path": [],
            "edges": [],
            "hops": 0,
            "total_weight": 0,
            "shared_documents": [dict(d) for d in shared],
            "error": "No path found" if not shared else None,
        }

    return {
        "path": result.nodes,
        "edges": result.edges,
        "hops": result.hops,
        "total_weight": result.total_weight,
        "shared_documents": [dict(d) for d in shared],
    }


@router.get("/export/intel-brief")
def export_intel_brief(
    min_risk: float = Query(0.5, ge=0, le=1),
    source: Optional[str] = Query(None),
):
    """Generate a markdown intelligence brief."""
    with get_db() as conn:
        # High-risk docs
        sql = """
            SELECT df.document_id, df.score as risk_score,
                   d.filename, d.title, d.category, d.source, d.date
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score >= ?
        """
        params = [min_risk]
        if source:
            sql += " AND d.source = ?"
            params.append(source)
        sql += " ORDER BY df.score DESC"
        risk_docs = conn.execute(sql, params).fetchall()

        # Key persons
        person_sql = """
            SELECT e.name, COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            JOIN document_forensics df ON df.document_id = de.document_id
            WHERE e.type = 'person'
              AND df.analysis_type = 'risk_score' AND df.score >= ?
        """
        person_params = [min_risk]
        if source:
            person_sql += " AND de.document_id IN (SELECT id FROM documents WHERE source = ?)"
            person_params.append(source)
        person_sql += " GROUP BY e.id ORDER BY doc_count DESC, total_mentions DESC LIMIT 30"
        persons = conn.execute(person_sql, person_params).fetchall()

        # AML flags
        aml_sql = """
            SELECT df.label, df.severity, COUNT(*) as count
            FROM document_forensics df
            WHERE df.analysis_type = 'aml_flag'
        """
        aml_params = []
        if source:
            aml_sql += " AND df.document_id IN (SELECT id FROM documents WHERE source = ?)"
            aml_params.append(source)
        aml_sql += " GROUP BY df.label, df.severity ORDER BY count DESC"
        aml_flags = conn.execute(aml_sql, aml_params).fetchall()

        # Stats
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        page_count = conn.execute("SELECT COALESCE(SUM(pages),0) FROM documents").fetchone()[0]

    # Build markdown
    lines = [
        "# DOSSIER — Intelligence Brief",
        f"**Generated**: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Risk Threshold**: {min_risk * 100:.0f}%+",
        f"**Source Filter**: {source or 'All Sources'}",
        "",
        "## Corpus Overview",
        f"- **Documents**: {doc_count:,}",
        f"- **Entities**: {entity_count:,}",
        f"- **Pages**: {page_count:,}",
        f"- **Flagged Documents**: {len(risk_docs)}",
        "",
        "## Key Persons",
        "| Name | Documents | Mentions |",
        "|------|-----------|----------|",
    ]
    for p in persons[:20]:
        lines.append(f"| {p['name']} | {p['doc_count']} | {p['total_mentions']:,} |")

    lines += ["", "## AML Flags", "| Flag | Severity | Count |", "|------|----------|-------|"]
    for f in aml_flags:
        lines.append(f"| {f['label'].replace('_', ' ')} | {f['severity']} | {f['count']} |")

    lines += [
        "",
        "## Highest Risk Documents",
        "| Risk | Document | Category | Source |",
        "|------|----------|----------|--------|",
    ]
    for d in risk_docs[:30]:
        score = f"{d['risk_score'] * 100:.0f}%"
        lines.append(
            f"| {score} | {d['title'] or d['filename']} | {d['category']} | {d['source'] or ''} |"
        )

    lines += ["", "---", "*Generated by DOSSIER Document Intelligence System*"]

    return {
        "markdown": "\n".join(lines),
        "summary": {
            "flagged_documents": len(risk_docs),
            "key_persons": len(persons),
            "aml_flags": len(aml_flags),
        },
    }


@router.get("/timeline/heatmap")
def timeline_heatmap():
    """Get event counts by date for heatmap visualization."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT event_date, COUNT(*) as count
            FROM events
            WHERE event_date IS NOT NULL
              AND length(event_date) >= 10
              AND event_date >= '1980-01-01'
              AND event_date <= '2026-12-31'
              AND confidence >= 0.5
            GROUP BY event_date
            ORDER BY event_date
        """).fetchall()
    return {"dates": [{"date": r["event_date"][:10], "count": r["count"]} for r in rows]}


# ═══════════════════════════════════════════


@router.get("/matrix/relationships")
def relationship_matrix(limit: int = Query(30, ge=5, le=100)):
    """Person-to-person relationship strength matrix."""
    with get_db() as conn:
        top_persons = conn.execute(
            """
            SELECT e.id, e.name, SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type = 'person'
            GROUP BY e.id
            ORDER BY total_mentions DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        person_ids = [p["id"] for p in top_persons]
        person_names = [p["name"] for p in top_persons]

        if len(person_ids) < 2:
            return {"entities": person_names, "matrix": [], "connections": []}

        placeholders = ",".join("?" * len(person_ids))
        connections = conn.execute(
            f"""
            SELECT entity_a_id, entity_b_id, weight
            FROM entity_connections
            WHERE entity_a_id IN ({placeholders})
              AND entity_b_id IN ({placeholders})
        """,
            person_ids + person_ids,
        ).fetchall()

    id_to_idx = {pid: i for i, pid in enumerate(person_ids)}
    n = len(person_ids)
    matrix = [[0] * n for _ in range(n)]
    conn_list = []

    for c in connections:
        i = id_to_idx.get(c["entity_a_id"])
        j = id_to_idx.get(c["entity_b_id"])
        if i is not None and j is not None:
            matrix[i][j] = c["weight"]
            matrix[j][i] = c["weight"]
            conn_list.append(
                {
                    "source": person_names[i],
                    "target": person_names[j],
                    "weight": c["weight"],
                }
            )

    return {"entities": person_names, "matrix": matrix, "connections": conn_list}


# ═══════════════════════════════════════════


@router.get("/geo/locations")
def geo_locations(limit: int = Query(50, ge=1, le=200)):
    """Place entities with document counts for map visualization."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT e.id, e.name,
                   COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type = 'place'
            GROUP BY e.id
            ORDER BY doc_count DESC, total_mentions DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        locations = []
        for r in rows:
            loc = dict(r)
            docs = conn.execute(
                """
                SELECT d.id, d.title, d.category
                FROM document_entities de
                JOIN documents d ON d.id = de.document_id
                WHERE de.entity_id = ?
                ORDER BY de.count DESC LIMIT 3
            """,
                (r["id"],),
            ).fetchall()
            loc["documents"] = [dict(d) for d in docs]
            locations.append(loc)

    return {"locations": locations}


# ═══════════════════════════════════════════


@router.get("/export/report")
def export_report(
    min_risk: float = Query(0.5, ge=0, le=1),
    source: Optional[str] = Query(None),
):
    """Generate a comprehensive HTML investigation report."""
    import datetime

    with get_db() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        page_count = conn.execute("SELECT COALESCE(SUM(pages),0) FROM documents").fetchone()[0]

        # Risk docs
        risk_sql = """
            SELECT df.document_id, df.score, d.title, d.filename, d.category, d.source
            FROM document_forensics df JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score >= ?
        """
        params: list = [min_risk]
        if source:
            risk_sql += " AND d.source = ?"
            params.append(source)
        risk_sql += " ORDER BY df.score DESC"
        risk_docs = conn.execute(risk_sql, params).fetchall()

        # Key persons
        persons = conn.execute("""
            SELECT e.name, COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type = 'person'
            GROUP BY e.id ORDER BY doc_count DESC LIMIT 20
        """).fetchall()

        # AML flags
        aml = conn.execute("""
            SELECT label, severity, COUNT(*) as count
            FROM document_forensics WHERE analysis_type = 'aml_flag'
            GROUP BY label, severity ORDER BY count DESC
        """).fetchall()

        # Anomalies — temporal spikes
        spikes = conn.execute("""
            SELECT event_date, COUNT(*) as count FROM events
            WHERE event_date IS NOT NULL AND length(event_date) >= 10
              AND confidence >= 0.5
            GROUP BY event_date ORDER BY count DESC LIMIT 10
        """).fetchall()

        # Communities
        try:
            from dossier.core.graph_analysis import GraphAnalyzer

            analyzer = GraphAnalyzer(conn)
            communities = analyzer.get_communities(min_size=3)
        except Exception:
            communities = []

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # Build HTML report
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>DOSSIER Investigation Report</title>
<style>
body{{font-family:'Helvetica Neue',sans-serif;max-width:900px;margin:0 auto;padding:40px;color:#222;line-height:1.6;}}
h1{{color:#c4473a;border-bottom:3px solid #c4473a;padding-bottom:10px;}}
h2{{color:#333;margin-top:30px;border-bottom:1px solid #ddd;padding-bottom:6px;}}
table{{border-collapse:collapse;width:100%;margin:12px 0;}}
th,td{{border:1px solid #ddd;padding:8px;text-align:left;font-size:13px;}}
th{{background:#f5f5f5;font-weight:600;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:600;}}
.badge.critical{{background:#fee;color:#c44;}} .badge.high{{background:#fec;color:#a63;}}
.badge.medium{{background:#ffd;color:#963;}} .badge.low{{background:#efe;color:#396;}}
.stat{{display:inline-block;text-align:center;padding:12px 24px;margin:4px;background:#f8f8f8;border-radius:6px;}}
.stat-val{{font-size:24px;font-weight:700;color:#c4473a;}} .stat-lbl{{font-size:11px;color:#888;text-transform:uppercase;}}
</style></head><body>
<h1>DOSSIER — Investigation Report</h1>
<p><strong>Generated:</strong> {now} | <strong>Risk Threshold:</strong> {min_risk * 100:.0f}%+
{f" | <strong>Source:</strong> {source}" if source else ""}</p>

<div>
<div class="stat"><div class="stat-val">{doc_count:,}</div><div class="stat-lbl">Documents</div></div>
<div class="stat"><div class="stat-val">{entity_count:,}</div><div class="stat-lbl">Entities</div></div>
<div class="stat"><div class="stat-val">{page_count:,}</div><div class="stat-lbl">Pages</div></div>
<div class="stat"><div class="stat-val">{len(risk_docs)}</div><div class="stat-lbl">Flagged</div></div>
</div>

<h2>Key Persons</h2>
<table><thead><tr><th>Name</th><th>Documents</th><th>Mentions</th></tr></thead><tbody>
{"".join(f"<tr><td>{p['name']}</td><td>{p['doc_count']}</td><td>{p['mentions']:,}</td></tr>" for p in persons)}
</tbody></table>

<h2>Highest Risk Documents</h2>
<table><thead><tr><th>Risk</th><th>Document</th><th>Category</th><th>Source</th></tr></thead><tbody>
{"".join(f'<tr><td><span class="badge {"critical" if d["score"] > 0.7 else "high" if d["score"] > 0.5 else "medium"}">{d["score"] * 100:.0f}%</span></td><td>{d["title"] or d["filename"]}</td><td>{d["category"]}</td><td>{d["source"] or ""}</td></tr>' for d in risk_docs[:30])}
</tbody></table>

<h2>AML Flags</h2>
<table><thead><tr><th>Flag</th><th>Severity</th><th>Count</th></tr></thead><tbody>
{"".join(f"<tr><td>{f['label'].replace('_', ' ')}</td><td>{f['severity']}</td><td>{f['count']}</td></tr>" for f in aml)}
</tbody></table>

<h2>Network Communities ({len(communities)} detected)</h2>"""

    for i, comm in enumerate(communities[:10]):
        members = ", ".join(m["name"] for m in comm.members[:15])
        html += f"<p><strong>Community {i + 1}</strong> ({comm.size} members, density {comm.density:.2f}): {members}</p>"

    html += f"""
<h2>Temporal Hotspots</h2>
<table><thead><tr><th>Date</th><th>Events</th></tr></thead><tbody>
{"".join(f"<tr><td>{s['event_date'][:10]}</td><td>{s['count']}</td></tr>" for s in spikes)}
</tbody></table>

<hr><p style="color:#888;font-size:11px;">Generated by DOSSIER Document Intelligence System — {now}</p>
</body></html>"""

    return {
        "html": html,
        "stats": {
            "documents": doc_count,
            "flagged": len(risk_docs),
            "persons": len(persons),
            "communities": len(communities),
        },
    }


# ═══════════════════════════════════════════


@router.get("/graph/communities-labeled")
def communities_labeled(min_size: int = Query(3, ge=2)):
    """Get communities with auto-generated labels based on top members."""
    from dossier.core.graph_analysis import GraphAnalyzer

    with get_db() as conn:
        try:
            analyzer = GraphAnalyzer(conn)
            communities = analyzer.get_communities(min_size=min_size)
        except Exception:
            logger.exception("Community detection error")
            return {"communities": [], "error": "Community detection failed"}

        result = []
        for i, comm in enumerate(communities):
            # Auto-label: use top 2-3 person names, or top member names
            persons = [m for m in comm.members if m.get("type") == "person"]
            if persons:
                label = " / ".join(m["name"] for m in persons[:3])
            else:
                label = " / ".join(m["name"] for m in comm.members[:3])

            # Get shared documents for this community
            member_ids = [m["entity_id"] for m in comm.members[:20]]
            if len(member_ids) >= 2:
                ph = ",".join("?" * len(member_ids))
                shared = conn.execute(
                    f"""
                    SELECT d.id, d.title, d.category, COUNT(DISTINCT de.entity_id) as member_count
                    FROM document_entities de
                    JOIN documents d ON d.id = de.document_id
                    WHERE de.entity_id IN ({ph})
                    GROUP BY d.id
                    HAVING COUNT(DISTINCT de.entity_id) >= 2
                    ORDER BY member_count DESC LIMIT 5
                """,
                    member_ids,
                ).fetchall()
            else:
                shared = []

            result.append(
                {
                    "id": i,
                    "label": label,
                    "size": comm.size,
                    "density": comm.density,
                    "members": [
                        dict(m)
                        if isinstance(m, dict)
                        else {"entity_id": m.entity_id, "name": m.name, "type": m.type}
                        if hasattr(m, "entity_id")
                        else m
                        for m in comm.members
                    ],
                    "shared_documents": [dict(d) for d in shared],
                }
            )

    return {"communities": result, "total": len(result)}


# ═══════════════════════════════════════════


@router.get("/timeline/overlay")
def timeline_overlay(
    entity_ids: str = Query(..., description="Comma-separated entity IDs"),
    limit: int = Query(200, ge=1, le=1000),
):
    """Get timeline events for multiple entities, grouped by entity for overlay comparison."""
    ids = [int(x.strip()) for x in entity_ids.split(",") if x.strip().isdigit()]
    if not ids:
        raise HTTPException(400, "At least one entity_id required")

    with get_db() as conn:
        ph = ",".join("?" * len(ids))
        rows = conn.execute(
            f"""
            SELECT DISTINCT e.id as entity_id, e.name as entity_name, e.type as entity_type,
                   ev.event_date, ev.precision, ev.confidence, ev.context,
                   ev.document_id, d.title as doc_title
            FROM events ev
            JOIN document_entities de ON de.document_id = ev.document_id
            JOIN entities e ON e.id = de.entity_id
            JOIN documents d ON d.id = ev.document_id
            WHERE e.id IN ({ph})
              AND ev.event_date IS NOT NULL
              AND ev.confidence >= 0.5
            ORDER BY ev.event_date, e.id
            LIMIT ?
        """,
            ids + [limit],
        ).fetchall()

        # Group by entity
        by_entity: dict[int, dict] = {}
        for r in rows:
            eid = r["entity_id"]
            if eid not in by_entity:
                by_entity[eid] = {
                    "entity_id": eid,
                    "entity_name": r["entity_name"],
                    "entity_type": r["entity_type"],
                    "events": [],
                }
            by_entity[eid]["events"].append(
                {
                    "date": r["event_date"],
                    "precision": r["precision"],
                    "context": r["context"],
                    "doc_id": r["document_id"],
                    "doc_title": r["doc_title"],
                }
            )

    return {"entities": list(by_entity.values()), "total_events": len(rows)}


# ═══════════════════════════════════════════


@router.get("/export/entities")
def export_entities(
    type: Optional[str] = Query(None),
    format: str = Query("json", description="json or csv"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Export entities as JSON or CSV."""
    with get_db() as conn:
        sql = """
            SELECT e.id, e.name, e.type, e.canonical,
                   SUM(de.count) as total_mentions,
                   COUNT(DISTINCT de.document_id) as doc_count
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
        """
        params: list = []
        if type:
            sql += " WHERE e.type = ?"
            params.append(type)
        sql += " GROUP BY e.id ORDER BY total_mentions DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()

    entities = [dict(r) for r in rows]

    if format == "csv":
        import csv
        import io

        out = io.StringIO()
        if entities:
            writer = csv.DictWriter(out, fieldnames=entities[0].keys())
            writer.writeheader()
            writer.writerows(entities)
        return JSONResponse(content={"csv": out.getvalue(), "count": len(entities)})

    return {"entities": entities, "count": len(entities)}


@router.get("/export/connections")
def export_connections(
    min_weight: int = Query(1, ge=1),
    format: str = Query("json"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Export entity connections as JSON or CSV."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT ec.entity_a_id, ea.name as entity_a_name, ea.type as entity_a_type,
                   ec.entity_b_id, eb.name as entity_b_name, eb.type as entity_b_type,
                   ec.weight
            FROM entity_connections ec
            JOIN entities ea ON ea.id = ec.entity_a_id
            JOIN entities eb ON eb.id = ec.entity_b_id
            WHERE ec.weight >= ?
            ORDER BY ec.weight DESC
            LIMIT ?
        """,
            (min_weight, limit),
        ).fetchall()

    connections = [dict(r) for r in rows]

    if format == "csv":
        import csv
        import io

        out = io.StringIO()
        if connections:
            writer = csv.DictWriter(out, fieldnames=connections[0].keys())
            writer.writeheader()
            writer.writerows(connections)
        return JSONResponse(content={"csv": out.getvalue(), "count": len(connections)})

    return {"connections": connections, "count": len(connections)}


@router.get("/export/timeline")
def export_timeline(
    format: str = Query("json"),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Export timeline events as JSON or CSV."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT ev.id, ev.event_date, ev.precision, ev.confidence,
                   ev.context, ev.document_id, d.title as doc_title
            FROM events ev
            JOIN documents d ON d.id = ev.document_id
            WHERE ev.event_date IS NOT NULL AND ev.confidence >= 0.5
            ORDER BY ev.event_date
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

    events = [dict(r) for r in rows]

    if format == "csv":
        import csv
        import io

        out = io.StringIO()
        if events:
            writer = csv.DictWriter(out, fieldnames=events[0].keys())
            writer.writeheader()
            writer.writerows(events)
        return JSONResponse(content={"csv": out.getvalue(), "count": len(events)})

    return {"events": events, "count": len(events)}


@router.get("/export/entity-graph")
def export_entity_graph(
    type: Optional[str] = Query(None),
    min_weight: int = Query(1, ge=1),
    limit: int = Query(500, ge=1, le=5000),
):
    """Export entity graph as D3/Gephi-compatible JSON (nodes + edges)."""
    with get_db() as conn:
        # Nodes
        node_sql = """
            SELECT e.id, e.name, e.type, e.canonical,
                   COALESCE(SUM(de.count), 0) as mentions,
                   COUNT(DISTINCT de.document_id) as doc_count
            FROM entities e
            LEFT JOIN document_entities de ON de.entity_id = e.id
        """
        params: list = []
        if type:
            node_sql += " WHERE e.type = ?"
            params.append(type)
        node_sql += " GROUP BY e.id ORDER BY mentions DESC LIMIT ?"
        params.append(limit)
        node_rows = conn.execute(node_sql, params).fetchall()

        node_ids = {r["id"] for r in node_rows}

        # Edges (only between included nodes)
        edge_rows = conn.execute(
            """
            SELECT ec.entity_a_id as source, ec.entity_b_id as target, ec.weight
            FROM entity_connections ec
            WHERE ec.weight >= ?
            ORDER BY ec.weight DESC
        """,
            (min_weight,),
        ).fetchall()

    nodes = [
        {
            "id": r["id"],
            "label": r["name"],
            "type": r["type"],
            "mentions": r["mentions"],
            "doc_count": r["doc_count"],
        }
        for r in node_rows
    ]
    edges = [
        {"source": r["source"], "target": r["target"], "weight": r["weight"]}
        for r in edge_rows
        if r["source"] in node_ids and r["target"] in node_ids
    ]

    return {"nodes": nodes, "edges": edges, "node_count": len(nodes), "edge_count": len(edges)}


@router.get("/export/documents")
def export_documents(
    category: Optional[str] = Query(None),
    format: str = Query("json", description="json or csv"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Export document metadata as JSON or CSV."""
    with get_db() as conn:
        sql = """
            SELECT d.id, d.filename, d.title, d.category, d.source, d.date,
                   d.pages, d.flagged, d.ingested_at,
                   COUNT(DISTINCT de.entity_id) as entity_count
            FROM documents d
            LEFT JOIN document_entities de ON de.document_id = d.id
        """
        params: list = []
        if category:
            sql += " WHERE d.category = ?"
            params.append(category)
        sql += " GROUP BY d.id ORDER BY d.ingested_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()

    documents = [dict(r) for r in rows]

    if format == "csv":
        import csv
        import io

        out = io.StringIO()
        if documents:
            writer = csv.DictWriter(out, fieldnames=documents[0].keys())
            writer.writeheader()
            writer.writerows(documents)
        return JSONResponse(content={"csv": out.getvalue(), "count": len(documents)})

    return {"documents": documents, "count": len(documents)}


# ═══════════════════════════════════════════


@router.get("/cluster-map")
def cluster_map(min_cluster_size: int = Query(3, ge=2, le=20)):
    """Get cluster data with inter-cluster similarity for visualization."""
    with get_db() as conn:
        # Get clusters
        clusters_raw = conn.execute(
            """
            SELECT d.category, COUNT(*) as doc_count,
                   GROUP_CONCAT(d.id) as doc_ids
            FROM documents d
            GROUP BY d.category
            HAVING doc_count >= ?
            ORDER BY doc_count DESC
        """,
            (min_cluster_size,),
        ).fetchall()

        clusters = []
        for c in clusters_raw:
            doc_ids = [int(x) for x in c["doc_ids"].split(",")][:50]
            placeholders = ",".join("?" * len(doc_ids))

            # Get top entities in this cluster
            top_entities = conn.execute(
                f"""
                SELECT e.name, e.type, COUNT(*) as count
                FROM document_entities de
                JOIN entities e ON e.id = de.entity_id
                WHERE de.document_id IN ({placeholders})
                GROUP BY e.id
                ORDER BY count DESC
                LIMIT 10
            """,
                doc_ids,
            ).fetchall()

            clusters.append(
                {
                    "category": c["category"],
                    "doc_count": c["doc_count"],
                    "top_entities": [dict(e) for e in top_entities],
                    "sample_doc_ids": doc_ids[:10],
                }
            )

        # Cross-cluster entity overlap
        overlaps = []
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                ents_i = {e["name"] for e in clusters[i]["top_entities"]}
                ents_j = {e["name"] for e in clusters[j]["top_entities"]}
                shared = ents_i & ents_j
                if shared:
                    overlaps.append(
                        {
                            "cluster_a": clusters[i]["category"],
                            "cluster_b": clusters[j]["category"],
                            "shared_entities": list(shared)[:10],
                            "overlap_count": len(shared),
                        }
                    )

    return {"clusters": clusters, "overlaps": overlaps}


# ═══════════════════════════════════════════


@router.get("/activity-heatmap")
def activity_heatmap(year: int = Query(None)):
    """Get daily activity counts for calendar heatmap."""
    with get_db() as conn:
        # Events by date
        event_dates = conn.execute("""
            SELECT SUBSTR(event_date, 1, 10) as date, COUNT(*) as count
            FROM events
            WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 10
            GROUP BY date
            ORDER BY date
        """).fetchall()

        # Documents by date
        doc_dates = conn.execute("""
            SELECT SUBSTR(date, 1, 10) as date, COUNT(*) as count
            FROM documents
            WHERE date IS NOT NULL AND LENGTH(date) >= 10
            GROUP BY date
            ORDER BY date
        """).fetchall()

        # Merge into a single map
        heatmap = {}
        for row in event_dates:
            d = row["date"]
            if year and not d.startswith(str(year)):
                continue
            heatmap[d] = heatmap.get(d, 0) + row["count"]
        for row in doc_dates:
            d = row["date"]
            if year and not d.startswith(str(year)):
                continue
            heatmap[d] = heatmap.get(d, 0) + row["count"]

        # Get available years
        years = sorted({d[:4] for d in heatmap.keys() if len(d) >= 4})

        # Summary stats
        total_active_days = len(heatmap)
        max_activity = max(heatmap.values()) if heatmap else 0
        peak_date = max(heatmap, key=heatmap.get) if heatmap else None

    return {
        "heatmap": [{"date": k, "count": v} for k, v in sorted(heatmap.items())],
        "years": years,
        "total_active_days": total_active_days,
        "max_activity": max_activity,
        "peak_date": peak_date,
    }


# ═══════════════════════════════════════════


@router.get("/xref-matrix")
def xref_matrix(entity_type: str = Query("person"), limit: int = Query(30)):
    """Entity-to-source cross-reference matrix."""
    with get_db() as conn:
        # Get top entities by doc count
        entities = conn.execute(
            """SELECT e.id, e.name, COUNT(DISTINCT de.document_id) as doc_count
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               WHERE e.type = ?
               GROUP BY e.id
               ORDER BY doc_count DESC LIMIT ?""",
            (entity_type, limit),
        ).fetchall()

        # Get all sources
        sources = conn.execute(
            """SELECT DISTINCT source FROM documents
               WHERE source IS NOT NULL AND source != ''
               ORDER BY source"""
        ).fetchall()
        source_names = [s["source"] for s in sources]

        # Build matrix
        matrix = []
        for ent in entities:
            row = conn.execute(
                """SELECT d.source, COUNT(DISTINCT d.id) as count
                   FROM document_entities de
                   JOIN documents d ON d.id = de.document_id
                   WHERE de.entity_id = ?
                     AND d.source IS NOT NULL AND d.source != ''
                   GROUP BY d.source""",
                (ent["id"],),
            ).fetchall()
            source_counts = {r["source"]: r["count"] for r in row}
            matrix.append(
                {
                    "entity": ent["name"],
                    "entity_id": ent["id"],
                    "total": ent["doc_count"],
                    "by_source": {s: source_counts.get(s, 0) for s in source_names},
                }
            )

    return {"matrix": matrix, "sources": source_names, "entity_type": entity_type}


# ═══════════════════════════════════════════


@router.get("/investigation-timeline")
def investigation_timeline():
    """Meta-timeline of the investigation: ingestion, analysis, and annotation events."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        _ensure_analyst_notes_table(conn)
        _ensure_audit_table(conn)

        events = []

        # Document ingestion events
        ingested = conn.execute(
            """SELECT id, title, filename, ingested_at, source, category
               FROM documents
               WHERE ingested_at IS NOT NULL
               ORDER BY ingested_at DESC LIMIT 50"""
        ).fetchall()
        for d in ingested:
            events.append(
                {
                    "type": "ingestion",
                    "date": d["ingested_at"],
                    "description": f"Ingested: {d['title'] or d['filename']}",
                    "detail": f"{d['category']} from {d['source'] or 'unknown'}",
                    "ref_id": d["id"],
                }
            )

        # Annotation events
        annotations = conn.execute(
            """SELECT a.created_at, a.note, a.text, d.title, d.id as doc_id
               FROM annotations a
               JOIN documents d ON d.id = a.document_id
               ORDER BY a.created_at DESC LIMIT 30"""
        ).fetchall()
        for a in annotations:
            events.append(
                {
                    "type": "annotation",
                    "date": a["created_at"],
                    "description": f"Annotation on: {a['title'] or 'Untitled'}",
                    "detail": a["note"] or a["text"] or "",
                    "ref_id": a["doc_id"],
                }
            )

        # Analyst notes
        notes = conn.execute(
            """SELECT an.created_at, an.note, an.author, d.title, d.id as doc_id
               FROM analyst_notes an
               JOIN documents d ON d.id = an.document_id
               ORDER BY an.created_at DESC LIMIT 30"""
        ).fetchall()
        for n in notes:
            events.append(
                {
                    "type": "analyst_note",
                    "date": n["created_at"],
                    "description": f"Note on: {n['title'] or 'Untitled'}",
                    "detail": f"{n['author'] or 'analyst'}: {n['note'][:100] if n['note'] else ''}",
                    "ref_id": n["doc_id"],
                }
            )

        # Audit log entries
        audit = conn.execute(
            """SELECT action, target_type, target_id, details, created_at
               FROM audit_log
               ORDER BY created_at DESC LIMIT 30"""
        ).fetchall()
        for a in audit:
            events.append(
                {
                    "type": "audit",
                    "date": a["created_at"],
                    "description": f"{a['action']}: {a['target_type']} #{a['target_id']}",
                    "detail": a["details"] or "",
                    "ref_id": a["target_id"],
                }
            )

        # Sort all events by date descending
        events.sort(key=lambda x: x["date"] or "", reverse=True)

    return {"events": events[:100]}


# ═══════════════════════════════════════════


@router.get("/keyword-cooccurrence")
def keyword_cooccurrence(limit: int = Query(40)):
    """Find keywords that frequently appear together in the same documents."""
    with get_db() as conn:
        pairs = conn.execute(
            """SELECT k1.word as word_a, k2.word as word_b,
                      COUNT(DISTINCT dk1.document_id) as shared_docs
               FROM document_keywords dk1
               JOIN document_keywords dk2 ON dk2.document_id = dk1.document_id
                 AND dk2.keyword_id > dk1.keyword_id
               JOIN keywords k1 ON k1.id = dk1.keyword_id
               JOIN keywords k2 ON k2.id = dk2.keyword_id
               WHERE k1.doc_count >= 5 AND k2.doc_count >= 5
               GROUP BY dk1.keyword_id, dk2.keyword_id
               HAVING shared_docs >= 3
               ORDER BY shared_docs DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

    return {"pairs": [dict(p) for p in pairs]}


# ═══════════════════════════════════════════


@router.get("/entity-path")
def entity_path(from_id: int = Query(...), to_id: int = Query(...)):
    """Find shortest path between two entities via connections."""
    with get_db() as conn:
        from_ent = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (from_id,)
        ).fetchone()
        to_ent = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (to_id,)
        ).fetchone()
        if not from_ent or not to_ent:
            raise HTTPException(404, "Entity not found")

        # Build adjacency from entity_connections
        edges = conn.execute(
            "SELECT entity_a_id, entity_b_id, weight FROM entity_connections"
        ).fetchall()

        adj: dict[int, list[tuple[int, int]]] = {}
        for e in edges:
            a, b, w = e["entity_a_id"], e["entity_b_id"], e["weight"]
            adj.setdefault(a, []).append((b, w))
            adj.setdefault(b, []).append((a, w))

        # BFS for shortest path
        from collections import deque

        visited = {from_id}
        queue = deque([(from_id, [from_id])])
        found_path = None

        while queue and not found_path:
            current, path = queue.popleft()
            if len(path) > 8:
                break
            for neighbor, _ in adj.get(current, []):
                if neighbor == to_id:
                    found_path = path + [neighbor]
                    break
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        # Resolve names for the path
        path_details = []
        if found_path:
            placeholders = ",".join("?" * len(found_path))
            ents = conn.execute(
                f"SELECT id, name, type FROM entities WHERE id IN ({placeholders})",
                found_path,
            ).fetchall()
            ent_map = {e["id"]: dict(e) for e in ents}
            path_details = [ent_map.get(eid, {"id": eid}) for eid in found_path]

    return {
        "from": dict(from_ent),
        "to": dict(to_ent),
        "path": path_details,
        "hops": len(path_details) - 1 if path_details else -1,
        "found": found_path is not None,
    }


@router.get("/entity-path-suggestions")
def entity_path_suggestions():
    """Get top entities for path-finding dropdowns."""
    with get_db() as conn:
        entities = conn.execute(
            """SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               GROUP BY e.id
               ORDER BY doc_count DESC LIMIT 200"""
        ).fetchall()
    return {"entities": [dict(e) for e in entities]}


# ═══════════════════════════════════════════


@router.get("/document-sentiment")
def document_sentiment(limit: int = Query(50)):
    """Analyze document tone distribution across corpus using forensic analysis data."""
    with get_db() as conn:
        # Get tone/sentiment from document_forensics
        tones = conn.execute(
            """SELECT df.label, COUNT(*) as count, AVG(df.score) as avg_score
               FROM document_forensics df
               WHERE df.analysis_type = 'tone'
               GROUP BY df.label
               ORDER BY count DESC"""
        ).fetchall()

        # Per-document tone breakdown
        doc_tones = conn.execute(
            """SELECT d.id, d.title, d.filename, d.category,
                      df.label as tone, df.score
               FROM document_forensics df
               JOIN documents d ON d.id = df.document_id
               WHERE df.analysis_type = 'tone'
               ORDER BY df.score DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

        # Severity distribution
        severity_dist = conn.execute(
            """SELECT severity, COUNT(*) as count
               FROM document_forensics
               GROUP BY severity
               ORDER BY count DESC"""
        ).fetchall()

    return {
        "tone_distribution": [dict(t) for t in tones],
        "documents": [dict(d) for d in doc_tones],
        "severity_distribution": [dict(s) for s in severity_dist],
    }


# ═══════════════════════════════════════════


@router.get("/source-timeline")
def source_timeline():
    """Per-source document distribution over time."""
    with get_db() as conn:
        sources = conn.execute(
            """SELECT DISTINCT source FROM documents
               WHERE source IS NOT NULL AND source != ''
               ORDER BY source"""
        ).fetchall()

        result = []
        all_years = set()
        for src in sources:
            yearly = conn.execute(
                """SELECT SUBSTR(date, 1, 4) as year, COUNT(*) as count
                   FROM documents
                   WHERE source = ?
                     AND date IS NOT NULL AND date != ''
                   GROUP BY year ORDER BY year""",
                (src["source"],),
            ).fetchall()
            by_year = {y["year"]: y["count"] for y in yearly}
            all_years.update(by_year.keys())
            total = conn.execute(
                "SELECT COUNT(*) FROM documents WHERE source = ?", (src["source"],)
            ).fetchone()[0]
            result.append({"source": src["source"], "total": total, "by_year": by_year})

    return {"sources": result, "years": sorted(all_years)}


# ═══════════════════════════════════════════


@router.get("/entity-frequency")
def entity_frequency(entity_type: str = Query("person"), limit: int = Query(20)):
    """Entity mention frequency over time."""
    with get_db() as conn:
        # Top entities
        top = conn.execute(
            """SELECT e.id, e.name, COUNT(DISTINCT de.document_id) as doc_count,
                      SUM(de.count) as total_mentions
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               WHERE e.type = ?
               GROUP BY e.id
               ORDER BY doc_count DESC LIMIT ?""",
            (entity_type, limit),
        ).fetchall()

        all_years = set()
        results = []
        for ent in top:
            yearly = conn.execute(
                """SELECT SUBSTR(d.date, 1, 4) as year, SUM(de.count) as mentions
                   FROM document_entities de
                   JOIN documents d ON d.id = de.document_id
                   WHERE de.entity_id = ?
                     AND d.date IS NOT NULL AND d.date != ''
                   GROUP BY year ORDER BY year""",
                (ent["id"],),
            ).fetchall()
            by_year = {y["year"]: y["mentions"] for y in yearly}
            all_years.update(by_year.keys())
            results.append(
                {
                    "id": ent["id"],
                    "name": ent["name"],
                    "doc_count": ent["doc_count"],
                    "total_mentions": ent["total_mentions"],
                    "by_year": by_year,
                }
            )

    return {"entities": results, "years": sorted(all_years), "type": entity_type}


# ═══════════════════════════════════════════


@router.get("/flagged-hub")
def flagged_hub():
    """Centralized view for flagged/bookmarked documents with notes and entities."""
    with get_db() as conn:
        _ensure_analyst_notes_table(conn)
        _ensure_annotations_table(conn)

        flagged = conn.execute(
            """SELECT d.id, d.title, d.filename, d.category, d.source, d.date,
                      d.pages, d.notes, d.flagged
               FROM documents d
               WHERE d.flagged = 1
               ORDER BY d.date DESC"""
        ).fetchall()

        results = []
        for doc in flagged:
            entities = conn.execute(
                """SELECT e.name, e.type, de.count
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ?
                   ORDER BY de.count DESC LIMIT 8""",
                (doc["id"],),
            ).fetchall()

            note_count = conn.execute(
                "SELECT COUNT(*) FROM analyst_notes WHERE document_id = ?",
                (doc["id"],),
            ).fetchone()[0]

            annotation_count = conn.execute(
                "SELECT COUNT(*) FROM annotations WHERE document_id = ?",
                (doc["id"],),
            ).fetchone()[0]

            results.append(
                {
                    **dict(doc),
                    "entities": [dict(e) for e in entities],
                    "analyst_notes": note_count,
                    "annotations": annotation_count,
                }
            )

    return {"flagged": results, "total": len(results)}


# ═══════════════════════════════════════════


@router.get("/relationship-graph")
def relationship_graph(
    entity_type: Optional[str] = None,
    min_weight: int = Query(1),
    limit: int = Query(100),
):
    """Weighted entity relationship graph with filtering."""
    with get_db() as conn:
        type_filter = ""
        params: list = [min_weight, limit]
        if entity_type:
            type_filter = "AND e1.type = ? AND e2.type = ?"
            params = [min_weight] + [entity_type, entity_type] + [limit]

        edges = conn.execute(
            f"""SELECT ec.entity_a_id, ec.entity_b_id, ec.weight,
                       e1.name as name_a, e1.type as type_a,
                       e2.name as name_b, e2.type as type_b
                FROM entity_connections ec
                JOIN entities e1 ON e1.id = ec.entity_a_id
                JOIN entities e2 ON e2.id = ec.entity_b_id
                WHERE ec.weight >= ?
                {type_filter}
                ORDER BY ec.weight DESC
                LIMIT ?""",
            params,
        ).fetchall()

        # Collect unique nodes
        nodes = {}
        for e in edges:
            if e["entity_a_id"] not in nodes:
                nodes[e["entity_a_id"]] = {
                    "id": e["entity_a_id"],
                    "name": e["name_a"],
                    "type": e["type_a"],
                }
            if e["entity_b_id"] not in nodes:
                nodes[e["entity_b_id"]] = {
                    "id": e["entity_b_id"],
                    "name": e["name_b"],
                    "type": e["type_b"],
                }

    return {
        "nodes": list(nodes.values()),
        "edges": [
            {
                "source": e["entity_a_id"],
                "target": e["entity_b_id"],
                "weight": e["weight"],
            }
            for e in edges
        ],
        "total_edges": len(edges),
    }


# ═══════════════════════════════════════════


@router.get("/document-sidebyside")
def document_sidebyside(doc_a: int = Query(...), doc_b: int = Query(...)):
    """Compare two documents side by side — shared and unique entities/keywords."""
    with get_db() as conn:
        da = conn.execute(
            "SELECT id, title, filename, category, source, date, pages FROM documents WHERE id = ?",
            (doc_a,),
        ).fetchone()
        db = conn.execute(
            "SELECT id, title, filename, category, source, date, pages FROM documents WHERE id = ?",
            (doc_b,),
        ).fetchone()
        if not da or not db:
            raise HTTPException(404, "Document not found")

        # Entities for each doc
        ents_a = conn.execute(
            """SELECT e.id, e.name, e.type, de.count
               FROM entities e JOIN document_entities de ON de.entity_id = e.id
               WHERE de.document_id = ? ORDER BY de.count DESC""",
            (doc_a,),
        ).fetchall()
        ents_b = conn.execute(
            """SELECT e.id, e.name, e.type, de.count
               FROM entities e JOIN document_entities de ON de.entity_id = e.id
               WHERE de.document_id = ? ORDER BY de.count DESC""",
            (doc_b,),
        ).fetchall()

        ids_a = {e["id"] for e in ents_a}
        ids_b = {e["id"] for e in ents_b}
        shared_ids = ids_a & ids_b

        # Keywords for each doc
        kw_a = conn.execute(
            """SELECT k.word, dk.count
               FROM keywords k JOIN document_keywords dk ON dk.keyword_id = k.id
               WHERE dk.document_id = ? ORDER BY dk.count DESC LIMIT 30""",
            (doc_a,),
        ).fetchall()
        kw_b = conn.execute(
            """SELECT k.word, dk.count
               FROM keywords k JOIN document_keywords dk ON dk.keyword_id = k.id
               WHERE dk.document_id = ? ORDER BY dk.count DESC LIMIT 30""",
            (doc_b,),
        ).fetchall()

        words_a = {k["word"] for k in kw_a}
        words_b = {k["word"] for k in kw_b}

    return {
        "doc_a": dict(da),
        "doc_b": dict(db),
        "entities_a": [dict(e) for e in ents_a],
        "entities_b": [dict(e) for e in ents_b],
        "shared_entity_count": len(shared_ids),
        "unique_a_count": len(ids_a - shared_ids),
        "unique_b_count": len(ids_b - shared_ids),
        "keywords_a": [dict(k) for k in kw_a],
        "keywords_b": [dict(k) for k in kw_b],
        "shared_keywords": list(words_a & words_b),
    }


# ═══════════════════════════════════════════


@router.get("/location-frequency")
def location_frequency():
    """Rank locations by mention frequency across the corpus."""
    with get_db() as conn:
        locations = conn.execute(
            """SELECT e.id, e.name,
                      COUNT(DISTINCT de.document_id) as doc_count,
                      SUM(de.count) as total_mentions,
                      GROUP_CONCAT(DISTINCT d.category) as categories,
                      GROUP_CONCAT(DISTINCT d.source) as sources
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               JOIN documents d ON d.id = de.document_id
               WHERE e.type = 'place'
               GROUP BY e.id
               ORDER BY doc_count DESC"""
        ).fetchall()

        # Co-location pairs (places that appear together)
        colocation = conn.execute(
            """SELECT e1.name as place_a, e2.name as place_b,
                      COUNT(DISTINCT de1.document_id) as shared_docs
               FROM document_entities de1
               JOIN document_entities de2 ON de2.document_id = de1.document_id
                 AND de2.entity_id > de1.entity_id
               JOIN entities e1 ON e1.id = de1.entity_id AND e1.type = 'place'
               JOIN entities e2 ON e2.id = de2.entity_id AND e2.type = 'place'
               GROUP BY de1.entity_id, de2.entity_id
               HAVING shared_docs >= 3
               ORDER BY shared_docs DESC LIMIT 20"""
        ).fetchall()

    return {
        "locations": [dict(loc) for loc in locations],
        "colocation_pairs": [dict(c) for c in colocation],
    }


# ═══════════════════════════════════════════


@router.get("/financial-profiles")
def financial_profiles(limit: int = Query(30)):
    """Per-entity financial risk assessment."""
    with get_db() as conn:
        # Entities linked to documents with financial indicators
        profiles = conn.execute(
            """SELECT e.id, e.name, e.type,
                      COUNT(DISTINCT fi.id) as indicator_count,
                      AVG(fi.risk_score) as avg_risk,
                      MAX(fi.risk_score) as max_risk,
                      GROUP_CONCAT(DISTINCT fi.indicator_type) as indicator_types,
                      COUNT(DISTINCT fi.document_id) as fin_doc_count
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               JOIN financial_indicators fi ON fi.document_id = de.document_id
               GROUP BY e.id
               HAVING indicator_count >= 2
               ORDER BY avg_risk DESC, indicator_count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

        # Overall financial summary
        summary = conn.execute(
            """SELECT indicator_type, COUNT(*) as count,
                      AVG(risk_score) as avg_risk,
                      MAX(risk_score) as max_risk
               FROM financial_indicators
               GROUP BY indicator_type
               ORDER BY avg_risk DESC"""
        ).fetchall()

    return {
        "profiles": [dict(p) for p in profiles],
        "summary": [dict(s) for s in summary],
    }


# ═══════════════════════════════════════════


@router.get("/category-distribution")
def category_distribution():
    """Visual category breakdown with temporal distribution."""
    with get_db() as conn:
        # Category counts
        categories = conn.execute(
            """SELECT category, COUNT(*) as count,
                      COALESCE(SUM(pages), 0) as total_pages,
                      COUNT(CASE WHEN flagged = 1 THEN 1 END) as flagged_count,
                      MIN(date) as earliest, MAX(date) as latest
               FROM documents
               GROUP BY category
               ORDER BY count DESC"""
        ).fetchall()

        # Per-category temporal distribution
        temporal = []
        all_years = set()
        for cat in categories:
            yearly = conn.execute(
                """SELECT SUBSTR(date, 1, 4) as year, COUNT(*) as count
                   FROM documents
                   WHERE category = ?
                     AND date IS NOT NULL AND date != ''
                   GROUP BY year ORDER BY year""",
                (cat["category"],),
            ).fetchall()
            by_year = {y["year"]: y["count"] for y in yearly}
            all_years.update(by_year.keys())
            temporal.append(
                {
                    "category": cat["category"],
                    "by_year": by_year,
                }
            )

        # Top entities per category
        cat_entities = []
        for cat in categories[:8]:
            top_ents = conn.execute(
                """SELECT e.name, e.type, SUM(de.count) as mentions
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   JOIN documents d ON d.id = de.document_id
                   WHERE d.category = ?
                   GROUP BY e.id
                   ORDER BY mentions DESC LIMIT 5""",
                (cat["category"],),
            ).fetchall()
            cat_entities.append(
                {
                    "category": cat["category"],
                    "entities": [dict(e) for e in top_ents],
                }
            )

    return {
        "categories": [dict(c) for c in categories],
        "temporal": temporal,
        "years": sorted(all_years),
        "category_entities": cat_entities,
    }


# ── Witness Overlap ──────────────────────────────────


@router.get("/witness-overlap")
def witness_overlap(min_shared: int = 2):
    """Entities that appear across multiple depositions/witness statements."""
    with get_db() as conn:
        # Find documents categorized as depositions or witness-related
        depo_docs = conn.execute(
            "SELECT id, title, filename FROM documents "
            "WHERE category IN ('deposition', 'witness', 'testimony') "
            "OR title LIKE '%deposition%' OR title LIKE '%testimony%' "
            "OR title LIKE '%witness%' "
            "ORDER BY title"
        ).fetchall()
        doc_ids = [d["id"] for d in depo_docs]
        if not doc_ids:
            return {"overlaps": [], "deposition_count": 0}

        placeholders = ",".join("?" * len(doc_ids))
        # Entities appearing in multiple deposition docs
        rows = conn.execute(
            f"SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count, "
            f"SUM(de.count) as total_mentions "
            f"FROM entities e "
            f"JOIN document_entities de ON de.entity_id = e.id "
            f"WHERE de.document_id IN ({placeholders}) "
            f"GROUP BY e.id HAVING doc_count >= ? "
            f"ORDER BY doc_count DESC, total_mentions DESC",
            doc_ids + [min_shared],
        ).fetchall()

        overlaps = []
        for r in rows:
            # Which specific depositions mention this entity
            docs = conn.execute(
                f"SELECT d.id, d.title, de.count FROM documents d "
                f"JOIN document_entities de ON de.document_id = d.id "
                f"WHERE de.entity_id = ? AND d.id IN ({placeholders}) "
                f"ORDER BY de.count DESC",
                [r["id"]] + doc_ids,
            ).fetchall()
            overlaps.append(
                {
                    "entity_id": r["id"],
                    "name": r["name"],
                    "type": r["type"],
                    "deposition_count": r["doc_count"],
                    "total_mentions": r["total_mentions"],
                    "depositions": [dict(d) for d in docs],
                }
            )

    return {"overlaps": overlaps, "deposition_count": len(depo_docs)}


# ── Document Age Analysis ───────────────────────────


@router.get("/document-age")
def document_age():
    """Analyze document age distribution."""
    with get_db() as conn:
        # Documents with dates
        dated = conn.execute(
            "SELECT id, title, filename, date, category, source, ingested_at "
            "FROM documents WHERE date IS NOT NULL AND date != '' "
            "ORDER BY date"
        ).fetchall()
        undated = conn.execute(
            "SELECT COUNT(*) as cnt FROM documents WHERE date IS NULL OR date = ''"
        ).fetchone()["cnt"]

    docs = [dict(d) for d in dated]

    # Decade distribution
    decades = {}
    for d in docs:
        year_str = d["date"][:4] if d["date"] and len(d["date"]) >= 4 else None
        if year_str and year_str.isdigit():
            decade = (int(year_str) // 10) * 10
            key = f"{decade}s"
            decades[key] = decades.get(key, 0) + 1

    # Oldest and newest
    oldest = docs[:10] if docs else []
    newest = docs[-10:][::-1] if docs else []

    # By year
    years = {}
    for d in docs:
        y = d["date"][:4] if d["date"] and len(d["date"]) >= 4 else "unknown"
        years[y] = years.get(y, 0) + 1

    return {
        "total_dated": len(docs),
        "total_undated": undated,
        "oldest": oldest,
        "newest": newest,
        "decades": decades,
        "by_year": dict(sorted(years.items())),
    }


# ── Entity Co-Appearances ───────────────────────────


@router.get("/entity-coappearances")
def entity_coappearances(limit: int = 50, entity_type: str = ""):
    """Entity pairs that co-appear most frequently across documents."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e1.type = ? AND e2.type = ?"
            params.extend([entity_type, entity_type])

        rows = conn.execute(
            f"SELECT e1.id as id_a, e1.name as name_a, e1.type as type_a, "
            f"e2.id as id_b, e2.name as name_b, e2.type as type_b, "
            f"COUNT(DISTINCT de1.document_id) as shared_docs, "
            f"SUM(de1.count + de2.count) as combined_mentions "
            f"FROM document_entities de1 "
            f"JOIN document_entities de2 ON de1.document_id = de2.document_id AND de1.entity_id < de2.entity_id "
            f"JOIN entities e1 ON e1.id = de1.entity_id "
            f"JOIN entities e2 ON e2.id = de2.entity_id "
            f"WHERE 1=1 {type_filter} "
            f"GROUP BY de1.entity_id, de2.entity_id "
            f"ORDER BY shared_docs DESC, combined_mentions DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()

    return {"pairs": [dict(r) for r in rows]}


# ── Unresolved Entities ─────────────────────────────


@router.get("/unresolved-entities")
def unresolved_entities(entity_type: str = ""):
    """Entities not yet resolved to a canonical form."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e.type = ?"
            params.append(entity_type)

        # Entities that have no entry in entity_resolutions as source
        rows = conn.execute(
            f"SELECT e.id, e.name, e.type, e.canonical, "
            f"COUNT(de.document_id) as doc_count, "
            f"COALESCE(SUM(de.count), 0) as total_mentions "
            f"FROM entities e "
            f"LEFT JOIN document_entities de ON de.entity_id = e.id "
            f"WHERE e.id NOT IN (SELECT source_entity_id FROM entity_resolutions) "
            f"AND e.id NOT IN (SELECT canonical_entity_id FROM entity_resolutions) "
            f"{type_filter} "
            f"GROUP BY e.id "
            f"ORDER BY doc_count DESC, total_mentions DESC",
            params,
        ).fetchall()

        # Also get resolved count
        resolved_count = conn.execute(
            "SELECT COUNT(DISTINCT source_entity_id) as cnt FROM entity_resolutions"
        ).fetchone()["cnt"]

    by_type = {}
    for r in rows:
        t = r["type"]
        by_type[t] = by_type.get(t, 0) + 1

    return {
        "unresolved": [dict(r) for r in rows],
        "total_unresolved": len(rows),
        "total_resolved": resolved_count,
        "by_type": by_type,
    }


# ── Document Completeness ───────────────────────────


@router.get("/document-completeness")
def document_completeness():
    """Score documents by metadata completeness."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, title, category, source, date, pages, raw_text, notes "
            "FROM documents ORDER BY id"
        ).fetchall()

    docs = []
    score_dist = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
    field_coverage = {
        "title": 0,
        "category": 0,
        "source": 0,
        "date": 0,
        "pages": 0,
        "raw_text": 0,
        "notes": 0,
    }
    total = len(rows)

    for r in rows:
        score = 0
        fields = {}
        for f in ["title", "category", "source", "date", "notes"]:
            val = r[f]
            has = val is not None and str(val).strip() != "" and val != "other"
            fields[f] = has
            if has:
                score += 1
                field_coverage[f] += 1
        # pages > 0
        has_pages = r["pages"] is not None and r["pages"] > 0
        fields["pages"] = has_pages
        if has_pages:
            score += 1
            field_coverage["pages"] += 1
        # raw_text
        has_text = r["raw_text"] is not None and len(r["raw_text"]) > 50
        fields["raw_text"] = has_text
        if has_text:
            score += 1
            field_coverage["raw_text"] += 1

        pct = round(score / 7 * 100)
        grade = (
            "excellent" if pct >= 85 else "good" if pct >= 60 else "fair" if pct >= 40 else "poor"
        )
        score_dist[grade] += 1

        docs.append(
            {
                "id": r["id"],
                "filename": r["filename"],
                "title": r["title"],
                "score": pct,
                "grade": grade,
                "fields": fields,
            }
        )

    # Sort by score ascending (worst first)
    docs.sort(key=lambda x: x["score"])
    avg = round(sum(d["score"] for d in docs) / total) if total else 0
    coverage = {k: round(v / total * 100) if total else 0 for k, v in field_coverage.items()}

    return {
        "documents": docs[:100],
        "total": total,
        "average_score": avg,
        "distribution": score_dist,
        "field_coverage": coverage,
    }


# ── Key Date Summary ────────────────────────────────


@router.get("/key-dates")
def key_dates(limit: int = 50):
    """Most significant dates across the investigation."""
    with get_db() as conn:
        # Aggregate events by date
        rows = conn.execute(
            "SELECT event_date, COUNT(*) as event_count, "
            "GROUP_CONCAT(DISTINCT context) as contexts "
            "FROM events "
            "WHERE event_date IS NOT NULL AND event_date != '' "
            "GROUP BY event_date "
            "ORDER BY event_count DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()

        dates = []
        for r in rows:
            # Get entities involved on this date
            ents = conn.execute(
                "SELECT DISTINCT e.name, e.type FROM entities e "
                "JOIN event_entities ee ON ee.entity_id = e.id "
                "JOIN events ev ON ev.id = ee.event_id "
                "WHERE ev.event_date = ? "
                "ORDER BY e.type, e.name LIMIT 15",
                (r["event_date"],),
            ).fetchall()
            # Get document count for this date
            doc_count = conn.execute(
                "SELECT COUNT(DISTINCT document_id) as cnt FROM events WHERE event_date = ?",
                (r["event_date"],),
            ).fetchone()["cnt"]

            dates.append(
                {
                    "date": r["event_date"],
                    "event_count": r["event_count"],
                    "document_count": doc_count,
                    "contexts": r["contexts"][:500] if r["contexts"] else "",
                    "entities": [dict(e) for e in ents],
                }
            )

        # Also get date range
        bounds = conn.execute(
            "SELECT MIN(event_date) as earliest, MAX(event_date) as latest "
            "FROM events WHERE event_date IS NOT NULL AND event_date != ''"
        ).fetchone()

        total_events = conn.execute("SELECT COUNT(*) as cnt FROM events").fetchone()["cnt"]

    return {
        "dates": dates,
        "earliest": bounds["earliest"] if bounds else None,
        "latest": bounds["latest"] if bounds else None,
        "total_events": total_events,
    }


# ── Alias Network ────────────────────────────────────


@router.get("/alias-network")
def alias_network(entity_type: str = ""):
    """Entity alias relationships."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e.type = ?"
            params.append(entity_type)

        rows = conn.execute(
            f"SELECT ea.id, ea.entity_id, ea.alias_name, e.name, e.type "
            f"FROM entity_aliases ea "
            f"JOIN entities e ON e.id = ea.entity_id "
            f"WHERE 1=1 {type_filter} "
            f"ORDER BY e.name, ea.alias_name",
            params,
        ).fetchall()

    # Group by entity
    entities = {}
    for r in rows:
        eid = r["entity_id"]
        if eid not in entities:
            entities[eid] = {
                "entity_id": eid,
                "name": r["name"],
                "type": r["type"],
                "aliases": [],
            }
        entities[eid]["aliases"].append({"alias_id": r["id"], "alias_name": r["alias_name"]})

    result = sorted(entities.values(), key=lambda x: len(x["aliases"]), reverse=True)
    return {
        "entities": result,
        "total_entities_with_aliases": len(result),
        "total_aliases": len(rows),
    }


# ── Document Length Analysis ─────────────────────────


@router.get("/document-length")
def document_length():
    """Analyze document sizes and page counts."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, title, pages, category, source, "
            "LENGTH(raw_text) as text_length "
            "FROM documents ORDER BY text_length DESC"
        ).fetchall()

    docs = [dict(r) for r in rows]
    total = len(docs)
    if not total:
        return {"documents": [], "stats": {}, "by_category": {}}

    lengths = [d["text_length"] or 0 for d in docs]
    pages = [d["pages"] or 0 for d in docs]

    stats = {
        "total_docs": total,
        "total_chars": sum(lengths),
        "total_pages": sum(pages),
        "avg_chars": round(sum(lengths) / total),
        "avg_pages": round(sum(pages) / total, 1),
        "max_chars": max(lengths),
        "min_chars": min(lengths),
        "max_pages": max(pages),
    }

    # By category
    by_cat = {}
    for d in docs:
        cat = d["category"] or "other"
        if cat not in by_cat:
            by_cat[cat] = {"count": 0, "total_chars": 0, "total_pages": 0}
        by_cat[cat]["count"] += 1
        by_cat[cat]["total_chars"] += d["text_length"] or 0
        by_cat[cat]["total_pages"] += d["pages"] or 0

    for cat in by_cat:
        c = by_cat[cat]
        c["avg_chars"] = round(c["total_chars"] / c["count"])
        c["avg_pages"] = round(c["total_pages"] / c["count"], 1)

    # Size buckets
    buckets = {"<1K": 0, "1K-10K": 0, "10K-50K": 0, "50K-100K": 0, ">100K": 0}
    for ln in lengths:
        if ln < 1000:
            buckets["<1K"] += 1
        elif ln < 10000:
            buckets["1K-10K"] += 1
        elif ln < 50000:
            buckets["10K-50K"] += 1
        elif ln < 100000:
            buckets["50K-100K"] += 1
        else:
            buckets[">100K"] += 1

    return {
        "documents": docs[:100],
        "stats": stats,
        "by_category": by_cat,
        "size_buckets": buckets,
    }


# ── Temporal Heatmap ─────────────────────────────────


@router.get("/temporal-heatmap")
def temporal_heatmap():
    """Events per month heatmap grid."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT event_date, COUNT(*) as cnt "
            "FROM events "
            "WHERE event_date IS NOT NULL AND event_date != '' "
            "AND LENGTH(event_date) >= 7 "
            "GROUP BY SUBSTR(event_date, 1, 7) "
            "ORDER BY event_date"
        ).fetchall()

    # Build year-month grid
    months = {}
    years = set()
    for r in rows:
        ym = r["event_date"][:7] if r["event_date"] else None
        if ym and len(ym) >= 7:
            months[ym] = r["cnt"]
            years.add(ym[:4])

    grid = []
    for year in sorted(years):
        row = {"year": year, "months": []}
        for m in range(1, 13):
            key = f"{year}-{m:02d}"
            row["months"].append({"month": m, "count": months.get(key, 0)})
        grid.append(row)

    max_count = max(months.values()) if months else 0
    return {
        "grid": grid,
        "max_count": max_count,
        "total_months": len(months),
        "years": sorted(years),
    }


# ── Entity Type Breakdown ───────────────────────────


@router.get("/entity-type-breakdown")
def entity_type_breakdown():
    """Detailed entity type analytics."""
    with get_db() as conn:
        # Counts by type
        type_counts = conn.execute(
            "SELECT type, COUNT(*) as cnt FROM entities GROUP BY type ORDER BY cnt DESC"
        ).fetchall()

        # Top entities per type
        types_detail = []
        for tc in type_counts:
            top = conn.execute(
                "SELECT e.id, e.name, COUNT(de.document_id) as doc_count, "
                "COALESCE(SUM(de.count), 0) as mentions "
                "FROM entities e "
                "LEFT JOIN document_entities de ON de.entity_id = e.id "
                "WHERE e.type = ? "
                "GROUP BY e.id "
                "ORDER BY doc_count DESC LIMIT 20",
                (tc["type"],),
            ).fetchall()
            types_detail.append(
                {
                    "type": tc["type"],
                    "count": tc["cnt"],
                    "top_entities": [dict(t) for t in top],
                }
            )

        # Entities with no document associations
        orphan_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM entities e "
            "WHERE NOT EXISTS (SELECT 1 FROM document_entities de WHERE de.entity_id = e.id)"
        ).fetchone()["cnt"]

        total = conn.execute("SELECT COUNT(*) as cnt FROM entities").fetchone()["cnt"]

    return {
        "types": types_detail,
        "total_entities": total,
        "orphan_entities": orphan_count,
    }


# ── Source Network ───────────────────────────────────


@router.get("/source-network")
def source_network(min_shared: int = 5):
    """Which sources share the most entities."""
    with get_db() as conn:
        # Get all sources
        sources = conn.execute(
            "SELECT DISTINCT source FROM documents WHERE source IS NOT NULL AND source != '' "
            "ORDER BY source"
        ).fetchall()
        source_names = [s["source"] for s in sources]

        # For each source pair, count shared entities
        pairs = []
        for i in range(len(source_names)):
            for j in range(i + 1, len(source_names)):
                shared = conn.execute(
                    "SELECT COUNT(DISTINCT de1.entity_id) as cnt "
                    "FROM document_entities de1 "
                    "JOIN documents d1 ON d1.id = de1.document_id "
                    "JOIN document_entities de2 ON de2.entity_id = de1.entity_id "
                    "JOIN documents d2 ON d2.id = de2.document_id "
                    "WHERE d1.source = ? AND d2.source = ?",
                    (source_names[i], source_names[j]),
                ).fetchone()["cnt"]
                if shared >= min_shared:
                    pairs.append(
                        {
                            "source_a": source_names[i],
                            "source_b": source_names[j],
                            "shared_entities": shared,
                        }
                    )

        # Entity counts per source
        source_stats = conn.execute(
            "SELECT d.source, COUNT(DISTINCT de.entity_id) as entity_count, "
            "COUNT(DISTINCT d.id) as doc_count "
            "FROM documents d "
            "JOIN document_entities de ON de.document_id = d.id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source "
            "ORDER BY entity_count DESC"
        ).fetchall()

    pairs.sort(key=lambda x: x["shared_entities"], reverse=True)
    return {
        "pairs": pairs,
        "sources": [dict(s) for s in source_stats],
        "total_sources": len(source_names),
    }


# ── Redaction Density ────────────────────────────────


@router.get("/entity-timeline-density")
def entity_timeline_density(limit: int = 50, entity_type: str = ""):
    """Entities ranked by how many distinct dates they appear on."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e.type = ?"
            params.append(entity_type)

        rows = conn.execute(
            f"SELECT e.id, e.name, e.type, "
            f"COUNT(DISTINCT ev.event_date) as unique_dates, "
            f"COUNT(ev.id) as total_events, "
            f"MIN(ev.event_date) as first_date, "
            f"MAX(ev.event_date) as last_date "
            f"FROM entities e "
            f"JOIN event_entities ee ON ee.entity_id = e.id "
            f"JOIN events ev ON ev.id = ee.event_id "
            f"WHERE ev.event_date IS NOT NULL AND ev.event_date != '' "
            f"{type_filter} "
            f"GROUP BY e.id "
            f"ORDER BY unique_dates DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()

    return {"entities": [dict(r) for r in rows]}


# ── Document Duplicates Finder ──────────────────────


@router.get("/document-duplicates")
def document_duplicates():
    """Find near-duplicate documents by title or hash similarity."""
    with get_db() as conn:
        # Exact hash duplicates
        hash_dupes = conn.execute(
            "SELECT file_hash, GROUP_CONCAT(id) as doc_ids, "
            "GROUP_CONCAT(filename, ' | ') as filenames, COUNT(*) as cnt "
            "FROM documents "
            "WHERE file_hash IS NOT NULL AND file_hash != '' "
            "GROUP BY file_hash HAVING cnt > 1 "
            "ORDER BY cnt DESC"
        ).fetchall()

        # Title duplicates (exact match)
        title_dupes = conn.execute(
            "SELECT title, GROUP_CONCAT(id) as doc_ids, "
            "GROUP_CONCAT(filename, ' | ') as filenames, COUNT(*) as cnt "
            "FROM documents "
            "WHERE title IS NOT NULL AND title != '' "
            "GROUP BY title HAVING cnt > 1 "
            "ORDER BY cnt DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

    return {
        "hash_duplicates": [dict(r) for r in hash_dupes],
        "title_duplicates": [dict(r) for r in title_dupes],
        "total_hash_dupes": sum(r["cnt"] for r in hash_dupes),
        "total_title_dupes": sum(r["cnt"] for r in title_dupes),
        "total_documents": total,
    }


# ── Connection Strength ─────────────────────────────


@router.get("/connection-strength")
def connection_strength(limit: int = 50, entity_type: str = ""):
    """Strongest entity-to-entity connections by weight."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND ea.type = ? AND eb.type = ?"
            params.extend([entity_type, entity_type])

        rows = conn.execute(
            f"SELECT ec.entity_a_id, ec.entity_b_id, ec.weight, "
            f"ea.name as name_a, ea.type as type_a, "
            f"eb.name as name_b, eb.type as type_b "
            f"FROM entity_connections ec "
            f"JOIN entities ea ON ea.id = ec.entity_a_id "
            f"JOIN entities eb ON eb.id = ec.entity_b_id "
            f"WHERE 1=1 {type_filter} "
            f"ORDER BY ec.weight DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()

    return {"connections": [dict(r) for r in rows]}


# ── Category Timeline ────────────────────────────────


@router.get("/category-timeline")
def category_timeline():
    """Document categories over time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, SUBSTR(date, 1, 7) as month, COUNT(*) as cnt "
            "FROM documents "
            "WHERE date IS NOT NULL AND date != '' AND LENGTH(date) >= 7 "
            "GROUP BY category, month "
            "ORDER BY month, category"
        ).fetchall()

    # Build timeline data
    months = {}
    categories = set()
    for r in rows:
        m = r["month"]
        cat = r["category"] or "other"
        categories.add(cat)
        if m not in months:
            months[m] = {}
        months[m][cat] = r["cnt"]

    timeline = []
    for m in sorted(months.keys()):
        entry = {"month": m}
        for cat in sorted(categories):
            entry[cat] = months[m].get(cat, 0)
        timeline.append(entry)

    return {
        "timeline": timeline,
        "categories": sorted(categories),
        "total_months": len(months),
    }


# ── Orphan Documents ────────────────────────────────


@router.get("/orphan-documents")
def orphan_documents():
    """Documents with no extracted entities."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.filename, d.title, d.category, d.source, d.date, "
            "d.pages, LENGTH(d.raw_text) as text_length "
            "FROM documents d "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM document_entities de WHERE de.document_id = d.id"
            ") "
            "ORDER BY d.id"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

    return {
        "orphans": [dict(r) for r in rows],
        "total_orphans": len(rows),
        "total_documents": total,
        "pct_orphans": round(len(rows) / total * 100, 1) if total else 0,
    }


# ── Entity First/Last Seen ──────────────────────────


@router.get("/entity-first-last")
def entity_first_last(limit: int = 100, entity_type: str = ""):
    """When each entity first and last appears in the corpus."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e.type = ?"
            params.append(entity_type)

        rows = conn.execute(
            f"SELECT e.id, e.name, e.type, "
            f"MIN(d.date) as first_seen, MAX(d.date) as last_seen, "
            f"COUNT(DISTINCT d.id) as doc_count "
            f"FROM entities e "
            f"JOIN document_entities de ON de.entity_id = e.id "
            f"JOIN documents d ON d.id = de.document_id "
            f"WHERE d.date IS NOT NULL AND d.date != '' "
            f"{type_filter} "
            f"GROUP BY e.id "
            f"ORDER BY doc_count DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()

    entities = []
    for r in rows:
        first = r["first_seen"] or ""
        last = r["last_seen"] or ""
        span_days = 0
        if first and last and len(first) >= 10 and len(last) >= 10:
            try:
                from datetime import datetime

                d1 = datetime.strptime(first[:10], "%Y-%m-%d")
                d2 = datetime.strptime(last[:10], "%Y-%m-%d")
                span_days = (d2 - d1).days
            except ValueError:
                span_days = 0
        entities.append(
            {
                "id": r["id"],
                "name": r["name"],
                "type": r["type"],
                "first_seen": first,
                "last_seen": last,
                "doc_count": r["doc_count"],
                "span_days": span_days,
            }
        )

    return {"entities": entities}


# ── Cross-Source Entities ────────────────────────────


@router.get("/cross-source-entities")
def cross_source_entities(min_sources: int = 2, entity_type: str = ""):
    """Entities that appear across multiple distinct sources."""
    with get_db() as conn:
        params: list = []
        type_filter = ""
        if entity_type:
            type_filter = "AND e.type = ?"
            params.append(entity_type)

        rows = conn.execute(
            f"SELECT e.id, e.name, e.type, "
            f"COUNT(DISTINCT d.source) as source_count, "
            f"COUNT(DISTINCT d.id) as doc_count, "
            f"COALESCE(SUM(de.count), 0) as total_mentions, "
            f"GROUP_CONCAT(DISTINCT d.source) as sources "
            f"FROM entities e "
            f"JOIN document_entities de ON de.entity_id = e.id "
            f"JOIN documents d ON d.id = de.document_id "
            f"WHERE d.source IS NOT NULL AND d.source != '' "
            f"{type_filter} "
            f"GROUP BY e.id HAVING source_count >= ? "
            f"ORDER BY source_count DESC, doc_count DESC "
            f"LIMIT 100",
            params + [min_sources],
        ).fetchall()

    return {"entities": [dict(r) for r in rows]}


# ── Page Count Distribution ──────────────────────────


@router.get("/page-distribution")
def page_distribution():
    """Documents grouped by page count ranges."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, title, pages, category, source FROM documents ORDER BY pages DESC"
        ).fetchall()

    total = len(rows)
    buckets = {"0": 0, "1-5": 0, "6-20": 0, "21-50": 0, "51-100": 0, "101-500": 0, ">500": 0}
    by_cat = {}

    for r in rows:
        p = r["pages"] or 0
        cat = r["category"] or "other"
        if cat not in by_cat:
            by_cat[cat] = {"count": 0, "total_pages": 0}
        by_cat[cat]["count"] += 1
        by_cat[cat]["total_pages"] += p

        if p == 0:
            buckets["0"] += 1
        elif p <= 5:
            buckets["1-5"] += 1
        elif p <= 20:
            buckets["6-20"] += 1
        elif p <= 50:
            buckets["21-50"] += 1
        elif p <= 100:
            buckets["51-100"] += 1
        elif p <= 500:
            buckets["101-500"] += 1
        else:
            buckets[">500"] += 1

    total_pages = sum((r["pages"] or 0) for r in rows)
    largest = [dict(r) for r in rows[:20]]

    return {
        "buckets": buckets,
        "by_category": by_cat,
        "total_documents": total,
        "total_pages": total_pages,
        "avg_pages": round(total_pages / total, 1) if total else 0,
        "largest": largest,
    }


# ── Entity Name Length ───────────────────────────────


@router.get("/entity-name-length")
def entity_name_length():
    """Entity name length analytics."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, type, LENGTH(name) as name_len FROM entities ORDER BY name_len DESC"
        ).fetchall()

    total = len(rows)
    if not total:
        return {"entities": [], "stats": {}, "buckets": {}}

    lengths = [r["name_len"] for r in rows]
    buckets = {"1-3": 0, "4-10": 0, "11-20": 0, "21-30": 0, ">30": 0}
    for ln in lengths:
        if ln <= 3:
            buckets["1-3"] += 1
        elif ln <= 10:
            buckets["4-10"] += 1
        elif ln <= 20:
            buckets["11-20"] += 1
        elif ln <= 30:
            buckets["21-30"] += 1
        else:
            buckets[">30"] += 1

    longest = [dict(r) for r in rows[:30]]
    shortest = [dict(r) for r in rows[-30:]][::-1]

    return {
        "longest": longest,
        "shortest": shortest,
        "stats": {
            "total": total,
            "avg_length": round(sum(lengths) / total, 1),
            "max_length": max(lengths),
            "min_length": min(lengths),
        },
        "buckets": buckets,
    }


# ── Document Ingest Timeline ────────────────────────


@router.get("/ingest-timeline")
def ingest_timeline():
    """When documents were ingested into the system."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(ingested_at, 1, 10) as day, COUNT(*) as cnt, "
            "GROUP_CONCAT(DISTINCT category) as categories "
            "FROM documents "
            "WHERE ingested_at IS NOT NULL "
            "GROUP BY day ORDER BY day"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

    timeline = [dict(r) for r in rows]
    return {
        "timeline": timeline,
        "total_documents": total,
        "ingest_days": len(timeline),
    }


# ── High-Value Targets ──────────────────────────────


@router.get("/high-value-targets")
def high_value_targets(limit: int = 50):
    """Entities scoring high across multiple metrics combined."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COUNT(DISTINCT de.document_id) as doc_count, "
            "COALESCE(SUM(de.count), 0) as total_mentions, "
            "COUNT(DISTINCT d.source) as source_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE e.type IN ('person', 'org') "
            "GROUP BY e.id "
            "ORDER BY doc_count DESC "
            "LIMIT 200",
            (),
        ).fetchall()

        # Get connection counts
        conn_counts = {}
        for r in rows:
            cnt = conn.execute(
                "SELECT COUNT(*) as cnt FROM entity_connections "
                "WHERE entity_a_id = ? OR entity_b_id = ?",
                (r["id"], r["id"]),
            ).fetchone()["cnt"]
            conn_counts[r["id"]] = cnt

        # Get event counts
        event_counts = {}
        for r in rows:
            cnt = conn.execute(
                "SELECT COUNT(*) as cnt FROM event_entities WHERE entity_id = ?",
                (r["id"],),
            ).fetchone()["cnt"]
            event_counts[r["id"]] = cnt

    targets = []
    for r in rows:
        conns = conn_counts.get(r["id"], 0)
        events = event_counts.get(r["id"], 0)
        score = (
            r["doc_count"] * 3
            + r["total_mentions"] * 0.01
            + r["source_count"] * 5
            + conns * 2
            + events * 0.5
        )
        targets.append(
            {
                "id": r["id"],
                "name": r["name"],
                "type": r["type"],
                "doc_count": r["doc_count"],
                "total_mentions": r["total_mentions"],
                "source_count": r["source_count"],
                "connections": conns,
                "events": events,
                "score": round(score, 1),
            }
        )

    targets.sort(key=lambda x: x["score"], reverse=True)
    return {"targets": targets[:limit]}


# ── Keyword Context ──────────────────────────────────


@router.get("/keyword-context")
def keyword_context(keyword: str = "", limit: int = 20):
    """Keywords shown with surrounding text snippets."""
    if not keyword.strip():
        return {"snippets": [], "keyword": ""}

    with get_db() as conn:
        kw = keyword.strip().lower()
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.raw_text "
            "FROM documents_fts fts "
            "JOIN documents d ON d.id = fts.rowid "
            "WHERE documents_fts MATCH ? "
            "LIMIT ?",
            (kw, limit),
        ).fetchall()

    snippets = []
    for r in rows:
        text = r["raw_text"] or ""
        lower_text = text.lower()
        idx = lower_text.find(kw)
        if idx >= 0:
            start = max(0, idx - 100)
            end = min(len(text), idx + len(kw) + 100)
            snippet = (
                ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
            )
        else:
            snippet = text[:200] + ("..." if len(text) > 200 else "")
        snippets.append(
            {
                "doc_id": r["id"],
                "title": r["title"] or r["filename"],
                "snippet": snippet,
            }
        )

    return {"snippets": snippets, "keyword": keyword, "total": len(snippets)}


# ── Entity Connections Map ───────────────────────────


@router.get("/entity-connections-map")
def entity_connections_map(entity_id: int = 0, limit: int = 50):
    """All direct connections for a selected entity."""
    if not entity_id:
        return {"entity": None, "connections": []}

    with get_db() as conn:
        entity = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not entity:
            return {"entity": None, "connections": []}

        rows = conn.execute(
            "SELECT ec.weight, "
            "CASE WHEN ec.entity_a_id = ? THEN ec.entity_b_id ELSE ec.entity_a_id END as other_id "
            "FROM entity_connections ec "
            "WHERE ec.entity_a_id = ? OR ec.entity_b_id = ? "
            "ORDER BY ec.weight DESC LIMIT ?",
            (entity_id, entity_id, entity_id, limit),
        ).fetchall()

        connections = []
        for r in rows:
            other = conn.execute(
                "SELECT id, name, type FROM entities WHERE id = ?", (r["other_id"],)
            ).fetchone()
            if other:
                connections.append(
                    {
                        "entity_id": other["id"],
                        "name": other["name"],
                        "type": other["type"],
                        "weight": r["weight"],
                    }
                )

    return {"entity": dict(entity), "connections": connections}


# ── Document Word Count ──────────────────────────────


@router.get("/document-word-count")
def document_word_count():
    """Word count stats across the corpus."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, title, category, source, raw_text FROM documents ORDER BY id"
        ).fetchall()

    docs = []
    total_words = 0
    by_cat = {}
    buckets = {"<100": 0, "100-500": 0, "500-2K": 0, "2K-10K": 0, "10K-50K": 0, ">50K": 0}

    for r in rows:
        text = r["raw_text"] or ""
        wc = len(text.split())
        total_words += wc
        cat = r["category"] or "other"
        if cat not in by_cat:
            by_cat[cat] = {"count": 0, "total_words": 0}
        by_cat[cat]["count"] += 1
        by_cat[cat]["total_words"] += wc

        if wc < 100:
            buckets["<100"] += 1
        elif wc < 500:
            buckets["100-500"] += 1
        elif wc < 2000:
            buckets["500-2K"] += 1
        elif wc < 10000:
            buckets["2K-10K"] += 1
        elif wc < 50000:
            buckets["10K-50K"] += 1
        else:
            buckets[">50K"] += 1

        docs.append(
            {
                "id": r["id"],
                "title": r["title"] or r["filename"],
                "category": cat,
                "word_count": wc,
            }
        )

    docs.sort(key=lambda x: x["word_count"], reverse=True)
    total = len(docs)

    return {
        "documents": docs[:50],
        "stats": {
            "total_docs": total,
            "total_words": total_words,
            "avg_words": round(total_words / total) if total else 0,
        },
        "buckets": buckets,
        "by_category": by_cat,
    }


# ── Mention Heatmap ──────────────────────────────────


@router.get("/mention-heatmap")
def mention_heatmap(limit_entities: int = 20, limit_docs: int = 30):
    """Entity mentions per document heatmap (top entities x top docs)."""
    with get_db() as conn:
        # Top entities by doc count
        top_ents = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count "
            "FROM entities e JOIN document_entities de ON de.entity_id = e.id "
            "WHERE e.type IN ('person', 'org') "
            "GROUP BY e.id ORDER BY doc_count DESC LIMIT ?",
            (limit_entities,),
        ).fetchall()

        # Top docs by entity count
        top_docs = conn.execute(
            "SELECT d.id, d.title, d.filename, COUNT(DISTINCT de.entity_id) as ent_count "
            "FROM documents d JOIN document_entities de ON de.document_id = d.id "
            "GROUP BY d.id ORDER BY ent_count DESC LIMIT ?",
            (limit_docs,),
        ).fetchall()

        ent_ids = [e["id"] for e in top_ents]
        doc_ids = [d["id"] for d in top_docs]

        if not ent_ids or not doc_ids:
            return {"entities": [], "documents": [], "matrix": []}

        ep = ",".join("?" * len(ent_ids))
        dp = ",".join("?" * len(doc_ids))
        cells = conn.execute(
            f"SELECT entity_id, document_id, count FROM document_entities "
            f"WHERE entity_id IN ({ep}) AND document_id IN ({dp})",
            ent_ids + doc_ids,
        ).fetchall()

    # Build matrix
    cell_map = {}
    for c in cells:
        cell_map[(c["entity_id"], c["document_id"])] = c["count"]

    matrix = []
    for e in top_ents:
        row = {"entity_id": e["id"], "name": e["name"], "type": e["type"], "cells": []}
        for d in top_docs:
            row["cells"].append(cell_map.get((e["id"], d["id"]), 0))
        matrix.append(row)

    return {
        "entities": [{"id": e["id"], "name": e["name"], "type": e["type"]} for e in top_ents],
        "documents": [
            {"id": d["id"], "title": (d["title"] or d["filename"])[:30]} for d in top_docs
        ],
        "matrix": matrix,
    }


# ── Source Quality ───────────────────────────────────


@router.get("/source-quality")
def source_quality():
    """Rate sources by avg metadata completeness of their docs."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, id, title, category, date, pages, raw_text, notes "
            "FROM documents WHERE source IS NOT NULL AND source != '' "
            "ORDER BY source"
        ).fetchall()

    sources = {}
    for r in rows:
        src = r["source"]
        if src not in sources:
            sources[src] = {"count": 0, "total_score": 0, "scores": []}

        score = 0
        for f in ["title", "category", "date", "notes"]:
            if r[f] and str(r[f]).strip() and r[f] != "other":
                score += 1
        if r["pages"] and r["pages"] > 0:
            score += 1
        if r["raw_text"] and len(r["raw_text"]) > 50:
            score += 1
        pct = round(score / 6 * 100)

        sources[src]["count"] += 1
        sources[src]["total_score"] += pct
        sources[src]["scores"].append(pct)

    result = []
    for src, data in sources.items():
        avg = round(data["total_score"] / data["count"]) if data["count"] else 0
        result.append(
            {
                "source": src,
                "doc_count": data["count"],
                "avg_completeness": avg,
                "min_completeness": min(data["scores"]) if data["scores"] else 0,
                "max_completeness": max(data["scores"]) if data["scores"] else 0,
            }
        )

    result.sort(key=lambda x: x["avg_completeness"])
    return {"sources": result}


# ── Event Density Calendar ───────────────────────────


@router.get("/event-calendar")
def event_calendar(year: str = ""):
    """Events per day in a calendar-like view."""
    with get_db() as conn:
        year_filter = ""
        params: list = []
        if year:
            year_filter = "AND event_date LIKE ?"
            params.append(f"{year}%")

        rows = conn.execute(
            f"SELECT event_date, COUNT(*) as cnt "
            f"FROM events "
            f"WHERE event_date IS NOT NULL AND event_date != '' "
            f"AND LENGTH(event_date) >= 10 "
            f"{year_filter} "
            f"GROUP BY event_date ORDER BY event_date",
            params,
        ).fetchall()

        years = conn.execute(
            "SELECT DISTINCT SUBSTR(event_date, 1, 4) as yr "
            "FROM events WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 10 "
            "ORDER BY yr"
        ).fetchall()

    days = [{"date": r["event_date"], "count": r["cnt"]} for r in rows]
    max_count = max(r["cnt"] for r in rows) if rows else 0

    return {
        "days": days,
        "max_count": max_count,
        "total_days": len(days),
        "available_years": [y["yr"] for y in years],
    }


# ── Entity Pair History ──────────────────────────────


@router.get("/entity-pair-history")
def entity_pair_history(entity_a: int = 0, entity_b: int = 0):
    """All documents where two entities co-occur."""
    if not entity_a or not entity_b:
        return {"entity_a": None, "entity_b": None, "documents": []}

    with get_db() as conn:
        ea = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_a,)
        ).fetchone()
        eb = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_b,)
        ).fetchone()

        if not ea or not eb:
            return {"entity_a": None, "entity_b": None, "documents": []}

        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.category, d.source, d.date, "
            "de1.count as count_a, de2.count as count_b "
            "FROM document_entities de1 "
            "JOIN document_entities de2 ON de1.document_id = de2.document_id "
            "JOIN documents d ON d.id = de1.document_id "
            "WHERE de1.entity_id = ? AND de2.entity_id = ? "
            "ORDER BY (de1.count + de2.count) DESC",
            (entity_a, entity_b),
        ).fetchall()

    return {
        "entity_a": dict(ea),
        "entity_b": dict(eb),
        "documents": [dict(r) for r in rows],
        "total": len(rows),
    }


# ── Financial Entity Links ───────────────────────────


@router.get("/financial-entity-links")
def financial_entity_links(limit: int = 50):
    """Entities connected to financial indicators."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COUNT(DISTINCT fi.id) as indicator_count, "
            "SUM(fi.risk_score) as total_risk, "
            "GROUP_CONCAT(DISTINCT fi.indicator_type) as indicator_types, "
            "COUNT(DISTINCT fi.document_id) as doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN financial_indicators fi ON fi.document_id = de.document_id "
            "GROUP BY e.id "
            "ORDER BY indicator_count DESC, total_risk DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()

    return {"entities": [dict(r) for r in rows]}


# ── Document Cluster by Source ───────────────────────


@router.get("/doc-source-cluster")
def doc_source_cluster():
    """Docs grouped by source with entity overlap stats."""
    with get_db() as conn:
        sources = conn.execute(
            "SELECT source, COUNT(*) as doc_count, "
            "SUM(pages) as total_pages "
            "FROM documents "
            "WHERE source IS NOT NULL AND source != '' "
            "GROUP BY source ORDER BY doc_count DESC"
        ).fetchall()

        clusters = []
        for s in sources:
            ent_count = conn.execute(
                "SELECT COUNT(DISTINCT de.entity_id) as cnt "
                "FROM document_entities de "
                "JOIN documents d ON d.id = de.document_id "
                "WHERE d.source = ?",
                (s["source"],),
            ).fetchone()["cnt"]

            top_ents = conn.execute(
                "SELECT e.name, e.type, SUM(de.count) as mentions "
                "FROM entities e "
                "JOIN document_entities de ON de.entity_id = e.id "
                "JOIN documents d ON d.id = de.document_id "
                "WHERE d.source = ? "
                "GROUP BY e.id ORDER BY mentions DESC LIMIT 5",
                (s["source"],),
            ).fetchall()

            cats = conn.execute(
                "SELECT category, COUNT(*) as cnt FROM documents "
                "WHERE source = ? GROUP BY category ORDER BY cnt DESC",
                (s["source"],),
            ).fetchall()

            clusters.append(
                {
                    "source": s["source"],
                    "doc_count": s["doc_count"],
                    "total_pages": s["total_pages"] or 0,
                    "unique_entities": ent_count,
                    "top_entities": [dict(e) for e in top_ents],
                    "categories": [dict(c) for c in cats],
                }
            )

    return {"clusters": clusters}


# ── Timeline Gaps ────────────────────────────────────


@router.get("/timeline-gaps")
def timeline_gaps(min_gap_days: int = 30):
    """Largest gaps in the event timeline."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT event_date FROM events "
            "WHERE event_date IS NOT NULL AND event_date != '' "
            "AND LENGTH(event_date) >= 10 "
            "ORDER BY event_date"
        ).fetchall()

    from datetime import datetime

    dates = []
    for r in rows:
        try:
            dates.append(datetime.strptime(r["event_date"][:10], "%Y-%m-%d"))
        except ValueError:
            continue

    gaps = []
    for i in range(1, len(dates)):
        delta = (dates[i] - dates[i - 1]).days
        if delta >= min_gap_days:
            gaps.append(
                {
                    "start": dates[i - 1].strftime("%Y-%m-%d"),
                    "end": dates[i].strftime("%Y-%m-%d"),
                    "gap_days": delta,
                }
            )

    gaps.sort(key=lambda x: x["gap_days"], reverse=True)
    return {
        "gaps": gaps,
        "total_gaps": len(gaps),
        "total_dates": len(dates),
        "date_range": {
            "start": dates[0].strftime("%Y-%m-%d") if dates else None,
            "end": dates[-1].strftime("%Y-%m-%d") if dates else None,
        },
    }


# ── Entity Degree Distribution ───────────────────────


@router.get("/entity-degree-distribution")
def entity_degree_distribution():
    """How many connections each entity has."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "(SELECT COUNT(*) FROM entity_connections ec "
            " WHERE ec.entity_a_id = e.id OR ec.entity_b_id = e.id) as degree "
            "FROM entities e "
            "WHERE (SELECT COUNT(*) FROM entity_connections ec "
            "  WHERE ec.entity_a_id = e.id OR ec.entity_b_id = e.id) > 0 "
            "ORDER BY degree DESC"
        ).fetchall()

    entities = [dict(r) for r in rows]
    degrees = [e["degree"] for e in entities]

    buckets = {"1": 0, "2-5": 0, "6-10": 0, "11-25": 0, "26-50": 0, ">50": 0}
    for d in degrees:
        if d == 1:
            buckets["1"] += 1
        elif d <= 5:
            buckets["2-5"] += 1
        elif d <= 10:
            buckets["6-10"] += 1
        elif d <= 25:
            buckets["11-25"] += 1
        elif d <= 50:
            buckets["26-50"] += 1
        else:
            buckets[">50"] += 1

    return {
        "entities": entities[:100],
        "total_connected": len(entities),
        "avg_degree": round(sum(degrees) / len(degrees), 1) if degrees else 0,
        "max_degree": max(degrees) if degrees else 0,
        "buckets": buckets,
    }


# ── Multi-Mention Docs ──────────────────────────────


@router.get("/multi-mention-docs")
def multi_mention_docs(limit: int = 50):
    """Documents with highest entity diversity."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.category, d.source, d.pages, "
            "COUNT(DISTINCT de.entity_id) as unique_entities, "
            "SUM(de.count) as total_mentions "
            "FROM documents d "
            "JOIN document_entities de ON de.document_id = d.id "
            "GROUP BY d.id "
            "ORDER BY unique_entities DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()

    return {"documents": [dict(r) for r in rows]}


# ── Flagged Summary ──────────────────────────────────


@router.get("/flagged-summary")
def flagged_summary():
    """Summary dashboard of all flagged documents."""
    with get_db() as conn:
        flagged = conn.execute(
            "SELECT d.id, d.title, d.filename, d.category, d.source, d.date, "
            "d.pages, d.notes, d.flagged "
            "FROM documents d WHERE d.flagged = 1 "
            "ORDER BY d.id"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

        # Entity counts in flagged docs
        if flagged:
            fids = [f["id"] for f in flagged]
            ph = ",".join("?" * len(fids))
            top_ents = conn.execute(
                f"SELECT e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count, "
                f"SUM(de.count) as mentions "
                f"FROM entities e "
                f"JOIN document_entities de ON de.entity_id = e.id "
                f"WHERE de.document_id IN ({ph}) "
                f"GROUP BY e.id ORDER BY doc_count DESC LIMIT 20",
                fids,
            ).fetchall()

            by_cat = conn.execute(
                f"SELECT category, COUNT(*) as cnt FROM documents "
                f"WHERE id IN ({ph}) GROUP BY category ORDER BY cnt DESC",
                fids,
            ).fetchall()
        else:
            top_ents = []
            by_cat = []

    return {
        "flagged": [dict(f) for f in flagged],
        "total_flagged": len(flagged),
        "total_documents": total,
        "pct_flagged": round(len(flagged) / total * 100, 1) if total else 0,
        "top_entities": [dict(e) for e in top_ents],
        "by_category": [dict(c) for c in by_cat],
    }


# ── Resolution Audit ──────────────────────────────


@router.get("/resolution-audit")
def resolution_audit():
    """Entity resolution decisions — source to canonical mapping."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT er.source_entity_id, er.canonical_entity_id, "
            "es.name as source_name, es.type as source_type, "
            "ec.name as canonical_name, ec.type as canonical_type "
            "FROM entity_resolutions er "
            "JOIN entities es ON es.id = er.source_entity_id "
            "JOIN entities ec ON ec.id = er.canonical_entity_id "
            "ORDER BY ec.name"
        ).fetchall()

    resolutions = [dict(r) for r in rows]

    by_type = {}
    for r in resolutions:
        t = r.get("source_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1

    return {
        "resolutions": resolutions[:200],
        "total": len(resolutions),
        "by_type": [
            {"type": k, "count": v}
            for k, v in sorted(by_type.items(), key=lambda x: x[1], reverse=True)
        ],
    }


# ── Document Shared Entities ─────────────────────


@router.get("/document-shared-entities")
def document_shared_entities():
    """Pairs of documents sharing the most entities."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT de1.document_id as doc_a, de2.document_id as doc_b, "
            "COUNT(*) as shared_count, "
            "d1.title as title_a, d1.filename as file_a, d1.category as cat_a, "
            "d2.title as title_b, d2.filename as file_b, d2.category as cat_b "
            "FROM document_entities de1 "
            "JOIN document_entities de2 ON de1.entity_id = de2.entity_id AND de1.document_id < de2.document_id "
            "JOIN documents d1 ON d1.id = de1.document_id "
            "JOIN documents d2 ON d2.id = de2.document_id "
            "GROUP BY de1.document_id, de2.document_id "
            "ORDER BY shared_count DESC "
            "LIMIT 100"
        ).fetchall()

    return {"pairs": [dict(r) for r in rows], "total": len(rows)}


# ── Source Date Range ────────────────────────────


@router.get("/source-date-range")
def source_date_range():
    """Earliest and latest document dates per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) as doc_count, "
            "MIN(date) as earliest, MAX(date) as latest, "
            "SUM(pages) as total_pages "
            "FROM documents "
            "WHERE source IS NOT NULL AND source != '' "
            "GROUP BY source "
            "ORDER BY doc_count DESC"
        ).fetchall()

    return {"sources": [dict(r) for r in rows], "total": len(rows)}


# ── Search History Stats ─────────────────────────


@router.get("/search-history-stats")
def search_history_stats():
    """Analysis of search queries from the search_history table."""
    with get_db() as conn:
        # Check if search_history table exists
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='search_history'"
        ).fetchone()
        if not table_check:
            return {"queries": [], "total": 0, "top_terms": [], "by_hour": []}

        rows = conn.execute(
            "SELECT query, COUNT(*) as cnt, MAX(searched_at) as last_searched "
            "FROM search_history "
            "GROUP BY query "
            "ORDER BY cnt DESC "
            "LIMIT 50"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as c FROM search_history").fetchone()["c"]

        by_hour = conn.execute(
            "SELECT CAST(strftime('%H', searched_at) AS INTEGER) as hour, COUNT(*) as cnt "
            "FROM search_history "
            "GROUP BY hour ORDER BY hour"
        ).fetchall()

    return {
        "queries": [dict(r) for r in rows],
        "total": total,
        "unique_queries": len(rows),
        "by_hour": [dict(h) for h in by_hour],
    }


# ── Category Entity Matrix ───────────────────────


@router.get("/category-entity-matrix")
def category_entity_matrix():
    """Entity type counts per document category."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.category, e.type, COUNT(DISTINCT e.id) as entity_count, "
            "SUM(de.count) as total_mentions "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "JOIN entities e ON e.id = de.entity_id "
            "WHERE d.category IS NOT NULL AND d.category != '' "
            "GROUP BY d.category, e.type "
            "ORDER BY d.category, entity_count DESC"
        ).fetchall()

    # Pivot into matrix
    matrix = {}
    entity_types = set()
    for r in rows:
        cat = r["category"]
        etype = r["type"]
        entity_types.add(etype)
        if cat not in matrix:
            matrix[cat] = {}
        matrix[cat][etype] = {"count": r["entity_count"], "mentions": r["total_mentions"]}

    categories = sorted(matrix.keys())
    entity_types = sorted(entity_types)

    return {
        "categories": categories,
        "entity_types": entity_types,
        "matrix": matrix,
        "raw": [dict(r) for r in rows],
    }


# ── Event Entity Ranking ────────────────────────


@router.get("/event-entity-ranking")
def event_entity_ranking():
    """Entities ranked by number of timeline events they appear in."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT ee.event_id) as event_count, "
            "MIN(ev.event_date) as first_event, MAX(ev.event_date) as last_event "
            "FROM event_entities ee "
            "JOIN entities e ON e.id = ee.entity_id "
            "JOIN events ev ON ev.id = ee.event_id "
            "GROUP BY e.id "
            "ORDER BY event_count DESC "
            "LIMIT 100"
        ).fetchall()

    entities = [dict(r) for r in rows]
    event_counts = [e["event_count"] for e in entities]

    return {
        "entities": entities,
        "total_ranked": len(entities),
        "avg_events": round(sum(event_counts) / len(event_counts), 1) if event_counts else 0,
        "max_events": max(event_counts) if event_counts else 0,
    }


# ── Entity Aliases List ──────────────────────────


@router.get("/entity-aliases-list")
def entity_aliases_list():
    """All entity aliases with canonical names."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.id, ea.entity_id, ea.alias_name as alias, e.name as canonical_name, e.type "
            "FROM entity_aliases ea "
            "JOIN entities e ON e.id = ea.entity_id "
            "ORDER BY e.name, ea.alias_name"
        ).fetchall()

    aliases = [dict(r) for r in rows]

    by_type = {}
    for a in aliases:
        t = a["type"]
        by_type[t] = by_type.get(t, 0) + 1

    return {
        "aliases": aliases[:300],
        "total": len(aliases),
        "by_type": [
            {"type": k, "count": v}
            for k, v in sorted(by_type.items(), key=lambda x: x[1], reverse=True)
        ],
    }


# ── Document Category Stats ────────────────────


@router.get("/category-stats")
def category_stats():
    """Detailed statistics per document category."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.category, COUNT(*) as doc_count, "
            "SUM(d.pages) as total_pages, "
            "COUNT(DISTINCT de.entity_id) as unique_entities, "
            "SUM(de.count) as total_mentions, "
            "COUNT(DISTINCT d.source) as source_count "
            "FROM documents d "
            "LEFT JOIN document_entities de ON de.document_id = d.id "
            "WHERE d.category IS NOT NULL AND d.category != '' "
            "GROUP BY d.category "
            "ORDER BY doc_count DESC"
        ).fetchall()

    categories = [dict(r) for r in rows]
    total_docs = sum(c["doc_count"] for c in categories)

    for c in categories:
        c["pct"] = round(c["doc_count"] / total_docs * 100, 1) if total_docs else 0

    return {"categories": categories, "total_categories": len(categories), "total_docs": total_docs}


# ── Redaction by Source ────────────────────────


@router.get("/entity-pair-codocs")
def entity_pair_codocs():
    """Top entity pairs and how many documents they co-appear in."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e1.name as entity_a, e1.type as type_a, "
            "e2.name as entity_b, e2.type as type_b, "
            "COUNT(DISTINCT de1.document_id) as shared_docs "
            "FROM document_entities de1 "
            "JOIN document_entities de2 ON de1.document_id = de2.document_id "
            "  AND de1.entity_id < de2.entity_id "
            "JOIN entities e1 ON e1.id = de1.entity_id "
            "JOIN entities e2 ON e2.id = de2.entity_id "
            "GROUP BY de1.entity_id, de2.entity_id "
            "ORDER BY shared_docs DESC "
            "LIMIT 100"
        ).fetchall()

    return {"pairs": [dict(r) for r in rows], "total": len(rows)}


# ── Timeline Event Types ──────────────────────


@router.get("/event-types")
def event_types():
    """Event date precision and confidence breakdown."""
    with get_db() as conn:
        by_precision = conn.execute(
            "SELECT precision, COUNT(*) as cnt FROM events GROUP BY precision ORDER BY cnt DESC"
        ).fetchall()

        by_confidence = conn.execute(
            "SELECT CASE "
            "  WHEN confidence >= 0.9 THEN 'high (>=0.9)' "
            "  WHEN confidence >= 0.7 THEN 'medium (0.7-0.9)' "
            "  WHEN confidence >= 0.5 THEN 'low (0.5-0.7)' "
            "  ELSE 'very low (<0.5)' "
            "END as bucket, COUNT(*) as cnt "
            "FROM events GROUP BY bucket ORDER BY cnt DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as c FROM events").fetchone()["c"]
        resolved = conn.execute(
            "SELECT COUNT(*) as c FROM events WHERE is_resolved = 1"
        ).fetchone()["c"]

    return {
        "total_events": total,
        "resolved": resolved,
        "unresolved": total - resolved,
        "by_precision": [dict(r) for r in by_precision],
        "by_confidence": [dict(r) for r in by_confidence],
    }


# ── Financial Indicator Summary ───────────────


@router.get("/financial-summary")
def financial_summary():
    """Financial indicator types and risk distribution."""
    with get_db() as conn:
        by_type = conn.execute(
            "SELECT indicator_type, COUNT(*) as cnt, "
            "ROUND(AVG(risk_score), 2) as avg_risk, "
            "ROUND(MAX(risk_score), 2) as max_risk "
            "FROM financial_indicators "
            "GROUP BY indicator_type ORDER BY cnt DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as c FROM financial_indicators").fetchone()["c"]

        top_risk = conn.execute(
            "SELECT fi.id, fi.indicator_type, fi.value, fi.risk_score, "
            "d.title, d.filename, d.id as doc_id "
            "FROM financial_indicators fi "
            "JOIN documents d ON d.id = fi.document_id "
            "ORDER BY fi.risk_score DESC LIMIT 50"
        ).fetchall()

    return {
        "total": total,
        "by_type": [dict(r) for r in by_type],
        "top_risk": [dict(r) for r in top_risk],
    }


# ── Entity Document Count ────────────────────────


@router.get("/entity-document-count")
def entity_document_count():
    """Entities ranked by number of documents they appear in."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count, "
            "SUM(de.count) as total_mentions "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "GROUP BY e.id "
            "ORDER BY doc_count DESC "
            "LIMIT 100"
        ).fetchall()

    entities = [dict(r) for r in rows]
    doc_counts = [e["doc_count"] for e in entities]

    return {
        "entities": entities,
        "total": len(entities),
        "avg_docs": round(sum(doc_counts) / len(doc_counts), 1) if doc_counts else 0,
        "max_docs": max(doc_counts) if doc_counts else 0,
    }


# ── Source Overlap ───────────────────────────────


@router.get("/source-overlap")
def source_overlap():
    """Sources sharing the most entities in common."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d1.source as source_a, d2.source as source_b, "
            "COUNT(DISTINCT de1.entity_id) as shared_entities "
            "FROM document_entities de1 "
            "JOIN documents d1 ON d1.id = de1.document_id "
            "JOIN document_entities de2 ON de1.entity_id = de2.entity_id AND de1.document_id != de2.document_id "
            "JOIN documents d2 ON d2.id = de2.document_id "
            "WHERE d1.source IS NOT NULL AND d2.source IS NOT NULL "
            "AND d1.source < d2.source "
            "GROUP BY d1.source, d2.source "
            "ORDER BY shared_entities DESC "
            "LIMIT 50"
        ).fetchall()

    return {"pairs": [dict(r) for r in rows], "total": len(rows)}


# ── Event Context Cloud ──────────────────────────


@router.get("/event-context")
def event_context():
    """Most common context snippets in timeline events."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT context, COUNT(*) as cnt "
            "FROM events "
            "WHERE context IS NOT NULL AND context != '' "
            "GROUP BY context "
            "ORDER BY cnt DESC "
            "LIMIT 100"
        ).fetchall()

        total = conn.execute(
            "SELECT COUNT(*) as c FROM events WHERE context IS NOT NULL AND context != ''"
        ).fetchone()["c"]

    return {"contexts": [dict(r) for r in rows], "total": total}


# ── Document Date Clusters ───────────────────────


@router.get("/document-date-clusters")
def document_date_clusters():
    """Documents grouped by year."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(date, 1, 4) as year, COUNT(*) as doc_count, "
            "SUM(pages) as total_pages, "
            "COUNT(DISTINCT source) as source_count "
            "FROM documents "
            "WHERE date IS NOT NULL AND date != '' AND LENGTH(date) >= 4 "
            "GROUP BY year "
            "ORDER BY year"
        ).fetchall()

    return {"clusters": [dict(r) for r in rows], "total": len(rows)}


# ── Redaction Patterns ───────────────────────────


@router.get("/entity-isolation")
def entity_isolation():
    """Entities with fewest connections (isolated nodes)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COALESCE(conn_count, 0) as connections, "
            "COALESCE(doc_count, 0) as documents "
            "FROM entities e "
            "LEFT JOIN ("
            "  SELECT entity_id, COUNT(DISTINCT document_id) as doc_count "
            "  FROM document_entities GROUP BY entity_id"
            ") de ON de.entity_id = e.id "
            "LEFT JOIN ("
            "  SELECT entity_id, COUNT(*) as conn_count FROM ("
            "    SELECT entity_a_id as entity_id FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id as entity_id FROM entity_connections"
            "  ) GROUP BY entity_id"
            ") ec ON ec.entity_id = e.id "
            "WHERE COALESCE(conn_count, 0) <= 2 "
            "ORDER BY connections ASC, documents DESC "
            "LIMIT 100"
        ).fetchall()

    entities = [dict(r) for r in rows]
    zero_conn = sum(1 for e in entities if e["connections"] == 0)

    return {
        "entities": entities,
        "total": len(entities),
        "zero_connections": zero_conn,
    }


# ── Entity Growth Rate ───────────────────────────


@router.get("/entity-growth")
def entity_growth():
    """New entities discovered over time by document ingest date."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(d.ingested_at) as ingest_date, "
            "COUNT(DISTINCT de.entity_id) as new_entities, "
            "COUNT(DISTINCT d.id) as docs_ingested "
            "FROM documents d "
            "JOIN document_entities de ON de.document_id = d.id "
            "WHERE d.ingested_at IS NOT NULL "
            "GROUP BY ingest_date "
            "ORDER BY ingest_date"
        ).fetchall()

    return {"timeline": [dict(r) for r in rows], "total_dates": len(rows)}


# ── Document Text Length Distribution ────────────


@router.get("/text-length-distribution")
def text_length_distribution():
    """Raw text length distribution across documents."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.category, d.source, "
            "LENGTH(d.raw_text) as text_length "
            "FROM documents d "
            "WHERE d.raw_text IS NOT NULL "
            "ORDER BY text_length DESC"
        ).fetchall()

    docs = [dict(r) for r in rows]
    lengths = [d["text_length"] for d in docs]

    buckets = {"<1K": 0, "1K-5K": 0, "5K-20K": 0, "20K-100K": 0, ">100K": 0}
    for tl in lengths:
        if tl < 1000:
            buckets["<1K"] += 1
        elif tl < 5000:
            buckets["1K-5K"] += 1
        elif tl < 20000:
            buckets["5K-20K"] += 1
        elif tl < 100000:
            buckets["20K-100K"] += 1
        else:
            buckets[">100K"] += 1

    return {
        "documents": docs[:100],
        "total": len(docs),
        "avg_length": round(sum(lengths) / len(lengths)) if lengths else 0,
        "max_length": max(lengths) if lengths else 0,
        "buckets": buckets,
    }


# ── Source Entity Density ────────────────────────


@router.get("/source-entity-density")
def source_entity_density():
    """Average entities per document by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, COUNT(DISTINCT d.id) as doc_count, "
            "COUNT(DISTINCT de.entity_id) as total_entities, "
            "ROUND(CAST(COUNT(DISTINCT de.entity_id) AS REAL) / COUNT(DISTINCT d.id), 1) as entities_per_doc "
            "FROM documents d "
            "LEFT JOIN document_entities de ON de.document_id = d.id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source "
            "ORDER BY entities_per_doc DESC"
        ).fetchall()

    return {"sources": [dict(r) for r in rows], "total": len(rows)}


# ── Event Timeline Heatmap ───────────────────────


@router.get("/event-heatmap")
def event_heatmap():
    """Events by year-month grid."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(event_date, 1, 4) as year, "
            "SUBSTR(event_date, 6, 2) as month, "
            "COUNT(*) as cnt "
            "FROM events "
            "WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 7 "
            "AND CAST(SUBSTR(event_date, 1, 4) AS INTEGER) BETWEEN 1950 AND 2030 "
            "GROUP BY year, month "
            "ORDER BY year, month"
        ).fetchall()

    return {"cells": [dict(r) for r in rows], "total": len(rows)}


# ── Connection Weight Distribution ───────────────


@router.get("/connection-weight-distribution")
def connection_weight_distribution():
    """Edge weight histogram for entity connections."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT weight, COUNT(*) as cnt FROM entity_connections GROUP BY weight ORDER BY weight"
        ).fetchall()

        stats = conn.execute(
            "SELECT COUNT(*) as total, ROUND(AVG(weight), 2) as avg_weight, "
            "MAX(weight) as max_weight, MIN(weight) as min_weight "
            "FROM entity_connections"
        ).fetchone()

    return {
        "distribution": [dict(r) for r in rows],
        "total": stats["total"],
        "avg_weight": stats["avg_weight"],
        "max_weight": stats["max_weight"],
        "min_weight": stats["min_weight"],
    }


# ── Multi-Source Entities ────────────────────────


@router.get("/multi-source-entities")
def multi_source_entities():
    """Entities appearing in 3+ different sources."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COUNT(DISTINCT d.source) as source_count, "
            "COUNT(DISTINCT d.id) as doc_count, "
            "GROUP_CONCAT(DISTINCT d.source) as sources "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY e.id "
            "HAVING source_count >= 3 "
            "ORDER BY source_count DESC, doc_count DESC "
            "LIMIT 100"
        ).fetchall()

    return {"entities": [dict(r) for r in rows], "total": len(rows)}


# ── Document Hash Audit ──────────────────────────


@router.get("/hash-audit")
def hash_audit():
    """Documents missing file hashes or with duplicate hashes."""
    with get_db() as conn:
        missing = conn.execute(
            "SELECT id, filename, title, category, source "
            "FROM documents WHERE file_hash IS NULL OR file_hash = ''"
        ).fetchall()

        dupes = conn.execute(
            "SELECT file_hash, COUNT(*) as cnt, "
            "GROUP_CONCAT(id) as doc_ids, "
            "GROUP_CONCAT(filename, ' | ') as filenames "
            "FROM documents "
            "WHERE file_hash IS NOT NULL AND file_hash != '' "
            "GROUP BY file_hash HAVING cnt > 1 "
            "ORDER BY cnt DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]
        hashed = conn.execute(
            "SELECT COUNT(*) as c FROM documents WHERE file_hash IS NOT NULL AND file_hash != ''"
        ).fetchone()["c"]

    return {
        "total_docs": total,
        "hashed": hashed,
        "missing_hash": [dict(r) for r in missing],
        "missing_count": len(missing),
        "duplicates": [dict(r) for r in dupes],
        "duplicate_groups": len(dupes),
    }


# ── Entity Canonical Coverage ────────────────────


@router.get("/canonical-coverage")
def canonical_coverage():
    """How many entities have canonical names set."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as c FROM entities").fetchone()["c"]
        with_canonical = conn.execute(
            "SELECT COUNT(*) as c FROM entities WHERE canonical IS NOT NULL AND canonical != ''"
        ).fetchone()["c"]

        by_type = conn.execute(
            "SELECT type, COUNT(*) as total, "
            "SUM(CASE WHEN canonical IS NOT NULL AND canonical != '' THEN 1 ELSE 0 END) as with_canonical "
            "FROM entities GROUP BY type ORDER BY total DESC"
        ).fetchall()

    return {
        "total": total,
        "with_canonical": with_canonical,
        "without_canonical": total - with_canonical,
        "coverage_pct": round(with_canonical / total * 100, 1) if total else 0,
        "by_type": [dict(r) for r in by_type],
    }


# ── FTS5 Index Stats ────────────────────────────


@router.get("/fts-stats")
def fts_stats():
    """Full-text search index statistics."""
    with get_db() as conn:
        indexed = conn.execute("SELECT COUNT(*) as c FROM documents_fts").fetchone()["c"]

        total = conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]

        # Sample recent search terms from FTS queries
        with_text = conn.execute(
            "SELECT COUNT(*) as c FROM documents WHERE raw_text IS NOT NULL AND raw_text != ''"
        ).fetchone()["c"]

        by_category = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM documents "
            "WHERE raw_text IS NOT NULL AND raw_text != '' "
            "GROUP BY category ORDER BY cnt DESC"
        ).fetchall()

    return {
        "indexed_docs": indexed,
        "total_docs": total,
        "with_text": with_text,
        "coverage_pct": round(indexed / total * 100, 1) if total else 0,
        "by_category": [dict(r) for r in by_category],
    }


# ── Event Resolution Rate ───────────────────────


@router.get("/event-resolution-rate")
def event_resolution_rate():
    """Resolved vs unresolved events by document source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, "
            "COUNT(*) as total_events, "
            "SUM(CASE WHEN ev.is_resolved = 1 THEN 1 ELSE 0 END) as resolved, "
            "SUM(CASE WHEN ev.is_resolved = 0 THEN 1 ELSE 0 END) as unresolved "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source "
            "ORDER BY total_events DESC"
        ).fetchall()

    sources = [dict(r) for r in rows]
    for s in sources:
        s["resolution_rate"] = (
            round(s["resolved"] / s["total_events"] * 100, 1) if s["total_events"] else 0
        )

    return {"sources": sources, "total": len(sources)}


# ── Top Entity Connections ──────────────────────


@router.get("/top-connections")
def top_connections():
    """Highest-weight entity connections."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ec.entity_a_id, ec.entity_b_id, ec.weight, "
            "ea.name as name_a, ea.type as type_a, "
            "eb.name as name_b, eb.type as type_b "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "ORDER BY ec.weight DESC "
            "LIMIT 100"
        ).fetchall()

    return {"connections": [dict(r) for r in rows], "total": len(rows)}


# ── Document Notes Summary ──────────────────────


@router.get("/document-notes")
def document_notes():
    """Documents with notes/annotations."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, title, filename, category, source, notes "
            "FROM documents "
            "WHERE notes IS NOT NULL AND notes != '' "
            "ORDER BY id DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]

    return {
        "documents": [dict(r) for r in rows],
        "with_notes": len(rows),
        "total_docs": total,
        "pct_noted": round(len(rows) / total * 100, 1) if total else 0,
    }


# ── Entity Name Duplicates ───────────────────────


@router.get("/entity-name-duplicates")
def entity_name_duplicates():
    """Entities with identical names but different types."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e1.id as id_a, e1.name, e1.type as type_a, "
            "e2.id as id_b, e2.type as type_b "
            "FROM entities e1 "
            "JOIN entities e2 ON e1.name = e2.name AND e1.id < e2.id AND e1.type != e2.type "
            "ORDER BY e1.name "
            "LIMIT 100"
        ).fetchall()

    return {"duplicates": [dict(r) for r in rows], "total": len(rows)}


# ── Document Ingest Velocity ────────────────────


@router.get("/ingest-velocity")
def ingest_velocity():
    """Documents ingested per hour."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT strftime('%Y-%m-%d %H:00', ingested_at) as hour_bucket, "
            "COUNT(*) as doc_count, SUM(pages) as total_pages "
            "FROM documents "
            "WHERE ingested_at IS NOT NULL "
            "GROUP BY hour_bucket ORDER BY hour_bucket"
        ).fetchall()

    buckets = [dict(r) for r in rows]
    counts = [b["doc_count"] for b in buckets]

    return {
        "buckets": buckets,
        "total_hours": len(buckets),
        "avg_per_hour": round(sum(counts) / len(counts), 1) if counts else 0,
        "max_per_hour": max(counts) if counts else 0,
    }


# ── Event Confidence Ranking ────────────────────


@router.get("/event-confidence-ranking")
def event_confidence_ranking():
    """Lowest-confidence events needing review."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ev.id, ev.event_date, ev.date_raw, ev.precision, ev.confidence, "
            "ev.context, d.id as doc_id, d.title, d.filename "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE ev.confidence IS NOT NULL "
            "ORDER BY ev.confidence ASC "
            "LIMIT 100"
        ).fetchall()

    return {"events": [dict(r) for r in rows], "total": len(rows)}


# ── Source Page Distribution ────────────────────


@router.get("/source-page-distribution")
def source_page_distribution():
    """Page count statistics per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) as doc_count, "
            "SUM(pages) as total_pages, "
            "ROUND(AVG(pages), 1) as avg_pages, "
            "MIN(pages) as min_pages, MAX(pages) as max_pages "
            "FROM documents "
            "WHERE source IS NOT NULL AND source != '' "
            "GROUP BY source ORDER BY total_pages DESC"
        ).fetchall()

    return {"sources": [dict(r) for r in rows], "total": len(rows)}


# ── Entity Type Ratio ───────────────────────────


@router.get("/entity-type-ratio")
def entity_type_ratio():
    """Entity type ratios per document source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, e.type, COUNT(DISTINCT e.id) as entity_count "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "JOIN entities e ON e.id = de.entity_id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source, e.type "
            "ORDER BY d.source, entity_count DESC"
        ).fetchall()

    matrix = {}
    types = set()
    for r in rows:
        src = r["source"]
        t = r["type"]
        types.add(t)
        if src not in matrix:
            matrix[src] = {}
        matrix[src][t] = r["entity_count"]

    sources = sorted(matrix.keys())
    types = sorted(types)

    return {"sources": sources, "types": types, "matrix": matrix}


# ── Connection Density by Type ──────────────────


@router.get("/connection-density")
def connection_density():
    """Connection counts between entity type pairs."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.type as type_a, eb.type as type_b, "
            "COUNT(*) as connection_count, SUM(ec.weight) as total_weight "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "GROUP BY ea.type, eb.type "
            "ORDER BY connection_count DESC"
        ).fetchall()

    return {"pairs": [dict(r) for r in rows], "total": len(rows)}


# ── Round 27 ──────────────────────────────────


@router.get("/document-readability")
def document_readability():
    """Text statistics per document — word count, avg word length."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, LENGTH(d.raw_text) AS char_count "
            "FROM documents d WHERE d.raw_text IS NOT NULL AND LENGTH(d.raw_text) > 0 "
            "ORDER BY char_count DESC LIMIT 100"
        ).fetchall()
    results = []
    for r in rows:
        text = r["char_count"]
        results.append(
            {
                "id": r["id"],
                "title": r["title"] or r["filename"],
                "char_count": text,
            }
        )
    # Also get aggregate stats
    with get_db() as conn:
        stats = conn.execute(
            "SELECT COUNT(*) as total, "
            "AVG(LENGTH(raw_text)) as avg_chars, "
            "MAX(LENGTH(raw_text)) as max_chars, "
            "MIN(LENGTH(raw_text)) as min_chars "
            "FROM documents WHERE raw_text IS NOT NULL AND LENGTH(raw_text) > 0"
        ).fetchone()
    return {
        "documents": results,
        "stats": {
            "total_with_text": stats["total"],
            "avg_chars": round(stats["avg_chars"] or 0, 1),
            "max_chars": stats["max_chars"] or 0,
            "min_chars": stats["min_chars"] or 0,
        },
    }


@router.get("/source-completeness")
def source_completeness():
    """Per-source coverage: docs, entities, events, pages."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, "
            "COUNT(DISTINCT d.id) AS doc_count, "
            "COUNT(DISTINCT e.id) AS entity_count, "
            "COUNT(DISTINCT ev.id) AS event_count, "
            "COALESCE(SUM(d.pages), 0) AS total_pages "
            "FROM documents d "
            "LEFT JOIN document_entities de ON de.document_id = d.id "
            "LEFT JOIN entities e ON e.id = de.entity_id "
            "LEFT JOIN events ev ON ev.document_id = d.id "
            "GROUP BY d.source ORDER BY doc_count DESC"
        ).fetchall()
    sources = []
    for r in rows:
        has_entities = r["entity_count"] > 0
        has_events = r["event_count"] > 0
        has_pages = r["total_pages"] > 0
        score = sum([has_entities, has_events, has_pages])
        sources.append(
            {
                "source": r["source"],
                "doc_count": r["doc_count"],
                "entity_count": r["entity_count"],
                "event_count": r["event_count"],
                "total_pages": r["total_pages"],
                "has_entities": has_entities,
                "has_events": has_events,
                "has_pages": has_pages,
                "completeness_score": score,
            }
        )
    return {"sources": sources}


@router.get("/orphan-events")
def orphan_events():
    """Events not linked to any entity via their document."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ev.id, ev.event_date, ev.date_raw, ev.precision, "
            "ev.confidence, ev.context, d.id AS doc_id, d.title, d.filename "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE ev.document_id NOT IN ("
            "  SELECT DISTINCT document_id FROM document_entities"
            ") "
            "ORDER BY ev.event_date LIMIT 200"
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM events "
            "WHERE document_id NOT IN (SELECT DISTINCT document_id FROM document_entities)"
        ).fetchone()["cnt"]
    return {
        "events": [dict(r) for r in rows],
        "total": total,
    }


@router.get("/entity-first-seen")
def entity_first_seen():
    """When each entity was first seen (earliest document created_at)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, MIN(d.ingested_at) AS first_seen, "
            "COUNT(DISTINCT d.id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id ORDER BY first_seen DESC LIMIT 200"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/page-density")
def page_density():
    """Entity density per page across documents."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.pages, "
            "COUNT(DISTINCT de.entity_id) AS entity_count, "
            "CASE WHEN d.pages > 0 "
            "  THEN ROUND(CAST(COUNT(DISTINCT de.entity_id) AS REAL) / d.pages, 2) "
            "  ELSE 0 END AS density "
            "FROM documents d "
            "LEFT JOIN document_entities de ON de.document_id = d.id "
            "WHERE d.pages > 0 "
            "GROUP BY d.id ORDER BY density DESC LIMIT 100"
        ).fetchall()
        avg = conn.execute(
            "SELECT AVG(sub.density) AS avg_density FROM ("
            "  SELECT CASE WHEN d.pages > 0 "
            "    THEN CAST(COUNT(DISTINCT de.entity_id) AS REAL) / d.pages "
            "    ELSE 0 END AS density "
            "  FROM documents d "
            "  LEFT JOIN document_entities de ON de.document_id = d.id "
            "  WHERE d.pages > 0 "
            "  GROUP BY d.id"
            ") sub"
        ).fetchone()
    return {
        "documents": [dict(r) for r in rows],
        "avg_density": round(avg["avg_density"] or 0, 2),
    }


@router.get("/duplicate-documents")
def duplicate_documents():
    """Documents sharing the same content hash."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.file_hash, COUNT(*) AS count, "
            "GROUP_CONCAT(d.id, ',') AS doc_ids, "
            "GROUP_CONCAT(COALESCE(d.title, d.filename), ' | ') AS titles "
            "FROM documents d "
            "WHERE d.file_hash IS NOT NULL "
            "GROUP BY d.file_hash HAVING COUNT(*) > 1 "
            "ORDER BY count DESC LIMIT 50"
        ).fetchall()
    return {
        "duplicates": [dict(r) for r in rows],
        "total_groups": len(rows),
    }


# ── Round 28 ──────────────────────────────────


@router.get("/entity-connections-timeline")
def entity_connections_timeline():
    """How entity connection count grows over ingestion time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(d.ingested_at) AS day, "
            "COUNT(DISTINCT de.entity_id) AS new_entities, "
            "COUNT(DISTINCT d.id) AS doc_count "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE d.ingested_at IS NOT NULL "
            "GROUP BY day ORDER BY day"
        ).fetchall()
    return {"timeline": [dict(r) for r in rows]}


@router.get("/source-cross-reference")
def source_cross_reference():
    """Entities shared between multiple sources."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT d.source) AS source_count, "
            "GROUP_CONCAT(DISTINCT d.source) AS sources "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id HAVING COUNT(DISTINCT d.source) > 1 "
            "ORDER BY source_count DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows], "total": len(rows)}


@router.get("/event-precision-stats")
def event_precision_stats():
    """Breakdown of event date precision levels."""
    with get_db() as conn:
        by_precision = conn.execute(
            "SELECT precision, COUNT(*) AS count FROM events GROUP BY precision ORDER BY count DESC"
        ).fetchall()
        by_resolved = conn.execute(
            "SELECT is_resolved, COUNT(*) AS count FROM events GROUP BY is_resolved"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM events").fetchone()["cnt"]
    return {
        "by_precision": [dict(r) for r in by_precision],
        "by_resolved": [dict(r) for r in by_resolved],
        "total": total,
    }


@router.get("/category-ingest-timeline")
def category_ingest_timeline():
    """Document ingestion by category over time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(ingested_at) AS day, category, COUNT(*) AS count "
            "FROM documents WHERE ingested_at IS NOT NULL "
            "GROUP BY day, category ORDER BY day"
        ).fetchall()
    timeline = {}
    for r in rows:
        day = r["day"]
        if day not in timeline:
            timeline[day] = {}
        timeline[day][r["category"]] = r["count"]
    return {"timeline": [{"day": d, **cats} for d, cats in timeline.items()]}


@router.get("/entity-hub-score")
def entity_hub_score():
    """Entities ranked by total connection count (hub nodes)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT entity_id, name, type, SUM(cnt) AS connections FROM ("
            "  SELECT ec.entity_a_id AS entity_id, e.name, e.type, COUNT(*) AS cnt "
            "  FROM entity_connections ec "
            "  JOIN entities e ON e.id = ec.entity_a_id "
            "  GROUP BY ec.entity_a_id "
            "  UNION ALL "
            "  SELECT ec.entity_b_id AS entity_id, e.name, e.type, COUNT(*) AS cnt "
            "  FROM entity_connections ec "
            "  JOIN entities e ON e.id = ec.entity_b_id "
            "  GROUP BY ec.entity_b_id"
            ") GROUP BY entity_id ORDER BY connections DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/entity-spread")
def entity_spread():
    """How many unique sources and documents each entity appears in."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COUNT(DISTINCT d.id) AS doc_count, "
            "COUNT(DISTINCT d.source) AS source_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id ORDER BY doc_count DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/document-size-buckets")
def document_size_buckets():
    """Documents grouped by page count ranges."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CASE "
            "  WHEN pages = 0 THEN '0 pages' "
            "  WHEN pages = 1 THEN '1 page' "
            "  WHEN pages BETWEEN 2 AND 10 THEN '2-10' "
            "  WHEN pages BETWEEN 11 AND 50 THEN '11-50' "
            "  WHEN pages BETWEEN 51 AND 200 THEN '51-200' "
            "  WHEN pages BETWEEN 201 AND 500 THEN '201-500' "
            "  ELSE '500+' END AS bucket, "
            "COUNT(*) AS count, "
            "SUM(pages) AS total_pages "
            "FROM documents GROUP BY bucket ORDER BY MIN(pages)"
        ).fetchall()
    return {"buckets": [dict(r) for r in rows]}


@router.get("/event-date-gaps")
def event_date_gaps():
    """Largest gaps between consecutive dated events."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT event_date FROM events "
            "WHERE event_date IS NOT NULL AND event_date != '' "
            "ORDER BY event_date"
        ).fetchall()
    dates = [r["event_date"] for r in rows]
    gaps = []
    for i in range(1, len(dates)):
        try:
            d1 = dates[i - 1][:10]
            d2 = dates[i][:10]
            from datetime import datetime

            dt1 = datetime.strptime(d1, "%Y-%m-%d")
            dt2 = datetime.strptime(d2, "%Y-%m-%d")
            delta = (dt2 - dt1).days
            if delta > 0:
                gaps.append({"start": d1, "end": d2, "days": delta})
        except (ValueError, TypeError):
            continue
    gaps.sort(key=lambda g: g["days"], reverse=True)
    return {"gaps": gaps[:50], "total_events_with_dates": len(dates)}


@router.get("/source-entity-overlap")
def source_entity_overlap():
    """Pairwise entity overlap between sources."""
    with get_db() as conn:
        sources = conn.execute(
            "SELECT DISTINCT source FROM documents WHERE source IS NOT NULL"
        ).fetchall()
        source_names = [s["source"] for s in sources]
        source_entities = {}
        for src in source_names:
            ents = conn.execute(
                "SELECT DISTINCT de.entity_id FROM document_entities de "
                "JOIN documents d ON d.id = de.document_id "
                "WHERE d.source = ?",
                (src,),
            ).fetchall()
            source_entities[src] = {r["entity_id"] for r in ents}
    pairs = []
    for i in range(len(source_names)):
        for j in range(i + 1, len(source_names)):
            sa, sb = source_names[i], source_names[j]
            overlap = len(source_entities[sa] & source_entities[sb])
            if overlap > 0:
                pairs.append(
                    {
                        "source_a": sa,
                        "source_b": sb,
                        "shared_entities": overlap,
                        "total_a": len(source_entities[sa]),
                        "total_b": len(source_entities[sb]),
                    }
                )
    pairs.sort(key=lambda p: p["shared_entities"], reverse=True)
    return {"pairs": pairs}


@router.get("/unresolved-entities-summary")
def unresolved_entities_summary():
    """Entities not yet canonically resolved."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "WHERE e.id NOT IN (SELECT source_entity_id FROM entity_resolutions) "
            "AND e.id NOT IN (SELECT canonical_entity_id FROM entity_resolutions) "
            "GROUP BY e.id ORDER BY doc_count DESC LIMIT 200"
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM entities "
            "WHERE id NOT IN (SELECT source_entity_id FROM entity_resolutions) "
            "AND id NOT IN (SELECT canonical_entity_id FROM entity_resolutions)"
        ).fetchone()["cnt"]
    return {"entities": [dict(r) for r in rows], "total": total}


@router.get("/connection-reciprocity")
def connection_reciprocity():
    """Bidirectional vs unidirectional entity connections."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) AS cnt FROM entity_connections").fetchone()["cnt"]
        bidirectional = conn.execute(
            "SELECT COUNT(*) AS cnt FROM entity_connections ec1 "
            "WHERE EXISTS ("
            "  SELECT 1 FROM entity_connections ec2 "
            "  WHERE ec2.entity_a_id = ec1.entity_b_id "
            "  AND ec2.entity_b_id = ec1.entity_a_id"
            ")"
        ).fetchone()["cnt"]
        by_weight = conn.execute(
            "SELECT "
            "CASE "
            "  WHEN weight = 1 THEN '1' "
            "  WHEN weight BETWEEN 2 AND 5 THEN '2-5' "
            "  WHEN weight BETWEEN 6 AND 20 THEN '6-20' "
            "  ELSE '20+' END AS bucket, "
            "COUNT(*) AS count "
            "FROM entity_connections GROUP BY bucket ORDER BY MIN(weight)"
        ).fetchall()
    return {
        "total": total,
        "bidirectional": bidirectional,
        "unidirectional": total - bidirectional,
        "by_weight": [dict(r) for r in by_weight],
    }


# ── Round 30 ──────────────────────────────────


@router.get("/entity-longevity")
def entity_longevity():
    """Entities spanning the widest date range in events."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "MIN(ev.event_date) AS earliest, MAX(ev.event_date) AS latest, "
            "COUNT(DISTINCT ev.id) AS event_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN events ev ON ev.document_id = de.document_id "
            "WHERE ev.event_date IS NOT NULL AND ev.event_date != '' "
            "GROUP BY e.id HAVING COUNT(DISTINCT ev.id) > 1 "
            "ORDER BY (JULIANDAY(MAX(ev.event_date)) - JULIANDAY(MIN(ev.event_date))) DESC "
            "LIMIT 100"
        ).fetchall()
    results = []
    for r in rows:
        try:
            from datetime import datetime

            d1 = datetime.strptime(r["earliest"][:10], "%Y-%m-%d")
            d2 = datetime.strptime(r["latest"][:10], "%Y-%m-%d")
            span = (d2 - d1).days
        except (ValueError, TypeError):
            span = 0
        results.append({**dict(r), "span_days": span})
    return {"entities": results}


@router.get("/document-flagged-ratio")
def document_flagged_ratio():
    """Flagged vs unflagged documents by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, "
            "SUM(CASE WHEN flagged = 1 THEN 1 ELSE 0 END) AS flagged, "
            "SUM(CASE WHEN flagged = 0 OR flagged IS NULL THEN 1 ELSE 0 END) AS unflagged, "
            "COUNT(*) AS total "
            "FROM documents GROUP BY source ORDER BY total DESC"
        ).fetchall()
        totals = conn.execute(
            "SELECT SUM(CASE WHEN flagged = 1 THEN 1 ELSE 0 END) AS flagged, "
            "COUNT(*) AS total FROM documents"
        ).fetchone()
    return {
        "sources": [dict(r) for r in rows],
        "total_flagged": totals["flagged"] or 0,
        "total_docs": totals["total"],
    }


@router.get("/event-cluster-density")
def event_cluster_density():
    """Months with the most events."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(event_date, 1, 7) AS month, COUNT(*) AS count "
            "FROM events "
            "WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 7 "
            "AND SUBSTR(event_date, 1, 4) BETWEEN '1900' AND '2100' "
            "GROUP BY month ORDER BY count DESC LIMIT 50"
        ).fetchall()
    return {"months": [dict(r) for r in rows]}


@router.get("/source-ingestion-summary")
def source_ingestion_summary():
    """Ingestion stats per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) AS doc_count, "
            "MIN(ingested_at) AS first_ingested, "
            "MAX(ingested_at) AS last_ingested, "
            "SUM(pages) AS total_pages "
            "FROM documents GROUP BY source ORDER BY doc_count DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-singletons")
def entity_singletons():
    """Entities appearing in only one document."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "GROUP BY e.id HAVING doc_count = 1 "
            "ORDER BY e.name LIMIT 200"
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM ("
            "  SELECT entity_id FROM document_entities "
            "  GROUP BY entity_id HAVING COUNT(DISTINCT document_id) = 1"
            ")"
        ).fetchone()["cnt"]
    return {"entities": [dict(r) for r in rows], "total": total}


@router.get("/page-text-coverage")
def page_text_coverage():
    """Documents with/without extracted text by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, "
            "SUM(CASE WHEN raw_text IS NOT NULL AND LENGTH(raw_text) > 0 THEN 1 ELSE 0 END) AS with_text, "
            "SUM(CASE WHEN raw_text IS NULL OR LENGTH(raw_text) = 0 THEN 1 ELSE 0 END) AS without_text, "
            "COUNT(*) AS total "
            "FROM documents GROUP BY source ORDER BY total DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


# ── Round 31 ──────────────────────────────────


@router.get("/entity-name-length-stats")
def entity_name_length_stats():
    """Distribution of entity name lengths."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CASE "
            "  WHEN LENGTH(name) <= 3 THEN '1-3' "
            "  WHEN LENGTH(name) BETWEEN 4 AND 10 THEN '4-10' "
            "  WHEN LENGTH(name) BETWEEN 11 AND 20 THEN '11-20' "
            "  WHEN LENGTH(name) BETWEEN 21 AND 40 THEN '21-40' "
            "  ELSE '40+' END AS bucket, "
            "COUNT(*) AS count "
            "FROM entities GROUP BY bucket ORDER BY MIN(LENGTH(name))"
        ).fetchall()
        stats = conn.execute(
            "SELECT AVG(LENGTH(name)) AS avg_len, "
            "MAX(LENGTH(name)) AS max_len, "
            "MIN(LENGTH(name)) AS min_len "
            "FROM entities"
        ).fetchone()
        longest = conn.execute(
            "SELECT id, name, type, LENGTH(name) AS name_len "
            "FROM entities ORDER BY name_len DESC LIMIT 20"
        ).fetchall()
    return {
        "buckets": [dict(r) for r in rows],
        "stats": {
            "avg_len": round(stats["avg_len"] or 0, 1),
            "max_len": stats["max_len"] or 0,
            "min_len": stats["min_len"] or 0,
        },
        "longest": [dict(r) for r in longest],
    }


@router.get("/document-notes-summary")
def document_notes_summary():
    """Documents with notes vs without."""
    with get_db() as conn:
        with_notes = conn.execute(
            "SELECT COUNT(*) AS cnt FROM documents WHERE notes IS NOT NULL AND LENGTH(notes) > 0"
        ).fetchone()["cnt"]
        total = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()["cnt"]
        recent = conn.execute(
            "SELECT id, title, filename, notes FROM documents "
            "WHERE notes IS NOT NULL AND LENGTH(notes) > 0 "
            "ORDER BY id DESC LIMIT 50"
        ).fetchall()
    return {
        "with_notes": with_notes,
        "without_notes": total - with_notes,
        "total": total,
        "recent": [dict(r) for r in recent],
    }


@router.get("/event-date-quality")
def event_date_quality():
    """Events with valid vs invalid/missing dates."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) AS cnt FROM events").fetchone()["cnt"]
        with_date = conn.execute(
            "SELECT COUNT(*) AS cnt FROM events WHERE event_date IS NOT NULL AND event_date != ''"
        ).fetchone()["cnt"]
        valid_range = conn.execute(
            "SELECT COUNT(*) AS cnt FROM events "
            "WHERE event_date IS NOT NULL AND event_date != '' "
            "AND SUBSTR(event_date, 1, 4) BETWEEN '1900' AND '2100'"
        ).fetchone()["cnt"]
        by_precision = conn.execute(
            "SELECT precision, COUNT(*) AS count FROM events GROUP BY precision ORDER BY count DESC"
        ).fetchall()
    return {
        "total": total,
        "with_date": with_date,
        "without_date": total - with_date,
        "valid_range": valid_range,
        "out_of_range": with_date - valid_range,
        "by_precision": [dict(r) for r in by_precision],
    }


@router.get("/source-category-matrix")
def source_category_matrix():
    """Category breakdown per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, category, COUNT(*) AS count "
            "FROM documents GROUP BY source, category ORDER BY source, count DESC"
        ).fetchall()
    matrix = {}
    categories = set()
    for r in rows:
        src = r["source"]
        if src not in matrix:
            matrix[src] = {}
        matrix[src][r["category"]] = r["count"]
        categories.add(r["category"])
    return {
        "sources": list(matrix.keys()),
        "categories": sorted(categories),
        "matrix": matrix,
    }


@router.get("/entity-type-growth")
def entity_type_growth():
    """Entity count by type over ingestion time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(d.ingested_at) AS day, e.type, "
            "COUNT(DISTINCT e.id) AS count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE d.ingested_at IS NOT NULL "
            "GROUP BY day, e.type ORDER BY day"
        ).fetchall()
    timeline = {}
    for r in rows:
        day = r["day"]
        if day not in timeline:
            timeline[day] = {}
        timeline[day][r["type"]] = r["count"]
    return {"timeline": [{"day": d, **types} for d, types in timeline.items()]}


@router.get("/connection-weight-stats")
def connection_weight_stats():
    """Statistical summary of connection weights."""
    with get_db() as conn:
        stats = conn.execute(
            "SELECT COUNT(*) AS total, "
            "AVG(weight) AS avg_weight, "
            "MAX(weight) AS max_weight, "
            "MIN(weight) AS min_weight, "
            "SUM(weight) AS total_weight "
            "FROM entity_connections"
        ).fetchone()
        top = conn.execute(
            "SELECT ea.name AS name_a, ea.type AS type_a, "
            "eb.name AS name_b, eb.type AS type_b, ec.weight "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "ORDER BY ec.weight DESC LIMIT 50"
        ).fetchall()
    return {
        "stats": {
            "total": stats["total"],
            "avg_weight": round(stats["avg_weight"] or 0, 2),
            "max_weight": stats["max_weight"] or 0,
            "min_weight": stats["min_weight"] or 0,
            "total_weight": stats["total_weight"] or 0,
        },
        "top_connections": [dict(r) for r in top],
    }


# ── Round 32 ──────────────────────────────────


@router.get("/entity-category-breakdown")
def entity_category_breakdown():
    """Entity types per document category."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.category, e.type, COUNT(DISTINCT e.id) AS count "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "JOIN entities e ON e.id = de.entity_id "
            "GROUP BY d.category, e.type ORDER BY d.category, count DESC"
        ).fetchall()
    matrix = {}
    types = set()
    for r in rows:
        cat = r["category"]
        if cat not in matrix:
            matrix[cat] = {}
        matrix[cat][r["type"]] = r["count"]
        types.add(r["type"])
    return {
        "categories": list(matrix.keys()),
        "types": sorted(types),
        "matrix": matrix,
    }


@router.get("/document-age-distribution")
def document_age_distribution():
    """How old documents are by ingestion date."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CAST(JULIANDAY('now') - JULIANDAY(ingested_at) AS INTEGER) AS age_days, "
            "COUNT(*) AS count "
            "FROM documents WHERE ingested_at IS NOT NULL "
            "GROUP BY age_days ORDER BY age_days"
        ).fetchall()
    buckets = {"< 1 day": 0, "1-7 days": 0, "8-30 days": 0, "31-90 days": 0, "90+ days": 0}
    for r in rows:
        age = r["age_days"]
        cnt = r["count"]
        if age < 1:
            buckets["< 1 day"] += cnt
        elif age <= 7:
            buckets["1-7 days"] += cnt
        elif age <= 30:
            buckets["8-30 days"] += cnt
        elif age <= 90:
            buckets["31-90 days"] += cnt
        else:
            buckets["90+ days"] += cnt
    return {"buckets": [{"range": k, "count": v} for k, v in buckets.items()]}


@router.get("/event-source-density")
def event_source_density():
    """Events per document by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, COUNT(ev.id) AS event_count, "
            "COUNT(DISTINCT d.id) AS doc_count, "
            "ROUND(CAST(COUNT(ev.id) AS REAL) / COUNT(DISTINCT d.id), 1) AS avg_events "
            "FROM documents d "
            "LEFT JOIN events ev ON ev.document_id = d.id "
            "GROUP BY d.source ORDER BY avg_events DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-pair-strength")
def entity_pair_strength():
    """Strongest entity pair connections by weight."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.name AS name_a, ea.type AS type_a, "
            "eb.name AS name_b, eb.type AS type_b, "
            "ec.weight, "
            "(SELECT COUNT(DISTINCT de.document_id) FROM document_entities de "
            " WHERE de.entity_id = ec.entity_a_id) AS docs_a, "
            "(SELECT COUNT(DISTINCT de.document_id) FROM document_entities de "
            " WHERE de.entity_id = ec.entity_b_id) AS docs_b "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "ORDER BY ec.weight DESC LIMIT 100"
        ).fetchall()
    return {"pairs": [dict(r) for r in rows]}


@router.get("/source-document-quality")
def source_document_quality():
    """Text/entity/event richness per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, COUNT(*) AS doc_count, "
            "ROUND(AVG(d.pages), 1) AS avg_pages, "
            "ROUND(100.0 * SUM(CASE WHEN d.raw_text IS NOT NULL AND LENGTH(d.raw_text) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS with_text, "
            "ROUND(100.0 * SUM(CASE WHEN d.date IS NOT NULL AND d.date != '' THEN 1 ELSE 0 END) / COUNT(*), 1) AS with_date "
            "FROM documents d GROUP BY d.source ORDER BY doc_count DESC"
        ).fetchall()
    return [
        {
            "source": r["source"],
            "doc_count": r["doc_count"],
            "avg_pages": r["avg_pages"] or 0,
            "with_text": r["with_text"],
            "with_date": r["with_date"],
            "quality_score": round((r["with_text"] + r["with_date"]) / 2, 1),
        }
        for r in rows
    ]


@router.get("/entity-alias-coverage")
def entity_alias_coverage():
    """Entities with vs without aliases."""
    with get_db() as conn:
        with_aliases = conn.execute(
            "SELECT COUNT(DISTINCT entity_id) AS cnt FROM entity_aliases"
        ).fetchone()["cnt"]
        total = conn.execute("SELECT COUNT(*) AS cnt FROM entities").fetchone()["cnt"]
        total_aliases = conn.execute("SELECT COUNT(*) AS cnt FROM entity_aliases").fetchone()["cnt"]
        top = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(ea.id) AS alias_count "
            "FROM entities e "
            "JOIN entity_aliases ea ON ea.entity_id = e.id "
            "GROUP BY e.id ORDER BY alias_count DESC LIMIT 50"
        ).fetchall()
        top_aliased = []
        for r in top:
            aliases = conn.execute(
                "SELECT alias_name FROM entity_aliases WHERE entity_id = ?", (r["id"],)
            ).fetchall()
            top_aliased.append(
                {
                    "name": r["name"],
                    "type": r["type"],
                    "alias_count": r["alias_count"],
                    "aliases": [a["alias_name"] for a in aliases],
                }
            )
    return {
        "total_entities": total,
        "with_aliases": with_aliases,
        "total_aliases": total_aliases,
        "coverage_pct": round(100.0 * with_aliases / total, 1) if total else 0,
        "top_aliased": top_aliased,
    }


@router.get("/entity-co-occurrence")
def entity_co_occurrence():
    """Entities that appear together in the same documents most often."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.name AS name_a, ea.type AS type_a, "
            "eb.name AS name_b, eb.type AS type_b, "
            "COUNT(DISTINCT da.document_id) AS shared_docs "
            "FROM document_entities da "
            "JOIN document_entities db ON da.document_id = db.document_id AND da.entity_id < db.entity_id "
            "JOIN entities ea ON ea.id = da.entity_id "
            "JOIN entities eb ON eb.id = db.entity_id "
            "GROUP BY da.entity_id, db.entity_id "
            "ORDER BY shared_docs DESC LIMIT 100"
        ).fetchall()
    return {"pairs": [dict(r) for r in rows]}


@router.get("/document-category-timeline")
def document_category_timeline():
    """Documents ingested per category over time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(ingested_at) AS day, category, COUNT(*) AS count "
            "FROM documents WHERE ingested_at IS NOT NULL "
            "GROUP BY day, category ORDER BY day"
        ).fetchall()
    timeline = {}
    categories = set()
    for r in rows:
        day = r["day"]
        if day not in timeline:
            timeline[day] = {}
        timeline[day][r["category"]] = r["count"]
        categories.add(r["category"])
    return {
        "days": sorted(timeline.keys()),
        "categories": sorted(categories),
        "data": timeline,
    }


@router.get("/event-resolution-breakdown")
def event_resolution_breakdown():
    """Resolved vs unresolved events by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, "
            "SUM(CASE WHEN ev.is_resolved = 1 THEN 1 ELSE 0 END) AS resolved, "
            "SUM(CASE WHEN ev.is_resolved = 0 OR ev.is_resolved IS NULL THEN 1 ELSE 0 END) AS unresolved, "
            "COUNT(*) AS total "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "GROUP BY d.source ORDER BY total DESC"
        ).fetchall()
    return {
        "sources": [
            {
                "source": r["source"],
                "resolved": r["resolved"],
                "unresolved": r["unresolved"],
                "total": r["total"],
                "pct_resolved": round(100.0 * r["resolved"] / r["total"], 1) if r["total"] else 0,
            }
            for r in rows
        ]
    }


@router.get("/entity-document-reach")
def entity_document_reach():
    """How many documents each entity appears in (top 100)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "GROUP BY e.id ORDER BY doc_count DESC LIMIT 100"
        ).fetchall()
        total_docs = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()["cnt"]
    return {
        "total_docs": total_docs,
        "entities": [
            {
                "name": r["name"],
                "type": r["type"],
                "doc_count": r["doc_count"],
                "pct": round(100.0 * r["doc_count"] / total_docs, 1) if total_docs else 0,
            }
            for r in rows
        ],
    }


@router.get("/source-overlap-matrix")
def source_overlap_matrix():
    """Which sources share the most entities."""
    with get_db() as conn:
        sources = conn.execute(
            "SELECT DISTINCT source FROM documents WHERE source IS NOT NULL ORDER BY source"
        ).fetchall()
        src_list = [s["source"] for s in sources]
        src_entities = {}
        for src in src_list:
            ents = conn.execute(
                "SELECT DISTINCT de.entity_id FROM document_entities de "
                "JOIN documents d ON d.id = de.document_id WHERE d.source = ?",
                (src,),
            ).fetchall()
            src_entities[src] = {e["entity_id"] for e in ents}
    matrix = {}
    for s1 in src_list:
        matrix[s1] = {}
        for s2 in src_list:
            matrix[s1][s2] = len(src_entities[s1] & src_entities[s2])
    return {"sources": src_list, "matrix": matrix}


@router.get("/entity-type-distribution")
def entity_type_distribution():
    """Count of entities per type with percentage."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT type, COUNT(*) AS count FROM entities GROUP BY type ORDER BY count DESC"
        ).fetchall()
        total = sum(r["count"] for r in rows)
    return {
        "total": total,
        "types": [
            {
                "type": r["type"],
                "count": r["count"],
                "pct": round(100.0 * r["count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/document-text-length")
def document_text_length():
    """Distribution of document text lengths."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, LENGTH(raw_text) AS text_len "
            "FROM documents WHERE raw_text IS NOT NULL AND LENGTH(raw_text) > 0 "
            "ORDER BY text_len DESC LIMIT 100"
        ).fetchall()
        stats = conn.execute(
            "SELECT COUNT(*) AS total, "
            "AVG(LENGTH(raw_text)) AS avg_len, "
            "MAX(LENGTH(raw_text)) AS max_len, "
            "MIN(LENGTH(raw_text)) AS min_len "
            "FROM documents WHERE raw_text IS NOT NULL AND LENGTH(raw_text) > 0"
        ).fetchone()
    return {
        "stats": {
            "total": stats["total"],
            "avg_len": round(stats["avg_len"] or 0),
            "max_len": stats["max_len"] or 0,
            "min_len": stats["min_len"] or 0,
        },
        "top_docs": [dict(r) for r in rows],
    }


@router.get("/event-confidence-distribution")
def event_confidence_distribution():
    """Distribution of event confidence scores."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CASE "
            "  WHEN confidence >= 0.9 THEN 'Very High (0.9-1.0)' "
            "  WHEN confidence >= 0.7 THEN 'High (0.7-0.9)' "
            "  WHEN confidence >= 0.5 THEN 'Medium (0.5-0.7)' "
            "  WHEN confidence >= 0.3 THEN 'Low (0.3-0.5)' "
            "  ELSE 'Very Low (0-0.3)' "
            "END AS bucket, "
            "COUNT(*) AS count "
            "FROM events WHERE confidence IS NOT NULL "
            "GROUP BY bucket ORDER BY MIN(confidence) DESC"
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM events WHERE confidence IS NOT NULL"
        ).fetchone()["cnt"]
    return {
        "total": total,
        "buckets": [
            {
                "bucket": r["bucket"],
                "count": r["count"],
                "pct": round(100.0 * r["count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/source-date-span")
def source_date_span():
    """Earliest and latest event dates per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, "
            "MIN(ev.event_date) AS earliest, "
            "MAX(ev.event_date) AS latest, "
            "COUNT(ev.id) AS event_count "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE ev.event_date IS NOT NULL AND ev.event_date != '' "
            "GROUP BY d.source ORDER BY earliest"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-mention-frequency")
def entity_mention_frequency():
    """How many times entities are mentioned across documents (top 100)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, COUNT(de.document_id) AS mentions "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "GROUP BY e.id ORDER BY mentions DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/connection-type-breakdown")
def connection_type_breakdown():
    """Connection counts grouped by entity type pairs."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.type AS type_a, eb.type AS type_b, "
            "COUNT(*) AS count, ROUND(AVG(ec.weight), 2) AS avg_weight "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "GROUP BY ea.type, eb.type ORDER BY count DESC"
        ).fetchall()
    return {"type_pairs": [dict(r) for r in rows]}


@router.get("/entity-isolation-score")
def entity_isolation_score():
    """Entities with no connections (isolated nodes)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "LEFT JOIN entity_connections ec1 ON ec1.entity_a_id = e.id "
            "LEFT JOIN entity_connections ec2 ON ec2.entity_b_id = e.id "
            "LEFT JOIN document_entities de ON de.entity_id = e.id "
            "WHERE ec1.entity_a_id IS NULL AND ec2.entity_b_id IS NULL "
            "GROUP BY e.id ORDER BY doc_count DESC LIMIT 100"
        ).fetchall()
        total_isolated = conn.execute(
            "SELECT COUNT(*) AS cnt FROM entities e "
            "WHERE NOT EXISTS (SELECT 1 FROM entity_connections ec "
            "WHERE ec.entity_a_id = e.id OR ec.entity_b_id = e.id)"
        ).fetchone()["cnt"]
        total = conn.execute("SELECT COUNT(*) AS cnt FROM entities").fetchone()["cnt"]
    return {
        "total_entities": total,
        "isolated_count": total_isolated,
        "isolated_pct": round(100.0 * total_isolated / total, 1) if total else 0,
        "top_isolated": [dict(r) for r in rows],
    }


@router.get("/document-category-balance")
def document_category_balance():
    """Document count per category with percentage of total."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, COUNT(*) AS count FROM documents "
            "GROUP BY category ORDER BY count DESC"
        ).fetchall()
        total = sum(r["count"] for r in rows)
    return {
        "total": total,
        "categories": [
            {
                "category": r["category"] or "uncategorized",
                "count": r["count"],
                "pct": round(100.0 * r["count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/event-temporal-density")
def event_temporal_density():
    """Events per year from event dates."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(event_date, 1, 4) AS year, COUNT(*) AS count "
            "FROM events WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 4 "
            "AND SUBSTR(event_date, 1, 4) GLOB '[0-9][0-9][0-9][0-9]' "
            "GROUP BY year ORDER BY year"
        ).fetchall()
    return {"years": [dict(r) for r in rows]}


@router.get("/source-entity-exclusivity")
def source_entity_exclusivity():
    """Entities that appear in only one source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, d.source, "
            "COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id "
            "HAVING COUNT(DISTINCT d.source) = 1 "
            "ORDER BY doc_count DESC LIMIT 100"
        ).fetchall()
        total_exclusive = conn.execute(
            "SELECT COUNT(*) AS cnt FROM ("
            "SELECT e.id FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id HAVING COUNT(DISTINCT d.source) = 1)"
        ).fetchone()["cnt"]
    return {
        "exclusive_count": total_exclusive,
        "entities": [dict(r) for r in rows],
    }


@router.get("/entity-name-pattern")
def entity_name_pattern():
    """Entity name length distribution and common patterns."""
    with get_db() as conn:
        stats = conn.execute(
            "SELECT AVG(LENGTH(name)) AS avg_len, "
            "MAX(LENGTH(name)) AS max_len, "
            "MIN(LENGTH(name)) AS min_len, "
            "COUNT(*) AS total "
            "FROM entities"
        ).fetchone()
        long_names = conn.execute(
            "SELECT name, type, LENGTH(name) AS name_len "
            "FROM entities ORDER BY name_len DESC LIMIT 50"
        ).fetchall()
        short_names = conn.execute(
            "SELECT name, type, LENGTH(name) AS name_len "
            "FROM entities WHERE LENGTH(name) <= 3 ORDER BY name_len LIMIT 50"
        ).fetchall()
    return {
        "stats": {
            "total": stats["total"],
            "avg_len": round(stats["avg_len"] or 0, 1),
            "max_len": stats["max_len"] or 0,
            "min_len": stats["min_len"] or 0,
        },
        "longest": [dict(r) for r in long_names],
        "shortest": [dict(r) for r in short_names],
    }


@router.get("/connection-cluster-summary")
def connection_cluster_summary():
    """Summary of connection weight clusters."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CASE "
            "  WHEN weight >= 100 THEN 'Very Strong (100+)' "
            "  WHEN weight >= 50 THEN 'Strong (50-99)' "
            "  WHEN weight >= 20 THEN 'Moderate (20-49)' "
            "  WHEN weight >= 5 THEN 'Weak (5-19)' "
            "  ELSE 'Very Weak (1-4)' "
            "END AS cluster, "
            "COUNT(*) AS count, "
            "ROUND(AVG(weight), 2) AS avg_weight "
            "FROM entity_connections "
            "GROUP BY cluster ORDER BY MIN(weight) DESC"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM entity_connections").fetchone()["cnt"]
    return {
        "total": total,
        "clusters": [
            {
                "cluster": r["cluster"],
                "count": r["count"],
                "avg_weight": r["avg_weight"],
                "pct": round(100.0 * r["count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/document-source-timeline")
def document_source_timeline():
    """Documents ingested per source over time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(ingested_at) AS day, source, COUNT(*) AS count "
            "FROM documents WHERE ingested_at IS NOT NULL "
            "GROUP BY day, source ORDER BY day"
        ).fetchall()
    timeline = {}
    sources = set()
    for r in rows:
        day = r["day"]
        if day not in timeline:
            timeline[day] = {}
        timeline[day][r["source"]] = r["count"]
        sources.add(r["source"])
    return {
        "days": sorted(timeline.keys()),
        "sources": sorted(sources),
        "data": timeline,
    }


@router.get("/entity-cross-type-connections")
def entity_cross_type_connections():
    """Connections between entities of different types vs same type."""
    with get_db() as conn:
        cross = conn.execute(
            "SELECT COUNT(*) AS cnt FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "WHERE ea.type != eb.type"
        ).fetchone()["cnt"]
        same = conn.execute(
            "SELECT COUNT(*) AS cnt FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "WHERE ea.type = eb.type"
        ).fetchone()["cnt"]
        total = cross + same
    return {
        "cross_type": cross,
        "same_type": same,
        "total": total,
        "cross_pct": round(100.0 * cross / total, 1) if total else 0,
        "same_pct": round(100.0 * same / total, 1) if total else 0,
    }


@router.get("/event-context-length")
def event_context_length():
    """Distribution of event context text lengths."""
    with get_db() as conn:
        stats = conn.execute(
            "SELECT COUNT(*) AS total, "
            "AVG(LENGTH(context)) AS avg_len, "
            "MAX(LENGTH(context)) AS max_len, "
            "MIN(LENGTH(context)) AS min_len, "
            "SUM(CASE WHEN context IS NULL OR LENGTH(context) = 0 THEN 1 ELSE 0 END) AS no_context "
            "FROM events"
        ).fetchone()
        top = conn.execute(
            "SELECT ev.id, ev.event_date, LENGTH(ev.context) AS ctx_len, "
            "d.filename "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE ev.context IS NOT NULL AND LENGTH(ev.context) > 0 "
            "ORDER BY ctx_len DESC LIMIT 50"
        ).fetchall()
    return {
        "stats": {
            "total": stats["total"],
            "avg_len": round(stats["avg_len"] or 0),
            "max_len": stats["max_len"] or 0,
            "min_len": stats["min_len"] or 0,
            "no_context": stats["no_context"],
        },
        "longest": [dict(r) for r in top],
    }


@router.get("/source-flagged-ratio")
def source_flagged_ratio():
    """Flagged document ratio per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) AS total, "
            "SUM(CASE WHEN flagged = 1 THEN 1 ELSE 0 END) AS flagged_count "
            "FROM documents GROUP BY source ORDER BY total DESC"
        ).fetchall()
    return {
        "sources": [
            {
                "source": r["source"] or "unknown",
                "total": r["total"],
                "flagged": r["flagged_count"],
                "pct": round(100.0 * r["flagged_count"] / r["total"], 1) if r["total"] else 0,
            }
            for r in rows
        ]
    }


@router.get("/entity-resolution-coverage")
def entity_resolution_coverage():
    """How many entities have been resolved."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) AS cnt FROM entities").fetchone()["cnt"]
        resolved = conn.execute(
            "SELECT COUNT(DISTINCT source_entity_id) AS cnt FROM entity_resolutions"
        ).fetchone()["cnt"]
        canonical = conn.execute(
            "SELECT COUNT(DISTINCT canonical_entity_id) AS cnt FROM entity_resolutions"
        ).fetchone()["cnt"]
        top = conn.execute(
            "SELECT e2.name AS canonical_name, e2.type AS canonical_type, "
            "COUNT(*) AS resolved_count "
            "FROM entity_resolutions er "
            "JOIN entities e2 ON e2.id = er.canonical_entity_id "
            "GROUP BY er.canonical_entity_id ORDER BY resolved_count DESC LIMIT 50"
        ).fetchall()
    return {
        "total_entities": total,
        "resolved_count": resolved,
        "canonical_count": canonical,
        "coverage_pct": round(100.0 * resolved / total, 1) if total else 0,
        "top_canonical": [dict(r) for r in top],
    }


@router.get("/connection-weight-histogram")
def connection_weight_histogram():
    """Fine-grained histogram of connection weights."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT weight, COUNT(*) AS count "
            "FROM entity_connections GROUP BY weight ORDER BY weight"
        ).fetchall()
    return {"bins": [dict(r) for r in rows]}


@router.get("/entity-degree-centrality")
def entity_degree_centrality():
    """Entity connection degree (number of unique partners)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, sub.degree, sub.total_weight "
            "FROM ("
            "  SELECT entity_id, COUNT(*) AS degree, SUM(w) AS total_weight FROM ("
            "    SELECT entity_a_id AS entity_id, weight AS w FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id AS entity_id, weight AS w FROM entity_connections"
            "  ) GROUP BY entity_id"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY sub.degree DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/document-title-analysis")
def document_title_analysis():
    """Document title length stats and common words."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT title, LENGTH(title) AS title_len "
            "FROM documents WHERE title IS NOT NULL AND title != '' "
            "ORDER BY title_len DESC"
        ).fetchall()
        no_title = conn.execute(
            "SELECT COUNT(*) AS cnt FROM documents WHERE title IS NULL OR title = ''"
        ).fetchone()["cnt"]
    total = len(rows)
    avg_len = round(sum(r["title_len"] for r in rows) / total, 1) if total else 0
    max_len = rows[0]["title_len"] if rows else 0
    return {
        "stats": {
            "with_title": total,
            "no_title": no_title,
            "avg_len": avg_len,
            "max_len": max_len,
        },
        "longest": [{"title": r["title"], "length": r["title_len"]} for r in rows[:50]],
    }


@router.get("/event-date-range-span")
def event_date_range_span():
    """Span between earliest and latest event dates per document."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.filename, "
            "MIN(ev.event_date) AS earliest, MAX(ev.event_date) AS latest, "
            "COUNT(ev.id) AS event_count "
            "FROM events ev "
            "JOIN documents d ON d.id = ev.document_id "
            "WHERE ev.event_date IS NOT NULL AND ev.event_date != '' "
            "GROUP BY d.id HAVING COUNT(ev.id) >= 2 "
            "ORDER BY event_count DESC LIMIT 100"
        ).fetchall()
    return {"documents": [dict(r) for r in rows]}


@router.get("/source-page-volume")
def source_page_volume():
    """Total pages per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, SUM(pages) AS total_pages, "
            "COUNT(*) AS doc_count, "
            "ROUND(AVG(pages), 1) AS avg_pages, "
            "MAX(pages) AS max_pages "
            "FROM documents GROUP BY source ORDER BY total_pages DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-alias-type-breakdown")
def entity_alias_type_breakdown():
    """Alias counts grouped by entity type."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.type, COUNT(ea.id) AS alias_count, "
            "COUNT(DISTINCT ea.entity_id) AS entity_count "
            "FROM entity_aliases ea "
            "JOIN entities e ON e.id = ea.entity_id "
            "GROUP BY e.type ORDER BY alias_count DESC"
        ).fetchall()
        total_aliases = sum(r["alias_count"] for r in rows)
    return {
        "total_aliases": total_aliases,
        "types": [
            {
                "type": r["type"],
                "alias_count": r["alias_count"],
                "entity_count": r["entity_count"],
                "pct": round(100.0 * r["alias_count"] / total_aliases, 1) if total_aliases else 0,
            }
            for r in rows
        ],
    }


@router.get("/connection-bridge-entities")
def connection_bridge_entities():
    """Entities that connect otherwise separate groups (high betweenness)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, sub.partners, sub.total_weight "
            "FROM ("
            "  SELECT entity_id, COUNT(DISTINCT partner_id) AS partners, "
            "  SUM(w) AS total_weight FROM ("
            "    SELECT entity_a_id AS entity_id, entity_b_id AS partner_id, weight AS w "
            "    FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id AS entity_id, entity_a_id AS partner_id, weight AS w "
            "    FROM entity_connections"
            "  ) GROUP BY entity_id HAVING partners >= 3"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY sub.partners DESC, sub.total_weight DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/entity-shared-sources")
def entity_shared_sources():
    """Entities that appear across the most sources."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, "
            "COUNT(DISTINCT d.source) AS source_count, "
            "COUNT(DISTINCT de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "GROUP BY e.id ORDER BY source_count DESC, doc_count DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/document-filename-pattern")
def document_filename_pattern():
    """Document filename extension and prefix analysis."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT "
            "CASE WHEN INSTR(filename, '.') > 0 "
            "  THEN LOWER(SUBSTR(filename, -INSTR(REPLACE(filename, '.', CHAR(0)), CHAR(0)))) "
            "  ELSE 'no extension' END AS extension, "
            "COUNT(*) AS count "
            "FROM documents GROUP BY extension ORDER BY count DESC"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()["cnt"]
    return {
        "total": total,
        "extensions": [
            {
                "extension": r["extension"],
                "count": r["count"],
                "pct": round(100.0 * r["count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/event-monthly-heatmap")
def event_monthly_heatmap():
    """Events per year-month for heatmap display."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(event_date, 1, 7) AS month, COUNT(*) AS count "
            "FROM events "
            "WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 7 "
            "AND SUBSTR(event_date, 1, 4) GLOB '[0-9][0-9][0-9][0-9]' "
            "GROUP BY month ORDER BY month"
        ).fetchall()
    return {"months": [dict(r) for r in rows]}


@router.get("/source-entity-concentration")
def source_entity_concentration():
    """Unique entities per document by source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, "
            "COUNT(DISTINCT de.entity_id) AS unique_entities, "
            "COUNT(DISTINCT d.id) AS doc_count, "
            "ROUND(CAST(COUNT(DISTINCT de.entity_id) AS REAL) / "
            "COUNT(DISTINCT d.id), 1) AS entities_per_doc "
            "FROM documents d "
            "LEFT JOIN document_entities de ON de.document_id = d.id "
            "GROUP BY d.source ORDER BY entities_per_doc DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-connection-strength-rank")
def entity_connection_strength_rank():
    """Entities ranked by their strongest single connection."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, sub.max_weight, sub.partner_name, sub.partner_type "
            "FROM ("
            "  SELECT entity_id, max_weight, partner_id, "
            "  (SELECT name FROM entities WHERE id = partner_id) AS partner_name, "
            "  (SELECT type FROM entities WHERE id = partner_id) AS partner_type "
            "  FROM ("
            "    SELECT entity_id, MAX(w) AS max_weight, partner_id FROM ("
            "      SELECT entity_a_id AS entity_id, entity_b_id AS partner_id, weight AS w "
            "      FROM entity_connections "
            "      UNION ALL "
            "      SELECT entity_b_id AS entity_id, entity_a_id AS partner_id, weight AS w "
            "      FROM entity_connections"
            "    ) GROUP BY entity_id"
            "  )"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY sub.max_weight DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/entity-type-per-source")
def entity_type_per_source():
    """Entity type breakdown per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, e.type, COUNT(DISTINCT e.id) AS count "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "JOIN entities e ON e.id = de.entity_id "
            "GROUP BY d.source, e.type ORDER BY d.source, count DESC"
        ).fetchall()
    sources = {}
    types = set()
    for r in rows:
        src = r["source"] or "unknown"
        if src not in sources:
            sources[src] = {}
        sources[src][r["type"]] = r["count"]
        types.add(r["type"])
    return {
        "sources": sorted(sources.keys()),
        "types": sorted(types),
        "matrix": sources,
    }


@router.get("/document-ingestion-gap")
def document_ingestion_gap():
    """Time gaps between document ingestions."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(ingested_at) AS day, COUNT(*) AS count "
            "FROM documents WHERE ingested_at IS NOT NULL "
            "GROUP BY day ORDER BY day"
        ).fetchall()
    gaps = []
    for i in range(1, len(rows)):
        prev = rows[i - 1]["day"]
        curr = rows[i]["day"]
        gaps.append(
            {
                "from": prev,
                "to": curr,
                "docs_before": rows[i - 1]["count"],
                "docs_after": rows[i]["count"],
            }
        )
    return {"ingestion_days": [dict(r) for r in rows], "gaps": gaps}


@router.get("/event-weekday-distribution")
def event_weekday_distribution():
    """Events grouped by day of week from event dates."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT CASE CAST(STRFTIME('%w', event_date) AS INTEGER) "
            "  WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' "
            "  WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' "
            "  WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday' END AS weekday, "
            "CAST(STRFTIME('%w', event_date) AS INTEGER) AS day_num, "
            "COUNT(*) AS count "
            "FROM events WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 10 "
            "GROUP BY day_num ORDER BY day_num"
        ).fetchall()
    return {"days": [dict(r) for r in rows]}


@router.get("/source-category-coverage")
def source_category_coverage():
    """Which categories each source covers."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, category, COUNT(*) AS count "
            "FROM documents GROUP BY source, category ORDER BY source, count DESC"
        ).fetchall()
    sources = {}
    categories = set()
    for r in rows:
        src = r["source"] or "unknown"
        if src not in sources:
            sources[src] = {}
        sources[src][r["category"] or "uncategorized"] = r["count"]
        categories.add(r["category"] or "uncategorized")
    return {
        "sources": sorted(sources.keys()),
        "categories": sorted(categories),
        "matrix": sources,
    }


@router.get("/entity-multi-alias-ratio")
def entity_multi_alias_ratio():
    """Entities with multiple aliases vs single alias."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT alias_bucket, COUNT(*) AS entity_count FROM ("
            "  SELECT entity_id, "
            "  CASE "
            "    WHEN COUNT(*) = 1 THEN '1 alias' "
            "    WHEN COUNT(*) = 2 THEN '2 aliases' "
            "    WHEN COUNT(*) = 3 THEN '3 aliases' "
            "    WHEN COUNT(*) <= 5 THEN '4-5 aliases' "
            "    ELSE '6+ aliases' "
            "  END AS alias_bucket "
            "  FROM entity_aliases GROUP BY entity_id"
            ") GROUP BY alias_bucket ORDER BY MIN(CASE alias_bucket "
            "  WHEN '1 alias' THEN 1 WHEN '2 aliases' THEN 2 "
            "  WHEN '3 aliases' THEN 3 WHEN '4-5 aliases' THEN 4 ELSE 5 END)"
        ).fetchall()
        total = sum(r["entity_count"] for r in rows)
    return {
        "total_with_aliases": total,
        "buckets": [
            {
                "bucket": r["alias_bucket"],
                "count": r["entity_count"],
                "pct": round(100.0 * r["entity_count"] / total, 1) if total else 0,
            }
            for r in rows
        ],
    }


@router.get("/connection-asymmetry")
def connection_asymmetry():
    """Entities with highly asymmetric connection weights."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.name, e.type, sub.min_w, sub.max_w, sub.avg_w, sub.conn_count, "
            "sub.max_w - sub.min_w AS spread "
            "FROM ("
            "  SELECT entity_id, MIN(w) AS min_w, MAX(w) AS max_w, "
            "  ROUND(AVG(w), 2) AS avg_w, COUNT(*) AS conn_count FROM ("
            "    SELECT entity_a_id AS entity_id, weight AS w FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id AS entity_id, weight AS w FROM entity_connections"
            "  ) GROUP BY entity_id HAVING COUNT(*) >= 3"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY spread DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


# ── Round 40 ─────────────────────────────────────────────────────────


@router.get("/entity-betweenness-score")
def entity_betweenness_score():
    """Entities ranked by how many unique partners they bridge (proxy betweenness)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, sub.partners "
            "FROM ("
            "  SELECT entity_id, COUNT(DISTINCT partner) AS partners FROM ("
            "    SELECT entity_a_id AS entity_id, entity_b_id AS partner FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id, entity_a_id FROM entity_connections"
            "  ) GROUP BY entity_id"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY sub.partners DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/document-source-diversity")
def document_source_diversity():
    """Shannon-style source diversity per category: how many distinct sources cover each."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, COUNT(*) AS doc_count, "
            "COUNT(DISTINCT source) AS distinct_sources "
            "FROM documents WHERE category IS NOT NULL AND category != '' "
            "GROUP BY category ORDER BY distinct_sources DESC"
        ).fetchall()
    return {"categories": [dict(r) for r in rows]}


@router.get("/event-burst-detection")
def event_burst_detection():
    """Find months with abnormally high event counts (> 2x average)."""
    with get_db() as conn:
        months = conn.execute(
            "SELECT SUBSTR(event_date, 1, 7) AS month, COUNT(*) AS cnt "
            "FROM events WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 7 "
            "GROUP BY month ORDER BY month"
        ).fetchall()
        if not months:
            return {"avg": 0, "threshold": 0, "bursts": []}
        total = sum(r["cnt"] for r in months)
        avg = total / len(months)
        threshold = avg * 2
        bursts = [dict(r) for r in months if r["cnt"] > threshold]
    return {"avg": round(avg, 2), "threshold": round(threshold, 2), "bursts": bursts}


@router.get("/source-unique-entities")
def source_unique_entities():
    """Entities that appear exclusively in one source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, COUNT(DISTINCT de.entity_id) AS unique_entities "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE de.entity_id IN ("
            "  SELECT de2.entity_id FROM document_entities de2 "
            "  JOIN documents d2 ON d2.id = de2.document_id "
            "  GROUP BY de2.entity_id "
            "  HAVING COUNT(DISTINCT d2.source) = 1"
            ") "
            "AND d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source ORDER BY unique_entities DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-lifecycle-span")
def entity_lifecycle_span():
    """Time span between first and last event appearance for each entity."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, "
            "MIN(ev.event_date) AS first_seen, MAX(ev.event_date) AS last_seen, "
            "CAST(JULIANDAY(MAX(ev.event_date)) - JULIANDAY(MIN(ev.event_date)) AS INTEGER) AS span_days "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN events ev ON ev.document_id = de.document_id "
            "WHERE ev.event_date IS NOT NULL AND LENGTH(ev.event_date) >= 10 "
            "GROUP BY e.id HAVING COUNT(DISTINCT ev.event_date) >= 2 "
            "ORDER BY span_days DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/connection-growth-timeline")
def connection_growth_timeline():
    """Connection count over time based on shared document dates."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(d.date, 1, 7) AS month, "
            "COUNT(DISTINCT ec.entity_a_id || '-' || ec.entity_b_id) AS connections "
            "FROM entity_connections ec "
            "JOIN document_entities de_a ON de_a.entity_id = ec.entity_a_id "
            "JOIN document_entities de_b ON de_b.entity_id = ec.entity_b_id "
            "  AND de_a.document_id = de_b.document_id "
            "JOIN documents d ON d.id = de_a.document_id "
            "WHERE d.date IS NOT NULL AND LENGTH(d.date) >= 7 "
            "GROUP BY month ORDER BY month"
        ).fetchall()
    return {"timeline": [dict(r) for r in rows]}


# ── Round 41 ─────────────────────────────────────────────────────────


@router.get("/entity-source-loyalty")
def entity_source_loyalty():
    """Entities categorized by how many distinct sources they appear in."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(DISTINCT d.source) AS source_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "JOIN documents d ON d.id = de.document_id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY e.id ORDER BY source_count ASC LIMIT 100"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM entities").fetchone()["c"]
        single = conn.execute(
            "SELECT COUNT(*) AS c FROM ("
            "  SELECT de.entity_id FROM document_entities de "
            "  JOIN documents d ON d.id = de.document_id "
            "  WHERE d.source IS NOT NULL AND d.source != '' "
            "  GROUP BY de.entity_id HAVING COUNT(DISTINCT d.source) = 1"
            ")"
        ).fetchone()["c"]
    return {
        "total_entities": total,
        "single_source": single,
        "multi_source": total - single,
        "entities": [dict(r) for r in rows],
    }


@router.get("/document-page-outliers")
def document_page_outliers():
    """Documents with page counts significantly above average."""
    with get_db() as conn:
        stats = conn.execute(
            "SELECT AVG(pages) AS avg_pages, "
            "MAX(pages) AS max_pages, "
            "COUNT(*) AS total "
            "FROM documents WHERE pages IS NOT NULL AND pages > 0"
        ).fetchone()
        avg_p = stats["avg_pages"] or 0
        threshold = avg_p * 3
        outliers = conn.execute(
            "SELECT id, title, filename, source, pages "
            "FROM documents WHERE pages IS NOT NULL AND pages > ? "
            "ORDER BY pages DESC LIMIT 50",
            (threshold,),
        ).fetchall()
    return {
        "avg_pages": round(avg_p, 1),
        "threshold": round(threshold, 1),
        "max_pages": stats["max_pages"] or 0,
        "outliers": [dict(r) for r in outliers],
    }


@router.get("/event-confidence-trend")
def event_confidence_trend():
    """Average event confidence score per month over time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(event_date, 1, 7) AS month, "
            "ROUND(AVG(confidence), 3) AS avg_confidence, "
            "COUNT(*) AS event_count "
            "FROM events "
            "WHERE event_date IS NOT NULL AND LENGTH(event_date) >= 7 "
            "AND confidence IS NOT NULL "
            "GROUP BY month ORDER BY month"
        ).fetchall()
    return {"months": [dict(r) for r in rows]}


@router.get("/source-ingestion-cadence")
def source_ingestion_cadence():
    """How frequently documents from each source were ingested."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) AS doc_count, "
            "MIN(ingested_at) AS first_ingested, "
            "MAX(ingested_at) AS last_ingested "
            "FROM documents "
            "WHERE source IS NOT NULL AND source != '' "
            "AND ingested_at IS NOT NULL "
            "GROUP BY source ORDER BY doc_count DESC"
        ).fetchall()
    return {"sources": [dict(r) for r in rows]}


@router.get("/entity-connection-density")
def entity_connection_density():
    """Ratio of actual connections to possible connections for top entities."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, sub.actual, sub.partners "
            "FROM ("
            "  SELECT entity_id, COUNT(*) AS actual, COUNT(DISTINCT partner) AS partners FROM ("
            "    SELECT entity_a_id AS entity_id, entity_b_id AS partner FROM entity_connections "
            "    UNION ALL "
            "    SELECT entity_b_id, entity_a_id FROM entity_connections"
            "  ) GROUP BY entity_id"
            ") sub "
            "JOIN entities e ON e.id = sub.entity_id "
            "ORDER BY sub.actual DESC LIMIT 100"
        ).fetchall()
    return {"entities": [dict(r) for r in rows]}


@router.get("/connection-temporal-overlap")
def connection_temporal_overlap():
    """Entity pairs that co-occur in documents from the same time periods."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ea.name AS entity_a, eb.name AS entity_b, "
            "COUNT(DISTINCT SUBSTR(d.date, 1, 7)) AS shared_months, "
            "MIN(d.date) AS earliest, MAX(d.date) AS latest "
            "FROM entity_connections ec "
            "JOIN entities ea ON ea.id = ec.entity_a_id "
            "JOIN entities eb ON eb.id = ec.entity_b_id "
            "JOIN document_entities de_a ON de_a.entity_id = ec.entity_a_id "
            "JOIN document_entities de_b ON de_b.entity_id = ec.entity_b_id "
            "  AND de_a.document_id = de_b.document_id "
            "JOIN documents d ON d.id = de_a.document_id "
            "WHERE d.date IS NOT NULL AND LENGTH(d.date) >= 7 "
            "GROUP BY ec.entity_a_id, ec.entity_b_id "
            "HAVING shared_months >= 2 "
            "ORDER BY shared_months DESC LIMIT 100"
        ).fetchall()
    return {"pairs": [dict(r) for r in rows]}


# ── Round 42 ─────────────────────────────────────────────────────────


@router.get("/entity-document-exclusivity")
def entity_document_exclusivity():
    """Entities that appear in only one document."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(de.document_id) AS doc_count "
            "FROM entities e "
            "JOIN document_entities de ON de.entity_id = e.id "
            "GROUP BY e.id HAVING doc_count = 1 "
            "ORDER BY e.name LIMIT 200"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM entities").fetchone()["c"]
    return {
        "total_entities": total,
        "exclusive_count": len(rows),
        "entities": [dict(r) for r in rows],
    }


@router.get("/document-flagged-timeline")
def document_flagged_timeline():
    """Flagged documents grouped by date."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT SUBSTR(date, 1, 7) AS month, COUNT(*) AS flagged_count "
            "FROM documents "
            "WHERE flagged = 1 AND date IS NOT NULL AND LENGTH(date) >= 7 "
            "GROUP BY month ORDER BY month"
        ).fetchall()
        total_flagged = conn.execute(
            "SELECT COUNT(*) AS c FROM documents WHERE flagged = 1"
        ).fetchone()["c"]
    return {"total_flagged": total_flagged, "months": [dict(r) for r in rows]}


@router.get("/event-precision-histogram")
def event_precision_histogram():
    """Distribution of event date precision values."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT COALESCE(precision, 'unknown') AS prec, COUNT(*) AS cnt "
            "FROM events GROUP BY prec ORDER BY cnt DESC"
        ).fetchall()
        total = sum(r["cnt"] for r in rows) if rows else 0
    results = []
    for r in rows:
        results.append(
            {
                "precision": r["prec"],
                "count": r["cnt"],
                "pct": round(r["cnt"] / total * 100, 1) if total else 0,
            }
        )
    return {"total_events": total, "buckets": results}


@router.get("/source-entity-type-mix")
def source_entity_type_mix():
    """Entity type breakdown per source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, e.type, COUNT(DISTINCT e.id) AS entity_count "
            "FROM document_entities de "
            "JOIN documents d ON d.id = de.document_id "
            "JOIN entities e ON e.id = de.entity_id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source, e.type ORDER BY d.source, entity_count DESC"
        ).fetchall()
    sources = {}
    for r in rows:
        s = r["source"]
        if s not in sources:
            sources[s] = []
        sources[s].append({"type": r["type"], "count": r["entity_count"]})
    return {"sources": [{"source": k, "types": v} for k, v in sources.items()]}


@router.get("/entity-alias-chain")
def entity_alias_chain():
    """Entities with most aliases and their alias names."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT e.id, e.name, e.type, COUNT(ea.id) AS alias_count "
            "FROM entities e "
            "JOIN entity_aliases ea ON ea.entity_id = e.id "
            "GROUP BY e.id ORDER BY alias_count DESC LIMIT 50"
        ).fetchall()
        result = []
        for r in rows:
            aliases = conn.execute(
                "SELECT alias_name FROM entity_aliases WHERE entity_id = ?",
                (r["id"],),
            ).fetchall()
            result.append(
                {
                    "id": r["id"],
                    "name": r["name"],
                    "type": r["type"],
                    "alias_count": r["alias_count"],
                    "aliases": [a["alias_name"] for a in aliases],
                }
            )
    return {"entities": result}


@router.get("/connection-weight-percentile")
def connection_weight_percentile():
    """Connection weights bucketed by percentile ranges."""
    with get_db() as conn:
        rows = conn.execute("SELECT weight FROM entity_connections ORDER BY weight").fetchall()
    if not rows:
        return {"total": 0, "percentiles": []}
    weights = [r["weight"] for r in rows]
    total = len(weights)
    buckets = [
        {"label": "0-25%", "start": 0, "end": total // 4},
        {"label": "25-50%", "start": total // 4, "end": total // 2},
        {"label": "50-75%", "start": total // 2, "end": 3 * total // 4},
        {"label": "75-100%", "start": 3 * total // 4, "end": total},
    ]
    result = []
    for b in buckets:
        segment = weights[b["start"] : b["end"]]
        if segment:
            result.append(
                {
                    "label": b["label"],
                    "count": len(segment),
                    "min_weight": min(segment),
                    "max_weight": max(segment),
                    "avg_weight": round(sum(segment) / len(segment), 2),
                }
            )
    return {"total": total, "percentiles": result}


# ═══════════════════════════════════════════
# VISUALIZATION
# ═══════════════════════════════════════════


@router.get("/visualization/timeline")
def visualization_timeline(
    min_confidence: float = Query(0.5, ge=0.0, le=1.0),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Events grouped by month with entity associations and risk scores."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT ev.id, ev.event_date, ev.precision, ev.confidence, ev.context,
                   ev.document_id, d.title as doc_title, d.category,
                   MAX(df.score) as max_risk
            FROM events ev
            JOIN documents d ON d.id = ev.document_id
            LEFT JOIN document_forensics df ON df.document_id = ev.document_id
            WHERE ev.event_date IS NOT NULL AND ev.confidence >= ?
            GROUP BY ev.id
            ORDER BY ev.event_date
            LIMIT ?
        """,
            (min_confidence, limit),
        ).fetchall()

        # Group by month
        months: dict = {}
        for r in rows:
            date_str = r["event_date"]
            month_key = date_str[:7] if date_str and len(date_str) >= 7 else "unknown"
            if month_key not in months:
                months[month_key] = {"month": month_key, "events": [], "event_count": 0}
            months[month_key]["events"].append(
                {
                    "id": r["id"],
                    "date": r["event_date"],
                    "confidence": r["confidence"],
                    "context": r["context"],
                    "document_id": r["document_id"],
                    "doc_title": r["doc_title"],
                    "risk_score": r["max_risk"],
                }
            )
            months[month_key]["event_count"] += 1

    timeline = sorted(months.values(), key=lambda m: m["month"])
    return {"months": timeline, "total_events": len(rows)}


@router.get("/visualization/entity-timeline/{entity_id}")
def visualization_entity_timeline(entity_id: int):
    """Timeline of events associated with a specific entity."""
    with get_db() as conn:
        entity = conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not entity:
            raise HTTPException(404, "Entity not found")

        rows = conn.execute(
            """
            SELECT ev.id, ev.event_date, ev.precision, ev.confidence, ev.context,
                   ev.document_id, d.title as doc_title
            FROM events ev
            JOIN documents d ON d.id = ev.document_id
            JOIN document_entities de ON de.document_id = ev.document_id
            WHERE de.entity_id = ? AND ev.event_date IS NOT NULL
            ORDER BY ev.event_date
        """,
            (entity_id,),
        ).fetchall()

    events = [
        {
            "id": r["id"],
            "date": r["event_date"],
            "precision": r["precision"],
            "confidence": r["confidence"],
            "context": r["context"],
            "document_id": r["document_id"],
            "doc_title": r["doc_title"],
        }
        for r in rows
    ]
    return {
        "entity": {"id": entity["id"], "name": entity["name"], "type": entity["type"]},
        "events": events,
        "event_count": len(events),
    }


# ── PDF Metadata / Provenance ───────────────────────────────────


@router.get("/documents/{doc_id}/pdf-metadata")
def get_document_pdf_metadata(doc_id: int):
    """Get PDF metadata for a single document."""
    from dossier.forensics.provenance import get_pdf_metadata

    with get_db() as conn:
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        meta = get_pdf_metadata(conn, doc_id)

    return {"document_id": doc_id, "pdf_metadata": meta}


@router.get("/pdf-metadata/stats")
def pdf_metadata_stats():
    """Corpus-wide PDF metadata statistics."""
    from dossier.forensics.provenance import get_corpus_metadata_stats

    with get_db() as conn:
        stats = get_corpus_metadata_stats(conn)

    return stats


@router.get("/pdf-metadata/search")
def pdf_metadata_search(
    author: Optional[str] = Query(None),
    creator: Optional[str] = Query(None),
    producer: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    """Search PDF metadata by author, creator, or producer."""
    from dossier.forensics.provenance import search_pdf_metadata

    with get_db() as conn:
        results = search_pdf_metadata(
            conn, author=author, creator=creator, producer=producer, limit=limit
        )

    return {"results": results, "count": len(results)}


@router.get("/pdf-metadata/timeline")
def pdf_metadata_timeline():
    """Creation/modification date timeline for metadata forensics."""
    from dossier.forensics.provenance import get_metadata_timeline

    with get_db() as conn:
        entries = get_metadata_timeline(conn)

    return {"entries": entries, "count": len(entries)}


@router.post("/pdf-metadata/extract-all")
def pdf_metadata_extract_all(force: bool = Query(False)):
    """Bulk-extract PDF metadata for all PDF documents in the corpus.

    By default, skips documents that already have metadata.
    Set force=true to re-extract for all PDFs.
    """
    from dossier.forensics.provenance import (
        _ensure_pdf_metadata_table,
        extract_pdf_metadata,
        store_pdf_metadata,
    )

    with get_db() as conn:
        _ensure_pdf_metadata_table(conn)

        if force:
            rows = conn.execute(
                "SELECT id, filepath FROM documents WHERE LOWER(filepath) LIKE '%.pdf'"
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT d.id, d.filepath FROM documents d
                LEFT JOIN document_pdf_metadata pm ON pm.document_id = d.id
                WHERE LOWER(d.filepath) LIKE '%.pdf' AND pm.id IS NULL
                """
            ).fetchall()

    extracted = 0
    skipped = 0
    errors = []

    for row in rows:
        doc_id = row["id"]
        filepath = row["filepath"]
        meta = extract_pdf_metadata(filepath, document_id=doc_id)
        if meta:
            with get_db() as conn:
                _ensure_pdf_metadata_table(conn)
                store_pdf_metadata(conn, meta)
                conn.commit()
            extracted += 1
        else:
            skipped += 1
            errors.append({"document_id": doc_id, "filepath": filepath})

    return {
        "extracted": extracted,
        "skipped": skipped,
        "total_pdfs": len(rows),
        "errors": errors[:50],
    }
