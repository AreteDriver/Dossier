"""DOSSIER — Search, keywords, connections, stats, dashboard routes."""

import re
from typing import Optional

from fastapi import APIRouter, Query

from dossier.api import utils
from dossier.db.database import get_db

router = APIRouter()


@router.get("/search")
def search_documents(
    q: str = Query("", description="Search query"),
    category: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Full-text search across all documents with optional filters."""
    with get_db() as conn:
        if q.strip():
            # FTS5 search with snippet generation
            # Strip all FTS5 metacharacters to prevent query injection
            fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", q.strip())
            fts_query = fts_query.strip()

            sql = """
                SELECT
                    d.id, d.filename, d.title, d.category, d.source, d.date,
                    d.pages, d.flagged, d.ingested_at,
                    snippet(documents_fts, 1, '<mark>', '</mark>', '...', 40) as excerpt,
                    rank
                FROM documents_fts
                JOIN documents d ON d.id = documents_fts.rowid
                WHERE documents_fts MATCH ?
            """
            params = [f'"{fts_query}"']

            if category:
                sql += " AND d.category = ?"
                params.append(category)

            sql += " ORDER BY rank LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = conn.execute(sql, params).fetchall()
        else:
            # No search query — list all documents
            sql = "SELECT id, filename, title, category, source, date, pages, flagged, ingested_at FROM documents WHERE 1=1"
            params = []

            if category:
                sql += " AND category = ?"
                params.append(category)

            sql += " ORDER BY ingested_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = conn.execute(sql, params).fetchall()

        results = []
        for row in rows:
            doc = dict(row)
            # Get entities for this document
            doc["entities"] = utils._get_doc_entities(conn, doc["id"])
            # Generate excerpt if not from FTS
            if "excerpt" not in doc or not doc.get("excerpt"):
                raw = conn.execute(
                    "SELECT raw_text FROM documents WHERE id = ?", (doc["id"],)
                ).fetchone()
                doc["excerpt"] = (raw["raw_text"][:300] + "...") if raw and raw["raw_text"] else ""
            results.append(doc)

        # Get total count
        if q.strip():
            total = len(results)  # FTS doesn't easily give total
        else:
            count_sql = "SELECT COUNT(*) as cnt FROM documents"
            count_params = []
            if category:
                count_sql += " WHERE category = ?"
                count_params.append(category)
            total = conn.execute(count_sql, count_params).fetchone()["cnt"]

    return {"results": results, "total": total, "query": q, "offset": offset, "limit": limit}


@router.get("/keywords")
def list_keywords(limit: int = Query(30, ge=1, le=200)):
    """Top keywords by total occurrence across all documents."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT word, total_count, doc_count
            FROM keywords
            ORDER BY total_count DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

    return {"keywords": [dict(r) for r in rows]}


@router.get("/connections")
def get_connections(entity_id: Optional[int] = None, min_weight: int = 1, limit: int = 50):
    """Get entity co-occurrence network. Optionally centered on a specific entity."""
    with get_db() as conn:
        if entity_id:
            rows = conn.execute(
                """
                SELECT
                    ea.name as source_name, ea.type as source_type,
                    eb.name as target_name, eb.type as target_type,
                    ec.weight
                FROM entity_connections ec
                JOIN entities ea ON ea.id = ec.entity_a_id
                JOIN entities eb ON eb.id = ec.entity_b_id
                WHERE (ec.entity_a_id = ? OR ec.entity_b_id = ?)
                  AND ec.weight >= ?
                ORDER BY ec.weight DESC
                LIMIT ?
            """,
                (entity_id, entity_id, min_weight, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    ea.name as source_name, ea.type as source_type,
                    eb.name as target_name, eb.type as target_type,
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

    return {"connections": [dict(r) for r in rows]}


@router.get("/stats")
def get_stats():
    """Dashboard statistics."""
    with get_db() as conn:
        doc_count = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]
        entity_count = conn.execute("SELECT COUNT(*) as cnt FROM entities").fetchone()["cnt"]
        page_count = conn.execute(
            "SELECT COALESCE(SUM(pages), 0) as cnt FROM documents"
        ).fetchone()["cnt"]
        flagged_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM documents WHERE flagged = 1"
        ).fetchone()["cnt"]

        # Category breakdown
        categories = conn.execute("""
            SELECT category, COUNT(*) as count
            FROM documents
            GROUP BY category
            ORDER BY count DESC
        """).fetchall()

        # Entity type breakdown
        entity_types = conn.execute("""
            SELECT type, COUNT(*) as count
            FROM entities
            GROUP BY type
            ORDER BY count DESC
        """).fetchall()

    return {
        "documents": doc_count,
        "entities": entity_count,
        "pages": page_count,
        "flagged": flagged_count,
        "categories": {r["category"]: r["count"] for r in categories},
        "entity_types": {r["type"]: r["count"] for r in entity_types},
    }


@router.get("/dashboard")
def dashboard_summary():
    """Comprehensive dashboard data in one call."""
    with get_db() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        page_count = conn.execute("SELECT COALESCE(SUM(pages),0) FROM documents").fetchone()[0]
        flagged_count = conn.execute("SELECT COUNT(*) FROM documents WHERE flagged = 1").fetchone()[
            0
        ]

        # Recent documents
        recent = conn.execute("""
            SELECT id, filename, title, category, source, date, pages, ingested_at
            FROM documents ORDER BY ingested_at DESC LIMIT 8
        """).fetchall()

        # Top risk alerts
        risk_alerts = conn.execute("""
            SELECT df.document_id, df.score as risk_score, d.title, d.filename, d.category, d.source
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score >= 0.7
            ORDER BY df.score DESC LIMIT 8
        """).fetchall()

        # Source breakdown
        sources = conn.execute("""
            SELECT source, COUNT(*) as count, SUM(pages) as pages
            FROM documents WHERE source IS NOT NULL AND source != ''
            GROUP BY source ORDER BY count DESC
        """).fetchall()

        # Entity resolution stats
        resolved_count = conn.execute("SELECT COUNT(*) FROM entity_resolutions").fetchone()[0]

        # AML summary
        aml_count = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM document_forensics WHERE analysis_type = 'aml_flag'"
        ).fetchone()[0]

        # Timeline event count
        try:
            event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        except Exception:
            event_count = 0

        # Category breakdown
        categories = conn.execute("""
            SELECT category, COUNT(*) as count FROM documents GROUP BY category ORDER BY count DESC
        """).fetchall()

        # Notes count
        notes_count = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE notes IS NOT NULL AND notes != ''"
        ).fetchone()[0]

    return {
        "documents": doc_count,
        "entities": entity_count,
        "pages": page_count,
        "flagged": flagged_count,
        "aml_flagged": aml_count,
        "resolved_entities": resolved_count,
        "timeline_events": event_count,
        "notes_count": notes_count,
        "recent_documents": [dict(r) for r in recent],
        "risk_alerts": [dict(r) for r in risk_alerts],
        "sources": [dict(r) for r in sources],
        "categories": {r["category"]: r["count"] for r in categories},
    }


@router.get("/search/advanced")
def advanced_search(
    q: str = Query(""),
    category: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    entity_name: Optional[str] = Query(None),
    flagged_only: bool = Query(False),
    min_risk: Optional[float] = Query(None),
    sort_by: str = Query("relevance"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Advanced search with multiple filter combinations."""
    with get_db() as conn:
        conditions = ["1=1"]
        params: list = []
        joins: list[str] = []

        if q.strip():
            fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", q.strip()).strip()
            joins.append("JOIN documents_fts ON documents_fts.rowid = d.id")
            conditions.append("documents_fts MATCH ?")
            params.append(f'"{fts_query}"')

        if category:
            conditions.append("d.category = ?")
            params.append(category)
        if source:
            conditions.append("d.source = ?")
            params.append(source)
        if date_from:
            conditions.append("d.date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("d.date <= ?")
            params.append(date_to)
        if flagged_only:
            conditions.append("d.flagged = 1")

        if entity_name:
            joins.append(
                "JOIN document_entities de_f ON de_f.document_id = d.id "
                "JOIN entities e_f ON e_f.id = de_f.entity_id"
            )
            conditions.append("LOWER(e_f.name) LIKE LOWER(?)")
            params.append(f"%{entity_name}%")

        if min_risk is not None:
            joins.append(
                "JOIN document_forensics df_r ON df_r.document_id = d.id "
                "AND df_r.analysis_type = 'risk_score'"
            )
            conditions.append("df_r.score >= ?")
            params.append(min_risk)

        order = "d.ingested_at DESC"
        if sort_by == "date":
            order = "d.date DESC"
        elif sort_by == "pages":
            order = "d.pages DESC"
        elif sort_by == "relevance" and q.strip():
            order = "rank"

        join_str = " ".join(joins)
        where_str = " AND ".join(conditions)

        sql = f"""
            SELECT DISTINCT d.id, d.filename, d.title, d.category, d.source,
                   d.date, d.pages, d.flagged, d.ingested_at
            FROM documents d {join_str}
            WHERE {where_str}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        rows = conn.execute(sql, params).fetchall()

        results = []
        for row in rows:
            doc = dict(row)
            doc["entities"] = utils._get_doc_entities(conn, doc["id"])
            raw = conn.execute(
                "SELECT raw_text FROM documents WHERE id = ?", (doc["id"],)
            ).fetchone()
            doc["excerpt"] = (raw["raw_text"][:300] + "...") if raw and raw["raw_text"] else ""
            results.append(doc)

        count_params = params[:-2]
        total = conn.execute(
            f"SELECT COUNT(DISTINCT d.id) FROM documents d {join_str} WHERE {where_str}",
            count_params,
        ).fetchone()[0]

    return {"results": results, "total": total, "offset": offset, "limit": limit}
