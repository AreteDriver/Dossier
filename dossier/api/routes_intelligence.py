"""DOSSIER — Intelligence analysis, patterns, AI routes."""

import re
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request

from dossier.api import utils
from dossier.db.database import get_db

router = APIRouter()


# ═══════════════════════════════════════════
# AI ROUTES (Ollama LLM)
# ═══════════════════════════════════════════


@router.post("/ai/summarize")
async def ai_summarize(request: Request):
    """Summarize a document using local LLM."""
    body = await request.json()
    doc_id = body.get("doc_id")
    if not doc_id:
        raise HTTPException(400, "doc_id required")

    with get_db() as conn:
        row = conn.execute(
            "SELECT title, raw_text FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
        text = (row["raw_text"] or "")[:8000]

    prompt = (
        "Summarize the following document concisely. Focus on key facts, names, "
        "dates, locations, and significant findings.\n\n"
        f"Document: {row['title'] or 'Untitled'}\n\n{text}\n\nSummary:"
    )
    summary = utils._ollama_generate(prompt)
    return {"doc_id": doc_id, "summary": summary.strip(), "model": "qwen2.5:14b"}


@router.post("/ai/ask")
async def ai_ask(request: Request):
    """Answer a question about the corpus using local LLM."""
    body = await request.json()
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(400, "question required")

    with get_db() as conn:
        fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", question).strip()
        rows = []
        if fts_query:
            rows = conn.execute(
                """
                SELECT d.id, d.title,
                       snippet(documents_fts, 1, '', '', '...', 80) as excerpt
                FROM documents_fts
                JOIN documents d ON d.id = documents_fts.rowid
                WHERE documents_fts MATCH ?
                ORDER BY rank LIMIT 5
            """,
                [f'"{fts_query}"'],
            ).fetchall()

    context = "\n\n".join(f"[{r['title']}]: {r['excerpt']}" for r in rows)
    prompt = (
        "You are analyzing a corpus of legal documents. Answer the question "
        "based on the document context provided. Be specific and cite document "
        "titles when possible.\n\n"
        f"Context:\n{context[:6000]}\n\nQuestion: {question}\n\nAnswer:"
    )
    answer = utils._ollama_generate(prompt, max_tokens=1500)
    sources = [{"id": r["id"], "title": r["title"]} for r in rows]
    return {
        "question": question,
        "answer": answer.strip(),
        "sources": sources,
        "model": "qwen2.5:14b",
    }


# ═══════════════════════════════════════════
# DUPLICATE DETECTION
# ═══════════════════════════════════════════


@router.get("/duplicates")
def detect_duplicates(
    threshold: float = Query(0.6, ge=0.1, le=1.0),
    limit: int = Query(50, ge=1, le=200),
):
    """Find potential duplicate document pairs based on title similarity and shared entities."""
    with get_db() as conn:
        # Find document pairs with very similar entity fingerprints
        pairs = conn.execute(
            """
            SELECT d1.id as id_a, d1.title as title_a, d1.filename as filename_a,
                   d1.category as category_a, d1.pages as pages_a,
                   d2.id as id_b, d2.title as title_b, d2.filename as filename_b,
                   d2.category as category_b, d2.pages as pages_b,
                   COUNT(DISTINCT de1.entity_id) as shared_entities,
                   (SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = d1.id) as total_a,
                   (SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = d2.id) as total_b
            FROM document_entities de1
            JOIN document_entities de2 ON de1.entity_id = de2.entity_id AND de2.document_id > de1.document_id
            JOIN documents d1 ON d1.id = de1.document_id
            JOIN documents d2 ON d2.id = de2.document_id
            WHERE de1.document_id < de2.document_id
            GROUP BY de1.document_id, de2.document_id
            HAVING CAST(COUNT(DISTINCT de1.entity_id) AS REAL) /
                   MAX(1, MIN(
                     (SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = d1.id),
                     (SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = d2.id)
                   )) >= ?
            ORDER BY shared_entities DESC
            LIMIT ?
        """,
            (threshold, limit),
        ).fetchall()

        results = []
        for r in pairs:
            d = dict(r)
            min_total = min(d["total_a"] or 1, d["total_b"] or 1)
            d["similarity"] = round(d["shared_entities"] / max(min_total, 1), 3)
            results.append(d)

    return {"duplicates": results, "threshold": threshold}


@router.post("/duplicates/dismiss")
async def dismiss_duplicate(request: Request):
    """Dismiss a duplicate pair (store in a dismissals table)."""
    body = await request.json()
    id_a = body.get("id_a")
    id_b = body.get("id_b")
    if not id_a or not id_b:
        raise HTTPException(400, "id_a and id_b required")

    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_dismissals (
                id_a INTEGER NOT NULL,
                id_b INTEGER NOT NULL,
                dismissed_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (id_a, id_b)
            )
        """)
        lo, hi = min(id_a, id_b), max(id_a, id_b)
        conn.execute(
            "INSERT OR IGNORE INTO duplicate_dismissals (id_a, id_b) VALUES (?, ?)",
            (lo, hi),
        )
    return {"dismissed": True, "id_a": lo, "id_b": hi}


# ═══════════════════════════════════════════
# DOCUMENT CLUSTERS (keyword fingerprints)
# ═══════════════════════════════════════════


@router.get("/clusters")
def document_clusters(
    min_cluster_size: int = Query(3, ge=2, le=20),
    limit: int = Query(20, ge=1, le=50),
):
    """Auto-group documents by shared keyword/entity fingerprints."""
    with get_db() as conn:
        # Build a doc-keyword matrix using top keywords per doc
        docs = conn.execute("""
            SELECT dk.document_id, k.word, dk.count
            FROM document_keywords dk
            JOIN keywords k ON k.id = dk.keyword_id
            ORDER BY dk.document_id, dk.count DESC
        """).fetchall()

        # Build keyword vectors
        doc_keywords: dict[int, dict[str, int]] = {}
        for r in docs:
            did = r["document_id"]
            if did not in doc_keywords:
                doc_keywords[did] = {}
            if len(doc_keywords[did]) < 30:  # Top 30 keywords per doc
                doc_keywords[did][r["word"]] = r["count"]

        # Simple clustering: group docs by their top keyword
        keyword_groups: dict[str, list[int]] = {}
        for did, kws in doc_keywords.items():
            if kws:
                top_kw = max(kws, key=kws.get)
                keyword_groups.setdefault(top_kw, []).append(did)

        # Filter to clusters with enough members and get doc details
        clusters = []
        for kw, doc_ids in sorted(keyword_groups.items(), key=lambda x: -len(x[1])):
            if len(doc_ids) < min_cluster_size:
                continue
            if len(clusters) >= limit:
                break

            ph = ",".join("?" * min(len(doc_ids), 10))
            sample_ids = doc_ids[:10]
            doc_rows = conn.execute(
                f"""
                SELECT id, title, filename, category, source, pages
                FROM documents WHERE id IN ({ph})
            """,
                sample_ids,
            ).fetchall()

            # Get shared entities across cluster
            all_ph = ",".join("?" * len(doc_ids))
            shared_entities = conn.execute(
                f"""
                SELECT e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count
                FROM document_entities de
                JOIN entities e ON e.id = de.entity_id
                WHERE de.document_id IN ({all_ph})
                GROUP BY e.id
                HAVING COUNT(DISTINCT de.document_id) >= ?
                ORDER BY doc_count DESC LIMIT 10
            """,
                doc_ids + [max(2, len(doc_ids) // 3)],
            ).fetchall()

            clusters.append(
                {
                    "keyword": kw,
                    "size": len(doc_ids),
                    "documents": [dict(r) for r in doc_rows],
                    "shared_entities": [dict(e) for e in shared_entities],
                }
            )

    return {"clusters": clusters, "total": len(clusters)}


# ═══════════════════════════════════════════
# PATTERN DETECTION
# ═══════════════════════════════════════════


@router.get("/patterns")
def detect_patterns(
    window_days: int = Query(30, ge=7, le=365),
    min_occurrences: int = Query(3, ge=2, le=20),
):
    """Detect recurring behavioral patterns across time windows."""
    with get_db() as conn:
        # 1. Recurring entity co-appearances via shared documents per date
        coappearance = conn.execute(
            """
            WITH dated_entities AS (
                SELECT DISTINCT de.entity_id, e.name, e.type, ev.event_date
                FROM events ev
                JOIN document_entities de ON de.document_id = ev.document_id
                JOIN entities e ON e.id = de.entity_id
                WHERE ev.confidence >= 0.5
                  AND ev.event_date IS NOT NULL AND LENGTH(ev.event_date) >= 10
                  AND e.type IN ('person', 'place', 'org')
            )
            SELECT d1.name as entity_a, d1.type as type_a,
                   d2.name as entity_b, d2.type as type_b,
                   COUNT(DISTINCT d1.event_date) as co_dates,
                   MIN(d1.event_date) as first_seen,
                   MAX(d1.event_date) as last_seen
            FROM dated_entities d1
            JOIN dated_entities d2 ON d1.event_date = d2.event_date
              AND d1.entity_id < d2.entity_id
            WHERE d1.type = 'person'
            GROUP BY d1.entity_id, d2.entity_id
            HAVING co_dates >= ?
            ORDER BY co_dates DESC
            LIMIT 30
        """,
            (min_occurrences,),
        ).fetchall()

        # 2. Entity activity bursts — entities with event clusters
        bursts = conn.execute(
            """
            SELECT e.name, e.type, e.id as entity_id,
                   SUBSTR(ev.event_date, 1, 7) as month,
                   COUNT(*) as event_count
            FROM events ev
            JOIN document_entities de ON de.document_id = ev.document_id
            JOIN entities e ON e.id = de.entity_id
            WHERE e.type = 'person' AND ev.event_date IS NOT NULL
              AND LENGTH(ev.event_date) >= 7 AND ev.confidence >= 0.5
            GROUP BY e.id, month
            HAVING COUNT(*) >= ?
            ORDER BY event_count DESC
            LIMIT 40
        """,
            (min_occurrences,),
        ).fetchall()

        # 3. Document category sequences — same entity appearing in docs of different categories
        category_patterns = conn.execute("""
            SELECT e.name, e.type, e.id as entity_id,
                   GROUP_CONCAT(DISTINCT d.category) as categories,
                   COUNT(DISTINCT d.category) as category_count,
                   COUNT(DISTINCT d.id) as doc_count
            FROM document_entities de
            JOIN entities e ON e.id = de.entity_id
            JOIN documents d ON d.id = de.document_id
            WHERE e.type IN ('person', 'org')
            GROUP BY e.id
            HAVING COUNT(DISTINCT d.category) >= 3
            ORDER BY category_count DESC, doc_count DESC
            LIMIT 20
        """).fetchall()

    return {
        "co_appearances": [dict(r) for r in coappearance],
        "activity_bursts": [dict(r) for r in bursts],
        "cross_category": [dict(r) for r in category_patterns],
    }


# ═══════════════════════════════════════════
# LINK ANALYSIS (CENTRALITY METRICS)
# ═══════════════════════════════════════════


@router.get("/link-analysis")
def link_analysis(
    min_connections: int = Query(3, ge=1, le=50),
    limit: int = Query(50, ge=10, le=200),
):
    """Compute centrality metrics for entity network."""
    with get_db() as conn:
        # Build adjacency from co-occurring entities
        edges = conn.execute(
            """
            SELECT de1.entity_id as src, de2.entity_id as dst, COUNT(*) as weight
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
              AND de1.entity_id < de2.entity_id
            GROUP BY de1.entity_id, de2.entity_id
            HAVING weight >= ?
            ORDER BY weight DESC
            LIMIT 5000
        """,
            (min_connections,),
        ).fetchall()

        if not edges:
            return {"entities": [], "edge_count": 0}

        # Compute degree centrality manually
        degree = {}
        for e in edges:
            degree[e["src"]] = degree.get(e["src"], 0) + e["weight"]
            degree[e["dst"]] = degree.get(e["dst"], 0) + e["weight"]

        # Get top entities by degree
        top_ids = sorted(degree, key=degree.get, reverse=True)[:limit]
        if not top_ids:
            return {"entities": [], "edge_count": len(edges)}

        placeholders = ",".join("?" * len(top_ids))
        entities = conn.execute(
            f"SELECT id, name, type FROM entities WHERE id IN ({placeholders})", top_ids
        ).fetchall()

        entity_map = {e["id"]: dict(e) for e in entities}
        max_degree = max(degree.values()) if degree else 1

        result = []
        for eid in top_ids:
            if eid in entity_map:
                ent = entity_map[eid]
                ent["degree"] = degree[eid]
                ent["degree_normalized"] = round(degree[eid] / max_degree, 4)
                # Count unique connections
                connections = set()
                for e in edges:
                    if e["src"] == eid:
                        connections.add(e["dst"])
                    elif e["dst"] == eid:
                        connections.add(e["src"])
                ent["connection_count"] = len(connections)
                result.append(ent)

    return {"entities": result, "edge_count": len(edges)}


# ═══════════════════════════════════════════
# COMMUNICATION FLOW
# ═══════════════════════════════════════════


@router.get("/communication-flow")
def communication_flow(
    entity_id: int = Query(None),
    limit: int = Query(50, ge=10, le=200),
):
    """Analyze entity-to-entity communication patterns from correspondence docs."""
    with get_db() as conn:
        base_query = """
            SELECT e1.id as source_id, e1.name as source_name, e1.type as source_type,
                   e2.id as target_id, e2.name as target_name, e2.type as target_type,
                   COUNT(DISTINCT de1.document_id) as doc_count,
                   GROUP_CONCAT(DISTINCT d.category) as categories,
                   MIN(d.date) as first_contact,
                   MAX(d.date) as last_contact
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
              AND de1.entity_id < de2.entity_id
            JOIN entities e1 ON e1.id = de1.entity_id
            JOIN entities e2 ON e2.id = de2.entity_id
            JOIN documents d ON d.id = de1.document_id
            WHERE e1.type = 'person' AND e2.type = 'person'
        """
        params = []
        if entity_id:
            base_query += " AND (de1.entity_id = ? OR de2.entity_id = ?)"
            params.extend([entity_id, entity_id])

        base_query += """
            GROUP BY e1.id, e2.id
            HAVING doc_count >= 2
            ORDER BY doc_count DESC
            LIMIT ?
        """
        params.append(limit)

        flows = conn.execute(base_query, params).fetchall()

        # Get top communicators
        communicators = {}
        for f in flows:
            fd = dict(f)
            for key in ["source_id", "target_id"]:
                eid = fd[key]
                if eid not in communicators:
                    communicators[eid] = {
                        "id": eid,
                        "name": fd["source_name"] if key == "source_id" else fd["target_name"],
                        "connections": 0,
                        "total_docs": 0,
                    }
                communicators[eid]["connections"] += 1
                communicators[eid]["total_docs"] += fd["doc_count"]

        top_communicators = sorted(
            communicators.values(), key=lambda x: x["total_docs"], reverse=True
        )[:20]

    return {
        "flows": [dict(f) for f in flows],
        "top_communicators": top_communicators,
        "total_flows": len(flows),
    }


# ═══════════════════════════════════════════
# FINANCIAL TRAIL TRACKER
# ═══════════════════════════════════════════


@router.get("/financial-trail")
def financial_trail(
    limit: int = Query(50, ge=10, le=200),
):
    """Track financial indicators, amounts, and entity connections."""
    with get_db() as conn:
        # Financial indicators from forensics
        indicators = conn.execute(
            """
            SELECT fi.id, fi.document_id, fi.indicator_type, fi.value,
                   fi.context, fi.risk_score,
                   d.title, d.filename, d.category
            FROM financial_indicators fi
            JOIN documents d ON d.id = fi.document_id
            ORDER BY fi.risk_score DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        # Entities associated with financial documents
        financial_entities = conn.execute("""
            SELECT e.id, e.name, e.type,
                   COUNT(DISTINCT fi.document_id) as financial_doc_count,
                   COUNT(DISTINCT fi.id) as indicator_count
            FROM financial_indicators fi
            JOIN document_entities de ON de.document_id = fi.document_id
            JOIN entities e ON e.id = de.entity_id
            WHERE e.type = 'person'
            GROUP BY e.id
            ORDER BY indicator_count DESC
            LIMIT 30
        """).fetchall()

        # Aggregate by indicator type
        by_type = conn.execute("""
            SELECT indicator_type, COUNT(*) as count,
                   AVG(risk_score) as avg_risk
            FROM financial_indicators
            GROUP BY indicator_type
            ORDER BY count DESC
        """).fetchall()

    return {
        "indicators": [dict(i) for i in indicators],
        "financial_entities": [dict(e) for e in financial_entities],
        "by_type": [dict(t) for t in by_type],
    }


# ═══════════════════════════════════════════
# WITNESS / DEPONENT INDEX
# ═══════════════════════════════════════════


@router.get("/witness-index")
def witness_index(limit: int = Query(50, ge=10, le=200)):
    """Index of witnesses/deponents extracted from deposition documents."""
    with get_db() as conn:
        # People who appear in depositions
        witnesses = conn.execute(
            """
            SELECT e.id, e.name, e.type,
                   COUNT(DISTINCT d.id) as deposition_count,
                   GROUP_CONCAT(DISTINCT d.id) as doc_ids,
                   GROUP_CONCAT(DISTINCT d.title) as doc_titles
            FROM document_entities de
            JOIN entities e ON e.id = de.entity_id
            JOIN documents d ON d.id = de.document_id
            WHERE e.type = 'person'
              AND d.category IN ('deposition', 'legal', 'report')
            GROUP BY e.id
            ORDER BY deposition_count DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        result = []
        for w in witnesses:
            wd = dict(w)
            # Get co-deponents (people who appear in the same depositions)
            doc_ids = [int(x) for x in (wd["doc_ids"] or "").split(",") if x]
            if doc_ids:
                placeholders = ",".join("?" * len(doc_ids))
                co_deponents = conn.execute(
                    f"""
                    SELECT e.id, e.name, COUNT(DISTINCT de.document_id) as shared
                    FROM document_entities de
                    JOIN entities e ON e.id = de.entity_id
                    WHERE de.document_id IN ({placeholders})
                      AND e.type = 'person' AND e.id != ?
                    GROUP BY e.id
                    ORDER BY shared DESC
                    LIMIT 10
                """,
                    doc_ids + [wd["id"]],
                ).fetchall()
                wd["co_deponents"] = [dict(c) for c in co_deponents]
            else:
                wd["co_deponents"] = []
            wd["doc_ids"] = doc_ids[:20]
            result.append(wd)

    return {"witnesses": result}


# ═══════════════════════════════════════════
# DOCUMENT GAPS (temporal)
# ═══════════════════════════════════════════


@router.get("/document-gaps")
def document_gaps(min_gap_days: int = Query(30)):
    """Find temporal gaps in the document record."""
    with get_db() as conn:
        # Get all dated documents sorted
        dated_docs = conn.execute(
            """SELECT id, title, filename, date, category, source
               FROM documents
               WHERE date IS NOT NULL AND date != ''
               ORDER BY date"""
        ).fetchall()

        if len(dated_docs) < 2:
            return {"gaps": [], "coverage": {}, "undated_count": 0}

        # Find gaps
        gaps = []
        for i in range(len(dated_docs) - 1):
            d1 = dated_docs[i]
            d2 = dated_docs[i + 1]
            try:
                from datetime import datetime

                dt1 = datetime.fromisoformat(d1["date"][:10])
                dt2 = datetime.fromisoformat(d2["date"][:10])
                delta = (dt2 - dt1).days
                if delta >= min_gap_days:
                    gaps.append(
                        {
                            "gap_days": delta,
                            "start_date": d1["date"][:10],
                            "end_date": d2["date"][:10],
                            "before_doc": {"id": d1["id"], "title": d1["title"] or d1["filename"]},
                            "after_doc": {"id": d2["id"], "title": d2["title"] or d2["filename"]},
                        }
                    )
            except (ValueError, TypeError):
                continue

        gaps.sort(key=lambda g: g["gap_days"], reverse=True)

        # Coverage summary by year
        year_counts = {}
        for d in dated_docs:
            try:
                yr = d["date"][:4]
                year_counts[yr] = year_counts.get(yr, 0) + 1
            except (TypeError, IndexError):
                continue

        undated = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE date IS NULL OR date = ''"
        ).fetchone()[0]

    return {
        "gaps": gaps[:50],
        "coverage": dict(sorted(year_counts.items())),
        "undated_count": undated,
        "total_dated": len(dated_docs),
    }


# ═══════════════════════════════════════════
# CORROBORATION ENGINE
# ═══════════════════════════════════════════


@router.get("/corroboration")
def corroboration(min_shared: int = Query(2)):
    """Find entities corroborated across multiple independent sources."""
    with get_db() as conn:
        # Entities appearing in documents from different sources
        corroborated = conn.execute(
            """SELECT e.id, e.name, e.type,
                      COUNT(DISTINCT d.id) as doc_count,
                      COUNT(DISTINCT d.source) as source_count,
                      GROUP_CONCAT(DISTINCT d.source) as sources
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               JOIN documents d ON d.id = de.document_id
               WHERE d.source IS NOT NULL AND d.source != ''
               GROUP BY e.id
               HAVING source_count >= ?
               ORDER BY source_count DESC, doc_count DESC
               LIMIT 100""",
            (min_shared,),
        ).fetchall()

        # Find entity pairs that appear together across multiple sources (strong corroboration)
        entity_pairs = conn.execute(
            """WITH pair_sources AS (
                 SELECT de1.entity_id as e1_id, de2.entity_id as e2_id,
                        d.source, d.id as doc_id
                 FROM document_entities de1
                 JOIN document_entities de2 ON de2.document_id = de1.document_id
                   AND de2.entity_id > de1.entity_id
                 JOIN documents d ON d.id = de1.document_id
                 WHERE d.source IS NOT NULL AND d.source != ''
               )
               SELECT e1.name as entity_a, e2.name as entity_b,
                      e1.type as type_a, e2.type as type_b,
                      COUNT(DISTINCT ps.source) as source_count,
                      COUNT(DISTINCT ps.doc_id) as doc_count
               FROM pair_sources ps
               JOIN entities e1 ON e1.id = ps.e1_id
               JOIN entities e2 ON e2.id = ps.e2_id
               GROUP BY ps.e1_id, ps.e2_id
               HAVING source_count >= ?
               ORDER BY source_count DESC, doc_count DESC
               LIMIT 50""",
            (min_shared,),
        ).fetchall()

    return {
        "corroborated_entities": [dict(c) for c in corroborated],
        "corroborated_pairs": [dict(p) for p in entity_pairs],
    }


# ═══════════════════════════════════════════
# DEPOSITION TRACKER
# ═══════════════════════════════════════════


@router.get("/depositions")
def depositions():
    """Track depositions and testimonies — who testified, when, key entities."""
    with get_db() as conn:
        # Find deposition-category documents with their entities
        depo_docs = conn.execute(
            """SELECT d.id, d.title, d.filename, d.date, d.source, d.pages,
                      d.category
               FROM documents d
               WHERE d.category = 'deposition'
                  OR LOWER(d.title) LIKE '%deposition%'
                  OR LOWER(d.title) LIKE '%testimony%'
                  OR LOWER(d.title) LIKE '%depo%'
                  OR LOWER(d.filename) LIKE '%deposition%'
                  OR LOWER(d.filename) LIKE '%testimony%'
               ORDER BY d.date"""
        ).fetchall()

        results = []
        for doc in depo_docs:
            # Get people mentioned in this deposition
            people = conn.execute(
                """SELECT e.id, e.name, de.count as mentions
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ? AND e.type = 'person'
                   ORDER BY de.count DESC LIMIT 10""",
                (doc["id"],),
            ).fetchall()

            # Get orgs mentioned
            orgs = conn.execute(
                """SELECT e.name, de.count as mentions
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ? AND e.type = 'org'
                   ORDER BY de.count DESC LIMIT 5""",
                (doc["id"],),
            ).fetchall()

            results.append(
                {
                    "doc_id": doc["id"],
                    "title": doc["title"] or doc["filename"],
                    "date": doc["date"],
                    "source": doc["source"],
                    "pages": doc["pages"],
                    "people": [dict(p) for p in people],
                    "orgs": [dict(o) for o in orgs],
                }
            )

        # Deponent summary (people who appear in deposition docs most)
        deponent_ids = [d["id"] for d in depo_docs]
        deponents = []
        if deponent_ids:
            placeholders = ",".join("?" * len(deponent_ids))
            deponents = conn.execute(
                f"""SELECT e.id, e.name, COUNT(DISTINCT de.document_id) as deposition_count,
                           SUM(de.count) as total_mentions
                    FROM entities e
                    JOIN document_entities de ON de.entity_id = e.id
                    WHERE de.document_id IN ({placeholders}) AND e.type = 'person'
                    GROUP BY e.id
                    ORDER BY deposition_count DESC, total_mentions DESC
                    LIMIT 30""",
                deponent_ids,
            ).fetchall()

    return {
        "depositions": results,
        "deponents": [dict(d) for d in deponents],
        "total": len(results),
    }


# ═══════════════════════════════════════════
# NARRATIVE BUILDER (auto-generate investigation summary)
# ═══════════════════════════════════════════


@router.get("/narrative")
def narrative_builder(entity_id: Optional[int] = None, limit: int = Query(50)):
    """Generate a structured investigation narrative from evidence chains, events, and key entities."""
    with get_db() as conn:
        # Key entities by document coverage
        top_entities = conn.execute(
            """SELECT e.id, e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count,
                      SUM(de.count) as total_mentions
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               WHERE e.type = 'person'
               GROUP BY e.id
               ORDER BY doc_count DESC LIMIT 20"""
        ).fetchall()

        # Timeline anchors (high-confidence dated events)
        event_filter = ""
        params: list = []
        if entity_id:
            event_filter = "JOIN event_entities ee ON ee.event_id = e.id AND ee.entity_id = ?"
            params.append(entity_id)

        timeline = conn.execute(
            f"""SELECT e.event_date, e.context, e.confidence, e.precision,
                       d.title as doc_title, d.id as doc_id
                FROM events e
                {event_filter}
                LEFT JOIN documents d ON d.id = e.document_id
                WHERE e.event_date IS NOT NULL AND e.event_date != ''
                  AND e.confidence >= 0.5
                ORDER BY e.event_date
                LIMIT ?""",
            params + [limit],
        ).fetchall()

        # Evidence chains
        chains = conn.execute(
            """SELECT ec.id, ec.name, ec.description,
                      COUNT(ecl.id) as link_count
               FROM evidence_chains ec
               LEFT JOIN evidence_chain_links ecl ON ecl.chain_id = ec.id
               GROUP BY ec.id
               ORDER BY link_count DESC LIMIT 10"""
        ).fetchall()

        # Financial indicators summary
        fin_summary = conn.execute(
            """SELECT indicator_type, COUNT(*) as count,
                      AVG(risk_score) as avg_risk
               FROM financial_indicators
               GROUP BY indicator_type
               ORDER BY avg_risk DESC"""
        ).fetchall()

        # Source distribution
        sources = conn.execute(
            """SELECT source, COUNT(*) as count
               FROM documents
               WHERE source IS NOT NULL AND source != ''
               GROUP BY source
               ORDER BY count DESC LIMIT 10"""
        ).fetchall()

    return {
        "key_people": [dict(e) for e in top_entities],
        "timeline_events": [dict(t) for t in timeline],
        "evidence_chains": [dict(c) for c in chains],
        "financial_summary": [dict(f) for f in fin_summary],
        "sources": [dict(s) for s in sources],
    }


# ═══════════════════════════════════════════
# CONTACT NETWORK (correspondence analysis)
# ═══════════════════════════════════════════


@router.get("/contact-network")
def contact_network(limit: int = Query(50)):
    """Analyze who-contacted-who from correspondence documents."""
    with get_db() as conn:
        # Find correspondence docs and extract person pairs
        corr_docs = conn.execute(
            """SELECT d.id, d.title, d.filename, d.date, d.source
               FROM documents d
               WHERE d.category = 'correspondence'
                  OR LOWER(d.title) LIKE '%from:%'
                  OR LOWER(d.title) LIKE '%to:%'
                  OR LOWER(d.title) LIKE '%email%'
                  OR d.category = 'email'
               ORDER BY d.date
               LIMIT 500"""
        ).fetchall()

        # For each correspondence doc, get the people involved
        contacts = {}
        doc_details = []
        for doc in corr_docs:
            people = conn.execute(
                """SELECT e.id, e.name, de.count
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ? AND e.type = 'person'
                   ORDER BY de.count DESC LIMIT 5""",
                (doc["id"],),
            ).fetchall()

            if len(people) >= 2:
                names = [p["name"] for p in people]
                doc_details.append(
                    {
                        "doc_id": doc["id"],
                        "title": doc["title"] or doc["filename"],
                        "date": doc["date"],
                        "participants": names,
                    }
                )
                # Track pair frequencies
                for i in range(len(names)):
                    for j in range(i + 1, len(names)):
                        key = tuple(sorted([names[i], names[j]]))
                        contacts[key] = contacts.get(key, 0) + 1

        # Sort pairs by frequency
        top_pairs = sorted(contacts.items(), key=lambda x: -x[1])[:limit]

    return {
        "pairs": [{"person_a": p[0][0], "person_b": p[0][1], "frequency": p[1]} for p in top_pairs],
        "correspondence_docs": doc_details[:100],
        "total_correspondence": len(corr_docs),
    }


# ═══════════════════════════════════════════
# KEY PHRASE TRENDS (phrase frequency over time)
# ═══════════════════════════════════════════


@router.get("/phrase-trends")
def phrase_trends(top_n: int = Query(20)):
    """Analyze key phrase frequency trends over time."""
    with get_db() as conn:
        # Top phrases overall
        top_phrases = conn.execute(
            """SELECT p.id, p.phrase, p.doc_count, p.total_count
               FROM phrases p
               ORDER BY p.doc_count DESC
               LIMIT ?""",
            (top_n,),
        ).fetchall()

        # For each top phrase, get temporal distribution
        phrase_data = []
        for phrase in top_phrases:
            temporal = conn.execute(
                """SELECT SUBSTR(d.date, 1, 4) as year, COUNT(*) as count
                   FROM document_phrases dp
                   JOIN documents d ON d.id = dp.document_id
                   WHERE dp.phrase_id = ?
                     AND d.date IS NOT NULL AND d.date != ''
                   GROUP BY year
                   ORDER BY year""",
                (phrase["id"],),
            ).fetchall()

            phrase_data.append(
                {
                    "phrase": phrase["phrase"],
                    "doc_count": phrase["doc_count"],
                    "total_count": phrase["total_count"],
                    "by_year": {t["year"]: t["count"] for t in temporal},
                }
            )

        # Get all years for axis
        all_years = conn.execute(
            """SELECT DISTINCT SUBSTR(date, 1, 4) as year
               FROM documents
               WHERE date IS NOT NULL AND date != ''
               ORDER BY year"""
        ).fetchall()

    return {
        "phrases": phrase_data,
        "years": [y["year"] for y in all_years],
    }


# ═══════════════════════════════════════════
# ENTITY DISAMBIGUATION (review ambiguous matches)
# ═══════════════════════════════════════════


@router.get("/entity-disambiguation")
def entity_disambiguation(min_docs: int = Query(2)):
    """Find potentially ambiguous entities that may need disambiguation."""
    with get_db() as conn:
        # Find entities with similar names (potential duplicates not yet resolved)
        # Group by first word of name for common-name detection
        ambiguous = conn.execute(
            """WITH entity_stats AS (
                 SELECT e.id, e.name, e.type, e.canonical,
                        COUNT(DISTINCT de.document_id) as doc_count,
                        SUM(de.count) as total_mentions
                 FROM entities e
                 LEFT JOIN document_entities de ON de.entity_id = e.id
                 GROUP BY e.id
                 HAVING doc_count >= ?
               )
               SELECT es1.id as id_a, es1.name as name_a, es1.type as type_a,
                      es1.doc_count as docs_a, es1.total_mentions as mentions_a,
                      es2.id as id_b, es2.name as name_b, es2.type as type_b,
                      es2.doc_count as docs_b, es2.total_mentions as mentions_b
               FROM entity_stats es1
               JOIN entity_stats es2 ON es2.id > es1.id
                 AND es1.type = es2.type
                 AND (
                   LOWER(es1.name) LIKE '%' || LOWER(es2.name) || '%'
                   OR LOWER(es2.name) LIKE '%' || LOWER(es1.name) || '%'
                 )
               ORDER BY es1.doc_count + es2.doc_count DESC
               LIMIT 50""",
            (min_docs,),
        ).fetchall()

        # Entities with very short names (likely abbreviations)
        short_entities = conn.execute(
            """SELECT e.id, e.name, e.type,
                      COUNT(DISTINCT de.document_id) as doc_count
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               WHERE LENGTH(e.name) <= 3
               GROUP BY e.id
               HAVING doc_count >= ?
               ORDER BY doc_count DESC LIMIT 30""",
            (min_docs,),
        ).fetchall()

        # Already resolved count
        resolved = conn.execute("SELECT COUNT(*) FROM entity_resolutions").fetchone()[0]

    return {
        "ambiguous_pairs": [dict(a) for a in ambiguous],
        "short_entities": [dict(s) for s in short_entities],
        "already_resolved": resolved,
    }


# ═══════════════════════════════════════════
# INFLUENCE SCORE (composite entity ranking)
# ═══════════════════════════════════════════


@router.get("/influence-scores")
def influence_scores(limit: int = Query(50)):
    """Rank entities by composite influence: doc coverage, connections, events, financial links."""
    with get_db() as conn:
        entities = conn.execute(
            """SELECT e.id, e.name, e.type,
                      COUNT(DISTINCT de.document_id) as doc_count,
                      COALESCE(SUM(de.count), 0) as total_mentions
               FROM entities e
               LEFT JOIN document_entities de ON de.entity_id = e.id
               GROUP BY e.id
               HAVING doc_count >= 2
               ORDER BY doc_count DESC
               LIMIT 500"""
        ).fetchall()

        results = []
        for ent in entities:
            eid = ent["id"]
            conn_weight = conn.execute(
                """SELECT COALESCE(SUM(weight), 0)
                   FROM entity_connections
                   WHERE entity_a_id = ? OR entity_b_id = ?""",
                (eid, eid),
            ).fetchone()[0]

            event_count = conn.execute(
                "SELECT COUNT(*) FROM event_entities WHERE entity_id = ?", (eid,)
            ).fetchone()[0]

            fin_links = conn.execute(
                """SELECT COUNT(DISTINCT fi.id)
                   FROM financial_indicators fi
                   JOIN documents d ON d.id = fi.document_id
                   JOIN document_entities de ON de.document_id = d.id
                   WHERE de.entity_id = ?""",
                (eid,),
            ).fetchone()[0]

            # Composite score: weighted sum
            score = (
                ent["doc_count"] * 3
                + ent["total_mentions"] * 0.1
                + conn_weight * 0.5
                + event_count * 2
                + fin_links * 5
            )

            results.append(
                {
                    "id": eid,
                    "name": ent["name"],
                    "type": ent["type"],
                    "doc_count": ent["doc_count"],
                    "mentions": ent["total_mentions"],
                    "connection_weight": conn_weight,
                    "event_count": event_count,
                    "financial_links": fin_links,
                    "influence_score": round(score, 1),
                }
            )

        results.sort(key=lambda x: x["influence_score"], reverse=True)

    return {"entities": results[:limit]}


# ═══════════════════════════════════════════
# DOCUMENT CLUSTERS BY ENTITY (shared entity signatures)
# ═══════════════════════════════════════════


@router.get("/entity-clusters")
def entity_clusters(min_shared: int = Query(3), limit: int = Query(30)):
    """Cluster documents by shared entity signatures."""
    with get_db() as conn:
        # Find document pairs sharing many entities
        pairs = conn.execute(
            """SELECT de1.document_id as doc_a, de2.document_id as doc_b,
                      COUNT(DISTINCT de1.entity_id) as shared_entities
               FROM document_entities de1
               JOIN document_entities de2 ON de2.entity_id = de1.entity_id
                 AND de2.document_id > de1.document_id
               GROUP BY de1.document_id, de2.document_id
               HAVING shared_entities >= ?
               ORDER BY shared_entities DESC
               LIMIT ?""",
            (min_shared, limit),
        ).fetchall()

        # Get doc details for the pairs
        doc_ids = set()
        for p in pairs:
            doc_ids.add(p["doc_a"])
            doc_ids.add(p["doc_b"])

        doc_info = {}
        if doc_ids:
            placeholders = ",".join("?" * len(doc_ids))
            docs = conn.execute(
                f"SELECT id, title, filename, category, source FROM documents WHERE id IN ({placeholders})",
                list(doc_ids),
            ).fetchall()
            for d in docs:
                doc_info[d["id"]] = dict(d)

        result_pairs = []
        for p in pairs:
            da = doc_info.get(p["doc_a"], {})
            db = doc_info.get(p["doc_b"], {})
            # Get the shared entities for this pair
            shared = conn.execute(
                """SELECT e.name, e.type
                   FROM document_entities de1
                   JOIN document_entities de2 ON de2.entity_id = de1.entity_id
                     AND de2.document_id = ?
                   JOIN entities e ON e.id = de1.entity_id
                   WHERE de1.document_id = ?
                   LIMIT 8""",
                (p["doc_b"], p["doc_a"]),
            ).fetchall()
            result_pairs.append(
                {
                    "doc_a": da,
                    "doc_b": db,
                    "shared_count": p["shared_entities"],
                    "shared_entities": [dict(s) for s in shared],
                }
            )

    return {"clusters": result_pairs}


# ═══════════════════════════════════════════
# COVER NAME DETECTION (potential aliases)
# ═══════════════════════════════════════════


@router.get("/cover-names")
def cover_name_detection():
    """Detect potential cover names / code names from entity patterns."""
    with get_db() as conn:
        # Entities that always co-occur with a specific person (possible alias)
        # Find person entities that appear in a subset of another person's documents
        cooccur = conn.execute(
            """WITH person_docs AS (
                 SELECT e.id, e.name, de.document_id
                 FROM entities e
                 JOIN document_entities de ON de.entity_id = e.id
                 WHERE e.type = 'person'
               )
               SELECT p1.name as primary_name, p1.id as primary_id,
                      p2.name as alias_candidate, p2.id as alias_id,
                      COUNT(DISTINCT p1.document_id) as shared_docs,
                      (SELECT COUNT(DISTINCT document_id) FROM document_entities WHERE entity_id = p2.id) as alias_total_docs
               FROM person_docs p1
               JOIN person_docs p2 ON p2.document_id = p1.document_id AND p2.id != p1.id
               WHERE LENGTH(p2.name) >= 2
               GROUP BY p1.id, p2.id
               HAVING shared_docs >= 3
                 AND alias_total_docs <= shared_docs * 1.2
                 AND alias_total_docs <= 10
               ORDER BY CAST(shared_docs AS FLOAT) / alias_total_docs DESC
               LIMIT 40"""
        ).fetchall()

        # Existing aliases from entity_aliases table
        known_aliases = conn.execute(
            """SELECT ea.alias_name, e.name as entity_name, e.type
               FROM entity_aliases ea
               JOIN entities e ON e.id = ea.entity_id
               ORDER BY e.name
               LIMIT 50"""
        ).fetchall()

        # Single-word entities that might be nicknames
        nicknames = conn.execute(
            """SELECT e.id, e.name, e.type,
                      COUNT(DISTINCT de.document_id) as doc_count
               FROM entities e
               JOIN document_entities de ON de.entity_id = e.id
               WHERE e.type = 'person'
                 AND e.name NOT LIKE '% %'
                 AND LENGTH(e.name) >= 3
                 AND LENGTH(e.name) <= 15
               GROUP BY e.id
               HAVING doc_count >= 2
               ORDER BY doc_count DESC LIMIT 30"""
        ).fetchall()

    return {
        "potential_aliases": [dict(c) for c in cooccur],
        "known_aliases": [dict(k) for k in known_aliases],
        "single_name_entities": [dict(n) for n in nicknames],
    }


# ═══════════════════════════════════════════
# FLIGHT LOG ANALYZER
# ═══════════════════════════════════════════


@router.get("/flight-analysis")
def flight_analysis():
    """Analyze flight-log documents for routes, passengers, and patterns."""
    with get_db() as conn:
        # Find flight-related documents
        flight_docs = conn.execute(
            """SELECT d.id, d.title, d.filename, d.date, d.source, d.pages
               FROM documents d
               WHERE d.category = 'flight'
                  OR LOWER(d.title) LIKE '%flight%'
                  OR LOWER(d.title) LIKE '%passenger%'
                  OR LOWER(d.title) LIKE '%manifest%'
                  OR LOWER(d.filename) LIKE '%flight%'
               ORDER BY d.date"""
        ).fetchall()

        flight_people = {}
        flight_places = {}
        doc_details = []

        for doc in flight_docs:
            people = conn.execute(
                """SELECT e.id, e.name, de.count
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ? AND e.type = 'person'
                   ORDER BY de.count DESC LIMIT 10""",
                (doc["id"],),
            ).fetchall()

            places = conn.execute(
                """SELECT e.name, de.count
                   FROM entities e
                   JOIN document_entities de ON de.entity_id = e.id
                   WHERE de.document_id = ? AND e.type = 'place'
                   ORDER BY de.count DESC LIMIT 5""",
                (doc["id"],),
            ).fetchall()

            for p in people:
                flight_people[p["name"]] = flight_people.get(p["name"], 0) + 1
            for p in places:
                flight_places[p["name"]] = flight_places.get(p["name"], 0) + 1

            doc_details.append(
                {
                    "doc_id": doc["id"],
                    "title": doc["title"] or doc["filename"],
                    "date": doc["date"],
                    "people": [dict(p) for p in people],
                    "places": [dict(p) for p in places],
                }
            )

        # Sort by frequency
        top_passengers = sorted(flight_people.items(), key=lambda x: -x[1])[:30]
        top_destinations = sorted(flight_places.items(), key=lambda x: -x[1])[:20]

    return {
        "flight_documents": doc_details,
        "top_passengers": [{"name": n, "flights": c} for n, c in top_passengers],
        "top_destinations": [{"name": n, "mentions": c} for n, c in top_destinations],
        "total_flight_docs": len(flight_docs),
    }
