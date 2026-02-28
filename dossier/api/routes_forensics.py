"""DOSSIER — Forensics, risk, redactions, OCR quality routes."""

from fastapi import APIRouter, Query, HTTPException, Request

from dossier.db.database import get_db

router = APIRouter()


# ═══════════════════════════════════════════
# FORENSICS
# ═══════════════════════════════════════════


@router.get("/forensics/summary")
def forensics_summary():
    """Forensic analysis overview across the entire corpus."""
    with get_db() as conn:
        total_analyzed = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM document_forensics"
        ).fetchone()[0]

        aml_flagged = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM document_forensics WHERE analysis_type = 'aml_flag'"
        ).fetchone()[0]

        high_risk = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM document_forensics WHERE analysis_type = 'risk_score' AND score > 0.5"
        ).fetchone()[0]

        fin_count = conn.execute("SELECT COUNT(*) FROM financial_indicators").fetchone()[0]

        # Topic breakdown
        topics = conn.execute("""
            SELECT label, COUNT(DISTINCT document_id) as doc_count,
                   ROUND(AVG(score), 3) as avg_score
            FROM document_forensics WHERE analysis_type = 'topic'
            GROUP BY label ORDER BY doc_count DESC
        """).fetchall()

        # Intent breakdown
        intents = conn.execute("""
            SELECT label, COUNT(DISTINCT document_id) as doc_count,
                   ROUND(AVG(score), 3) as avg_score
            FROM document_forensics WHERE analysis_type = 'intent'
            GROUP BY label ORDER BY doc_count DESC
        """).fetchall()

        # AML severity breakdown
        aml_severity = conn.execute("""
            SELECT severity, COUNT(*) as count
            FROM document_forensics WHERE analysis_type = 'aml_flag'
            GROUP BY severity ORDER BY count DESC
        """).fetchall()

        # AML flag type breakdown
        aml_types = conn.execute("""
            SELECT label, COUNT(*) as count, severity
            FROM document_forensics WHERE analysis_type = 'aml_flag'
            GROUP BY label ORDER BY count DESC
        """).fetchall()

        # Risk distribution buckets
        risk_dist = conn.execute("""
            SELECT
                CASE
                    WHEN score <= 0.1 THEN 'minimal'
                    WHEN score <= 0.3 THEN 'low'
                    WHEN score <= 0.5 THEN 'medium'
                    WHEN score <= 0.7 THEN 'high'
                    ELSE 'critical'
                END as level,
                COUNT(*) as count
            FROM document_forensics WHERE analysis_type = 'risk_score'
            GROUP BY level
        """).fetchall()

        # Codeword summary
        codeword_count = conn.execute(
            "SELECT COUNT(DISTINCT label) FROM document_forensics WHERE analysis_type = 'codeword'"
        ).fetchone()[0]

    # Ensure all risk levels present
    risk_levels = {"minimal": 0, "low": 0, "medium": 0, "high": 0, "critical": 0}
    for r in risk_dist:
        risk_levels[r["level"]] = r["count"]

    return {
        "total_analyzed": total_analyzed,
        "aml_flagged": aml_flagged,
        "high_risk": high_risk,
        "financial_indicators": fin_count,
        "codewords_detected": codeword_count,
        "topics": [dict(r) for r in topics],
        "intents": [dict(r) for r in intents],
        "aml_severity": {r["severity"]: r["count"] for r in aml_severity},
        "aml_types": [dict(r) for r in aml_types],
        "risk_distribution": risk_levels,
    }


@router.get("/forensics/risk-documents")
def forensics_risk_documents(limit: int = Query(20, ge=1, le=100)):
    """Documents ranked by risk score, highest first."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT df.document_id, df.score as risk_score,
                   d.filename, d.title, d.category, d.source
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score > 0
            ORDER BY df.score DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        results = []
        for row in rows:
            doc = dict(row)
            # Get AML flags for this doc
            flags = conn.execute(
                """
                SELECT label, severity, evidence
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'aml_flag'
                ORDER BY severity DESC
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["aml_flags"] = [dict(f) for f in flags]

            # Get topics
            topics = conn.execute(
                """
                SELECT label, score
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'topic'
                ORDER BY score DESC LIMIT 3
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["topics"] = [dict(t) for t in topics]
            results.append(doc)

    return {"documents": results}


@router.get("/forensics/financial")
def forensics_financial(limit: int = Query(50, ge=1, le=200)):
    """Financial indicators across the corpus."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT fi.id, fi.document_id, fi.indicator_type, fi.value,
                   fi.context, fi.risk_score,
                   d.title, d.filename
            FROM financial_indicators fi
            JOIN documents d ON d.id = fi.document_id
            ORDER BY fi.risk_score DESC, fi.id DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        # Type breakdown
        type_counts = conn.execute("""
            SELECT indicator_type, COUNT(*) as count
            FROM financial_indicators
            GROUP BY indicator_type ORDER BY count DESC
        """).fetchall()

    return {
        "indicators": [dict(r) for r in rows],
        "type_counts": {r["indicator_type"]: r["count"] for r in type_counts},
    }


@router.get("/forensics/codewords")
def forensics_codewords(limit: int = Query(30, ge=1, le=100)):
    """Detected codewords/suspicious language across the corpus."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT label as word,
                   COUNT(DISTINCT document_id) as doc_count,
                   GROUP_CONCAT(DISTINCT evidence) as contexts
            FROM document_forensics
            WHERE analysis_type = 'codeword'
            GROUP BY label
            ORDER BY doc_count DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

    return {"codewords": [dict(r) for r in rows]}


@router.get("/forensics/harvest")
def forensics_harvest(min_risk: float = Query(0.5, ge=0, le=1)):
    """Harvest critical and high-risk intelligence for review.

    Returns a structured report of the most significant findings:
    - High-risk documents with their full forensic profiles
    - All high-severity AML flags with evidence
    - Large financial transactions
    - Suspicious codewords in context
    - Key entities appearing in high-risk documents
    """
    with get_db() as conn:
        # High-risk documents with details
        risk_docs = conn.execute(
            """
            SELECT df.document_id, df.score as risk_score,
                   d.filename, d.title, d.category, d.source, d.date
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score >= ?
            ORDER BY df.score DESC
        """,
            (min_risk,),
        ).fetchall()

        documents = []
        for row in risk_docs:
            doc = dict(row)

            # AML flags
            flags = conn.execute(
                """
                SELECT label, severity, evidence
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'aml_flag'
                ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["aml_flags"] = [dict(f) for f in flags]

            # Topics and intents
            topics = conn.execute(
                """
                SELECT label, score FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'topic'
                ORDER BY score DESC
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["topics"] = [dict(t) for t in topics]

            intents = conn.execute(
                """
                SELECT label, score FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'intent'
                ORDER BY score DESC
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["intents"] = [dict(i) for i in intents]

            # Financial indicators
            indicators = conn.execute(
                """
                SELECT indicator_type, value, context, risk_score
                FROM financial_indicators
                WHERE document_id = ?
                ORDER BY risk_score DESC
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["financial_indicators"] = [dict(fi) for fi in indicators]

            # Codewords
            codewords = conn.execute(
                """
                SELECT label, evidence FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'codeword'
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["codewords"] = [dict(c) for c in codewords]

            # Key entities
            entities = conn.execute(
                """
                SELECT e.name, e.type, de.count
                FROM document_entities de
                JOIN entities e ON e.id = de.entity_id
                WHERE de.document_id = ?
                ORDER BY de.count DESC LIMIT 20
            """,
                (doc["document_id"],),
            ).fetchall()
            doc["entities"] = [dict(e) for e in entities]

            documents.append(doc)

        # All high-severity AML flags across corpus
        high_severity_flags = conn.execute("""
            SELECT df.label, df.severity, df.evidence, df.document_id,
                   d.title, d.filename
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'aml_flag' AND df.severity = 'high'
            ORDER BY df.label, d.title
        """).fetchall()

        # Largest financial amounts
        top_financial = conn.execute("""
            SELECT fi.indicator_type, fi.value, fi.context, fi.risk_score,
                   fi.document_id, d.title, d.filename
            FROM financial_indicators fi
            JOIN documents d ON d.id = fi.document_id
            WHERE fi.indicator_type = 'currency_amount'
            ORDER BY fi.risk_score DESC, fi.value DESC
            LIMIT 50
        """).fetchall()

        # Entities most connected to high-risk documents
        key_persons = conn.execute(
            """
            SELECT e.name, COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            JOIN document_forensics df ON df.document_id = de.document_id
            WHERE e.type = 'person'
              AND df.analysis_type = 'risk_score' AND df.score >= ?
            GROUP BY e.id
            ORDER BY doc_count DESC, total_mentions DESC
            LIMIT 30
        """,
            (min_risk,),
        ).fetchall()

    return {
        "min_risk_threshold": min_risk,
        "total_flagged_documents": len(documents),
        "documents": documents,
        "high_severity_aml_flags": [dict(f) for f in high_severity_flags],
        "top_financial_amounts": [dict(f) for f in top_financial],
        "key_persons_in_high_risk_docs": [dict(p) for p in key_persons],
    }


@router.get("/forensics/phrases")
def forensics_phrases(limit: int = Query(30, ge=1, le=100)):
    """Top repeated phrases (n-grams) across the corpus."""
    # Filter out noise phrases (HTML artifacts, content-id fragments, etc.)
    noise = {"cid", "nbsp", "amp", "quot", "http", "https", "www", "com", "org", "net"}
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT p.phrase, p.doc_count, p.total_count
            FROM phrases p
            WHERE p.doc_count > 0
            ORDER BY p.total_count DESC
            LIMIT ?
        """,
            (limit * 3,),
        ).fetchall()

    # Post-filter noise
    filtered = []
    for r in rows:
        words = set(r["phrase"].split())
        if words.issubset(noise) or len(words - noise) == 0:
            continue
        filtered.append(dict(r))
        if len(filtered) >= limit:
            break

    return {"phrases": filtered}


@router.get("/forensics/{doc_id}")
def forensics_document(doc_id: int):
    """Full forensic report for a single document."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, filename, title, category, source, date FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        forensics = conn.execute(
            """
            SELECT analysis_type, label, score, severity, evidence
            FROM document_forensics
            WHERE document_id = ?
            ORDER BY analysis_type, score DESC
        """,
            (doc_id,),
        ).fetchall()

        indicators = conn.execute(
            """
            SELECT indicator_type, value, context, risk_score
            FROM financial_indicators
            WHERE document_id = ?
            ORDER BY risk_score DESC
        """,
            (doc_id,),
        ).fetchall()

        phrases = conn.execute(
            """
            SELECT p.phrase, dp.count
            FROM document_phrases dp
            JOIN phrases p ON p.id = dp.phrase_id
            WHERE dp.document_id = ?
            ORDER BY dp.count DESC LIMIT 20
        """,
            (doc_id,),
        ).fetchall()

    # Group forensics by type
    grouped = {}
    for row in forensics:
        atype = row["analysis_type"]
        if atype not in grouped:
            grouped[atype] = []
        grouped[atype].append(
            {
                "label": row["label"],
                "score": row["score"],
                "severity": row["severity"],
                "evidence": row["evidence"],
            }
        )

    risk_score = 0.0
    if "risk_score" in grouped and grouped["risk_score"]:
        risk_score = grouped["risk_score"][0]["score"]

    return {
        "document": dict(doc),
        "risk_score": risk_score,
        "topics": grouped.get("topic", []),
        "intents": grouped.get("intent", []),
        "aml_flags": grouped.get("aml_flag", []),
        "codewords": grouped.get("codeword", []),
        "financial_indicators": [dict(r) for r in indicators],
        "phrases": [dict(r) for r in phrases],
    }


# ═══════════════════════════════════════════
# ANOMALY DETECTION
# ═══════════════════════════════════════════


@router.get("/anomalies")
def detect_anomalies():
    """Detect anomalous patterns in the corpus."""
    anomalies: dict = {
        "temporal_spikes": [],
        "entity_anomalies": [],
        "financial_clusters": [],
        "isolated_entities": [],
    }

    with get_db() as conn:
        # 1. Temporal spikes — dates with unusually high event counts
        date_counts = conn.execute("""
            SELECT event_date, COUNT(*) as count
            FROM events
            WHERE event_date IS NOT NULL AND length(event_date) >= 10
              AND confidence >= 0.5
            GROUP BY event_date ORDER BY count DESC
        """).fetchall()

        if date_counts:
            counts = [r["count"] for r in date_counts]
            avg_count = sum(counts) / len(counts)
            threshold = max(avg_count * 3, 5)
            for r in date_counts:
                if r["count"] >= threshold:
                    anomalies["temporal_spikes"].append(
                        {
                            "date": r["event_date"][:10],
                            "count": r["count"],
                            "avg": round(avg_count, 1),
                            "ratio": round(r["count"] / avg_count, 1) if avg_count > 0 else 0,
                        }
                    )
                if len(anomalies["temporal_spikes"]) >= 20:
                    break

        # 2. Entity co-occurrence anomalies — high co-occurrence relative to frequency
        entity_anoms = conn.execute("""
            SELECT ea.name as entity_a, eb.name as entity_b,
                   ea.type as type_a, eb.type as type_b,
                   ec.weight,
                   (SELECT SUM(de.count) FROM document_entities de
                    WHERE de.entity_id = ec.entity_a_id) as freq_a,
                   (SELECT SUM(de.count) FROM document_entities de
                    WHERE de.entity_id = ec.entity_b_id) as freq_b
            FROM entity_connections ec
            JOIN entities ea ON ea.id = ec.entity_a_id
            JOIN entities eb ON eb.id = ec.entity_b_id
            WHERE ea.type = 'person' AND eb.type IN ('person', 'org', 'place')
              AND ec.weight >= 3
            ORDER BY CAST(ec.weight AS REAL) / (
                COALESCE((SELECT SUM(de.count) FROM document_entities de
                          WHERE de.entity_id = ec.entity_a_id), 1) +
                COALESCE((SELECT SUM(de.count) FROM document_entities de
                          WHERE de.entity_id = ec.entity_b_id), 1)
            ) DESC
            LIMIT 20
        """).fetchall()

        for r in entity_anoms:
            freq_sum = (r["freq_a"] or 1) + (r["freq_b"] or 1)
            anomalies["entity_anomalies"].append(
                {
                    "entity_a": r["entity_a"],
                    "entity_b": r["entity_b"],
                    "type_a": r["type_a"],
                    "type_b": r["type_b"],
                    "co_occurrences": r["weight"],
                    "ratio": round(r["weight"] / freq_sum * 100, 1),
                }
            )

        # 3. Financial clusters — documents with many financial indicators
        fin_clusters = conn.execute("""
            SELECT d.id, d.title, d.filename, d.category, d.source,
                   COUNT(*) as indicator_count,
                   ROUND(AVG(fi.risk_score), 3) as avg_risk,
                   GROUP_CONCAT(DISTINCT fi.indicator_type) as types
            FROM financial_indicators fi
            JOIN documents d ON d.id = fi.document_id
            GROUP BY d.id
            HAVING COUNT(*) >= 3
            ORDER BY indicator_count DESC
            LIMIT 15
        """).fetchall()
        anomalies["financial_clusters"] = [dict(r) for r in fin_clusters]

        # 4. Isolated high-mention entities — many mentions but very few documents
        isolated = conn.execute("""
            SELECT e.name, e.type,
                   SUM(de.count) as total_mentions,
                   COUNT(DISTINCT de.document_id) as doc_count
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type IN ('person', 'org')
            GROUP BY e.id
            HAVING SUM(de.count) >= 10 AND COUNT(DISTINCT de.document_id) <= 2
            ORDER BY total_mentions DESC
            LIMIT 20
        """).fetchall()
        anomalies["isolated_entities"] = [dict(r) for r in isolated]

    return anomalies


# ═══════════════════════════════════════════
# RISK DASHBOARD
# ═══════════════════════════════════════════


@router.get("/risk/dashboard")
def risk_dashboard():
    """Aggregate risk view: distribution, by source, top clusters, trends."""
    with get_db() as conn:
        # Risk distribution histogram
        buckets = conn.execute("""
            SELECT
                CASE
                    WHEN score >= 0.9 THEN 'critical'
                    WHEN score >= 0.7 THEN 'high'
                    WHEN score >= 0.5 THEN 'medium'
                    WHEN score >= 0.3 THEN 'low'
                    ELSE 'minimal'
                END as level,
                COUNT(*) as count,
                ROUND(AVG(score), 3) as avg_score
            FROM document_forensics
            WHERE analysis_type = 'risk_score'
            GROUP BY level
            ORDER BY avg_score DESC
        """).fetchall()

        # Risk by source
        by_source = conn.execute("""
            SELECT d.source, COUNT(*) as doc_count,
                   ROUND(AVG(df.score), 3) as avg_risk,
                   ROUND(MAX(df.score), 3) as max_risk,
                   SUM(CASE WHEN df.score >= 0.7 THEN 1 ELSE 0 END) as high_risk_count
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score'
            GROUP BY d.source
            ORDER BY avg_risk DESC
        """).fetchall()

        # Top risk documents
        top_docs = conn.execute("""
            SELECT d.id, d.title, d.filename, d.category, d.source,
                   df.score, d.pages
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score'
            ORDER BY df.score DESC
            LIMIT 25
        """).fetchall()

        # Risk clusters by category
        by_category = conn.execute("""
            SELECT d.category, COUNT(*) as doc_count,
                   ROUND(AVG(df.score), 3) as avg_risk,
                   SUM(CASE WHEN df.score >= 0.7 THEN 1 ELSE 0 END) as high_risk_count
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score'
            GROUP BY d.category
            ORDER BY avg_risk DESC
        """).fetchall()

        # Risk trend by ingestion date
        trend = conn.execute("""
            SELECT DATE(d.ingested_at) as date,
                   COUNT(*) as doc_count,
                   ROUND(AVG(df.score), 3) as avg_risk,
                   SUM(CASE WHEN df.score >= 0.7 THEN 1 ELSE 0 END) as high_risk_count
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score'
            GROUP BY DATE(d.ingested_at)
            ORDER BY date
        """).fetchall()

        # Overall stats
        overall = conn.execute("""
            SELECT COUNT(*) as total_scored,
                   ROUND(AVG(score), 3) as avg_risk,
                   ROUND(MAX(score), 3) as max_risk,
                   SUM(CASE WHEN score >= 0.7 THEN 1 ELSE 0 END) as high_risk_total,
                   SUM(CASE WHEN score >= 0.5 THEN 1 ELSE 0 END) as medium_plus_total
            FROM document_forensics
            WHERE analysis_type = 'risk_score'
        """).fetchone()

    return {
        "overall": dict(overall) if overall else {},
        "distribution": [dict(r) for r in buckets],
        "by_source": [dict(r) for r in by_source],
        "by_category": [dict(r) for r in by_category],
        "top_documents": [dict(r) for r in top_docs],
        "trend": [dict(r) for r in trend],
    }


# ═══════════════════════════════════════════
# REDACTION TOOL
# ═══════════════════════════════════════════


def _ensure_redactions_table(conn):
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


@router.get("/documents/{doc_id}/redactions")
def get_redactions(doc_id: int):
    """Get all redaction spans for a document."""
    with get_db() as conn:
        _ensure_redactions_table(conn)
        rows = conn.execute(
            "SELECT * FROM redactions WHERE document_id = ? ORDER BY start_offset",
            (doc_id,),
        ).fetchall()
    return {"document_id": doc_id, "redactions": [dict(r) for r in rows]}


@router.post("/documents/{doc_id}/redactions")
async def add_redaction(doc_id: int, request: Request):
    """Add a redaction span to a document."""
    body = await request.json()
    start = body.get("start_offset")
    end = body.get("end_offset")
    reason = body.get("reason", "")
    if start is None or end is None:
        raise HTTPException(400, "start_offset and end_offset required")

    with get_db() as conn:
        doc = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        _ensure_redactions_table(conn)
        cur = conn.execute(
            "INSERT INTO redactions (document_id, start_offset, end_offset, reason) VALUES (?, ?, ?, ?)",
            (doc_id, start, end, reason),
        )
    return {"id": cur.lastrowid, "document_id": doc_id, "added": True}


@router.delete("/redactions/{redaction_id}")
def delete_redaction(redaction_id: int):
    """Delete a redaction span."""
    with get_db() as conn:
        _ensure_redactions_table(conn)
        conn.execute("DELETE FROM redactions WHERE id = ?", (redaction_id,))
    return {"id": redaction_id, "deleted": True}


@router.get("/documents/{doc_id}/redacted-text")
def get_redacted_text(doc_id: int):
    """Get document text with redactions applied."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, title, filename, raw_text FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        _ensure_redactions_table(conn)
        redactions = conn.execute(
            "SELECT start_offset, end_offset, reason FROM redactions WHERE document_id = ? ORDER BY start_offset DESC",
            (doc_id,),
        ).fetchall()

        text = doc["raw_text"] or ""
        for r in redactions:
            start = max(0, r["start_offset"])
            end = min(len(text), r["end_offset"])
            text = text[:start] + "[REDACTED]" + text[end:]

    return {
        "document_id": doc_id,
        "title": doc["title"] or doc["filename"],
        "redacted_text": text,
        "redaction_count": len(redactions),
    }


@router.get("/redaction-analysis")
def redaction_analysis():
    """Analyze redaction density and patterns across documents."""
    with get_db() as conn:
        # Per-document redaction counts
        doc_redactions = conn.execute(
            """SELECT d.id, d.title, d.filename, d.category, d.pages,
                      COUNT(r.id) as redaction_count,
                      SUM(r.end_offset - r.start_offset) as total_chars_redacted
               FROM documents d
               JOIN redactions r ON r.document_id = d.id
               GROUP BY d.id
               ORDER BY redaction_count DESC"""
        ).fetchall()

        # Redaction reasons breakdown
        reasons = conn.execute(
            """SELECT reason, COUNT(*) as count
               FROM redactions
               WHERE reason IS NOT NULL AND reason != ''
               GROUP BY reason ORDER BY count DESC"""
        ).fetchall()

        # Category distribution
        category_stats = conn.execute(
            """SELECT d.category, COUNT(DISTINCT d.id) as doc_count,
                      COUNT(r.id) as redaction_count
               FROM documents d
               JOIN redactions r ON r.document_id = d.id
               GROUP BY d.category ORDER BY redaction_count DESC"""
        ).fetchall()

        # Total docs with/without redactions
        total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        docs_with_redactions = conn.execute(
            "SELECT COUNT(DISTINCT document_id) FROM redactions"
        ).fetchone()[0]

    return {
        "documents": [dict(d) for d in doc_redactions],
        "reasons": [dict(r) for r in reasons],
        "by_category": [dict(c) for c in category_stats],
        "summary": {
            "total_documents": total_docs,
            "documents_with_redactions": docs_with_redactions,
            "documents_clean": total_docs - docs_with_redactions,
            "total_redactions": sum(d["redaction_count"] for d in doc_redactions),
        },
    }


@router.get("/redaction-density")
def redaction_density():
    """Documents ranked by redaction density."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.filename, d.title, d.pages, d.category, "
            "LENGTH(d.raw_text) as text_length, "
            "COUNT(r.id) as redaction_count "
            "FROM documents d "
            "LEFT JOIN redactions r ON r.document_id = d.id "
            "GROUP BY d.id "
            "HAVING redaction_count > 0 "
            "ORDER BY redaction_count DESC"
        ).fetchall()

        total_docs = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]
        total_redactions = conn.execute("SELECT COUNT(*) as cnt FROM redactions").fetchone()["cnt"]

    docs = []
    for r in rows:
        text_len = r["text_length"] or 1
        density = round(r["redaction_count"] / (text_len / 1000), 2)
        docs.append(
            {
                "id": r["id"],
                "filename": r["filename"],
                "title": r["title"],
                "pages": r["pages"],
                "category": r["category"],
                "redaction_count": r["redaction_count"],
                "text_length": r["text_length"],
                "density_per_1k_chars": density,
            }
        )

    docs.sort(key=lambda x: x["density_per_1k_chars"], reverse=True)
    return {
        "documents": docs,
        "total_redacted_docs": len(docs),
        "total_docs": total_docs,
        "total_redactions": total_redactions,
        "pct_docs_redacted": round(len(docs) / total_docs * 100, 1) if total_docs else 0,
    }


@router.get("/redaction-by-source")
def redaction_by_source():
    """Redaction counts grouped by document source."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.source, COUNT(r.id) as redaction_count, "
            "COUNT(DISTINCT r.document_id) as docs_with_redactions, "
            "COUNT(DISTINCT d.id) as total_docs "
            "FROM documents d "
            "LEFT JOIN redactions r ON r.document_id = d.id "
            "WHERE d.source IS NOT NULL AND d.source != '' "
            "GROUP BY d.source "
            "ORDER BY redaction_count DESC"
        ).fetchall()

    sources = [dict(r) for r in rows]
    for s in sources:
        s["redaction_rate"] = (
            round(s["docs_with_redactions"] / s["total_docs"] * 100, 1) if s["total_docs"] else 0
        )

    return {"sources": sources, "total": len(sources)}


@router.get("/redaction-patterns")
def redaction_patterns():
    """Redaction reason breakdown and size distribution."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as c FROM redactions").fetchone()["c"]

        by_reason = conn.execute(
            "SELECT reason, COUNT(*) as cnt "
            "FROM redactions "
            "WHERE reason IS NOT NULL AND reason != '' "
            "GROUP BY reason ORDER BY cnt DESC "
            "LIMIT 30"
        ).fetchall()

        by_size = conn.execute(
            "SELECT CASE "
            "  WHEN (end_offset - start_offset) < 50 THEN 'small (<50)' "
            "  WHEN (end_offset - start_offset) < 200 THEN 'medium (50-200)' "
            "  ELSE 'large (200+)' "
            "END as size_bucket, COUNT(*) as cnt "
            "FROM redactions "
            "GROUP BY size_bucket ORDER BY cnt DESC"
        ).fetchall()

        top_docs = conn.execute(
            "SELECT d.id, d.title, d.filename, d.source, COUNT(r.id) as redaction_count "
            "FROM redactions r "
            "JOIN documents d ON d.id = r.document_id "
            "GROUP BY r.document_id "
            "ORDER BY redaction_count DESC LIMIT 20"
        ).fetchall()

    return {
        "total": total,
        "by_reason": [dict(r) for r in by_reason],
        "by_size": [dict(r) for r in by_size],
        "top_docs": [dict(r) for r in top_docs],
    }


@router.get("/redaction-density-ranking")
def redaction_density_ranking():
    """Redaction count per document, ranked by density."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT d.id, d.title, d.filename, d.pages, "
            "COUNT(r.id) AS redaction_count, "
            "CASE WHEN d.pages > 0 "
            "  THEN ROUND(CAST(COUNT(r.id) AS REAL) / d.pages, 2) "
            "  ELSE 0 END AS density "
            "FROM documents d "
            "JOIN redactions r ON r.document_id = d.id "
            "GROUP BY d.id ORDER BY density DESC LIMIT 100"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM redactions").fetchone()["cnt"]
    return {"documents": [dict(r) for r in rows], "total_redactions": total}


@router.get("/redaction-timeline")
def redaction_timeline():
    """Redactions over time by document ingestion date."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DATE(d.ingested_at) AS day, COUNT(r.id) AS redaction_count, "
            "COUNT(DISTINCT d.id) AS doc_count "
            "FROM redactions r "
            "JOIN documents d ON d.id = r.document_id "
            "WHERE d.ingested_at IS NOT NULL "
            "GROUP BY day ORDER BY day"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM redactions").fetchone()["cnt"]
    return {
        "total_redactions": total,
        "timeline": [dict(r) for r in rows],
    }


@router.get("/redaction-document-coverage")
def redaction_document_coverage():
    """How many documents have redactions vs don't."""
    with get_db() as conn:
        # Check if redactions table exists
        has_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='redactions'"
        ).fetchone()
        if not has_table:
            total = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()["cnt"]
            return {
                "total_docs": total,
                "with_redactions": 0,
                "without_redactions": total,
                "coverage_pct": 0,
                "top_redacted": [],
            }
        with_redactions = conn.execute(
            "SELECT COUNT(DISTINCT document_id) AS cnt FROM redactions"
        ).fetchone()["cnt"]
        total = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()["cnt"]
        top = conn.execute(
            "SELECT d.id, d.filename, COUNT(r.id) AS redaction_count "
            "FROM redactions r "
            "JOIN documents d ON d.id = r.document_id "
            "GROUP BY d.id ORDER BY redaction_count DESC LIMIT 50"
        ).fetchall()
    return {
        "total_docs": total,
        "with_redactions": with_redactions,
        "without_redactions": total - with_redactions,
        "coverage_pct": round(100.0 * with_redactions / total, 1) if total else 0,
        "top_redacted": [dict(r) for r in top],
    }


# ═══════════════════════════════════════════
# OCR QUALITY
# ═══════════════════════════════════════════


@router.get("/documents/{doc_id}/ocr-quality")
def document_ocr_quality(doc_id: int):
    """Analyze OCR quality per page for a document."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, title, filename, raw_text, pages FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        raw = doc["raw_text"] or ""
        # Split on form-feed or into ~3000 char chunks
        if "\f" in raw:
            chunks = [c for c in raw.split("\f") if c.strip()]
        elif len(raw) > 3000:
            chunks = [raw[i : i + 3000] for i in range(0, len(raw), 3000)]
        else:
            chunks = [raw] if raw else []

        page_quality = []
        total_score = 0
        for idx, text in enumerate(chunks):
            chars = len(text)

            # Quality heuristics
            alpha_ratio = sum(1 for c in text if c.isalpha()) / max(chars, 1)
            space_ratio = sum(1 for c in text if c == " ") / max(chars, 1)
            garbage_chars = sum(1 for c in text if ord(c) > 127 and not c.isalpha()) / max(chars, 1)
            word_count = len(text.split())
            avg_word_len = sum(len(w) for w in text.split()) / max(word_count, 1)

            # Score: 0-1
            score = 1.0
            if chars < 50:
                score *= 0.3  # Very short page
            if alpha_ratio < 0.4:
                score *= 0.5  # Low alpha content
            if garbage_chars > 0.05:
                score *= 0.4  # High garbage
            if space_ratio < 0.05 or space_ratio > 0.5:
                score *= 0.6  # Abnormal spacing
            if avg_word_len > 15 or avg_word_len < 2:
                score *= 0.5  # Abnormal word lengths

            score = round(min(max(score, 0), 1), 3)
            total_score += score

            page_quality.append(
                {
                    "page_number": idx + 1,
                    "char_count": chars,
                    "word_count": word_count,
                    "quality_score": score,
                    "alpha_ratio": round(alpha_ratio, 3),
                    "issues": (
                        (["short_text"] if chars < 50 else [])
                        + (["low_alpha"] if alpha_ratio < 0.4 else [])
                        + (["garbage_chars"] if garbage_chars > 0.05 else [])
                        + (["spacing_abnormal"] if space_ratio < 0.05 or space_ratio > 0.5 else [])
                        + (
                            ["word_length_abnormal"]
                            if avg_word_len > 15 or avg_word_len < 2
                            else []
                        )
                    ),
                }
            )

        avg_score = round(total_score / max(len(chunks), 1), 3)
        problem_pages = [p for p in page_quality if p["quality_score"] < 0.5]

    return {
        "document": {"id": doc["id"], "title": doc["title"], "filename": doc["filename"]},
        "page_count": len(chunks),
        "average_quality": avg_score,
        "problem_page_count": len(problem_pages),
        "pages": page_quality,
    }


@router.get("/ocr-quality-overview")
def ocr_quality_overview(limit: int = Query(50, ge=10, le=200)):
    """Overview of OCR quality across all documents."""
    with get_db() as conn:
        docs = conn.execute(
            """
            SELECT d.id, d.title, d.filename, d.category,
                   d.pages as page_count,
                   LENGTH(d.raw_text) as total_chars,
                   CASE WHEN d.pages > 0 THEN LENGTH(d.raw_text) / d.pages ELSE LENGTH(d.raw_text) END as avg_page_chars
            FROM documents d
            WHERE d.raw_text IS NOT NULL
            ORDER BY avg_page_chars ASC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

        result = []
        for d in docs:
            dd = dict(d)
            avg_chars = dd["avg_page_chars"] or 0
            # Simple quality estimate based on average chars per page
            if avg_chars > 500:
                dd["estimated_quality"] = "good"
            elif avg_chars > 200:
                dd["estimated_quality"] = "fair"
            elif avg_chars > 50:
                dd["estimated_quality"] = "poor"
            else:
                dd["estimated_quality"] = "very_poor"
            result.append(dd)

    return {"documents": result}
