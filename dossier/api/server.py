"""
DOSSIER — FastAPI Backend
REST API for the Document Intelligence System.

Endpoints:
  GET  /api/search?q=...&category=...&entity_type=...
  GET  /api/documents
  GET  /api/documents/{id}
  GET  /api/entities?type=...&limit=...
  GET  /api/keywords?limit=...
  GET  /api/connections?entity_id=...
  GET  /api/stats
  POST /api/upload
  POST /api/ingest-directory
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, Request, UploadFile, File, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from dossier.db.database import get_db, init_db
from dossier.ingestion.pipeline import ingest_file, ingest_directory
from dossier.forensics.api_timeline import router as timeline_router
from dossier.core.api_resolver import router as resolver_router
from dossier.core.api_graph import router as graph_router

logger = logging.getLogger(__name__)

app = FastAPI(title="DOSSIER", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(timeline_router, prefix="/api/timeline", tags=["timeline"])
app.include_router(resolver_router, prefix="/api/resolver", tags=["resolver"])
app.include_router(graph_router, prefix="/api/graph", tags=["graph"])


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all handler to prevent stack traces from leaking to clients."""
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


UPLOAD_DIR = Path(__file__).parent.parent / "data" / "inbox"
MAX_UPLOAD_SIZE = int(os.environ.get("DOSSIER_MAX_UPLOAD_MB", "100")) * 1024 * 1024  # bytes
ALLOWED_BASE_DIRS: list[Path] = [
    Path(p) for p in os.environ.get("DOSSIER_ALLOWED_DIRS", str(Path.home())).split(os.pathsep) if p
]


def _validate_path(dirpath: str) -> Path:
    """Validate a directory path against traversal and symlink attacks.

    Raises HTTPException 403 if the path resolves outside ALLOWED_BASE_DIRS.
    """
    resolved = Path(dirpath).resolve()
    for allowed in ALLOWED_BASE_DIRS:
        if resolved == allowed.resolve() or allowed.resolve() in resolved.parents:
            return resolved
    raise HTTPException(403, "Access denied: path is outside allowed directories")


def _sanitize_filename(name: str) -> str:
    """Sanitize an uploaded filename to prevent path injection.

    Returns a safe filename (basename only, no leading dots, no special chars).
    Falls back to a uuid-based name if sanitized result is empty.
    """
    # Take only the final path component
    basename = Path(name).name if name else ""
    # Split into stem and suffix
    p = Path(basename)
    stem = p.stem.lstrip(".")
    suffix = p.suffix  # e.g. ".txt"
    # Replace disallowed characters
    stem = re.sub(r"[^a-zA-Z0-9_\-.]", "_", stem)
    # Strip leading/trailing underscores
    stem = stem.strip("_")
    if not stem:
        stem = f"upload_{uuid4().hex[:8]}"
    return stem + suffix


async def _read_upload(file: UploadFile) -> bytes:
    """Read an uploaded file with size limit enforcement.

    Raises HTTPException 413 if the file exceeds MAX_UPLOAD_SIZE.
    """
    chunks = []
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)  # 1MB chunks
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_UPLOAD_SIZE:
            raise HTTPException(
                413, f"File exceeds maximum upload size of {MAX_UPLOAD_SIZE // (1024 * 1024)}MB"
            )
        chunks.append(chunk)
    return b"".join(chunks)


@app.on_event("startup")
def startup():
    init_db()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════
# SEARCH
# ═══════════════════════════════════════════


@app.get("/api/search")
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
            doc["entities"] = _get_doc_entities(conn, doc["id"])
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


# ═══════════════════════════════════════════
# DOCUMENTS
# ═══════════════════════════════════════════


@app.get("/api/documents")
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
            doc["entities"] = _get_doc_entities(conn, doc["id"])
            results.append(doc)

        total = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()["cnt"]

    return {"documents": results, "total": total}


@app.get("/api/documents/{doc_id}")
def get_document(doc_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")

        doc = dict(row)
        doc["entities"] = _get_doc_entities(conn, doc_id)

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


@app.post("/api/documents/{doc_id}/flag")
def toggle_flag(doc_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT flagged FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
        new_val = 0 if row["flagged"] else 1
        conn.execute("UPDATE documents SET flagged = ? WHERE id = ?", (new_val, doc_id))
    return {"id": doc_id, "flagged": bool(new_val)}


# ═══════════════════════════════════════════
# ENTITIES
# ═══════════════════════════════════════════


@app.get("/api/entities")
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


@app.get("/api/entities/{entity_id}/documents")
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
# KEYWORDS
# ═══════════════════════════════════════════


@app.get("/api/keywords")
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


# ═══════════════════════════════════════════
# CONNECTIONS (Entity co-occurrence network)
# ═══════════════════════════════════════════


@app.get("/api/connections")
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


# ═══════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════


@app.get("/api/stats")
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


# ═══════════════════════════════════════════
# FILE UPLOAD / INGESTION
# ═══════════════════════════════════════════


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    source: str = Query("Manual Upload"),
    date: str = Query(""),
):
    """Upload and ingest a single file."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Sanitize filename and enforce upload size limit
    safe_name = _sanitize_filename(file.filename or "")
    content = await _read_upload(file)
    dest = UPLOAD_DIR / safe_name
    with open(dest, "wb") as f:
        f.write(content)

    # Ingest
    result = ingest_file(str(dest), source=source, date=date)

    if result["success"]:
        return JSONResponse(result, status_code=201)
    else:
        return JSONResponse(result, status_code=409 if "Duplicate" in result["message"] else 422)


@app.post("/api/ingest-directory")
def ingest_dir(dirpath: str = Query(...)):
    """Ingest all supported files from a directory path on disk."""
    path = _validate_path(dirpath)
    if not path.exists() or not path.is_dir():
        raise HTTPException(400, f"Directory not found: {dirpath}")

    results = ingest_directory(str(path))
    success = sum(1 for r in results if r["success"])
    failed = len(results) - success

    return {"ingested": success, "failed": failed, "details": results}


@app.post("/api/upload-email")
async def upload_email(
    file: UploadFile = File(...),
    source: str = Query("Email Upload"),
    corpus: str = Query(""),
):
    """Upload and ingest an email file (eml, mbox, json, csv)."""
    from dossier.ingestion.email_pipeline import ingest_email_file

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_filename(file.filename or "")
    content = await _read_upload(file)
    dest = UPLOAD_DIR / safe_name
    with open(dest, "wb") as f:
        f.write(content)

    results = ingest_email_file(str(dest), source=source, corpus=corpus)
    success = sum(1 for r in results if r.get("success"))
    failed = len(results) - success

    status = 201 if success > 0 else 422
    return JSONResponse(
        {"ingested": success, "failed": failed, "details": results}, status_code=status
    )


@app.post("/api/ingest-emails-directory")
def ingest_emails_dir(
    dirpath: str = Query(...),
    source: str = Query("Email Import"),
    corpus: str = Query(""),
):
    """Ingest all email files from a directory on disk."""
    from dossier.ingestion.email_pipeline import ingest_email_directory

    path = _validate_path(dirpath)
    if not path.exists() or not path.is_dir():
        raise HTTPException(400, f"Directory not found: {dirpath}")

    result = ingest_email_directory(str(path), source=source, corpus=corpus)
    return result


@app.post("/api/lobbying/generate")
def generate_lobbying():
    """Generate and ingest Podesta Group lobbying records."""
    from dossier.ingestion.scrapers.fara_lobbying import (
        create_lobbying_index,
        generate_ingestable_documents,
        ingest_lobbying_docs,
    )

    create_lobbying_index()
    count = generate_ingestable_documents()
    ingest_lobbying_docs()
    return {"message": f"Generated and ingested {count} lobbying documents"}


# ═══════════════════════════════════════════
# FORENSICS
# ═══════════════════════════════════════════


@app.get("/api/forensics/summary")
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


@app.get("/api/forensics/risk-documents")
def forensics_risk_documents(limit: int = Query(20, ge=1, le=100)):
    """Documents ranked by risk score, highest first."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT df.document_id, df.score as risk_score,
                   d.filename, d.title, d.category, d.source
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score > 0
            ORDER BY df.score DESC
            LIMIT ?
        """, (limit,)).fetchall()

        results = []
        for row in rows:
            doc = dict(row)
            # Get AML flags for this doc
            flags = conn.execute("""
                SELECT label, severity, evidence
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'aml_flag'
                ORDER BY severity DESC
            """, (doc["document_id"],)).fetchall()
            doc["aml_flags"] = [dict(f) for f in flags]

            # Get topics
            topics = conn.execute("""
                SELECT label, score
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'topic'
                ORDER BY score DESC LIMIT 3
            """, (doc["document_id"],)).fetchall()
            doc["topics"] = [dict(t) for t in topics]
            results.append(doc)

    return {"documents": results}


@app.get("/api/forensics/financial")
def forensics_financial(limit: int = Query(50, ge=1, le=200)):
    """Financial indicators across the corpus."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT fi.id, fi.document_id, fi.indicator_type, fi.value,
                   fi.context, fi.risk_score,
                   d.title, d.filename
            FROM financial_indicators fi
            JOIN documents d ON d.id = fi.document_id
            ORDER BY fi.risk_score DESC, fi.id DESC
            LIMIT ?
        """, (limit,)).fetchall()

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


@app.get("/api/forensics/codewords")
def forensics_codewords(limit: int = Query(30, ge=1, le=100)):
    """Detected codewords/suspicious language across the corpus."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT label as word,
                   COUNT(DISTINCT document_id) as doc_count,
                   GROUP_CONCAT(DISTINCT evidence) as contexts
            FROM document_forensics
            WHERE analysis_type = 'codeword'
            GROUP BY label
            ORDER BY doc_count DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return {"codewords": [dict(r) for r in rows]}


@app.get("/api/forensics/harvest")
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
        risk_docs = conn.execute("""
            SELECT df.document_id, df.score as risk_score,
                   d.filename, d.title, d.category, d.source, d.date
            FROM document_forensics df
            JOIN documents d ON d.id = df.document_id
            WHERE df.analysis_type = 'risk_score' AND df.score >= ?
            ORDER BY df.score DESC
        """, (min_risk,)).fetchall()

        documents = []
        for row in risk_docs:
            doc = dict(row)

            # AML flags
            flags = conn.execute("""
                SELECT label, severity, evidence
                FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'aml_flag'
                ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END
            """, (doc["document_id"],)).fetchall()
            doc["aml_flags"] = [dict(f) for f in flags]

            # Topics and intents
            topics = conn.execute("""
                SELECT label, score FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'topic'
                ORDER BY score DESC
            """, (doc["document_id"],)).fetchall()
            doc["topics"] = [dict(t) for t in topics]

            intents = conn.execute("""
                SELECT label, score FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'intent'
                ORDER BY score DESC
            """, (doc["document_id"],)).fetchall()
            doc["intents"] = [dict(i) for i in intents]

            # Financial indicators
            indicators = conn.execute("""
                SELECT indicator_type, value, context, risk_score
                FROM financial_indicators
                WHERE document_id = ?
                ORDER BY risk_score DESC
            """, (doc["document_id"],)).fetchall()
            doc["financial_indicators"] = [dict(fi) for fi in indicators]

            # Codewords
            codewords = conn.execute("""
                SELECT label, evidence FROM document_forensics
                WHERE document_id = ? AND analysis_type = 'codeword'
            """, (doc["document_id"],)).fetchall()
            doc["codewords"] = [dict(c) for c in codewords]

            # Key entities
            entities = conn.execute("""
                SELECT e.name, e.type, de.count
                FROM document_entities de
                JOIN entities e ON e.id = de.entity_id
                WHERE de.document_id = ?
                ORDER BY de.count DESC LIMIT 20
            """, (doc["document_id"],)).fetchall()
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
        key_persons = conn.execute("""
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
        """, (min_risk,)).fetchall()

    return {
        "min_risk_threshold": min_risk,
        "total_flagged_documents": len(documents),
        "documents": documents,
        "high_severity_aml_flags": [dict(f) for f in high_severity_flags],
        "top_financial_amounts": [dict(f) for f in top_financial],
        "key_persons_in_high_risk_docs": [dict(p) for p in key_persons],
    }


@app.get("/api/sources")
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


@app.get("/api/documents/{doc_id}/text")
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


@app.get("/api/graph/path-between")
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
        except Exception as e:
            logger.exception("Graph path error")
            result = None

        # Also find shared documents
        shared = conn.execute("""
            SELECT DISTINCT d.id, d.title, d.filename, d.category, d.source
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            JOIN documents d ON d.id = de1.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
            ORDER BY d.title
            LIMIT 20
        """, (src["id"], tgt["id"])).fetchall()

    if not result:
        return {
            "path": [], "edges": [], "hops": 0, "total_weight": 0,
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


@app.get("/api/export/intel-brief")
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
        f"**Risk Threshold**: {min_risk*100:.0f}%+",
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
        lines.append(f"| {f['label'].replace('_',' ')} | {f['severity']} | {f['count']} |")

    lines += ["", "## Highest Risk Documents", "| Risk | Document | Category | Source |", "|------|----------|----------|--------|"]
    for d in risk_docs[:30]:
        score = f"{d['risk_score']*100:.0f}%"
        lines.append(f"| {score} | {d['title'] or d['filename']} | {d['category']} | {d['source'] or ''} |")

    lines += ["", "---", "*Generated by DOSSIER Document Intelligence System*"]

    return {"markdown": "\n".join(lines), "summary": {
        "flagged_documents": len(risk_docs),
        "key_persons": len(persons),
        "aml_flags": len(aml_flags),
    }}


@app.get("/api/documents/{doc_id}/notes")
def get_document_notes(doc_id: int):
    """Get notes for a document."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT notes, flagged FROM documents WHERE id = ?", (doc_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Document not found")
    return {"document_id": doc_id, "notes": row["notes"] or "", "flagged": bool(row["flagged"])}


@app.post("/api/documents/{doc_id}/notes")
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


@app.get("/api/timeline/heatmap")
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


@app.get("/api/dashboard")
def dashboard_summary():
    """Comprehensive dashboard data in one call."""
    with get_db() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        page_count = conn.execute("SELECT COALESCE(SUM(pages),0) FROM documents").fetchone()[0]
        flagged_count = conn.execute("SELECT COUNT(*) FROM documents WHERE flagged = 1").fetchone()[0]

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
        resolved_count = conn.execute(
            "SELECT COUNT(*) FROM entity_resolutions"
        ).fetchone()[0]

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


@app.get("/api/forensics/phrases")
def forensics_phrases(limit: int = Query(30, ge=1, le=100)):
    """Top repeated phrases (n-grams) across the corpus."""
    # Filter out noise phrases (HTML artifacts, content-id fragments, etc.)
    noise = {"cid", "nbsp", "amp", "quot", "http", "https", "www", "com", "org", "net"}
    with get_db() as conn:
        rows = conn.execute("""
            SELECT p.phrase, p.doc_count, p.total_count
            FROM phrases p
            WHERE p.doc_count > 0
            ORDER BY p.total_count DESC
            LIMIT ?
        """, (limit * 3,)).fetchall()

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


@app.get("/api/forensics/{doc_id}")
def forensics_document(doc_id: int):
    """Full forensic report for a single document."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, filename, title, category, source, date FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")

        forensics = conn.execute("""
            SELECT analysis_type, label, score, severity, evidence
            FROM document_forensics
            WHERE document_id = ?
            ORDER BY analysis_type, score DESC
        """, (doc_id,)).fetchall()

        indicators = conn.execute("""
            SELECT indicator_type, value, context, risk_score
            FROM financial_indicators
            WHERE document_id = ?
            ORDER BY risk_score DESC
        """, (doc_id,)).fetchall()

        phrases = conn.execute("""
            SELECT p.phrase, dp.count
            FROM document_phrases dp
            JOIN phrases p ON p.id = dp.phrase_id
            WHERE dp.document_id = ?
            ORDER BY dp.count DESC LIMIT 20
        """, (doc_id,)).fetchall()

    # Group forensics by type
    grouped = {}
    for row in forensics:
        atype = row["analysis_type"]
        if atype not in grouped:
            grouped[atype] = []
        grouped[atype].append({
            "label": row["label"],
            "score": row["score"],
            "severity": row["severity"],
            "evidence": row["evidence"],
        })

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
# HELPERS
# ═══════════════════════════════════════════


def _get_doc_entities(conn, doc_id: int) -> dict:
    """Get entities grouped by type for a document."""
    rows = conn.execute(
        """
        SELECT e.name, e.type, de.count
        FROM document_entities de
        JOIN entities e ON e.id = de.entity_id
        WHERE de.document_id = ?
        ORDER BY de.count DESC
    """,
        (doc_id,),
    ).fetchall()

    grouped = {"people": [], "places": [], "orgs": [], "dates": []}
    type_map = {"person": "people", "place": "places", "org": "orgs", "date": "dates"}

    for r in rows:
        key = type_map.get(r["type"], r["type"])
        if key in grouped:
            grouped[key].append({"name": r["name"], "count": r["count"]})

    return grouped


# ═══════════════════════════════════════════
# STATIC FILES (serve the frontend)
# ═══════════════════════════════════════════

STATIC_DIR = Path(__file__).parent.parent / "static"


@app.get("/")
def serve_frontend():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"message": "DOSSIER API is running. Place index.html in /static to serve the frontend."}
