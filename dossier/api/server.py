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

import json
import logging
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, Request, UploadFile, File, Query, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
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


@app.get("/api/entities/search")
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


@app.get("/api/forensics/financial")
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


@app.get("/api/forensics/codewords")
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


@app.get("/api/forensics/phrases")
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
# AI SUMMARIZER (Ollama)
# ═══════════════════════════════════════════

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")


def _ollama_generate(prompt: str, model: str = "qwen2.5:14b", max_tokens: int = 1024) -> str:
    """Call Ollama API to generate text. Raises HTTPException 503 if unavailable."""
    payload = json.dumps(
        {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": max_tokens, "temperature": 0.3},
        }
    ).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            result = json.loads(resp.read())
            return result.get("response", "")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise HTTPException(503, f"Ollama unavailable: {e}")


@app.post("/api/ai/summarize")
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
    summary = _ollama_generate(prompt)
    return {"doc_id": doc_id, "summary": summary.strip(), "model": "qwen2.5:14b"}


@app.post("/api/ai/ask")
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
    answer = _ollama_generate(prompt, max_tokens=1500)
    sources = [{"id": r["id"], "title": r["title"]} for r in rows]
    return {
        "question": question,
        "answer": answer.strip(),
        "sources": sources,
        "model": "qwen2.5:14b",
    }


# ═══════════════════════════════════════════
# RELATIONSHIP MATRIX
# ═══════════════════════════════════════════


@app.get("/api/matrix/relationships")
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
# GEOSPATIAL
# ═══════════════════════════════════════════


@app.get("/api/geo/locations")
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
# ADVANCED SEARCH
# ═══════════════════════════════════════════


@app.get("/api/search/advanced")
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
            doc["entities"] = _get_doc_entities(conn, doc["id"])
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


# ═══════════════════════════════════════════
# INVESTIGATION BOARD
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


@app.get("/api/board")
def get_board():
    """Get all investigation board items."""
    with get_db() as conn:
        _ensure_board_table(conn)
        rows = conn.execute("SELECT * FROM board_items ORDER BY created_at").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/board")
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


@app.put("/api/board/{item_id}")
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


@app.delete("/api/board/{item_id}")
def delete_board_item(item_id: int):
    """Remove an item from the investigation board."""
    with get_db() as conn:
        _ensure_board_table(conn)
        conn.execute("DELETE FROM board_items WHERE id = ?", (item_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# ANOMALY DETECTION
# ═══════════════════════════════════════════


@app.get("/api/anomalies")
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
# ENTITY PROFILES
# ═══════════════════════════════════════════


@app.get("/api/entities/{entity_id}/profile")
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
# DOCUMENT SIMILARITY
# ═══════════════════════════════════════════


@app.get("/api/documents/{doc_id}/similar")
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
# REPORT GENERATOR
# ═══════════════════════════════════════════


@app.get("/api/export/report")
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
# ENTITY TAGGING
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


@app.get("/api/entities/{entity_id}/tags")
def get_entity_tags(entity_id: int):
    """Get tags for an entity."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        rows = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ? ORDER BY tag",
            (entity_id,),
        ).fetchall()
    return {"entity_id": entity_id, "tags": [r["tag"] for r in rows]}


@app.post("/api/entities/{entity_id}/tags")
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


@app.delete("/api/entities/{entity_id}/tags/{tag}")
def remove_entity_tag(entity_id: int, tag: str):
    """Remove a tag from an entity."""
    with get_db() as conn:
        _ensure_tags_table(conn)
        conn.execute(
            "DELETE FROM entity_tags WHERE entity_id = ? AND tag = ?",
            (entity_id, tag),
        )
    return {"entity_id": entity_id, "tag": tag, "removed": True}


@app.get("/api/entities/by-tag")
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


@app.get("/api/tags")
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
# WATCHLIST
# ═══════════════════════════════════════════


def _ensure_watchlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            entity_id INTEGER PRIMARY KEY,
            notes TEXT DEFAULT '',
            added_at TEXT DEFAULT (datetime('now'))
        )
    """)


@app.get("/api/watchlist")
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


@app.post("/api/watchlist")
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


@app.delete("/api/watchlist/{entity_id}")
def remove_from_watchlist(entity_id: int):
    """Remove an entity from the watchlist."""
    with get_db() as conn:
        _ensure_watchlist_table(conn)
        conn.execute("DELETE FROM watchlist WHERE entity_id = ?", (entity_id,))
    return {"entity_id": entity_id, "removed": True}


# ═══════════════════════════════════════════
# NETWORK COMMUNITIES
# ═══════════════════════════════════════════


@app.get("/api/graph/communities-labeled")
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
# DUPLICATE DETECTION
# ═══════════════════════════════════════════


@app.get("/api/duplicates")
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


@app.post("/api/duplicates/dismiss")
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
# ENTITY TIMELINE OVERLAY
# ═══════════════════════════════════════════


@app.get("/api/timeline/overlay")
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
# DOCUMENT ANNOTATIONS
# ═══════════════════════════════════════════


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


@app.get("/api/documents/{doc_id}/annotations")
def get_annotations(doc_id: int):
    """Get all annotations for a document."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        rows = conn.execute(
            "SELECT * FROM annotations WHERE document_id = ? ORDER BY start_offset",
            (doc_id,),
        ).fetchall()
    return {"document_id": doc_id, "annotations": [dict(r) for r in rows]}


@app.post("/api/documents/{doc_id}/annotations")
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


@app.delete("/api/annotations/{annotation_id}")
def delete_annotation(annotation_id: int):
    """Delete an annotation."""
    with get_db() as conn:
        _ensure_annotations_table(conn)
        conn.execute("DELETE FROM annotations WHERE id = ?", (annotation_id,))
    return {"id": annotation_id, "deleted": True}


@app.get("/api/annotations/search")
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
# TAG ANALYTICS
# ═══════════════════════════════════════════


@app.get("/api/tags/analytics")
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


@app.post("/api/tags/bulk")
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
# EXPORT ENDPOINTS
# ═══════════════════════════════════════════


@app.get("/api/export/entities")
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


@app.get("/api/export/connections")
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
                   ec.weight, ec.co_document_count
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


@app.get("/api/export/timeline")
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


# ═══════════════════════════════════════════
# RISK SCORING DASHBOARD
# ═══════════════════════════════════════════


@app.get("/api/risk/dashboard")
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


@app.get("/api/documents/{doc_id}/redactions")
def get_redactions(doc_id: int):
    """Get all redaction spans for a document."""
    with get_db() as conn:
        _ensure_redactions_table(conn)
        rows = conn.execute(
            "SELECT * FROM redactions WHERE document_id = ? ORDER BY start_offset",
            (doc_id,),
        ).fetchall()
    return {"document_id": doc_id, "redactions": [dict(r) for r in rows]}


@app.post("/api/documents/{doc_id}/redactions")
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


@app.delete("/api/redactions/{redaction_id}")
def delete_redaction(redaction_id: int):
    """Delete a redaction span."""
    with get_db() as conn:
        _ensure_redactions_table(conn)
        conn.execute("DELETE FROM redactions WHERE id = ?", (redaction_id,))
    return {"id": redaction_id, "deleted": True}


@app.get("/api/documents/{doc_id}/redacted-text")
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


# ═══════════════════════════════════════════
# AUDIT TRAIL
# ═══════════════════════════════════════════


def _ensure_audit_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            target_type TEXT,
            target_id INTEGER,
            details TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _log_audit(conn, action: str, target_type: str = "", target_id: int = 0, details: str = ""):
    """Record an audit trail entry."""
    _ensure_audit_table(conn)
    conn.execute(
        "INSERT INTO audit_log (action, target_type, target_id, details) VALUES (?, ?, ?, ?)",
        (action, target_type, target_id, details),
    )


@app.get("/api/audit")
def get_audit_log(
    action: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Get audit trail entries."""
    with get_db() as conn:
        _ensure_audit_table(conn)
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


@app.post("/api/audit")
async def add_audit_entry(request: Request):
    """Manually add an audit entry (for frontend-tracked actions)."""
    body = await request.json()
    action = body.get("action", "")
    if not action:
        raise HTTPException(400, "action required")
    with get_db() as conn:
        _log_audit(
            conn,
            action,
            body.get("target_type", ""),
            body.get("target_id", 0),
            body.get("details", ""),
        )
    return {"logged": True}


# ═══════════════════════════════════════════
# ENTITY MERGE
# ═══════════════════════════════════════════


@app.get("/api/entities/merge-preview")
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


@app.post("/api/entities/merge")
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
        _log_audit(
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
# DOCUMENT CLUSTERS
# ═══════════════════════════════════════════


@app.get("/api/clusters")
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
# SAVED QUERIES
# ═══════════════════════════════════════════


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


@app.get("/api/saved-queries")
def list_saved_queries():
    """Get all saved queries."""
    with get_db() as conn:
        _ensure_saved_queries_table(conn)
        rows = conn.execute("SELECT * FROM saved_queries ORDER BY created_at DESC").fetchall()
    return {"queries": [dict(r) for r in rows]}


@app.post("/api/saved-queries")
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


@app.delete("/api/saved-queries/{query_id}")
def delete_saved_query(query_id: int):
    """Delete a saved query."""
    with get_db() as conn:
        _ensure_saved_queries_table(conn)
        conn.execute("DELETE FROM saved_queries WHERE id = ?", (query_id,))
    return {"id": query_id, "deleted": True}


# ═══════════════════════════════════════════
# CROSS-REFERENCE
# ═══════════════════════════════════════════


@app.get("/api/documents/{doc_id}/cross-references")
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
# EVIDENCE CHAINS
# ═══════════════════════════════════════════


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


@app.get("/api/evidence-chains")
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


@app.get("/api/evidence-chains/{chain_id}")
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


@app.post("/api/evidence-chains")
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
        _log_audit(conn, "create_chain", "chain", cur.lastrowid, name)
    return {"id": cur.lastrowid, "name": name}


@app.post("/api/evidence-chains/{chain_id}/links")
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


@app.delete("/api/evidence-chains/{chain_id}")
def delete_evidence_chain(chain_id: int):
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        conn.execute("DELETE FROM evidence_chain_links WHERE chain_id = ?", (chain_id,))
        conn.execute("DELETE FROM evidence_chains WHERE id = ?", (chain_id,))
    return {"deleted": True}


@app.delete("/api/evidence-chain-links/{link_id}")
def delete_chain_link(link_id: int):
    with get_db() as conn:
        _ensure_evidence_chains_table(conn)
        conn.execute("DELETE FROM evidence_chain_links WHERE id = ?", (link_id,))
    return {"deleted": True}


@app.get("/api/evidence-chains/{chain_id}/export")
def export_evidence_chain(chain_id: int):
    """Export evidence chain as HTML case brief."""
    import datetime

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
# PATTERN DETECTION
# ═══════════════════════════════════════════


@app.get("/api/patterns")
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
# DOCUMENT COMPARISON
# ═══════════════════════════════════════════


@app.get("/api/compare-documents")
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
# ENTITY ALIASES
# ═══════════════════════════════════════════


def _ensure_aliases_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL,
            alias_name TEXT NOT NULL,
            UNIQUE(entity_id, alias_name)
        )
    """)


@app.get("/api/entities/{entity_id}/aliases")
def get_aliases(entity_id: int):
    with get_db() as conn:
        _ensure_aliases_table(conn)
        rows = conn.execute(
            "SELECT * FROM entity_aliases WHERE entity_id = ? ORDER BY alias_name", (entity_id,)
        ).fetchall()
    return {"entity_id": entity_id, "aliases": [dict(r) for r in rows]}


@app.post("/api/entities/{entity_id}/aliases")
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
        _log_audit(conn, "add_alias", "entity", entity_id, alias)
    return {"entity_id": entity_id, "alias": alias, "added": True}


@app.delete("/api/aliases/{alias_id}")
def delete_alias(alias_id: int):
    with get_db() as conn:
        _ensure_aliases_table(conn)
        conn.execute("DELETE FROM entity_aliases WHERE id = ?", (alias_id,))
    return {"deleted": True}


@app.get("/api/aliases/resolve")
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
# SENTIMENT & TONE ANALYSIS
# ═══════════════════════════════════════════


@app.get("/api/documents/{doc_id}/tone")
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
# INVESTIGATION SNAPSHOTS
# ═══════════════════════════════════════════


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


@app.get("/api/snapshots")
def list_snapshots():
    with get_db() as conn:
        _ensure_snapshots_table(conn)
        rows = conn.execute(
            "SELECT id, name, description, created_at FROM investigation_snapshots ORDER BY created_at DESC"
        ).fetchall()
    return {"snapshots": [dict(r) for r in rows]}


@app.post("/api/snapshots")
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
            for r in conn.execute("SELECT document_id, x, y, notes FROM board_items").fetchall()
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
        _log_audit(conn, "create_snapshot", "snapshot", cur.lastrowid, name)

    return {"id": cur.lastrowid, "name": name}


@app.get("/api/snapshots/{snapshot_id}")
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


@app.delete("/api/snapshots/{snapshot_id}")
def delete_snapshot(snapshot_id: int):
    with get_db() as conn:
        _ensure_snapshots_table(conn)
        conn.execute("DELETE FROM investigation_snapshots WHERE id = ?", (snapshot_id,))
    return {"deleted": True}


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
# KEYWORD ALERTS
# ═══════════════════════════════════════════


def _ensure_keyword_alerts_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS keyword_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now')),
            is_active INTEGER DEFAULT 1
        )
    """)


@app.get("/api/keyword-alerts")
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


@app.post("/api/keyword-alerts")
async def create_keyword_alert(request: Request):
    body = await request.json()
    keyword = body.get("keyword", "").strip()
    if not keyword:
        raise HTTPException(400, "keyword required")
    with get_db() as conn:
        _ensure_keyword_alerts_table(conn)
        conn.execute("INSERT OR IGNORE INTO keyword_alerts (keyword) VALUES (?)", (keyword,))
        _log_audit(conn, "create_keyword_alert", "keyword", 0, keyword)
    return {"keyword": keyword, "created": True}


@app.delete("/api/keyword-alerts/{alert_id}")
def delete_keyword_alert(alert_id: int):
    with get_db() as conn:
        _ensure_keyword_alerts_table(conn)
        conn.execute("DELETE FROM keyword_alerts WHERE id = ?", (alert_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# LINK ANALYSIS (CENTRALITY METRICS)
# ═══════════════════════════════════════════


@app.get("/api/link-analysis")
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
# ANALYST NOTES
# ═══════════════════════════════════════════


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


@app.get("/api/documents/{doc_id}/analyst-notes")
def get_analyst_notes(doc_id: int):
    with get_db() as conn:
        _ensure_analyst_notes_table(conn)
        notes = conn.execute(
            "SELECT * FROM analyst_notes WHERE document_id = ? ORDER BY created_at DESC", (doc_id,)
        ).fetchall()
    return {"document_id": doc_id, "notes": [dict(n) for n in notes]}


@app.post("/api/documents/{doc_id}/analyst-notes")
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
        _log_audit(conn, "add_note", "document", doc_id, note[:100])
    return {"id": cur.lastrowid, "document_id": doc_id, "note": note, "author": author}


@app.delete("/api/notes/{note_id}")
def delete_note(note_id: int):
    with get_db() as conn:
        _ensure_analyst_notes_table(conn)
        conn.execute("DELETE FROM analyst_notes WHERE id = ?", (note_id,))
    return {"deleted": True}


# ═══════════════════════════════════════════
# COMMUNICATION FLOW
# ═══════════════════════════════════════════


@app.get("/api/communication-flow")
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
# OCR QUALITY VIEWER
# ═══════════════════════════════════════════


@app.get("/api/documents/{doc_id}/ocr-quality")
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


@app.get("/api/ocr-quality-overview")
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


# ═══════════════════════════════════════════
# CASE FILE BUILDER
# ═══════════════════════════════════════════


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


@app.get("/api/case-files")
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


@app.post("/api/case-files")
async def create_case_file(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    desc = body.get("description", "").strip()
    if not name:
        raise HTTPException(400, "name required")
    with get_db() as conn:
        _ensure_case_files_table(conn)
        cur = conn.execute("INSERT INTO case_files (name, description) VALUES (?, ?)", (name, desc))
        _log_audit(conn, "create_case_file", "case_file", cur.lastrowid, name)
    return {"id": cur.lastrowid, "name": name}


@app.get("/api/case-files/{case_id}")
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


@app.post("/api/case-files/{case_id}/items")
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


@app.delete("/api/case-file-items/{item_id}")
def remove_case_file_item(item_id: int):
    with get_db() as conn:
        _ensure_case_files_table(conn)
        conn.execute("DELETE FROM case_file_items WHERE id = ?", (item_id,))
    return {"deleted": True}


@app.delete("/api/case-files/{case_id}")
def delete_case_file(case_id: int):
    with get_db() as conn:
        _ensure_case_files_table(conn)
        conn.execute("DELETE FROM case_file_items WHERE case_file_id = ?", (case_id,))
        conn.execute("DELETE FROM case_files WHERE id = ?", (case_id,))
    return {"deleted": True}


@app.get("/api/case-files/{case_id}/export")
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


# ═══════════════════════════════════════════
# FINANCIAL TRAIL TRACKER
# ═══════════════════════════════════════════


@app.get("/api/financial-trail")
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
# ENTITY DOSSIER EXPORT
# ═══════════════════════════════════════════


@app.get("/api/entities/{entity_id}/dossier-export")
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
        html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
        <title>Dossier: {entity["name"]}</title>
        <style>body{{font-family:sans-serif;max-width:900px;margin:40px auto;padding:0 20px;color:#222;}}
        h1{{border-bottom:3px solid #c4473a;padding-bottom:8px;}}
        h2{{color:#c4473a;margin-top:30px;border-bottom:1px solid #ddd;padding-bottom:4px;}}
        .tag{{display:inline-block;background:#f0f0f0;padding:2px 8px;border-radius:4px;margin:2px;font-size:12px;}}
        table{{border-collapse:collapse;width:100%;margin:10px 0;}} th,td{{border:1px solid #ddd;padding:6px 10px;text-align:left;font-size:13px;}}
        th{{background:#f5f5f5;}} .meta{{color:#666;font-size:13px;}}</style></head><body>
        <h1>{entity["name"]}</h1>
        <p class="meta">Type: {entity["type"]} | Entity ID: {entity["id"]} | Generated: {__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M")}</p>"""

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


# ═══════════════════════════════════════════
# DOCUMENT CLUSTER VISUALIZATION DATA
# ═══════════════════════════════════════════


@app.get("/api/cluster-map")
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
# WITNESS / DEPONENT INDEX
# ═══════════════════════════════════════════


@app.get("/api/witness-index")
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
# ACTIVITY HEATMAP CALENDAR
# ═══════════════════════════════════════════


@app.get("/api/activity-heatmap")
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
# BULK TAGGER
# ═══════════════════════════════════════════


@app.post("/api/bulk-tag")
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
        _log_audit(
            conn,
            "bulk_tag",
            "documents",
            len(doc_ids),
            f"tag={tag}, category={category}, count={updated}",
        )

    return {"updated": updated, "total_requested": len(doc_ids)}


@app.get("/api/bulk-tag-suggestions")
def bulk_tag_suggestions():
    """Get existing categories and common tags for the bulk tagger UI."""
    with get_db() as conn:
        categories = conn.execute(
            "SELECT category, COUNT(*) as count FROM documents GROUP BY category ORDER BY count DESC"
        ).fetchall()

        # Extract [tag:*] patterns from notes
        tag_rows = conn.execute("SELECT notes FROM documents WHERE notes LIKE '%[tag:%'").fetchall()
        import re

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
# ENTITY TIMELINE (per-entity chronological view)
# ═══════════════════════════════════════════


@app.get("/api/entities/{entity_id}/timeline")
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
# SOURCE CREDIBILITY (rate and track sources)
# ═══════════════════════════════════════════


def _ensure_source_ratings_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_ratings (
            source TEXT PRIMARY KEY,
            rating TEXT DEFAULT 'C',
            notes TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@app.get("/api/source-credibility")
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


@app.post("/api/source-credibility/{source}/rate")
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


# ═══════════════════════════════════════════
# DOCUMENT GAPS (temporal gap analysis)
# ═══════════════════════════════════════════


@app.get("/api/document-gaps")
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
# REDACTION ANALYSIS
# ═══════════════════════════════════════════


@app.get("/api/redaction-analysis")
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


# ═══════════════════════════════════════════
# CORROBORATION ENGINE
# ═══════════════════════════════════════════


@app.get("/api/corroboration")
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


@app.get("/api/depositions")
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


@app.get("/api/narrative")
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


@app.get("/api/contact-network")
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
# DOCUMENT PROVENANCE (chain of custody tracking)
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


@app.get("/api/documents/{doc_id}/provenance")
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


@app.post("/api/documents/{doc_id}/provenance")
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


@app.get("/api/provenance-summary")
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


# ═══════════════════════════════════════════
# KEY PHRASE TRENDS (phrase frequency over time)
# ═══════════════════════════════════════════


@app.get("/api/phrase-trends")
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


@app.get("/api/entity-disambiguation")
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
# INVESTIGATION STATS (comprehensive metrics)
# ═══════════════════════════════════════════


@app.get("/api/investigation-stats")
def investigation_stats():
    """Comprehensive investigation metrics dashboard."""
    with get_db() as conn:
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


# ═══════════════════════════════════════════
# INFLUENCE SCORE (composite entity ranking)
# ═══════════════════════════════════════════


@app.get("/api/influence-scores")
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


@app.get("/api/entity-clusters")
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


@app.get("/api/cover-names")
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


@app.get("/api/flight-analysis")
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


# ═══════════════════════════════════════════
# CROSS-REFERENCE MATRIX (entity × source)
# ═══════════════════════════════════════════


@app.get("/api/xref-matrix")
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
# INVESTIGATION TIMELINE (meta-timeline)
# ═══════════════════════════════════════════


@app.get("/api/investigation-timeline")
def investigation_timeline():
    """Meta-timeline of the investigation: ingestion, analysis, and annotation events."""
    with get_db() as conn:
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
# KEYWORD CO-OCCURRENCE
# ═══════════════════════════════════════════


@app.get("/api/keyword-cooccurrence")
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
# ENTITY NETWORK PATHS (shortest path)
# ═══════════════════════════════════════════


@app.get("/api/entity-path")
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


@app.get("/api/entity-path-suggestions")
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
# DOCUMENT SENTIMENT (tone distribution)
# ═══════════════════════════════════════════


@app.get("/api/document-sentiment")
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
# SOURCE TIMELINE (per-source temporal distribution)
# ═══════════════════════════════════════════


@app.get("/api/source-timeline")
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
# ENTITY FREQUENCY RANK (over time)
# ═══════════════════════════════════════════


@app.get("/api/entity-frequency")
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
# FLAGGED DOCUMENTS HUB
# ═══════════════════════════════════════════


@app.get("/api/flagged-hub")
def flagged_hub():
    """Centralized view for flagged/bookmarked documents with notes and entities."""
    with get_db() as conn:
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
# ENTITY RELATIONSHIPS GRAPH (weighted, filterable)
# ═══════════════════════════════════════════


@app.get("/api/relationship-graph")
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
# DOCUMENT SIDE-BY-SIDE COMPARISON
# ═══════════════════════════════════════════


@app.get("/api/document-sidebyside")
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
# LOCATION FREQUENCY
# ═══════════════════════════════════════════


@app.get("/api/location-frequency")
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
# FINANCIAL RISK PROFILES (per-entity)
# ═══════════════════════════════════════════


@app.get("/api/financial-profiles")
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
# SEARCH HISTORY (persistent)
# ═══════════════════════════════════════════


def _ensure_search_history_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            result_count INTEGER DEFAULT 0,
            searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@app.get("/api/search-history")
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


@app.post("/api/search-history")
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


@app.delete("/api/search-history")
def clear_search_history():
    """Clear all search history."""
    with get_db() as conn:
        _ensure_search_history_table(conn)
        conn.execute("DELETE FROM search_history")
        conn.commit()
    return {"status": "cleared"}


# ═══════════════════════════════════════════
# CATEGORY DISTRIBUTION (visual breakdown)
# ═══════════════════════════════════════════


@app.get("/api/category-distribution")
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


@app.get("/api/witness-overlap")
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


@app.get("/api/document-age")
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


@app.get("/api/entity-coappearances")
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


@app.get("/api/unresolved-entities")
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


@app.get("/api/document-completeness")
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


@app.get("/api/key-dates")
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


@app.get("/api/alias-network")
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


@app.get("/api/document-length")
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


@app.get("/api/temporal-heatmap")
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


@app.get("/api/entity-type-breakdown")
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


@app.get("/api/source-network")
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


@app.get("/api/redaction-density")
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


# ── Entity Timeline Density ──────────────────────────


@app.get("/api/entity-timeline-density")
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


@app.get("/api/document-duplicates")
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


@app.get("/api/connection-strength")
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


@app.get("/api/category-timeline")
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


@app.get("/api/orphan-documents")
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


@app.get("/api/entity-first-last")
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
                pass
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


@app.get("/api/cross-source-entities")
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


@app.get("/api/page-distribution")
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


@app.get("/api/entity-name-length")
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


@app.get("/api/ingest-timeline")
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


@app.get("/api/high-value-targets")
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


@app.get("/api/keyword-context")
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


@app.get("/api/entity-connections-map")
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


@app.get("/api/document-word-count")
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


@app.get("/api/mention-heatmap")
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


@app.get("/api/source-quality")
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


@app.get("/api/event-calendar")
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


@app.get("/api/entity-pair-history")
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


@app.get("/api/financial-entity-links")
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


@app.get("/api/doc-source-cluster")
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


@app.get("/api/timeline-gaps")
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


@app.get("/api/entity-degree-distribution")
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


@app.get("/api/multi-mention-docs")
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


@app.get("/api/flagged-summary")
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


@app.get("/api/resolution-audit")
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


@app.get("/api/document-shared-entities")
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


@app.get("/api/source-date-range")
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


@app.get("/api/search-history-stats")
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


@app.get("/api/category-entity-matrix")
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


@app.get("/api/event-entity-ranking")
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


@app.get("/api/entity-aliases-list")
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


@app.get("/api/category-stats")
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


@app.get("/api/redaction-by-source")
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


# ── Entity Pair Co-Documents ───────────────────


@app.get("/api/entity-pair-codocs")
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


@app.get("/api/event-types")
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


@app.get("/api/financial-summary")
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


@app.get("/api/entity-document-count")
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


@app.get("/api/source-overlap")
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


@app.get("/api/event-context")
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


@app.get("/api/document-date-clusters")
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


@app.get("/api/redaction-patterns")
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


# ── Entity Isolation Score ───────────────────────


@app.get("/api/entity-isolation")
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


@app.get("/api/entity-growth")
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


@app.get("/api/text-length-distribution")
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


@app.get("/api/source-entity-density")
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


@app.get("/api/event-heatmap")
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


@app.get("/api/connection-weight-distribution")
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


@app.get("/api/multi-source-entities")
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


@app.get("/api/hash-audit")
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


@app.get("/api/canonical-coverage")
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


@app.get("/api/fts-stats")
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


@app.get("/api/event-resolution-rate")
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


@app.get("/api/top-connections")
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


@app.get("/api/document-notes")
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


@app.get("/api/entity-name-duplicates")
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


@app.get("/api/ingest-velocity")
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


@app.get("/api/event-confidence-ranking")
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


@app.get("/api/source-page-distribution")
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


@app.get("/api/entity-type-ratio")
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


@app.get("/api/connection-density")
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


@app.get("/api/document-readability")
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


@app.get("/api/source-completeness")
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


@app.get("/api/orphan-events")
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


@app.get("/api/entity-first-seen")
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


@app.get("/api/page-density")
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


@app.get("/api/duplicate-documents")
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


@app.get("/api/entity-connections-timeline")
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


@app.get("/api/source-cross-reference")
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


@app.get("/api/event-precision-stats")
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


@app.get("/api/category-ingest-timeline")
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


@app.get("/api/entity-hub-score")
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


@app.get("/api/redaction-density-ranking")
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


# ── Round 29 ──────────────────────────────────


@app.get("/api/entity-spread")
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


@app.get("/api/document-size-buckets")
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


@app.get("/api/event-date-gaps")
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


@app.get("/api/source-entity-overlap")
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


@app.get("/api/unresolved-entities-summary")
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


@app.get("/api/connection-reciprocity")
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


@app.get("/api/entity-longevity")
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


@app.get("/api/document-flagged-ratio")
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


@app.get("/api/event-cluster-density")
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


@app.get("/api/source-ingestion-summary")
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


@app.get("/api/entity-singletons")
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


@app.get("/api/page-text-coverage")
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
