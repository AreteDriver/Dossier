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
# AI SUMMARIZER (Ollama)
# ═══════════════════════════════════════════

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")


def _ollama_generate(prompt: str, model: str = "qwen2.5:14b", max_tokens: int = 1024) -> str:
    """Call Ollama API to generate text. Raises HTTPException 503 if unavailable."""
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": max_tokens, "temperature": 0.3},
    }).encode()
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
            rows = conn.execute("""
                SELECT d.id, d.title,
                       snippet(documents_fts, 1, '', '', '...', 80) as excerpt
                FROM documents_fts
                JOIN documents d ON d.id = documents_fts.rowid
                WHERE documents_fts MATCH ?
                ORDER BY rank LIMIT 5
            """, [f'"{fts_query}"']).fetchall()

    context = "\n\n".join(f"[{r['title']}]: {r['excerpt']}" for r in rows)
    prompt = (
        "You are analyzing a corpus of legal documents. Answer the question "
        "based on the document context provided. Be specific and cite document "
        "titles when possible.\n\n"
        f"Context:\n{context[:6000]}\n\nQuestion: {question}\n\nAnswer:"
    )
    answer = _ollama_generate(prompt, max_tokens=1500)
    sources = [{"id": r["id"], "title": r["title"]} for r in rows]
    return {"question": question, "answer": answer.strip(), "sources": sources, "model": "qwen2.5:14b"}


# ═══════════════════════════════════════════
# RELATIONSHIP MATRIX
# ═══════════════════════════════════════════


@app.get("/api/matrix/relationships")
def relationship_matrix(limit: int = Query(30, ge=5, le=100)):
    """Person-to-person relationship strength matrix."""
    with get_db() as conn:
        top_persons = conn.execute("""
            SELECT e.id, e.name, SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type = 'person'
            GROUP BY e.id
            ORDER BY total_mentions DESC
            LIMIT ?
        """, (limit,)).fetchall()

        person_ids = [p["id"] for p in top_persons]
        person_names = [p["name"] for p in top_persons]

        if len(person_ids) < 2:
            return {"entities": person_names, "matrix": [], "connections": []}

        placeholders = ",".join("?" * len(person_ids))
        connections = conn.execute(f"""
            SELECT entity_a_id, entity_b_id, weight
            FROM entity_connections
            WHERE entity_a_id IN ({placeholders})
              AND entity_b_id IN ({placeholders})
        """, person_ids + person_ids).fetchall()

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
            conn_list.append({
                "source": person_names[i], "target": person_names[j],
                "weight": c["weight"],
            })

    return {"entities": person_names, "matrix": matrix, "connections": conn_list}


# ═══════════════════════════════════════════
# GEOSPATIAL
# ═══════════════════════════════════════════


@app.get("/api/geo/locations")
def geo_locations(limit: int = Query(50, ge=1, le=200)):
    """Place entities with document counts for map visualization."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT e.id, e.name,
                   COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
            WHERE e.type = 'place'
            GROUP BY e.id
            ORDER BY doc_count DESC, total_mentions DESC
            LIMIT ?
        """, (limit,)).fetchall()

        locations = []
        for r in rows:
            loc = dict(r)
            docs = conn.execute("""
                SELECT d.id, d.title, d.category
                FROM document_entities de
                JOIN documents d ON d.id = de.document_id
                WHERE de.entity_id = ?
                ORDER BY de.count DESC LIMIT 3
            """, (r["id"],)).fetchall()
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
            doc["excerpt"] = (
                (raw["raw_text"][:300] + "...") if raw and raw["raw_text"] else ""
            )
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
            conn.execute(
                f"UPDATE board_items SET {', '.join(updates)} WHERE id = ?", params
            )

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
                    anomalies["temporal_spikes"].append({
                        "date": r["event_date"][:10],
                        "count": r["count"],
                        "avg": round(avg_count, 1),
                        "ratio": round(r["count"] / avg_count, 1) if avg_count > 0 else 0,
                    })
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
            anomalies["entity_anomalies"].append({
                "entity_a": r["entity_a"], "entity_b": r["entity_b"],
                "type_a": r["type_a"], "type_b": r["type_b"],
                "co_occurrences": r["weight"],
                "ratio": round(r["weight"] / freq_sum * 100, 1),
            })

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
        docs = conn.execute("""
            SELECT d.id, d.title, d.filename, d.category, d.source, d.date,
                   d.pages, de.count as mentions
            FROM document_entities de
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY de.count DESC
        """, (entity_id,)).fetchall()

        # Risk exposure — docs with risk scores
        risk_docs = conn.execute("""
            SELECT df.score, d.id, d.title
            FROM document_entities de
            JOIN document_forensics df ON df.document_id = de.document_id
              AND df.analysis_type = 'risk_score'
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY df.score DESC LIMIT 10
        """, (entity_id,)).fetchall()

        avg_risk = 0.0
        if risk_docs:
            avg_risk = sum(r["score"] for r in risk_docs) / len(risk_docs)

        # Timeline events mentioning this entity
        timeline = conn.execute("""
            SELECT ev.event_date, ev.precision, ev.confidence, ev.context,
                   ev.document_id
            FROM events ev
            JOIN document_entities de ON de.document_id = ev.document_id
            WHERE de.entity_id = ? AND ev.event_date IS NOT NULL
              AND ev.confidence >= 0.5
            ORDER BY ev.event_date
            LIMIT 50
        """, (entity_id,)).fetchall()

        # Top co-occurring entities
        cooccurring = conn.execute("""
            SELECT e.id, e.name, e.type, ec.weight
            FROM entity_connections ec
            JOIN entities e ON e.id = CASE
                WHEN ec.entity_a_id = ? THEN ec.entity_b_id
                ELSE ec.entity_a_id END
            WHERE (ec.entity_a_id = ? OR ec.entity_b_id = ?)
              AND ec.weight >= 1
            ORDER BY ec.weight DESC
            LIMIT 30
        """, (entity_id, entity_id, entity_id)).fetchall()

        # Tags
        _ensure_tags_table(conn)
        tags = conn.execute(
            "SELECT tag FROM entity_tags WHERE entity_id = ? ORDER BY tag",
            (entity_id,),
        ).fetchall()

        # Watchlist status
        _ensure_watchlist_table(conn)
        watched = conn.execute(
            "SELECT 1 FROM watchlist WHERE entity_id = ?", (entity_id,)
        ).fetchone() is not None

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
        similar = conn.execute("""
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
        """, (doc_id, doc_id, limit)).fetchall()

        # Get source doc entity count for similarity score
        src_total = conn.execute(
            "SELECT COUNT(DISTINCT entity_id) FROM document_entities WHERE document_id = ?",
            (doc_id,),
        ).fetchone()[0] or 1

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
<p><strong>Generated:</strong> {now} | <strong>Risk Threshold:</strong> {min_risk*100:.0f}%+
{f' | <strong>Source:</strong> {source}' if source else ''}</p>

<div>
<div class="stat"><div class="stat-val">{doc_count:,}</div><div class="stat-lbl">Documents</div></div>
<div class="stat"><div class="stat-val">{entity_count:,}</div><div class="stat-lbl">Entities</div></div>
<div class="stat"><div class="stat-val">{page_count:,}</div><div class="stat-lbl">Pages</div></div>
<div class="stat"><div class="stat-val">{len(risk_docs)}</div><div class="stat-lbl">Flagged</div></div>
</div>

<h2>Key Persons</h2>
<table><thead><tr><th>Name</th><th>Documents</th><th>Mentions</th></tr></thead><tbody>
{''.join(f'<tr><td>{p["name"]}</td><td>{p["doc_count"]}</td><td>{p["mentions"]:,}</td></tr>' for p in persons)}
</tbody></table>

<h2>Highest Risk Documents</h2>
<table><thead><tr><th>Risk</th><th>Document</th><th>Category</th><th>Source</th></tr></thead><tbody>
{''.join(f'<tr><td><span class="badge {"critical" if d["score"]>0.7 else "high" if d["score"]>0.5 else "medium"}">{d["score"]*100:.0f}%</span></td><td>{d["title"] or d["filename"]}</td><td>{d["category"]}</td><td>{d["source"] or ""}</td></tr>' for d in risk_docs[:30])}
</tbody></table>

<h2>AML Flags</h2>
<table><thead><tr><th>Flag</th><th>Severity</th><th>Count</th></tr></thead><tbody>
{''.join(f'<tr><td>{f["label"].replace("_"," ")}</td><td>{f["severity"]}</td><td>{f["count"]}</td></tr>' for f in aml)}
</tbody></table>

<h2>Network Communities ({len(communities)} detected)</h2>"""

    for i, comm in enumerate(communities[:10]):
        members = ", ".join(m["name"] for m in comm.members[:15])
        html += f"<p><strong>Community {i+1}</strong> ({comm.size} members, density {comm.density:.2f}): {members}</p>"

    html += f"""
<h2>Temporal Hotspots</h2>
<table><thead><tr><th>Date</th><th>Events</th></tr></thead><tbody>
{''.join(f'<tr><td>{s["event_date"][:10]}</td><td>{s["count"]}</td></tr>' for s in spikes)}
</tbody></table>

<hr><p style="color:#888;font-size:11px;">Generated by DOSSIER Document Intelligence System — {now}</p>
</body></html>"""

    return {"html": html, "stats": {
        "documents": doc_count, "flagged": len(risk_docs),
        "persons": len(persons), "communities": len(communities),
    }}


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
        rows = conn.execute("""
            SELECT e.id, e.name, e.type, et.tag,
                   COUNT(DISTINCT de.document_id) as doc_count,
                   SUM(de.count) as total_mentions
            FROM entity_tags et
            JOIN entities e ON e.id = et.entity_id
            LEFT JOIN document_entities de ON de.entity_id = e.id
            WHERE et.tag = ?
            GROUP BY e.id
            ORDER BY total_mentions DESC
        """, (tag,)).fetchall()
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
        except Exception as e:
            logger.exception("Community detection error")
            return {"communities": [], "error": str(e)}

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
                shared = conn.execute(f"""
                    SELECT d.id, d.title, d.category, COUNT(DISTINCT de.entity_id) as member_count
                    FROM document_entities de
                    JOIN documents d ON d.id = de.document_id
                    WHERE de.entity_id IN ({ph})
                    GROUP BY d.id
                    HAVING COUNT(DISTINCT de.entity_id) >= 2
                    ORDER BY member_count DESC LIMIT 5
                """, member_ids).fetchall()
            else:
                shared = []

            result.append({
                "id": i,
                "label": label,
                "size": comm.size,
                "density": comm.density,
                "members": [dict(m) if isinstance(m, dict) else
                            {"entity_id": m.entity_id, "name": m.name, "type": m.type}
                            if hasattr(m, "entity_id") else m
                            for m in comm.members],
                "shared_documents": [dict(d) for d in shared],
            })

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
        pairs = conn.execute("""
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
        """, (threshold, limit)).fetchall()

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
        rows = conn.execute(f"""
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
        """, ids + [limit]).fetchall()

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
            by_entity[eid]["events"].append({
                "date": r["event_date"],
                "precision": r["precision"],
                "context": r["context"],
                "doc_id": r["document_id"],
                "doc_title": r["doc_title"],
            })

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
        rows = conn.execute("""
            SELECT a.*, d.title as doc_title, d.filename as doc_filename
            FROM annotations a
            JOIN documents d ON d.id = a.document_id
            WHERE a.note LIKE ? OR a.text LIKE ?
            ORDER BY a.created_at DESC
            LIMIT 50
        """, (f"%{q}%", f"%{q}%")).fetchall()
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
        rows = conn.execute("""
            SELECT ec.entity_a_id, ea.name as entity_a_name, ea.type as entity_a_type,
                   ec.entity_b_id, eb.name as entity_b_name, eb.type as entity_b_type,
                   ec.weight, ec.co_document_count
            FROM entity_connections ec
            JOIN entities ea ON ea.id = ec.entity_a_id
            JOIN entities eb ON eb.id = ec.entity_b_id
            WHERE ec.weight >= ?
            ORDER BY ec.weight DESC
            LIMIT ?
        """, (min_weight, limit)).fetchall()

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
        rows = conn.execute("""
            SELECT ev.id, ev.event_date, ev.precision, ev.confidence,
                   ev.context, ev.document_id, d.title as doc_title
            FROM events ev
            JOIN documents d ON d.id = ev.document_id
            WHERE ev.event_date IS NOT NULL AND ev.confidence >= 0.5
            ORDER BY ev.event_date
            LIMIT ?
        """, (limit,)).fetchall()

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
        doc = conn.execute("SELECT id, title, filename, raw_text FROM documents WHERE id = ?", (doc_id,)).fetchone()
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

    return {"document_id": doc_id, "title": doc["title"] or doc["filename"], "redacted_text": text, "redaction_count": len(redactions)}


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
        _log_audit(conn, action, body.get("target_type", ""), body.get("target_id", 0), body.get("details", ""))
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
        src = conn.execute("SELECT id, name, type, canonical FROM entities WHERE id = ?", (source_id,)).fetchone()
        tgt = conn.execute("SELECT id, name, type, canonical FROM entities WHERE id = ?", (target_id,)).fetchone()
        if not src or not tgt:
            raise HTTPException(404, "Entity not found")

        # Count docs and mentions for each
        src_stats = conn.execute("""
            SELECT COUNT(DISTINCT document_id) as doc_count, SUM(count) as mentions
            FROM document_entities WHERE entity_id = ?
        """, (source_id,)).fetchone()
        tgt_stats = conn.execute("""
            SELECT COUNT(DISTINCT document_id) as doc_count, SUM(count) as mentions
            FROM document_entities WHERE entity_id = ?
        """, (target_id,)).fetchone()

        # Shared documents
        shared = conn.execute("""
            SELECT COUNT(DISTINCT de1.document_id)
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
        """, (source_id, target_id)).fetchone()[0]

        # Tags
        _ensure_tags_table(conn)
        src_tags = [r["tag"] for r in conn.execute("SELECT tag FROM entity_tags WHERE entity_id = ?", (source_id,)).fetchall()]
        tgt_tags = [r["tag"] for r in conn.execute("SELECT tag FROM entity_tags WHERE entity_id = ?", (target_id,)).fetchall()]

        # Connections
        src_conns = conn.execute("SELECT COUNT(*) FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?", (source_id, source_id)).fetchone()[0]
        tgt_conns = conn.execute("SELECT COUNT(*) FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?", (target_id, target_id)).fetchone()[0]

    return {
        "source": {**dict(src), "doc_count": src_stats["doc_count"], "mentions": src_stats["mentions"], "tags": src_tags, "connections": src_conns},
        "target": {**dict(tgt), "doc_count": tgt_stats["doc_count"], "mentions": tgt_stats["mentions"], "tags": tgt_tags, "connections": tgt_conns},
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
        shared_docs = conn.execute("""
            SELECT de1.document_id, de1.count as src_count, de2.count as tgt_count
            FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
        """, (source_id, target_id)).fetchall()

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
        conn.execute("DELETE FROM entity_connections WHERE entity_a_id = ? OR entity_b_id = ?", (source_id, source_id))

        # Transfer tags
        _ensure_tags_table(conn)
        src_tags = conn.execute("SELECT tag FROM entity_tags WHERE entity_id = ?", (source_id,)).fetchall()
        for t in src_tags:
            conn.execute("INSERT OR IGNORE INTO entity_tags (entity_id, tag) VALUES (?, ?)", (target_id, t["tag"]))
        conn.execute("DELETE FROM entity_tags WHERE entity_id = ?", (source_id,))

        # Transfer watchlist
        _ensure_watchlist_table(conn)
        watched = conn.execute("SELECT notes FROM watchlist WHERE entity_id = ?", (source_id,)).fetchone()
        if watched:
            conn.execute("INSERT OR IGNORE INTO watchlist (entity_id, notes) VALUES (?, ?)", (target_id, watched["notes"]))
            conn.execute("DELETE FROM watchlist WHERE entity_id = ?", (source_id,))

        # Delete source entity
        conn.execute("DELETE FROM entities WHERE id = ?", (source_id,))

        # Audit
        _log_audit(conn, "entity_merge", "entity", target_id,
                   f"Merged '{src['name']}' (#{source_id}) into '{tgt['name']}' (#{target_id})")

    return {"merged": True, "source_id": source_id, "target_id": target_id, "target_name": tgt["name"]}


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
            doc_rows = conn.execute(f"""
                SELECT id, title, filename, category, source, pages
                FROM documents WHERE id IN ({ph})
            """, sample_ids).fetchall()

            # Get shared entities across cluster
            all_ph = ",".join("?" * len(doc_ids))
            shared_entities = conn.execute(f"""
                SELECT e.name, e.type, COUNT(DISTINCT de.document_id) as doc_count
                FROM document_entities de
                JOIN entities e ON e.id = de.entity_id
                WHERE de.document_id IN ({all_ph})
                GROUP BY e.id
                HAVING COUNT(DISTINCT de.document_id) >= ?
                ORDER BY doc_count DESC LIMIT 10
            """, doc_ids + [max(2, len(doc_ids) // 3)]).fetchall()

            clusters.append({
                "keyword": kw,
                "size": len(doc_ids),
                "documents": [dict(r) for r in doc_rows],
                "shared_entities": [dict(e) for e in shared_entities],
            })

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
            (name, body.get("query_text", ""), body.get("category", ""), body.get("entity_type", ""), body.get("source", "")),
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
            entity_matches = conn.execute("""
                SELECT DISTINCT e.id, e.name, e.type
                FROM entities e
                WHERE LENGTH(e.name) >= 3 AND LOWER(e.name) != ''
                  AND ? LIKE '%' || LOWER(e.name) || '%'
                ORDER BY LENGTH(e.name) DESC
                LIMIT 20
            """, (text_lower,)).fetchall()

            if entity_matches:
                entity_ids = [e["id"] for e in entity_matches]
                ph = ",".join("?" * len(entity_ids))
                xrefs = conn.execute(f"""
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
                """, entity_ids + [doc_id, limit]).fetchall()
                results = [dict(r) for r in xrefs]

            # Also try FTS if text is meaningful enough
            if len(text.strip()) >= 5 and len(results) < limit:
                fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", text.strip())[:100]
                try:
                    fts_results = conn.execute("""
                        SELECT d.id, d.title, d.filename, d.category, d.source,
                               snippet(documents_fts, 1, '<mark>', '</mark>', '...', 20) as excerpt
                        FROM documents_fts
                        JOIN documents d ON d.id = documents_fts.rowid
                        WHERE documents_fts MATCH ? AND d.id != ?
                        LIMIT ?
                    """, (f'"{fts_query}"', doc_id, limit)).fetchall()

                    existing_ids = {r["id"] for r in results}
                    for fr in fts_results:
                        if fr["id"] not in existing_ids:
                            results.append({**dict(fr), "matching_entities": 0, "matched_names": "", "fts_match": True})
                except Exception:
                    pass  # FTS match may fail on certain inputs

        return {
            "doc_id": doc_id,
            "cross_references": results[:limit],
            "query_text": text[:200],
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
