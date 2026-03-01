"""PDF metadata extraction and storage for forensic provenance analysis.

Extracts embedded metadata (author, creator, producer, dates, encryption)
from PDF documents during ingestion and provides query functions for
corpus-wide forensic analysis.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class PDFMetadata:
    """Structured PDF metadata extracted from a document."""

    document_id: int
    author: str | None
    creator: str | None
    producer: str | None
    title: str | None
    subject: str | None
    keywords: str | None
    creation_date: str | None
    modification_date: str | None
    encrypted: bool
    page_count: int
    file_size: int


# ── Date parsing ────────────────────────────────────────────────


def _parse_pdf_date(date_str: str | None) -> str | None:
    """Convert PDF date format to ISO 8601.

    PDF dates follow the format: D:YYYYMMDDHHmmSSOHH'mm'
    where O is +, -, or Z for timezone offset.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    # Strip the D: prefix
    s = date_str.strip()
    if s.startswith("D:"):
        s = s[2:]

    # Remove timezone info for parsing (we'll note it but store as-is)
    s = s.replace("'", "")

    # Try progressively shorter formats
    formats = [
        ("%Y%m%d%H%M%S", 14),
        ("%Y%m%d%H%M", 12),
        ("%Y%m%d%H", 10),
        ("%Y%m%d", 8),
        ("%Y%m", 6),
        ("%Y", 4),
    ]

    # Strip timezone suffix (Z, +HH00, -HH00, etc.)
    clean = re.sub(r"[Z+-]\d*$", "", s).strip()

    for fmt, min_len in formats:
        if len(clean) >= min_len:
            try:
                dt = datetime.strptime(clean[:min_len], fmt)
                return dt.isoformat()
            except ValueError:
                continue

    return None


# ── Extraction ──────────────────────────────────────────────────


def extract_pdf_metadata(filepath: str, document_id: int) -> PDFMetadata | None:
    """Extract metadata from a PDF file.

    Returns None for non-PDF files or if extraction fails entirely.
    """
    if not filepath.lower().endswith(".pdf"):
        return None

    try:
        file_size = os.path.getsize(filepath)
    except OSError:
        return None

    try:
        import pdfplumber

        with pdfplumber.open(filepath) as pdf:
            meta = pdf.metadata or {}
            page_count = len(pdf.pages)

            # Check encryption — pdfplumber wraps pdfminer which sets this
            encrypted = bool(getattr(pdf, "is_encrypted", False))
            if not encrypted:
                # Fallback: check if metadata has encryption indicators
                encrypted = bool(meta.get("Encrypt") or meta.get("/Encrypt"))

            return PDFMetadata(
                document_id=document_id,
                author=meta.get("Author") or meta.get("/Author") or None,
                creator=meta.get("Creator") or meta.get("/Creator") or None,
                producer=meta.get("Producer") or meta.get("/Producer") or None,
                title=meta.get("Title") or meta.get("/Title") or None,
                subject=meta.get("Subject") or meta.get("/Subject") or None,
                keywords=meta.get("Keywords") or meta.get("/Keywords") or None,
                creation_date=_parse_pdf_date(
                    meta.get("CreationDate") or meta.get("/CreationDate")
                ),
                modification_date=_parse_pdf_date(meta.get("ModDate") or meta.get("/ModDate")),
                encrypted=encrypted,
                page_count=page_count,
                file_size=file_size,
            )
    except Exception:
        logger.exception("PDF metadata extraction failed for %s", filepath)
        return None


# ── Database ────────────────────────────────────────────────────

PDF_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_pdf_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL UNIQUE REFERENCES documents(id) ON DELETE CASCADE,
    author TEXT,
    creator TEXT,
    producer TEXT,
    title TEXT,
    subject TEXT,
    keywords TEXT,
    creation_date TEXT,
    modification_date TEXT,
    encrypted BOOLEAN DEFAULT 0,
    page_count INTEGER,
    file_size INTEGER,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pdf_meta_author ON document_pdf_metadata(author);
CREATE INDEX IF NOT EXISTS idx_pdf_meta_creator ON document_pdf_metadata(creator);
CREATE INDEX IF NOT EXISTS idx_pdf_meta_producer ON document_pdf_metadata(producer);
CREATE INDEX IF NOT EXISTS idx_pdf_meta_creation ON document_pdf_metadata(creation_date);
"""


def _ensure_pdf_metadata_table(conn) -> None:
    """Create the document_pdf_metadata table if it doesn't exist."""
    conn.executescript(PDF_METADATA_SCHEMA)


def store_pdf_metadata(conn, metadata: PDFMetadata) -> None:
    """Insert or replace PDF metadata for a document."""
    conn.execute(
        """
        INSERT OR REPLACE INTO document_pdf_metadata
            (document_id, author, creator, producer, title, subject, keywords,
             creation_date, modification_date, encrypted, page_count, file_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            metadata.document_id,
            metadata.author,
            metadata.creator,
            metadata.producer,
            metadata.title,
            metadata.subject,
            metadata.keywords,
            metadata.creation_date,
            metadata.modification_date,
            metadata.encrypted,
            metadata.page_count,
            metadata.file_size,
        ),
    )


# ── Queries ─────────────────────────────────────────────────────


def get_pdf_metadata(conn, document_id: int) -> dict | None:
    """Get PDF metadata for a single document."""
    _ensure_pdf_metadata_table(conn)
    row = conn.execute(
        "SELECT * FROM document_pdf_metadata WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    return dict(row) if row else None


def get_corpus_metadata_stats(conn) -> dict:
    """Aggregate PDF metadata statistics across the corpus."""
    _ensure_pdf_metadata_table(conn)

    total = conn.execute("SELECT COUNT(*) as c FROM document_pdf_metadata").fetchone()["c"]
    if total == 0:
        return {
            "total_pdfs": 0,
            "top_authors": [],
            "top_creators": [],
            "top_producers": [],
            "encrypted_count": 0,
            "date_range": {"earliest": None, "latest": None},
            "total_pages": 0,
            "total_size_bytes": 0,
        }

    # Top authors
    top_authors = conn.execute(
        """
        SELECT author, COUNT(*) as count FROM document_pdf_metadata
        WHERE author IS NOT NULL GROUP BY author ORDER BY count DESC LIMIT 10
    """
    ).fetchall()

    # Top creators
    top_creators = conn.execute(
        """
        SELECT creator, COUNT(*) as count FROM document_pdf_metadata
        WHERE creator IS NOT NULL GROUP BY creator ORDER BY count DESC LIMIT 10
    """
    ).fetchall()

    # Top producers
    top_producers = conn.execute(
        """
        SELECT producer, COUNT(*) as count FROM document_pdf_metadata
        WHERE producer IS NOT NULL GROUP BY producer ORDER BY count DESC LIMIT 10
    """
    ).fetchall()

    # Encryption count
    encrypted = conn.execute(
        "SELECT COUNT(*) as c FROM document_pdf_metadata WHERE encrypted = 1"
    ).fetchone()["c"]

    # Date range
    earliest = conn.execute(
        "SELECT MIN(creation_date) as d FROM document_pdf_metadata WHERE creation_date IS NOT NULL"
    ).fetchone()["d"]
    latest = conn.execute(
        "SELECT MAX(creation_date) as d FROM document_pdf_metadata WHERE creation_date IS NOT NULL"
    ).fetchone()["d"]

    # Totals
    agg = conn.execute(
        "SELECT COALESCE(SUM(page_count), 0) as pages, COALESCE(SUM(file_size), 0) as size "
        "FROM document_pdf_metadata"
    ).fetchone()

    return {
        "total_pdfs": total,
        "top_authors": [{"author": r["author"], "count": r["count"]} for r in top_authors],
        "top_creators": [{"creator": r["creator"], "count": r["count"]} for r in top_creators],
        "top_producers": [{"producer": r["producer"], "count": r["count"]} for r in top_producers],
        "encrypted_count": encrypted,
        "date_range": {"earliest": earliest, "latest": latest},
        "total_pages": agg["pages"],
        "total_size_bytes": agg["size"],
    }


def search_pdf_metadata(
    conn,
    author: str | None = None,
    creator: str | None = None,
    producer: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Search PDF metadata by author, creator, or producer."""
    _ensure_pdf_metadata_table(conn)

    sql = """
        SELECT pm.*, d.title as doc_title, d.filename, d.category
        FROM document_pdf_metadata pm
        JOIN documents d ON d.id = pm.document_id
        WHERE 1=1
    """
    params: list = []

    if author:
        sql += " AND pm.author LIKE ?"
        params.append(f"%{author}%")
    if creator:
        sql += " AND pm.creator LIKE ?"
        params.append(f"%{creator}%")
    if producer:
        sql += " AND pm.producer LIKE ?"
        params.append(f"%{producer}%")

    sql += " ORDER BY pm.document_id LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_metadata_timeline(conn) -> list[dict]:
    """Get creation/modification date distribution for forensic analysis."""
    _ensure_pdf_metadata_table(conn)

    rows = conn.execute(
        """
        SELECT pm.document_id, pm.creation_date, pm.modification_date,
               pm.author, pm.creator, pm.producer,
               d.title as doc_title, d.filename
        FROM document_pdf_metadata pm
        JOIN documents d ON d.id = pm.document_id
        WHERE pm.creation_date IS NOT NULL OR pm.modification_date IS NOT NULL
        ORDER BY COALESCE(pm.creation_date, pm.modification_date)
    """
    ).fetchall()

    return [dict(r) for r in rows]
