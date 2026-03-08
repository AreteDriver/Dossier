"""Shared constants and helpers for Dossier API router modules."""

import json
import logging
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException, UploadFile

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────

UPLOAD_DIR = Path(__file__).parent.parent / "data" / "inbox"
MAX_UPLOAD_SIZE = int(os.environ.get("DOSSIER_MAX_UPLOAD_MB", "100")) * 1024 * 1024  # bytes
ALLOWED_BASE_DIRS: list[Path] = [
    Path(p) for p in os.environ.get("DOSSIER_ALLOWED_DIRS", str(Path.home())).split(os.pathsep) if p
]
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")


# ── Helpers ──────────────────────────────────────────────────────


def _validate_path(dirpath: str) -> Path:
    """Validate a directory path against traversal and symlink attacks.

    Raises HTTPException 403 if the path resolves outside ALLOWED_BASE_DIRS.
    """
    resolved = Path(dirpath).resolve()
    resolved_str = str(resolved)
    for allowed in ALLOWED_BASE_DIRS:
        allowed_str = str(allowed.resolve())
        if resolved_str == allowed_str or resolved_str.startswith(allowed_str + os.sep):
            # Reconstruct from validated string to break taint tracking
            return Path(resolved_str)
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


def _safe_upload_dest(safe_name: str) -> Path:
    """Build an upload destination path and verify it stays within UPLOAD_DIR.

    Raises HTTPException 400 if the resolved path escapes the upload directory.
    """
    dest = (UPLOAD_DIR / safe_name).resolve()
    upload_root = UPLOAD_DIR.resolve()
    if not (dest == upload_root or upload_root in dest.parents):
        raise HTTPException(400, "Invalid filename")
    return dest


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
