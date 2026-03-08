"""DOSSIER — Ingestion API routes (upload, directory ingest, email, lobbying)."""

import logging

from fastapi import APIRouter, File, Query, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from dossier.api import utils
from dossier.ingestion.pipeline import ingest_file, ingest_directory

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source: str = Query("Manual Upload"),
    date: str = Query(""),
):
    """Upload and ingest a single file."""
    utils.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Sanitize filename and enforce upload size limit
    safe_name = utils._sanitize_filename(file.filename or "")
    content = await utils._read_upload(file)
    dest = utils._safe_upload_dest(safe_name)
    with open(dest, "wb") as f:
        f.write(content)

    # Ingest
    result = ingest_file(str(dest), source=source, date=date)

    if result["success"]:
        return JSONResponse(result, status_code=201)
    else:
        return JSONResponse(result, status_code=409 if "Duplicate" in result["message"] else 422)


@router.post("/ingest-directory")
def ingest_dir(dirpath: str = Query(...)):
    """Ingest all supported files from a directory path on disk."""
    path = utils._validate_path(dirpath)
    if not path.exists() or not path.is_dir():
        raise HTTPException(400, "Directory not found")

    results = ingest_directory(str(path))
    success = sum(1 for r in results if r["success"])
    failed = len(results) - success

    return {"ingested": success, "failed": failed, "details": results}


@router.post("/upload-email")
async def upload_email(
    file: UploadFile = File(...),
    source: str = Query("Email Upload"),
    corpus: str = Query(""),
):
    """Upload and ingest an email file (eml, mbox, json, csv)."""
    from dossier.ingestion.email_pipeline import ingest_email_file

    utils.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = utils._sanitize_filename(file.filename or "")
    content = await utils._read_upload(file)
    dest = utils._safe_upload_dest(safe_name)
    with open(dest, "wb") as f:
        f.write(content)

    try:
        results = ingest_email_file(str(dest), source=source, corpus=corpus)
    except Exception:
        logger.exception("Email ingestion failed for uploaded file")
        raise HTTPException(422, "Email ingestion failed") from None

    success = sum(1 for r in results if r.get("success"))
    failed = len(results) - success

    # Sanitize results to prevent leaking internal details
    safe_details = [
        {"success": r.get("success", False), "message": str(r.get("message", ""))} for r in results
    ]

    status = 201 if success > 0 else 422
    return JSONResponse(
        {"ingested": success, "failed": failed, "details": safe_details}, status_code=status
    )


@router.post("/ingest-emails-directory")
def ingest_emails_dir(
    dirpath: str = Query(...),
    source: str = Query("Email Import"),
    corpus: str = Query(""),
):
    """Ingest all email files from a directory on disk."""
    from dossier.ingestion.email_pipeline import ingest_email_directory

    path = utils._validate_path(dirpath)
    if not path.exists() or not path.is_dir():
        raise HTTPException(400, "Directory not found")

    result = ingest_email_directory(str(path), source=source, corpus=corpus)
    return result


@router.post("/lobbying/generate")
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
