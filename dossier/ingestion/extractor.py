"""
DOSSIER — Text Extraction Pipeline
Handles: PDF (native + OCR), plain text, HTML, images.
"""

import os
import hashlib
import subprocess
import tempfile
from pathlib import Path
from typing import Optional


def file_hash(filepath: str) -> str:
    """SHA-256 hash for dedup."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_text(filepath: str) -> dict:
    """
    Extract text from a file. Returns dict with:
      - text: extracted text content
      - pages: page count (if applicable)
      - method: extraction method used
    """
    path = Path(filepath)
    suffix = path.suffix.lower()

    extractors = {
        ".pdf": _extract_pdf,
        ".txt": _extract_text,
        ".md": _extract_text,
        ".html": _extract_html,
        ".htm": _extract_html,
        ".png": _extract_image_ocr,
        ".jpg": _extract_image_ocr,
        ".jpeg": _extract_image_ocr,
        ".tiff": _extract_image_ocr,
        ".tif": _extract_image_ocr,
        ".bmp": _extract_image_ocr,
    }

    extractor = extractors.get(suffix)
    if not extractor:
        return {"text": "", "pages": 0, "method": "unsupported"}

    return extractor(filepath)


def _extract_pdf(filepath: str) -> dict:
    """Extract from PDF — tries native text first, falls back to OCR."""
    import pdfplumber

    text_parts = []
    pages = 0

    try:
        with pdfplumber.open(filepath) as pdf:
            pages = len(pdf.pages)
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        print(f"[EXTRACT] pdfplumber failed on {filepath}: {e}")

    full_text = "\n\n".join(text_parts).strip()

    # If we got very little text, the PDF is probably scanned — try OCR
    if len(full_text) < 100 and pages > 0:
        print(f"[EXTRACT] Low text yield ({len(full_text)} chars), attempting OCR...")
        ocr_result = _ocr_pdf(filepath)
        if len(ocr_result) > len(full_text):
            return {"text": ocr_result, "pages": pages, "method": "pdf_ocr"}

    return {"text": full_text, "pages": pages, "method": "pdf_native"}


def _ocr_pdf(filepath: str) -> str:
    """OCR a PDF by converting pages to images then running tesseract."""
    try:
        # Use pdftoppm to convert PDF to images, then tesseract each
        with tempfile.TemporaryDirectory() as tmpdir:
            # Convert PDF to PNG images
            subprocess.run(
                ["pdftoppm", "-png", "-r", "300", filepath, os.path.join(tmpdir, "page")],
                capture_output=True, timeout=120
            )

            text_parts = []
            for img_file in sorted(Path(tmpdir).glob("*.png")):
                result = subprocess.run(
                    ["tesseract", str(img_file), "stdout", "--psm", "3"],
                    capture_output=True, text=True, timeout=60
                )
                if result.stdout.strip():
                    text_parts.append(result.stdout.strip())

            return "\n\n".join(text_parts)
    except Exception as e:
        print(f"[OCR] Failed: {e}")
        return ""


def _extract_text(filepath: str) -> dict:
    """Plain text / markdown."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        # Estimate pages (rough: ~3000 chars per page)
        pages = max(1, len(text) // 3000)
        return {"text": text, "pages": pages, "method": "plaintext"}
    except Exception as e:
        print(f"[EXTRACT] Text read failed: {e}")
        return {"text": "", "pages": 0, "method": "plaintext_error"}


def _extract_html(filepath: str) -> dict:
    """Strip HTML tags, extract text content."""
    import re
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()

        # Remove script/style blocks
        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        # Remove tags
        text = re.sub(r"<[^>]+>", " ", html)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()

        pages = max(1, len(text) // 3000)
        return {"text": text, "pages": pages, "method": "html"}
    except Exception as e:
        print(f"[EXTRACT] HTML parse failed: {e}")
        return {"text": "", "pages": 0, "method": "html_error"}


def _extract_image_ocr(filepath: str) -> dict:
    """OCR an image file directly."""
    try:
        result = subprocess.run(
            ["tesseract", filepath, "stdout", "--psm", "3"],
            capture_output=True, text=True, timeout=60
        )
        text = result.stdout.strip()
        return {"text": text, "pages": 1, "method": "image_ocr"}
    except Exception as e:
        print(f"[OCR] Image OCR failed: {e}")
        return {"text": "", "pages": 1, "method": "image_ocr_error"}
