#!/usr/bin/env python3
"""
DOSSIER — WikiLeaks Podesta Email Downloader

Downloads the Podesta email corpus from WikiLeaks.
The emails are publicly available at: https://wikileaks.org/podesta-emails/

Strategy:
  WikiLeaks provides individual email pages at:
    https://wikileaks.org/podesta-emails/emailid/{1..58660}

  This script can:
    1. Download individual emails by ID
    2. Batch download ranges of emails
    3. Resume interrupted downloads
    4. Save as structured JSON for easy ingestion

  NOTE: WikiLeaks may rate-limit or block scrapers.
  Use responsibly with delays between requests.
  Consider using existing dumps if available (faster + more reliable).

  Known data sources for bulk download:
    - archive.org mirrors
    - kaggle datasets (search "podesta emails")
    - Direct WikiLeaks search API

Usage:
    python -m dossier.ingestion.scrapers.wikileaks_podesta --range 1 100
    python -m dossier.ingestion.scrapers.wikileaks_podesta --id 1234
    python -m dossier.ingestion.scrapers.wikileaks_podesta --from-file email_ids.txt
"""

import os
import sys
import json
import time
import hashlib
import re
import argparse
from pathlib import Path
from datetime import datetime

# Optional: use requests if available, fall back to urllib
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    import urllib.request
    import urllib.error
    HAS_REQUESTS = False


BASE_URL = "https://wikileaks.org/podesta-emails/emailid/"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "podesta_emails"
PROGRESS_FILE = OUTPUT_DIR / "_progress.json"
TOTAL_EMAILS = 58660  # Known total count


def download_email(email_id: int, delay: float = 1.0) -> dict:
    """Download a single email from WikiLeaks by ID."""
    url = f"{BASE_URL}{email_id}"
    output_file = OUTPUT_DIR / f"podesta_{email_id:06d}.json"

    # Skip if already downloaded
    if output_file.exists():
        return {"id": email_id, "status": "skipped", "message": "Already downloaded"}

    try:
        if HAS_REQUESTS:
            resp = requests.get(url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; research-tool)"
            })
            if resp.status_code == 404:
                return {"id": email_id, "status": "not_found"}
            resp.raise_for_status()
            html = resp.text
        else:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; research-tool)"
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")

        # Parse the WikiLeaks HTML page
        email_data = _parse_wikileaks_page(html, email_id)

        # Save as JSON
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(email_data, f, indent=2, ensure_ascii=False)

        if delay > 0:
            time.sleep(delay)

        return {"id": email_id, "status": "success", "subject": email_data.get("subject", "")}

    except Exception as e:
        return {"id": email_id, "status": "error", "message": str(e)}


def _parse_wikileaks_page(html: str, email_id: int) -> dict:
    """Parse a WikiLeaks Podesta email HTML page into structured data."""

    def extract(label):
        # Try table cell format
        pattern = rf'<td[^>]*>\s*{label}\s*</td>\s*<td[^>]*>(.*?)</td>'
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            return re.sub(r"<[^>]+>", "", match.group(1)).strip()
        # Try label: value format
        pattern2 = rf'{label}[:\s]+(.*?)(?:<|$)'
        match2 = re.search(pattern2, html, re.IGNORECASE)
        return match2.group(1).strip() if match2 else ""

    # Extract email body from various possible containers
    body = ""
    for pattern in [
        r'<div[^>]*class="[^"]*email-body[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="[^"]*email-body[^"]*"[^>]*>(.*?)</div>',
        r'<pre[^>]*>(.*?)</pre>',
    ]:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            body = re.sub(r"<[^>]+>", " ", match.group(1))
            body = re.sub(r"&nbsp;", " ", body)
            body = re.sub(r"&amp;", "&", body)
            body = re.sub(r"&lt;", "<", body)
            body = re.sub(r"&gt;", ">", body)
            body = re.sub(r"\s+", " ", body).strip()
            break

    # Extract attachments
    attachments = []
    att_pattern = r'<a[^>]*href="([^"]*)"[^>]*>[^<]*attachment[^<]*</a>'
    for match in re.finditer(att_pattern, html, re.IGNORECASE):
        attachments.append({"url": match.group(1), "filename": match.group(1).split("/")[-1]})

    return {
        "id": email_id,
        "wikileaks_url": f"{BASE_URL}{email_id}",
        "subject": extract("Subject"),
        "from": extract("From"),
        "to": extract("To"),
        "cc": extract("CC") or extract("Cc"),
        "date": extract("Date"),
        "body": body,
        "attachments": attachments,
        "downloaded_at": datetime.now().isoformat(),
    }


def download_range(start: int, end: int, delay: float = 1.5):
    """Download a range of email IDs."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load progress
    progress = _load_progress()
    completed = set(progress.get("completed", []))

    total = end - start + 1
    success = 0
    errors = 0

    print(f"\n[WIKILEAKS] Downloading emails {start} to {end} ({total} total)")
    print(f"[WIKILEAKS] Already completed: {len(completed)} in this range\n")

    for i, eid in enumerate(range(start, end + 1)):
        if eid in completed:
            continue

        result = download_email(eid, delay=delay)

        if result["status"] == "success":
            success += 1
            completed.add(eid)
            if success % 10 == 0:
                print(f"  [{success}/{total}] Downloaded #{eid}: {result.get('subject', '')[:60]}")
        elif result["status"] == "skipped":
            completed.add(eid)
        elif result["status"] == "error":
            errors += 1
            print(f"  [ERROR] #{eid}: {result.get('message', '')}")
            if errors > 10:
                print("  Too many errors, pausing for 30s...")
                time.sleep(30)
                errors = 0

        # Save progress every 50 emails
        if (i + 1) % 50 == 0:
            _save_progress({"completed": sorted(completed), "last_id": eid})

    _save_progress({"completed": sorted(completed), "last_id": end})
    print(f"\n[WIKILEAKS] Complete: {success} downloaded, {len(completed)} total in range")


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"completed": []}


def _save_progress(data: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f)


def ingest_downloaded(limit: int = 0):
    """Ingest all downloaded Podesta emails into DOSSIER."""
    from dossier.db.database import init_db
    from dossier.ingestion.email_pipeline import ingest_email_file

    init_db()

    json_files = sorted(OUTPUT_DIR.glob("podesta_*.json"))
    if limit:
        json_files = json_files[:limit]

    total = len(json_files)
    success = 0
    failed = 0

    print(f"\n[INGEST] Processing {total} Podesta emails...")

    for i, f in enumerate(json_files):
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{total}] {success} ingested, {failed} failed")

        results = ingest_email_file(
            str(f),
            source="WikiLeaks Podesta Emails",
            corpus="Podesta"
        )
        for r in results:
            if r.get("success"):
                success += 1
            else:
                failed += 1

    print(f"\n[INGEST] Complete: {success} ingested, {failed} failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download WikiLeaks Podesta emails")
    parser.add_argument("--id", type=int, help="Download a single email by ID")
    parser.add_argument("--range", nargs=2, type=int, metavar=("START", "END"),
                        help="Download a range of email IDs")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Delay between requests (seconds)")
    parser.add_argument("--ingest", action="store_true",
                        help="Ingest all downloaded emails into DOSSIER")
    parser.add_argument("--ingest-limit", type=int, default=0,
                        help="Limit number of emails to ingest (0 = all)")

    args = parser.parse_args()

    if args.ingest:
        ingest_downloaded(limit=args.ingest_limit)
    elif args.id:
        result = download_email(args.id)
        print(json.dumps(result, indent=2))
    elif args.range:
        download_range(args.range[0], args.range[1], delay=args.delay)
    else:
        parser.print_help()
