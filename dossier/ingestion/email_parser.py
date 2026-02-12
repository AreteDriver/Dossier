"""
DOSSIER — Email Parser
Handles: raw emails, .eml files, mbox archives, WikiLeaks HTML dumps,
and structured email data (CSV/JSON exports).

Extracts: headers (from, to, cc, bcc, date, subject), body text,
attachment metadata, and thread chains.
"""

import re
import email
import mailbox
import json
import csv
from email import policy
from email.utils import parsedate_to_datetime, parseaddr, getaddresses
from pathlib import Path
from typing import Optional
from datetime import datetime


def parse_email_file(filepath: str) -> list[dict]:
    """
    Parse an email file and return list of email dicts.
    Handles: .eml, .mbox, .txt (raw email), .json, .csv
    """
    path = Path(filepath)
    suffix = path.suffix.lower()

    parsers = {
        ".eml": _parse_eml,
        ".mbox": _parse_mbox,
        ".json": _parse_json_emails,
        ".csv": _parse_csv_emails,
        ".txt": _parse_raw_email_text,
        ".html": _parse_wikileaks_html,
        ".htm": _parse_wikileaks_html,
    }

    parser = parsers.get(suffix, _parse_raw_email_text)
    return parser(filepath)


def parse_email_string(raw_text: str) -> dict:
    """Parse a single email from raw text string."""
    msg = email.message_from_string(raw_text, policy=policy.default)
    return _email_to_dict(msg)


def _parse_eml(filepath: str) -> list[dict]:
    """Parse a single .eml file."""
    with open(filepath, "rb") as f:
        msg = email.message_from_binary_file(f, policy=policy.default)
    return [_email_to_dict(msg)]


def _parse_mbox(filepath: str) -> list[dict]:
    """Parse an mbox archive (can contain thousands of emails)."""
    mbox = mailbox.mbox(filepath)
    results = []
    for msg in mbox:
        try:
            results.append(_email_to_dict(msg))
        except Exception as e:
            print(f"[EMAIL] Failed to parse message in mbox: {e}")
    return results


def _parse_json_emails(filepath: str) -> list[dict]:
    """Parse emails from JSON (WikiLeaks export format or custom)."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    if isinstance(data, list):
        emails = data
    elif isinstance(data, dict) and "emails" in data:
        emails = data["emails"]
    else:
        emails = [data]

    results = []
    for item in emails:
        results.append({
            "message_id": item.get("id", item.get("message_id", "")),
            "subject": item.get("subject", ""),
            "from_addr": item.get("from", item.get("from_addr", "")),
            "from_name": _extract_name(item.get("from", "")),
            "to": _parse_addr_list(item.get("to", "")),
            "cc": _parse_addr_list(item.get("cc", "")),
            "bcc": _parse_addr_list(item.get("bcc", "")),
            "date": item.get("date", ""),
            "body": item.get("body", item.get("text", item.get("content", ""))),
            "attachments": item.get("attachments", []),
            "headers": {k: v for k, v in item.items() if k not in {
                "body", "text", "content", "attachments"
            }},
            "raw": json.dumps(item),
        })
    return results


def _parse_csv_emails(filepath: str) -> list[dict]:
    """Parse emails from CSV export."""
    results = []
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Flexible column mapping
            results.append({
                "message_id": row.get("id", row.get("message_id", row.get("ID", ""))),
                "subject": row.get("subject", row.get("Subject", "")),
                "from_addr": row.get("from", row.get("From", row.get("sender", ""))),
                "from_name": _extract_name(row.get("from", row.get("From", ""))),
                "to": _parse_addr_list(row.get("to", row.get("To", ""))),
                "cc": _parse_addr_list(row.get("cc", row.get("Cc", ""))),
                "bcc": _parse_addr_list(row.get("bcc", row.get("Bcc", ""))),
                "date": row.get("date", row.get("Date", "")),
                "body": row.get("body", row.get("Body", row.get("content", row.get("Content", "")))),
                "attachments": [],
                "headers": dict(row),
                "raw": str(row),
            })
    return results


def _parse_raw_email_text(filepath: str) -> list[dict]:
    """Try to parse a text file as raw email or email-like content."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    # Check if it looks like a standard email
    if re.match(r"^(From|To|Subject|Date|Message-ID):", content, re.MULTILINE):
        try:
            msg = email.message_from_string(content, policy=policy.default)
            return [_email_to_dict(msg)]
        except Exception:
            pass

    # Check if it contains multiple emails separated by "From " lines (mbox-ish)
    if content.startswith("From ") or "\nFrom " in content:
        emails = re.split(r"\n(?=From )", content)
        results = []
        for raw in emails:
            try:
                msg = email.message_from_string(raw.strip(), policy=policy.default)
                results.append(_email_to_dict(msg))
            except Exception:
                continue
        if results:
            return results

    # Fall back: treat as a single email-like document
    return [{
        "message_id": "",
        "subject": _guess_subject(content),
        "from_addr": "",
        "from_name": "",
        "to": [],
        "cc": [],
        "bcc": [],
        "date": "",
        "body": content,
        "attachments": [],
        "headers": {},
        "raw": content,
    }]


def _parse_wikileaks_html(filepath: str) -> list[dict]:
    """
    Parse WikiLeaks Podesta email HTML pages.
    WikiLeaks format has structured divs with email metadata and body.
    """
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    # Extract email fields from WikiLeaks HTML structure
    def extract_field(label):
        pattern = rf'<td[^>]*>\s*{label}\s*</td>\s*<td[^>]*>(.*?)</td>'
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            # Strip HTML tags
            return re.sub(r"<[^>]+>", "", match.group(1)).strip()
        # Also try simpler patterns
        pattern2 = rf'{label}:\s*([^\n<]+)'
        match2 = re.search(pattern2, html, re.IGNORECASE)
        return match2.group(1).strip() if match2 else ""

    # Try to get the email body
    body_match = re.search(
        r'<div[^>]*class="[^"]*email-body[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if not body_match:
        # Try pre tag
        body_match = re.search(r"<pre[^>]*>(.*?)</pre>", html, re.DOTALL)

    body = ""
    if body_match:
        body = re.sub(r"<[^>]+>", " ", body_match.group(1))
        body = re.sub(r"\s+", " ", body).strip()
    else:
        # Strip all HTML and use everything
        body = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        body = re.sub(r"<[^>]+>", " ", body)
        body = re.sub(r"\s+", " ", body).strip()

    subject = extract_field("Subject")
    from_addr = extract_field("From")
    to_field = extract_field("To")
    cc_field = extract_field("CC") or extract_field("Cc")
    date_field = extract_field("Date")

    # Try to extract WikiLeaks email ID from URL or content
    wl_id_match = re.search(r"emailid/(\d+)", html)
    wl_id = wl_id_match.group(1) if wl_id_match else ""

    return [{
        "message_id": wl_id,
        "subject": subject,
        "from_addr": from_addr,
        "from_name": _extract_name(from_addr),
        "to": _parse_addr_list(to_field),
        "cc": _parse_addr_list(cc_field),
        "bcc": [],
        "date": date_field,
        "body": body,
        "attachments": [],
        "headers": {"wikileaks_id": wl_id},
        "raw": html,
    }]


# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

def _email_to_dict(msg) -> dict:
    """Convert an email.message.Message to our standard dict format."""
    # Get body
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    break
            elif content_type == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode("utf-8", errors="replace")
                    body = re.sub(r"<[^>]+>", " ", html)
                    body = re.sub(r"\s+", " ", body).strip()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode("utf-8", errors="replace")

    # Get attachments
    attachments = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                attachments.append({
                    "filename": part.get_filename() or "unknown",
                    "content_type": part.get_content_type(),
                    "size": len(part.get_payload(decode=True) or b""),
                })

    # Parse date
    date_str = msg.get("Date", "")
    try:
        parsed_date = parsedate_to_datetime(date_str)
        date_str = parsed_date.isoformat()
    except Exception:
        pass

    from_field = msg.get("From", "")
    from_name, from_addr = parseaddr(from_field)

    return {
        "message_id": msg.get("Message-ID", ""),
        "subject": msg.get("Subject", ""),
        "from_addr": from_addr or from_field,
        "from_name": from_name or _extract_name(from_field),
        "to": [{"name": n, "addr": a} for n, a in getaddresses(msg.get_all("To", []))],
        "cc": [{"name": n, "addr": a} for n, a in getaddresses(msg.get_all("Cc", []))],
        "bcc": [{"name": n, "addr": a} for n, a in getaddresses(msg.get_all("Bcc", []))],
        "date": date_str,
        "body": body,
        "attachments": attachments,
        "headers": {k: v for k, v in msg.items()},
        "raw": msg.as_string() if hasattr(msg, "as_string") else str(msg),
    }


def _extract_name(addr_string: str) -> str:
    """Extract display name from email address string."""
    if not addr_string:
        return ""
    name, addr = parseaddr(addr_string)
    if name:
        return name
    # Try to extract name from email prefix
    if "@" in addr_string:
        prefix = addr_string.split("@")[0]
        # Convert john.podesta to John Podesta
        parts = re.split(r"[._-]", prefix)
        return " ".join(p.capitalize() for p in parts if p)
    return addr_string


def _parse_addr_list(field: str) -> list[dict]:
    """Parse a comma-separated address list."""
    if not field:
        return []
    addrs = getaddresses([field])
    return [{"name": n or _extract_name(a), "addr": a} for n, a in addrs if a]


def _guess_subject(text: str) -> str:
    """Guess a subject line from text content."""
    lines = text.strip().split("\n")
    for line in lines[:5]:
        line = line.strip()
        if line.startswith("Subject:"):
            return line[8:].strip()
        if line.startswith("Re:") or line.startswith("Fwd:"):
            return line.strip()
    # First non-empty short line
    for line in lines[:10]:
        line = line.strip()
        if 5 < len(line) < 100:
            return line
    return "Untitled Email"
