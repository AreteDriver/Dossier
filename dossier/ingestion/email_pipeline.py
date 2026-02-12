"""
DOSSIER — Email Ingestion Pipeline
Specialized pipeline for email corpuses (Podesta, etc).

Handles: bulk email import, contact graph extraction, thread chaining,
and email-specific entity extraction from headers + body.
"""

import hashlib
import json
from pathlib import Path
from datetime import datetime

from dossier.ingestion.email_parser import parse_email_file, parse_email_string
from dossier.core.ner import extract_entities, classify_document
from dossier.db.database import get_db


def ingest_email_file(filepath: str, source: str = "", corpus: str = "") -> list[dict]:
    """
    Ingest an email file (may contain multiple emails).
    Returns list of results, one per email.
    """
    path = Path(filepath)
    if not path.exists():
        return [{"success": False, "message": f"File not found: {filepath}"}]

    try:
        emails = parse_email_file(filepath)
    except Exception as e:
        return [{"success": False, "message": f"Parse error: {e}"}]

    results = []
    for email_data in emails:
        result = _ingest_single_email(email_data, source=source, corpus=corpus, origin_file=str(path))
        results.append(result)

    return results


def ingest_email_directory(dirpath: str, source: str = "", corpus: str = "") -> dict:
    """
    Ingest all email files in a directory.
    Returns summary with counts.
    """
    dirpath = Path(dirpath)
    supported = {".eml", ".mbox", ".json", ".csv", ".txt", ".html", ".htm"}
    all_results = []

    files = sorted(f for f in dirpath.rglob("*") if f.suffix.lower() in supported and f.is_file())
    total = len(files)

    for i, f in enumerate(files):
        if (i + 1) % 100 == 0:
            print(f"[EMAIL-INGEST] Processing {i+1}/{total}...")
        results = ingest_email_file(str(f), source=source, corpus=corpus)
        all_results.extend(results)

    success = sum(1 for r in all_results if r.get("success"))
    failed = len(all_results) - success

    print(f"[EMAIL-INGEST] Complete: {success} ingested, {failed} failed out of {len(all_results)} emails")
    return {"ingested": success, "failed": failed, "total": len(all_results), "details": all_results}


def _ingest_single_email(email_data: dict, source: str = "", corpus: str = "", origin_file: str = "") -> dict:
    """Ingest a single parsed email into the database."""

    body = email_data.get("body", "")
    subject = email_data.get("subject", "")
    from_addr = email_data.get("from_addr", "")
    from_name = email_data.get("from_name", "")
    date = email_data.get("date", "")
    message_id = email_data.get("message_id", "")

    # Build full searchable text: headers + body
    header_text = _build_header_text(email_data)
    full_text = f"{header_text}\n\n{body}"

    if len(full_text.strip()) < 10:
        return {"success": False, "message": "Empty email"}

    # Hash for dedup (use message_id if available, otherwise hash content)
    if message_id:
        content_hash = hashlib.sha256(message_id.encode()).hexdigest()
    else:
        content_hash = hashlib.sha256(full_text.encode()).hexdigest()

    # Check for duplicate
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM documents WHERE file_hash = ?", (content_hash,)
        ).fetchone()
        if existing:
            return {"success": False, "message": f"Duplicate email (id={existing['id']})"}

    # ─── NER on body text ───
    entities = extract_entities(body)

    # ─── Extract entities from email headers ───
    header_entities = _extract_header_entities(email_data)

    # Merge header entities into NER results
    for person in header_entities["people"]:
        _merge_entity(entities["people"], person)
    for org in header_entities["orgs"]:
        _merge_entity(entities["orgs"], org)

    # ─── Determine category ───
    category = "email"
    # Sub-categorize based on content
    if email_data.get("attachments"):
        attachment_names = [a.get("filename", "") for a in email_data["attachments"]]
        # Note attachment info in the text for keyword extraction
        full_text += "\n\nAttachments: " + ", ".join(attachment_names)

    # ─── Build title ───
    title = subject if subject else f"Email from {from_name or from_addr}"
    if corpus:
        title = f"[{corpus}] {title}"

    # ─── Normalize date ───
    normalized_date = _normalize_date(date)

    # ─── Store ───
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO documents (filename, filepath, title, category, source, date, pages, file_hash, raw_text, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"email_{content_hash[:12]}",
            origin_file,
            title,
            category,
            source or corpus or "Email Import",
            normalized_date,
            1,  # emails are 1 "page"
            content_hash,
            full_text,
            json.dumps({
                "message_id": message_id,
                "from": from_addr,
                "from_name": from_name,
                "to": email_data.get("to", []),
                "cc": email_data.get("cc", []),
                "attachments": email_data.get("attachments", []),
                "corpus": corpus,
            }),
        ))
        doc_id = cursor.lastrowid

        # Store entities
        entity_count = 0
        for etype, elist in [("person", entities["people"]), ("place", entities["places"]),
                              ("org", entities["orgs"]), ("date", entities["dates"])]:
            for ent in elist:
                canonical = ent["name"].lower().strip()
                conn.execute("""
                    INSERT INTO entities (name, type, canonical)
                    VALUES (?, ?, ?)
                    ON CONFLICT(canonical, type) DO NOTHING
                """, (ent["name"], etype, canonical))

                entity_row = conn.execute(
                    "SELECT id FROM entities WHERE canonical = ? AND type = ?",
                    (canonical, etype)
                ).fetchone()

                if entity_row:
                    conn.execute("""
                        INSERT INTO document_entities (document_id, entity_id, count)
                        VALUES (?, ?, ?)
                        ON CONFLICT(document_id, entity_id) DO UPDATE SET count = count + excluded.count
                    """, (doc_id, entity_row["id"], ent["count"]))
                    entity_count += 1

        # Store keywords
        keyword_count = 0
        for kw in entities["keywords"][:30]:
            conn.execute("""
                INSERT INTO keywords (word, total_count, doc_count)
                VALUES (?, ?, 1)
                ON CONFLICT(word) DO UPDATE SET
                    total_count = total_count + excluded.total_count,
                    doc_count = doc_count + 1
            """, (kw["word"], kw["count"]))

            kw_row = conn.execute(
                "SELECT id FROM keywords WHERE word = ?", (kw["word"],)
            ).fetchone()
            if kw_row:
                conn.execute("""
                    INSERT INTO document_keywords (document_id, keyword_id, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(document_id, keyword_id) DO UPDATE SET count = count + excluded.count
                """, (doc_id, kw_row["id"], kw["count"]))
                keyword_count += 1

        # Build co-occurrence connections
        doc_entity_ids = [
            row["entity_id"]
            for row in conn.execute(
                "SELECT entity_id FROM document_entities WHERE document_id = ?", (doc_id,)
            ).fetchall()
        ]
        for i, eid_a in enumerate(doc_entity_ids):
            for eid_b in doc_entity_ids[i+1:]:
                a, b = min(eid_a, eid_b), max(eid_a, eid_b)
                conn.execute("""
                    INSERT INTO entity_connections (entity_a_id, entity_b_id, weight)
                    VALUES (?, ?, 1)
                    ON CONFLICT(entity_a_id, entity_b_id) DO UPDATE SET weight = weight + 1
                """, (a, b))

    return {
        "success": True,
        "document_id": doc_id,
        "message": f"Ingested: {title[:60]}",
        "subject": subject,
    }


def _build_header_text(email_data: dict) -> str:
    """Build searchable text from email headers."""
    parts = []
    if email_data.get("subject"):
        parts.append(f"Subject: {email_data['subject']}")
    if email_data.get("from_addr"):
        parts.append(f"From: {email_data.get('from_name', '')} <{email_data['from_addr']}>")
    for field in ["to", "cc", "bcc"]:
        addrs = email_data.get(field, [])
        if addrs:
            formatted = ", ".join(
                f"{a.get('name', '')} <{a.get('addr', '')}>" if isinstance(a, dict) else str(a)
                for a in addrs
            )
            parts.append(f"{field.upper()}: {formatted}")
    if email_data.get("date"):
        parts.append(f"Date: {email_data['date']}")
    return "\n".join(parts)


def _extract_header_entities(email_data: dict) -> dict:
    """Extract people and orgs from email headers."""
    people = []
    orgs = set()

    # From
    if email_data.get("from_name"):
        people.append({"name": email_data["from_name"], "count": 1})
    if email_data.get("from_addr") and "@" in email_data["from_addr"]:
        domain = email_data["from_addr"].split("@")[1].lower()
        org = _domain_to_org(domain)
        if org:
            orgs.add(org)

    # To, CC, BCC
    for field in ["to", "cc", "bcc"]:
        for addr in email_data.get(field, []):
            if isinstance(addr, dict):
                if addr.get("name"):
                    people.append({"name": addr["name"], "count": 1})
                if addr.get("addr") and "@" in addr["addr"]:
                    domain = addr["addr"].split("@")[1].lower()
                    org = _domain_to_org(domain)
                    if org:
                        orgs.add(org)

    return {
        "people": people,
        "orgs": [{"name": o, "count": 1} for o in orgs],
    }


# ─── Domain → Organization mapping ───
DOMAIN_ORG_MAP = {
    "hillaryclinton.com": "Hillary Clinton Campaign",
    "hrcoffice.com": "Hillary Clinton Office",
    "clintonfoundation.org": "Clinton Foundation",
    "americanprogress.org": "Center for American Progress",
    "podesta.com": "Podesta Group",
    "podestagroupdc.com": "Podesta Group",
    "dnc.org": "Democratic National Committee",
    "gop.com": "Republican National Committee",
    "whitehouse.gov": "White House",
    "state.gov": "State Department",
    "senate.gov": "U.S. Senate",
    "house.gov": "U.S. House",
    "justice.gov": "Department of Justice",
    "fbi.gov": "FBI",
    "cia.gov": "CIA",
    "jpmorgan.com": "JPMorgan",
    "deutschebank.com": "Deutsche Bank",
    "goldmansachs.com": "Goldman Sachs",
    "morganstanley.com": "Morgan Stanley",
    "washingtonpost.com": "Washington Post",
    "nytimes.com": "New York Times",
    "politico.com": "Politico",
    "cnn.com": "CNN",
    "foxnews.com": "Fox News",
    "harvard.edu": "Harvard University",
    "georgetown.edu": "Georgetown University",
    "stanford.edu": "Stanford University",
}


def _domain_to_org(domain: str) -> str:
    """Map an email domain to an organization name."""
    domain = domain.lower().strip()
    if domain in DOMAIN_ORG_MAP:
        return DOMAIN_ORG_MAP[domain]
    # Skip common personal email domains
    personal = {"gmail.com", "yahoo.com", "hotmail.com", "aol.com", "outlook.com",
                "icloud.com", "me.com", "mac.com", "comcast.net", "msn.com"}
    if domain in personal:
        return ""
    # For unknown domains, extract org name from domain
    parts = domain.split(".")
    if len(parts) >= 2 and parts[-1] in {"com", "org", "gov", "edu", "net"}:
        return parts[-2].capitalize()
    return ""


def _merge_entity(entity_list: list, new_entity: dict):
    """Merge a new entity into an existing list, combining counts if duplicate."""
    for existing in entity_list:
        if existing["name"].lower() == new_entity["name"].lower():
            existing["count"] += new_entity["count"]
            return
    entity_list.append(new_entity)


def _normalize_date(date_str: str) -> str:
    """Try to normalize a date string to YYYY-MM-DD format."""
    if not date_str:
        return ""

    # Already ISO format
    if len(date_str) >= 10 and date_str[4] == "-":
        return date_str[:10]

    # Try common formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str[:10] if len(date_str) >= 10 else date_str
