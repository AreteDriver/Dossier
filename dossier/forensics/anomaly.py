"""Anomaly detection module for corpus-level pattern analysis.

Each function takes pre-fetched data and returns a list of anomaly dicts with:
  type, severity, description, evidence, affected_ids
"""

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone


def detect_temporal_gaps(events: list[dict], min_gap_days: int = 90) -> list[dict]:
    """Find suspicious gaps in the event timeline."""
    dates = []
    for ev in events:
        d = ev.get("event_date") or ev.get("date")
        if d and len(str(d)) >= 10:
            try:
                dates.append((datetime.strptime(str(d)[:10], "%Y-%m-%d"), ev.get("id")))
            except ValueError:
                continue

    if len(dates) < 2:
        return []

    dates.sort(key=lambda x: x[0])
    anomalies = []
    for i in range(1, len(dates)):
        gap = (dates[i][0] - dates[i - 1][0]).days
        if gap >= min_gap_days:
            severity = "high" if gap >= 365 else "medium" if gap >= 180 else "low"
            anomalies.append(
                {
                    "type": "temporal_gap",
                    "severity": severity,
                    "description": f"{gap}-day gap between {dates[i - 1][0].date()} and {dates[i][0].date()}",
                    "evidence": {
                        "gap_days": gap,
                        "start_date": str(dates[i - 1][0].date()),
                        "end_date": str(dates[i][0].date()),
                    },
                    "affected_ids": [dates[i - 1][1], dates[i][1]],
                }
            )

    return anomalies


def detect_activity_bursts(events: list[dict], std_threshold: float = 2.0) -> list[dict]:
    """Detect unusually dense periods of activity."""
    monthly: Counter[str] = Counter()
    month_events: dict[str, list] = {}
    for ev in events:
        d = ev.get("event_date") or ev.get("date")
        if d and len(str(d)) >= 7:
            key = str(d)[:7]
            monthly[key] += 1
            month_events.setdefault(key, []).append(ev.get("id"))

    if len(monthly) < 3:
        return []

    counts = list(monthly.values())
    mean = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    if stdev == 0:
        return []

    threshold = mean + std_threshold * stdev
    anomalies = []
    for month, count in monthly.most_common():
        if count >= threshold:
            anomalies.append(
                {
                    "type": "activity_burst",
                    "severity": "high" if count >= mean + 3 * stdev else "medium",
                    "description": f"{month}: {count} events (mean={mean:.1f}, threshold={threshold:.1f})",
                    "evidence": {
                        "month": month,
                        "count": count,
                        "mean": round(mean, 1),
                        "std_dev": round(stdev, 1),
                    },
                    "affected_ids": month_events.get(month, []),
                }
            )

    return anomalies


def detect_page_outliers(documents: list[dict], std_factor: float = 3.0) -> list[dict]:
    """Find documents with page counts far from the norm."""
    pages_list = [(d.get("id"), d.get("pages", 0)) for d in documents if d.get("pages", 0) > 0]
    if len(pages_list) < 3:
        return []

    counts = [p for _, p in pages_list]
    mean = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    if stdev == 0:
        return []

    threshold = mean + std_factor * stdev
    anomalies = []
    for doc_id, pages in pages_list:
        if pages >= threshold:
            anomalies.append(
                {
                    "type": "page_outlier",
                    "severity": "medium",
                    "description": f"Document {doc_id}: {pages} pages (mean={mean:.0f}, threshold={threshold:.0f})",
                    "evidence": {
                        "pages": pages,
                        "mean": round(mean, 1),
                        "std_dev": round(stdev, 1),
                    },
                    "affected_ids": [doc_id],
                }
            )

    return anomalies


def detect_ingestion_anomalies(documents: list[dict], gap_hours: int = 168) -> list[dict]:
    """Detect bulk dumps and ingestion gaps."""
    timestamps = []
    for d in documents:
        ts = d.get("ingested_at")
        if ts:
            try:
                timestamps.append(
                    (
                        datetime.fromisoformat(
                            str(ts).replace("Z", "+00:00").replace("+00:00", "")
                        ),
                        d.get("id"),
                    )
                )
            except (ValueError, TypeError):
                continue

    if len(timestamps) < 2:
        return []

    timestamps.sort(key=lambda x: x[0])
    anomalies = []

    # Detect bulk dumps (many docs in short window)
    window = timedelta(hours=1)
    i = 0
    while i < len(timestamps):
        j = i + 1
        while j < len(timestamps) and (timestamps[j][0] - timestamps[i][0]) <= window:
            j += 1
        count = j - i
        if count >= 10:
            anomalies.append(
                {
                    "type": "bulk_dump",
                    "severity": "medium",
                    "description": f"{count} documents ingested within 1 hour at {timestamps[i][0]}",
                    "evidence": {"count": count, "start": str(timestamps[i][0])},
                    "affected_ids": [t[1] for t in timestamps[i:j]],
                }
            )
        i = j if j > i + 1 else i + 1

    # Detect ingestion gaps
    gap_delta = timedelta(hours=gap_hours)
    for i in range(1, len(timestamps)):
        diff = timestamps[i][0] - timestamps[i - 1][0]
        if diff >= gap_delta:
            anomalies.append(
                {
                    "type": "ingestion_gap",
                    "severity": "low",
                    "description": f"{diff.days}-day ingestion gap ending {timestamps[i][0].date()}",
                    "evidence": {
                        "gap_days": diff.days,
                        "start": str(timestamps[i - 1][0].date()),
                        "end": str(timestamps[i][0].date()),
                    },
                    "affected_ids": [timestamps[i - 1][1], timestamps[i][1]],
                }
            )

    return anomalies


def detect_missing_metadata(documents: list[dict]) -> list[dict]:
    """Find documents missing critical metadata fields."""
    fields = ["date", "source", "category"]
    anomalies = []
    for d in documents:
        missing = [f for f in fields if not d.get(f)]
        if missing:
            anomalies.append(
                {
                    "type": "missing_metadata",
                    "severity": "low" if len(missing) == 1 else "medium",
                    "description": f"Document {d.get('id')}: missing {', '.join(missing)}",
                    "evidence": {"missing_fields": missing},
                    "affected_ids": [d.get("id")],
                }
            )

    return anomalies


def detect_isolation_anomalies(entities: list[dict], connections: list[dict]) -> list[dict]:
    """Find entities with high mentions but few connections."""
    connected_ids: set[int] = set()
    for c in connections:
        connected_ids.add(c.get("entity_a_id", 0))
        connected_ids.add(c.get("entity_b_id", 0))

    anomalies = []
    for e in entities:
        mentions = e.get("total_mentions", 0) or e.get("mentions", 0)
        eid = e.get("id")
        if mentions >= 10 and eid not in connected_ids:
            anomalies.append(
                {
                    "type": "isolation_anomaly",
                    "severity": "medium",
                    "description": f"{e.get('name')} ({e.get('type')}): {mentions} mentions, no connections",
                    "evidence": {"mentions": mentions, "connections": 0},
                    "affected_ids": [eid],
                }
            )

    return anomalies


def detect_sudden_appearances(entities: list[dict], events: list[dict]) -> list[dict]:
    """Find entities that appear suddenly in many events in a short window."""
    # Build per-entity date list from events
    entity_dates: dict[int, list[str]] = {}
    for ev in events:
        d = ev.get("event_date") or ev.get("date")
        eid = ev.get("entity_id")
        if d and eid:
            entity_dates.setdefault(eid, []).append(str(d)[:10])

    entity_map = {e["id"]: e for e in entities if "id" in e}

    anomalies = []
    for eid, dates in entity_dates.items():
        if len(dates) < 5:
            continue
        unique = sorted(set(dates))
        if len(unique) < 2:
            continue
        try:
            first = datetime.strptime(unique[0], "%Y-%m-%d")
            last = datetime.strptime(unique[-1], "%Y-%m-%d")
        except ValueError:
            continue
        span = (last - first).days
        if span <= 30 and len(dates) >= 5:
            ent = entity_map.get(eid, {})
            anomalies.append(
                {
                    "type": "sudden_appearance",
                    "severity": "high",
                    "description": (
                        f"{ent.get('name', f'Entity {eid}')}: {len(dates)} events in {span} days"
                    ),
                    "evidence": {
                        "event_count": len(dates),
                        "span_days": span,
                        "first_date": unique[0],
                        "last_date": unique[-1],
                    },
                    "affected_ids": [eid],
                }
            )

    return anomalies


# ── Provenance anomaly detection ──────────────────────────────


def _parse_iso(date_str: str | None) -> datetime | None:
    """Parse an ISO 8601 date string, returning None on failure."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00").replace("+00:00", ""))
    except (ValueError, TypeError):
        return None


def detect_date_inconsistencies(metadata: list[dict]) -> list[dict]:
    """Find PDF metadata with suspicious date patterns."""
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    anomalies = []

    for m in metadata:
        doc_id = m.get("document_id") or m.get("id")
        creation = _parse_iso(m.get("creation_date"))
        modification = _parse_iso(m.get("modification_date"))

        # Creation date after modification date
        if creation and modification and creation > modification:
            anomalies.append(
                {
                    "type": "date_inconsistency",
                    "severity": "high",
                    "description": (
                        f"Document {doc_id}: creation date "
                        f"({creation.date()}) after modification "
                        f"date ({modification.date()})"
                    ),
                    "evidence": {
                        "creation_date": str(creation),
                        "modification_date": str(modification),
                    },
                    "affected_ids": [doc_id],
                }
            )

        # Future dates
        for label, dt in [("creation", creation), ("modification", modification)]:
            if dt and dt > now:
                anomalies.append(
                    {
                        "type": "future_date",
                        "severity": "high",
                        "description": (
                            f"Document {doc_id}: {label} date ({dt.date()}) is in the future"
                        ),
                        "evidence": {"field": label, "date": str(dt)},
                        "affected_ids": [doc_id],
                    }
                )

        # Ancient creation + recent modification (>20yr gap)
        if creation and modification:
            gap_years = (modification - creation).days / 365.25
            if gap_years > 20:
                anomalies.append(
                    {
                        "type": "suspicious_date_gap",
                        "severity": "medium",
                        "description": (
                            f"Document {doc_id}: {gap_years:.0f}-year gap "
                            f"between creation ({creation.date()}) and "
                            f"modification ({modification.date()})"
                        ),
                        "evidence": {
                            "gap_years": round(gap_years, 1),
                            "creation_date": str(creation),
                            "modification_date": str(modification),
                        },
                        "affected_ids": [doc_id],
                    }
                )

    return anomalies


def detect_metadata_stripping(metadata: list[dict]) -> list[dict]:
    """Detect documents where metadata fields have been stripped."""
    anomalies = []

    for m in metadata:
        doc_id = m.get("document_id") or m.get("id")
        author = m.get("author")
        creator = m.get("creator")
        producer = m.get("producer")
        title = m.get("title")

        all_null = not any([author, creator, producer, title])
        author_only = not author and any([creator, producer])

        if all_null:
            anomalies.append(
                {
                    "type": "metadata_stripped",
                    "severity": "medium",
                    "description": (
                        f"Document {doc_id}: all key metadata fields "
                        f"are empty (author, creator, producer, title)"
                    ),
                    "evidence": {"stripped_fields": ["author", "creator", "producer", "title"]},
                    "affected_ids": [doc_id],
                }
            )
        elif author_only:
            anomalies.append(
                {
                    "type": "author_stripped",
                    "severity": "low",
                    "description": (
                        f"Document {doc_id}: author field stripped but creator/producer present"
                    ),
                    "evidence": {
                        "creator": creator,
                        "producer": producer,
                    },
                    "affected_ids": [doc_id],
                }
            )

    return anomalies


def detect_producer_inconsistencies(metadata: list[dict]) -> list[dict]:
    """Find authors using suspiciously many different PDF producers."""
    author_producers: dict[str, dict[str, list]] = defaultdict(
        lambda: {"producers": set(), "doc_ids": []}
    )

    for m in metadata:
        author = m.get("author")
        producer = m.get("producer")
        doc_id = m.get("document_id") or m.get("id")
        if author and producer:
            author_producers[author]["producers"].add(producer)
            author_producers[author]["doc_ids"].append(doc_id)

    anomalies = []
    for author, info in author_producers.items():
        if len(info["producers"]) >= 3:
            anomalies.append(
                {
                    "type": "producer_inconsistency",
                    "severity": "medium",
                    "description": (
                        f"Author '{author}' used {len(info['producers'])} different producers"
                    ),
                    "evidence": {
                        "author": author,
                        "producers": sorted(info["producers"]),
                    },
                    "affected_ids": info["doc_ids"],
                }
            )

    return anomalies


def detect_creation_clusters(metadata: list[dict], window_seconds: int = 60) -> list[dict]:
    """Find clusters of documents created within a tight time window."""
    entries = []
    for m in metadata:
        dt = _parse_iso(m.get("creation_date"))
        doc_id = m.get("document_id") or m.get("id")
        if dt and doc_id is not None:
            entries.append((dt, doc_id))

    if len(entries) < 3:
        return []

    entries.sort(key=lambda x: x[0])
    window = timedelta(seconds=window_seconds)
    anomalies = []
    i = 0

    while i < len(entries):
        j = i + 1
        while j < len(entries) and (entries[j][0] - entries[i][0]) <= window:
            j += 1
        count = j - i
        if count >= 3:
            anomalies.append(
                {
                    "type": "creation_cluster",
                    "severity": "medium",
                    "description": (
                        f"{count} documents created within "
                        f"{window_seconds}s of each other at "
                        f"{entries[i][0]}"
                    ),
                    "evidence": {
                        "timestamp": str(entries[i][0]),
                        "count": count,
                    },
                    "affected_ids": [e[1] for e in entries[i:j]],
                }
            )
            i = j
        else:
            i += 1

    return anomalies
