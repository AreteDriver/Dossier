# DOSSIER Timeline Module — Integration Guide

## Files to Add

```
dossier/
├── forensics/
│   ├── __init__.py          → NEW: dossier/forensics/__init__.py
│   ├── timeline.py          → NEW: dossier/forensics/timeline.py (core engine)
│   └── api_timeline.py      → NEW: dossier/forensics/api_timeline.py (API routes)
└── tests/
    └── test_timeline.py     → NEW: dossier/tests/test_timeline.py
```

## Step 1: Add Dependency

Add `python-dateutil` to your `requirements.txt`:
```
python-dateutil>=2.8.0
```

## Step 2: Initialize Timeline Tables

In `dossier/db/database.py`, add to your `init_db()` function:

```python
from dossier.forensics.timeline import init_timeline_tables

def init_db():
    # ... existing schema creation ...
    with get_db() as conn:
        init_timeline_tables(conn)
```

Or run it standalone:
```python
from dossier.db.database import get_db
from dossier.forensics.timeline import init_timeline_tables

with get_db() as conn:
    init_timeline_tables(conn)
```

## Step 3: Wire Into Ingestion Pipeline

In `dossier/ingestion/pipeline.py`, after NER runs, add timeline extraction:

```python
from dossier.forensics.timeline import TimelineExtractor, store_events

def ingest_file(filepath, source="", date=""):
    # ... existing extraction and NER ...

    # After NER completes and entities are stored:
    entity_rows = conn.execute("SELECT name FROM entities").fetchall()
    entity_names = [r["name"] for r in entity_rows]

    extractor = TimelineExtractor(entity_names=entity_names)
    events = extractor.extract_events(raw_text, document_id=doc_id)
    store_events(conn, events)

    # Add to stats
    result["stats"]["timeline_events"] = len(events)
    result["stats"]["unresolved_dates"] = sum(1 for e in events if not e.is_resolved)
```

## Step 4: Mount API Routes

In `dossier/api/server.py`:

```python
from dossier.forensics.api_timeline import router as timeline_router

app.include_router(timeline_router, prefix="/api/timeline", tags=["timeline"])
```

## Step 5: Add CLI Command

In `dossier/__main__.py`, add a `timeline` command:

```python
elif cmd == "timeline":
    timeline_cmd()

def timeline_cmd():
    from dossier.db.database import init_db, get_db
    from dossier.forensics.timeline import query_timeline

    init_db()
    # Parse args for --start, --end, --entity
    start = end = entity = None
    args = sys.argv[2:]
    for i, arg in enumerate(args):
        if arg == "--start" and i + 1 < len(args): start = args[i + 1]
        elif arg == "--end" and i + 1 < len(args): end = args[i + 1]
        elif arg == "--entity" and i + 1 < len(args): entity = args[i + 1]

    with get_db() as conn:
        events = query_timeline(conn, start_date=start, end_date=end, entity_name=entity)

    if not events:
        print("No timeline events found.")
        return

    print(f"\n─── Timeline ({len(events)} events) ───\n")
    for e in events:
        resolved = "✓" if e.get("is_resolved") else "?"
        date_str = e["event_date"] or "[relative]"
        print(f"  {resolved} {date_str:12s}  [{e['precision']:6s}]  {e['doc_title']}")
        print(f"    {e['context'][:120]}")
        if e.get("entities"):
            names = ", ".join(ent["name"] for ent in e["entities"])
            print(f"    → {names}")
        print()
```

## API Endpoints Added

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/timeline` | Query timeline with filters (start, end, entity, confidence) |
| GET | `/api/timeline/stats` | Timeline summary stats |
| GET | `/api/timeline/unresolved` | Relative dates needing manual review |
| POST | `/api/timeline/extract/{doc_id}` | Run extraction on a single document |
| POST | `/api/timeline/extract-all` | Rebuild timeline for entire corpus |

## Running Tests

```bash
pytest dossier/tests/test_timeline.py -v
```

## What's Next

The timeline module is designed to be extended with:
1. **Provenance module** (`forensics/provenance.py`) — PDF metadata extraction
2. **Anomaly module** (`forensics/anomaly.py`) — gap detection, metadata outliers
3. **Timeline visualization** — add to the frontend as a horizontal scrollable timeline
