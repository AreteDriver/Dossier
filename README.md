# DOSSIER — Document Intelligence System

A local-first document analysis platform for ingesting, searching, and mapping connections across large document collections. Built for investigative research, FOIA analysis, and document-heavy investigations.

## Features

- **Multi-format ingestion** — PDF (native + OCR), plain text, HTML, images
- **Named Entity Recognition** — Extracts people, places, organizations, dates using custom domain-aware NER
- **Full-text search** — SQLite FTS5 with Porter stemming and Unicode support
- **Auto-classification** — Categorizes documents as depositions, flight logs, correspondence, reports, legal filings
- **Keyword frequency tracking** — Tracks word frequency across the entire corpus with per-document counts
- **Entity co-occurrence network** — Maps which entities appear together across documents
- **Duplicate detection** — SHA-256 file hashing prevents re-ingestion
- **REST API** — FastAPI with automatic OpenAPI docs
- **CLI tools** — Ingest, search, and query from the command line

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database
python -m dossier init

# Ingest a file
python -m dossier ingest path/to/document.pdf --source "FOIA Release"

# Ingest an entire directory
python -m dossier ingest-dir path/to/documents/ --source "Court Records"

# Start the web server
python -m dossier serve

# Search from CLI
python -m dossier search "palm beach"

# View stats
python -m dossier stats

# List top entities
python -m dossier entities person
```

## API Endpoints

Once running (`python -m dossier serve`), visit `http://localhost:8000/docs` for interactive API docs.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/search?q=...` | Full-text search with FTS5 |
| GET | `/api/documents` | List all documents |
| GET | `/api/documents/{id}` | Get document detail + entities + keywords |
| GET | `/api/entities?type=person` | Top entities by type |
| GET | `/api/entities/{id}/documents` | Documents containing an entity |
| GET | `/api/keywords` | Top keywords corpus-wide |
| GET | `/api/connections` | Entity co-occurrence network |
| GET | `/api/stats` | Dashboard statistics |
| POST | `/api/upload` | Upload and ingest a file |
| POST | `/api/ingest-directory?dirpath=...` | Batch ingest a directory |

## Project Structure

```
dossier/
├── __main__.py          # CLI entry point
├── api/
│   └── server.py        # FastAPI REST API
├── core/
│   └── ner.py           # NER engine + classifier + keyword extraction
├── db/
│   └── database.py      # SQLite schema + FTS5 + connection manager
├── ingestion/
│   ├── extractor.py     # Text extraction (PDF, OCR, HTML, images)
│   └── pipeline.py      # Ingestion orchestrator
├── data/
│   ├── inbox/           # Upload staging area
│   ├── processed/       # Organized by category
│   └── dossier.db       # SQLite database
├── static/
│   └── index.html       # Web UI (place the frontend here)
└── requirements.txt
```

## Extending the NER

The NER engine in `core/ner.py` uses gazetteers (known entity lists) that you can extend:

```python
# Add known people
KNOWN_PEOPLE.add("john doe")

# Add known places
KNOWN_PLACES.add("some location")

# Add known organizations
KNOWN_ORGS.add("some corp")
```

## Customizing Categories

Edit `CATEGORY_SIGNALS` in `core/ner.py` to adjust auto-classification signals or add new categories.

## Tech Stack

- **Python 3.10+**
- **FastAPI** — REST API
- **SQLite + FTS5** — Storage and full-text search
- **pdfplumber** — PDF text extraction
- **Tesseract OCR** — Scanned document/image OCR
- **Custom NER** — Regex + heuristic entity extraction

## License

MIT — Use for research purposes.
