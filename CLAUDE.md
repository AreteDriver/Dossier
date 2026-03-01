# CLAUDE.md — Dossier

## Project Overview

Local-first document intelligence system

## Current State

- **Version**: 0.6.0
- **Language**: Python
- **Tests**: 1055
- **Files**: 89 across 1 languages

## Architecture

```
Dossier/
├── dossier/
│   ├── api/
│   │   ├── server.py              (84 lines — app setup, middleware, router mounts)
│   │   ├── utils.py               (shared constants + helpers)
│   │   ├── routes_ingestion.py    (5 routes — upload, directory, email ingest)
│   │   ├── routes_search.py       (6 routes — FTS5, advanced, keywords, connections)
│   │   ├── routes_documents.py    (14 routes — CRUD, text, notes, provenance)
│   │   ├── routes_entities.py     (17 routes — CRUD, tags, aliases, merge, profiles)
│   │   ├── routes_forensics.py    (22 routes — forensics, redactions, risk, OCR)
│   │   ├── routes_collaboration.py (27 routes — annotations, audit, watchlist, alerts)
│   │   ├── routes_investigation.py (23 routes — board, case files, evidence chains)
│   │   ├── routes_intelligence.py  (21 routes — AI, duplicates, patterns, narrative)
│   │   └── routes_analytics.py     (194 routes — metrics/analytics/provenance endpoints)
│   ├── core/
│   ├── data/
│   ├── db/
│   ├── forensics/
│   ├── ingestion/
│   └── static/
├── tests/
├── pyproject.toml
├── requirements.txt
```

### API Module Pattern
All router modules follow the same pattern:
- `from dossier.api import utils` (module import, not name import)
- Access shared state via `utils.UPLOAD_DIR`, `utils._validate_path()`, etc.
- This ensures monkeypatching `utils.ATTR` in tests propagates correctly

## Tech Stack

- **Language**: Python
- **Framework**: fastapi
- **Package Manager**: pip
- **Linters**: ruff
- **Formatters**: ruff
- **Test Frameworks**: pytest
- **CI/CD**: GitHub Actions

## Coding Standards

- **Naming**: snake_case
- **Quote Style**: double quotes
- **Type Hints**: present
- **Docstrings**: google style
- **Imports**: absolute
- **Path Handling**: pathlib
- **Line Length (p95)**: 79 characters

## Common Commands

```bash
# test
pytest tests/ -v
# lint
ruff check . && ruff format --check .
# format
ruff format .
# coverage
pytest --cov=dossier tests/
```

## Anti-Patterns (Do NOT Do)

- Do NOT commit secrets, API keys, or credentials
- Do NOT skip writing tests for new code
- Do NOT use `os.path` — use `pathlib.Path` everywhere
- Do NOT use bare `except:` — catch specific exceptions
- Do NOT use mutable default arguments
- Do NOT use `print()` for logging — use the `logging` module
- Do NOT use synchronous database calls in async endpoints
- Do NOT return raw dicts — use Pydantic response models

## Dependencies

### Core
- fastapi
- uvicorn

### Dev
- pytest
- pytest-cov
- pytest-benchmark
- httpx
- ruff

## Domain Context

### Key Models/Classes
- `CandidateMatch`
- `Community`
- `DatePrecision`
- `EntityResolver`
- `ExtractedDate`
- `GraphAnalyzer`
- `GraphStats`
- `MergeAction`
- `NodeMetrics`
- `PathResult`
- `ResolutionResult`
- `PDFMetadata`

### Domain Terms
- Court Records
- DOSSIER
- Document Intelligence System
- FOIA
- NER

### API Endpoints
- `/`
- `/api/activity-heatmap`
- `/api/ai/ask`
- `/api/ai/summarize`
- `/api/alias-network`
- `/api/aliases/resolve`
- `/api/annotations/search`
- `/api/anomalies`
- `/api/audit`
- `/api/board`
- `/api/bulk-tag`
- `/api/case-files`
- `/api/export/documents`
- `/api/export/entity-graph`
- `/api/export/entities`
- `/api/visualization/timeline`
- `/api/documents/{doc_id}/pdf-metadata`
- `/api/pdf-metadata/stats`
- `/api/pdf-metadata/search`
- `/api/pdf-metadata/timeline`
- `/api/pdf-metadata/extract-all`

### Enums/Constants
- `APPROXIMATE`
- `AUTO_MERGE`
- `BASE_URL`
- `DAY`
- `FARA_SEARCH_URL`
- `LDA_BASE_URL`
- `MONTH`
- `NO_MERGE`
- `RELATIVE`
- `RESOLVER_SCHEMA`

## Git Conventions

- Commit messages: Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`)
- Branch naming: `feat/description`, `fix/description`
- Run tests before committing
