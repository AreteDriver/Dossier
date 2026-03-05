# DOSSIER

**Open-source document intelligence for investigative analytics.**
80% of enterprise document analysis capability. 0% of the infrastructure cost.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/AreteDriver/DOSSIER/actions/workflows/ci.yml/badge.svg)](https://github.com/AreteDriver/DOSSIER/actions)

---

## What It Does

DOSSIER ingests investigative document corpora — PDFs, emails, scanned images, legal filings — and surfaces the connections that manual review misses.

**Core capabilities:**
- Ingest PDFs (native text + OCR fallback), emails (.eml/.mbox/JSON/CSV), HTML, images
- Named entity extraction via custom gazetteers (no GPU, no retraining)
- Auto-classification by document type: deposition, filing, correspondence, financial record
- Full-text search via SQLite FTS5 — zero infrastructure, laptop-ready
- Entity co-occurrence graph: who appears with whom, across how many documents, from which sources
- SHA-256 deduplication at ingestion — re-runs are safe

---

## Who It's For

- Investigative journalists working FOIA corpora
- Legal discovery teams without six-figure tooling budgets
- Compliance analysts processing document dumps
- Anyone where documents are evidence and connections get missed

---

## Quick Start

```bash
pip install -r requirements.txt
python -m dossier init
python -m dossier ingest-dir /path/to/documents --source "Source Name"
python -m dossier serve
# API: http://localhost:8000/docs
# UI:  http://localhost:8000
```

---

## Architecture

```
Raw File → Format Detection → Text Extraction → NER → Classification → FTS5 Index → Entity Graph

├─ PDF ────→ pdfplumber (native) / Tesseract (OCR fallback)
├─ Email ──→ Header parsing + body + attachment recursion
├─ HTML ───→ Tag stripping + structure preservation
└─ Image ──→ OCR with quality flagging
```

**Key decisions:**

| Choice | Why |
|---|---|
| SQLite + FTS5 over Elasticsearch | Zero infra, single file, Porter stemming, transactional consistency |
| Custom NER over spaCy/HuggingFace | Domain-specific patterns, no GPU, explainable output |
| Weighted signal classifier | Investigator-overridable, no black-box confidence scores |
| SHA-256 dedup at intake | Prevents silent re-processing corruption |

---

## API

```
GET /api/search?q=...&type=deposition&date_start=2001&date_end=2005
GET /api/connections                          # Full entity co-occurrence network
GET /api/entities/{id}/documents              # All documents containing an entity
GET /api/documents/{id}                       # Full document with extracted entities
```

---

## Status

- [x] Ingestion pipeline (PDF, email, HTML, image)
- [x] FTS5 full-text search
- [x] Custom NER + entity linking
- [x] Auto-classification
- [x] Co-occurrence graph
- [x] REST API
- [ ] Timeline reconstruction (in progress)
- [ ] Document provenance/metadata forensics
- [ ] Redaction detection

---

## Real-World Example

A corpus of 482 investigative documents was ingested — court filings, flight logs, correspondence, financial records. DOSSIER extracted 5,902 named entities and built a co-occurrence graph across all documents.

**What it found that keyword search missed:**
- A fax number appearing in 11 unrelated filings from different jurisdictions — connecting entities that had no obvious textual overlap
- 24,428 timeline events reconstructed from date extraction across the full corpus
- Entity clusters revealing which people appeared together most frequently, weighted by document source diversity

The co-occurrence graph surfaces *structural* relationships in document collections — connections that exist in the pattern of appearances, not in the text itself. A human reviewer would need weeks to find what the graph reveals in seconds.

---

## Contributing

Issues, PRs, and edge-case corpora welcome. If you work in legal discovery, investigative journalism, or FOIA analysis — try it and tell me what breaks.

---

*Built with SQLite, FastAPI, pdfplumber, Tesseract, and 17 years of operations experience.*
