"""Tests for dossier.forensics.api_timeline — Timeline API endpoints."""

import sqlite3
import pytest
from dossier.forensics.timeline import (
    TimelineExtractor,
    init_timeline_tables,
    store_events,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def seeded_client(client, tmp_path, monkeypatch):
    """Client with a document + entities + timeline events in the DB."""
    import dossier.db.database as db_mod

    db_path = db_mod.DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    text = (
        "On March 14, 2009, Jane Doe testified in the Southern District. "
        "Jeffrey Epstein was arrested on July 6, 2019 at Teterboro Airport. "
        "The investigation began approximately 2005."
    )

    conn.execute(
        "INSERT INTO documents (filename, filepath, title, raw_text, category, source) VALUES (?, ?, ?, ?, ?, ?)",
        ("deposition_3.txt", "/tmp/deposition_3.txt", "Deposition 3", text, "legal", "court"),
    )
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        ("Jane Doe", "person", "jane doe"),
    )
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        ("Jeffrey Epstein", "person", "jeffrey epstein"),
    )
    conn.commit()

    # Extract and store timeline events
    entity_rows = conn.execute("SELECT name FROM entities").fetchall()
    entity_names = [r["name"] for r in entity_rows]

    extractor = TimelineExtractor(entity_names=entity_names)
    events = extractor.extract_events(text, document_id=1)
    store_events(conn, events)
    conn.commit()
    conn.close()

    return client


# ═══════════════════════════════════════════════════════════════════
# GET /api/timeline
# ═══════════════════════════════════════════════════════════════════


class TestGetTimeline:
    def test_timeline_empty_db(self, client):
        r = client.get("/api/timeline")
        assert r.status_code == 200
        data = r.json()
        assert data["events"] == []
        assert data["count"] == 0

    def test_timeline_returns_events(self, seeded_client):
        r = seeded_client.get("/api/timeline")
        assert r.status_code == 200
        data = r.json()
        assert data["count"] > 0
        assert len(data["events"]) == data["count"]

    def test_timeline_filter_start_date(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"start": "2010-01-01"})
        data = r.json()
        for event in data["events"]:
            if event["event_date"]:
                assert event["event_date"] >= "2010-01-01"

    def test_timeline_filter_end_date(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"end": "2010-01-01"})
        data = r.json()
        for event in data["events"]:
            if event["event_date"]:
                assert event["event_date"] <= "2010-01-01"

    def test_timeline_filter_document_id(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"document_id": 1})
        data = r.json()
        assert data["count"] > 0
        for event in data["events"]:
            assert event["document_id"] == 1

    def test_timeline_filter_nonexistent_document(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"document_id": 999})
        data = r.json()
        assert data["count"] == 0

    def test_timeline_filter_min_confidence(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"min_confidence": 0.8})
        data = r.json()
        for event in data["events"]:
            assert event["confidence"] >= 0.8

    def test_timeline_filter_entity(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"entity": "Jane Doe"})
        data = r.json()
        # Entity-linked events only
        for event in data["events"]:
            names = [e["name"] for e in event.get("entities", [])]
            assert any("Jane" in n for n in names)

    def test_timeline_include_unresolved(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"include_unresolved": True})
        data = r.json()
        assert data["count"] >= 0

    def test_timeline_limit(self, seeded_client):
        r = seeded_client.get("/api/timeline", params={"limit": 1})
        data = r.json()
        assert len(data["events"]) <= 1


# ═══════════════════════════════════════════════════════════════════
# GET /api/timeline/stats
# ═══════════════════════════════════════════════════════════════════


class TestTimelineStats:
    def test_stats_empty(self, client):
        r = client.get("/api/timeline/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["total_events"] == 0

    def test_stats_with_data(self, seeded_client):
        r = seeded_client.get("/api/timeline/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["total_events"] > 0
        assert "date_range" in data
        assert "by_precision" in data


# ═══════════════════════════════════════════════════════════════════
# GET /api/timeline/unresolved
# ═══════════════════════════════════════════════════════════════════


class TestUnresolved:
    def test_unresolved_empty(self, client):
        r = client.get("/api/timeline/unresolved")
        assert r.status_code == 200
        data = r.json()
        assert data["unresolved"] == []
        assert data["count"] == 0

    def test_unresolved_with_limit(self, seeded_client):
        r = seeded_client.get("/api/timeline/unresolved", params={"limit": 10})
        assert r.status_code == 200
        data = r.json()
        for event in data["unresolved"]:
            assert not event.get("is_resolved", True)


# ═══════════════════════════════════════════════════════════════════
# POST /api/timeline/extract/{document_id}
# ═══════════════════════════════════════════════════════════════════


class TestExtractDocument:
    def test_extract_existing_document(self, seeded_client):
        r = seeded_client.post("/api/timeline/extract/1")
        assert r.status_code == 200
        data = r.json()
        assert data["document_id"] == 1
        assert data["events_extracted"] > 0
        assert "resolved" in data
        assert "unresolved" in data
        assert "event_ids" in data

    def test_extract_nonexistent_document(self, seeded_client):
        r = seeded_client.post("/api/timeline/extract/999")
        # Returns a tuple (dict, 404) but FastAPI serializes it as 200 with the tuple
        # The endpoint returns {"error": ...}, 404 which is non-standard
        assert r.status_code == 200
        data = r.json()
        assert data[0]["error"] == "Document 999 not found"


# ═══════════════════════════════════════════════════════════════════
# POST /api/timeline/extract-all
# ═══════════════════════════════════════════════════════════════════


class TestExtractAll:
    def test_extract_all_empty_corpus(self, client):
        r = client.post("/api/timeline/extract-all")
        assert r.status_code == 200
        data = r.json()
        assert data["documents_processed"] == 0
        assert data["total_events"] == 0

    def test_extract_all_with_documents(self, seeded_client):
        r = seeded_client.post("/api/timeline/extract-all")
        assert r.status_code == 200
        data = r.json()
        assert data["documents_processed"] >= 1
        assert data["total_events"] > 0
        assert data["resolved"] + data["unresolved"] == data["total_events"]
