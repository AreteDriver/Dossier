"""Tests for timeline visualization data endpoints."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


class TestVisualizationTimeline:
    def test_timeline_returns_months(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/timeline")
        assert r.status_code == 200
        data = r.json()
        assert "months" in data
        assert "total_events" in data

    def test_timeline_month_structure(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/timeline")
        data = r.json()
        if data["months"]:
            month = data["months"][0]
            assert "month" in month
            assert "events" in month
            assert "event_count" in month
            assert month["event_count"] == len(month["events"])

    def test_timeline_event_structure(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/timeline")
        data = r.json()
        for month in data["months"]:
            for event in month["events"]:
                assert "id" in event
                assert "date" in event
                assert "confidence" in event
                assert "document_id" in event

    def test_timeline_min_confidence(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/timeline", params={"min_confidence": 0.9})
        assert r.status_code == 200

    def test_timeline_limit(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/timeline", params={"limit": 5})
        assert r.status_code == 200
        assert r.json()["total_events"] <= 5


class TestVisualizationEntityTimeline:
    def test_entity_timeline(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities LIMIT 1").fetchone()
        if not ent:
            pytest.skip("No entities")
        r = client.get(f"/api/visualization/entity-timeline/{ent['id']}")
        assert r.status_code == 200
        data = r.json()
        assert "entity" in data
        assert "events" in data
        assert "event_count" in data
        assert data["entity"]["id"] == ent["id"]

    def test_entity_timeline_not_found(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/visualization/entity-timeline/99999")
        assert r.status_code == 404
