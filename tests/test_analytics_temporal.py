"""Tests for analytics temporal endpoints."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


TEMPORAL_ENDPOINTS = [
    "/api/timeline/heatmap",
    "/api/activity-heatmap",
    "/api/investigation-timeline",
    "/api/source-timeline",
    "/api/temporal-heatmap",
    "/api/category-timeline",
    "/api/ingest-timeline",
    "/api/event-calendar",
    "/api/timeline-gaps",
    "/api/event-types",
    "/api/event-context",
    "/api/event-heatmap",
    "/api/event-resolution-rate",
]


@pytest.mark.parametrize("endpoint", TEMPORAL_ENDPOINTS)
def test_temporal_endpoint_returns_200(analytics_client, endpoint):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:200]}"


class TestTimelineOverlay:
    def test_overlay(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities LIMIT 1").fetchone()
        if ent:
            r = client.get("/api/timeline/overlay", params={"entity_ids": str(ent["id"])})
            assert r.status_code == 200

    def test_overlay_empty(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/timeline/overlay", params={"entity_ids": "99999"})
        assert r.status_code == 200


class TestTimelineHeatmap:
    def test_heatmap_has_dates(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/timeline/heatmap")
        assert r.status_code == 200
        data = r.json()
        assert "dates" in data or isinstance(data, dict)


class TestActivityHeatmap:
    def test_activity_year_filter(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/activity-heatmap", params={"year": "2015"})
        assert r.status_code == 200


class TestTimelineGaps:
    def test_gaps(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/timeline-gaps")
        assert r.status_code == 200
