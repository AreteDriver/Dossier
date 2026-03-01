"""Tests for analytics source/export/misc endpoints."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


MISC_ENDPOINTS = [
    "/api/document-sentiment",
    "/api/financial-profiles",
    "/api/category-distribution",
    "/api/location-frequency",
    "/api/key-dates",
    "/api/source-network",
    "/api/alias-network",
    "/api/witness-overlap",
    "/api/unresolved-entities",
    "/api/flagged-hub",
    "/api/keyword-cooccurrence",
]


@pytest.mark.parametrize("endpoint", MISC_ENDPOINTS)
def test_misc_endpoint_returns_200(analytics_client, endpoint):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:200]}"


class TestExportTimelineFormats:
    def test_timeline_json(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/timeline", params={"format": "json"})
        assert r.status_code == 200

    def test_timeline_csv(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/timeline", params={"format": "csv"})
        assert r.status_code == 200
