"""Tests for analytics connection endpoints."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


CONNECTION_ENDPOINTS = [
    "/api/connection-strength",
    "/api/connection-weight-distribution",
    "/api/top-connections",
    "/api/connection-density",
    "/api/connection-reciprocity",
    "/api/connection-weight-stats",
    "/api/source-overlap-matrix",
    "/api/connection-type-breakdown",
    "/api/connection-cluster-summary",
    "/api/connection-weight-histogram",
    "/api/connection-bridge-entities",
    "/api/connection-asymmetry",
    "/api/connection-weight-percentile",
]


@pytest.mark.parametrize("endpoint", CONNECTION_ENDPOINTS)
def test_connection_endpoint_returns_200(analytics_client, endpoint):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:200]}"


class TestConnectionStrength:
    def test_min_weight(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-strength", params={"min_weight": 1})
        assert r.status_code == 200

    def test_high_min_weight(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-strength", params={"min_weight": 9999})
        assert r.status_code == 200


class TestTopConnections:
    def test_limit(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/top-connections", params={"limit": 5})
        assert r.status_code == 200
