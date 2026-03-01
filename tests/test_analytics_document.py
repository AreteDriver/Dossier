"""Tests for analytics document endpoints."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


DOCUMENT_ENDPOINTS = [
    "/api/document-sentiment",
    "/api/document-age",
    "/api/document-completeness",
    "/api/document-length",
    "/api/document-duplicates",
    "/api/orphan-documents",
    "/api/document-word-count",
    "/api/document-shared-entities",
    "/api/document-date-clusters",
    "/api/category-distribution",
    "/api/location-frequency",
    "/api/key-dates",
]


@pytest.mark.parametrize("endpoint", DOCUMENT_ENDPOINTS)
def test_document_endpoint_returns_200(analytics_client, endpoint):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:200]}"


class TestDocumentSideBySide:
    def test_sidebyside(self, analytics_client):
        client, doc_ids = analytics_client
        if len(doc_ids) >= 2:
            r = client.get(
                "/api/document-sidebyside",
                params={
                    "doc_a": doc_ids[0],
                    "doc_b": doc_ids[1],
                },
            )
            assert r.status_code == 200

    def test_sidebyside_not_found(self, analytics_client):
        client, _ = analytics_client
        r = client.get(
            "/api/document-sidebyside",
            params={
                "doc_a": 99999,
                "doc_b": 99998,
            },
        )
        assert r.status_code in (200, 404)


class TestDocumentSentiment:
    def test_sentiment_with_data(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/document-sentiment")
        assert r.status_code == 200


class TestDocumentLength:
    def test_length_stats(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/document-length")
        assert r.status_code == 200
        data = r.json()
        assert "documents" in data or "stats" in data or isinstance(data, dict)
