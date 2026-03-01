"""Bulk parametrized tests for remaining analytics endpoints.

Covers ~130 endpoints that are simple SQL aggregation patterns.
Each test verifies the endpoint returns 200 with seeded data.
"""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


# All uncovered analytics endpoints — simple GET, no required params
ANALYTICS_ENDPOINTS = [
    "/api/cross-source-entities",
    "/api/page-distribution",
    "/api/high-value-targets",
    "/api/mention-heatmap",
    "/api/source-quality",
    "/api/doc-source-cluster",
    "/api/multi-mention-docs",
    "/api/flagged-summary",
    "/api/resolution-audit",
    "/api/source-date-range",
    "/api/search-history-stats",
    "/api/category-stats",
    "/api/financial-summary",
    "/api/source-overlap",
    "/api/text-length-distribution",
    "/api/multi-source-entities",
    "/api/hash-audit",
    "/api/canonical-coverage",
    "/api/fts-stats",
    "/api/document-notes",
    "/api/ingest-velocity",
    "/api/event-confidence-ranking",
    "/api/source-page-distribution",
    "/api/document-readability",
    "/api/source-completeness",
    "/api/orphan-events",
    "/api/page-density",
    "/api/duplicate-documents",
    "/api/source-cross-reference",
    "/api/event-precision-stats",
    "/api/category-ingest-timeline",
    "/api/document-size-buckets",
    "/api/event-date-gaps",
    "/api/unresolved-entities-summary",
    "/api/document-flagged-ratio",
    "/api/event-cluster-density",
    "/api/source-ingestion-summary",
    "/api/page-text-coverage",
    "/api/entity-name-length-stats",
    "/api/document-notes-summary",
    "/api/event-date-quality",
    "/api/source-category-matrix",
    "/api/entity-type-growth",
    "/api/entity-category-breakdown",
    "/api/document-age-distribution",
    "/api/event-source-density",
    "/api/source-document-quality",
    "/api/entity-alias-coverage",
    "/api/entity-co-occurrence",
    "/api/document-category-timeline",
    "/api/event-resolution-breakdown",
    "/api/entity-document-reach",
    "/api/entity-type-distribution",
    "/api/document-text-length",
    "/api/event-confidence-distribution",
    "/api/source-date-span",
    "/api/entity-mention-frequency",
    "/api/entity-isolation-score",
    "/api/document-category-balance",
    "/api/event-temporal-density",
    "/api/source-entity-exclusivity",
    "/api/entity-name-pattern",
    "/api/document-source-timeline",
    "/api/entity-cross-type-connections",
    "/api/event-context-length",
    "/api/source-flagged-ratio",
    "/api/entity-resolution-coverage",
    "/api/entity-degree-centrality",
    "/api/document-title-analysis",
    "/api/event-date-range-span",
    "/api/source-page-volume",
    "/api/entity-alias-type-breakdown",
    "/api/entity-shared-sources",
    "/api/document-filename-pattern",
    "/api/event-monthly-heatmap",
    "/api/source-entity-concentration",
    "/api/entity-connection-strength-rank",
    "/api/entity-type-per-source",
    "/api/document-ingestion-gap",
    "/api/event-weekday-distribution",
    "/api/source-category-coverage",
    "/api/entity-multi-alias-ratio",
    "/api/entity-betweenness-score",
    "/api/document-source-diversity",
    "/api/event-burst-detection",
    "/api/source-unique-entities",
    "/api/entity-lifecycle-span",
    "/api/connection-growth-timeline",
    "/api/entity-source-loyalty",
    "/api/document-page-outliers",
    "/api/event-confidence-trend",
    "/api/source-ingestion-cadence",
    "/api/entity-connection-density",
    "/api/connection-temporal-overlap",
    "/api/entity-document-exclusivity",
    "/api/document-flagged-timeline",
    "/api/event-precision-histogram",
    "/api/source-entity-type-mix",
    "/api/entity-alias-chain",
]


@pytest.mark.parametrize("endpoint", ANALYTICS_ENDPOINTS)
def test_analytics_endpoint(analytics_client, endpoint):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:300]}"


# Endpoints requiring specific params
class TestAnalyticsWithParams:
    def test_keyword_context(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/keyword-context", params={"keyword": "epstein"})
        assert r.status_code == 200

    def test_entity_connections_map(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db
        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities LIMIT 1").fetchone()
        if ent:
            r = client.get("/api/entity-connections-map", params={"entity_id": ent["id"]})
            assert r.status_code == 200

    def test_entity_pair_strength(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db
        with get_db() as conn:
            persons = conn.execute("SELECT id FROM entities WHERE type = 'person' LIMIT 2").fetchall()
        if len(persons) >= 2:
            r = client.get("/api/entity-pair-strength", params={
                "entity_a_id": persons[0]["id"],
                "entity_b_id": persons[1]["id"],
            })
            assert r.status_code == 200

    def test_connection_weight_histogram(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-weight-histogram")
        assert r.status_code == 200

    def test_connection_bridge_entities(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-bridge-entities")
        assert r.status_code == 200

    def test_connection_asymmetry(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-asymmetry")
        assert r.status_code == 200

    def test_connection_weight_percentile(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/connection-weight-percentile")
        assert r.status_code == 200
