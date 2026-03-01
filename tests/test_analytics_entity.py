"""Tests for analytics entity endpoints — parametrized for coverage."""

import pytest

from tests.conftest import seed_analytics_data


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


# Parametrized: simple GET endpoints that return 200 with expected keys
ENTITY_ENDPOINTS = [
    ("/api/entity-frequency", {"entities"}),
    ("/api/entity-type-breakdown", None),
    ("/api/entity-timeline-density", None),
    ("/api/entity-first-last", None),
    ("/api/entity-name-length", None),
    ("/api/entity-degree-distribution", None),
    ("/api/entity-aliases-list", None),
    ("/api/entity-document-count", None),
    ("/api/entity-isolation", None),
    ("/api/entity-growth", None),
    ("/api/source-entity-density", None),
    ("/api/entity-name-duplicates", None),
    ("/api/entity-type-ratio", None),
    ("/api/entity-first-seen", None),
    ("/api/entity-hub-score", None),
    ("/api/entity-spread", None),
    ("/api/entity-longevity", None),
    ("/api/entity-singletons", None),
    ("/api/entity-coappearances", None),
    ("/api/unresolved-entities", None),
    ("/api/flagged-hub", None),
    ("/api/keyword-cooccurrence", None),
    ("/api/category-entity-matrix", None),
    ("/api/event-entity-ranking", None),
    ("/api/witness-overlap", None),
    ("/api/financial-profiles", None),
    ("/api/alias-network", None),
    ("/api/source-network", None),
    ("/api/financial-entity-links", None),
    ("/api/entity-connections-timeline", None),
    ("/api/source-entity-overlap", None),
]


@pytest.mark.parametrize("endpoint,expected_keys", ENTITY_ENDPOINTS)
def test_entity_endpoint_returns_200(analytics_client, endpoint, expected_keys):
    client, _ = analytics_client
    r = client.get(endpoint)
    assert r.status_code == 200, f"{endpoint} returned {r.status_code}: {r.text[:200]}"
    if expected_keys:
        data = r.json()
        assert expected_keys.issubset(set(data.keys())), f"{endpoint}: missing keys"


class TestEntityConnectionsMap:
    def test_connections_map(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities WHERE type = 'person' LIMIT 1").fetchone()
        if ent:
            r = client.get("/api/entity-connections-map", params={"entity_id": ent["id"]})
            assert r.status_code == 200

    def test_connections_map_missing(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/entity-connections-map", params={"entity_id": 99999})
        # May return 200 with empty or 404
        assert r.status_code in (200, 404)


class TestEntityPairHistory:
    def test_pair_history(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            persons = conn.execute(
                "SELECT id FROM entities WHERE type = 'person' LIMIT 2"
            ).fetchall()
        if len(persons) >= 2:
            r = client.get(
                "/api/entity-pair-history",
                params={
                    "entity_a": persons[0]["id"],
                    "entity_b": persons[1]["id"],
                },
            )
            assert r.status_code == 200

    def test_pair_history_empty(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/entity-pair-history")
        assert r.status_code == 200
        assert r.json()["documents"] == []


class TestEntityPairCodocs:
    def test_pair_codocs(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            persons = conn.execute(
                "SELECT id FROM entities WHERE type = 'person' LIMIT 2"
            ).fetchall()
        if len(persons) >= 2:
            r = client.get(
                "/api/entity-pair-codocs",
                params={
                    "entity_a_id": persons[0]["id"],
                    "entity_b_id": persons[1]["id"],
                },
            )
            assert r.status_code == 200


class TestEntityPath:
    def test_entity_path(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            persons = conn.execute("SELECT id FROM entities LIMIT 2").fetchall()
        if len(persons) >= 2:
            r = client.get(
                "/api/entity-path",
                params={
                    "from_id": persons[0]["id"],
                    "to_id": persons[1]["id"],
                },
            )
            assert r.status_code == 200

    def test_entity_path_suggestions(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/entity-path-suggestions")
        assert r.status_code == 200
