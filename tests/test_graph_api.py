"""Tests for dossier.core.api_graph — Graph Analysis API endpoints."""

import sqlite3

import pytest


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def seeded_graph_client(client):
    """Client with entities and connections seeded for graph testing."""
    import dossier.db.database as db_mod

    db_path = db_mod.DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Insert entities
    entities = [
        (1, "Alice", "person", "alice"),
        (2, "Bob", "person", "bob"),
        (3, "Carol", "person", "carol"),
        (4, "Acme Corp", "org", "acme corp"),
        (5, "New York", "place", "new york"),
        (6, "Frank", "person", "frank"),  # isolated
    ]
    conn.executemany(
        "INSERT INTO entities (id, name, type, canonical) VALUES (?, ?, ?, ?)", entities
    )

    # Insert connections
    connections = [
        (1, 2, 5),  # Alice-Bob
        (2, 3, 3),  # Bob-Carol
        (1, 4, 2),  # Alice-Acme
        (3, 5, 1),  # Carol-NYC
        (4, 5, 4),  # Acme-NYC
    ]
    conn.executemany(
        "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
        connections,
    )
    conn.commit()
    conn.close()

    return client


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/stats
# ═══════════════════════════════════════════════════════════════════


class TestStats:
    def test_empty_db(self, client):
        r = client.get("/api/graph/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["node_count"] == 0

    def test_with_data(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["node_count"] == 5
        assert data["edge_count"] == 5
        assert data["components"] == 1

    def test_type_filter(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/stats", params={"type": "person"})
        assert r.status_code == 200
        data = r.json()
        assert data["node_count"] == 3  # Alice, Bob, Carol


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/centrality
# ═══════════════════════════════════════════════════════════════════


class TestCentrality:
    def test_default_degree(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/centrality")
        assert r.status_code == 200
        data = r.json()
        assert data["metric"] == "degree"
        assert len(data["results"]) > 0
        assert "score" in data["results"][0]

    def test_betweenness(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/centrality", params={"metric": "betweenness"})
        assert r.status_code == 200
        assert r.json()["metric"] == "betweenness"

    def test_invalid_metric_400(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/centrality", params={"metric": "bogus"})
        assert r.status_code == 400

    def test_limit(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/centrality", params={"limit": 2})
        assert r.status_code == 200
        assert len(r.json()["results"]) == 2

    def test_empty_db(self, client):
        r = client.get("/api/graph/centrality")
        assert r.status_code == 200
        assert r.json()["results"] == []


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/communities
# ═══════════════════════════════════════════════════════════════════


class TestCommunities:
    def test_with_data(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/communities")
        assert r.status_code == 200
        data = r.json()
        assert len(data["communities"]) > 0
        assert "members" in data["communities"][0]

    def test_min_size_filter(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/communities", params={"min_size": 100})
        assert r.status_code == 200
        assert len(r.json()["communities"]) == 0

    def test_empty_db(self, client):
        r = client.get("/api/graph/communities")
        assert r.status_code == 200
        assert r.json()["communities"] == []


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/path
# ═══════════════════════════════════════════════════════════════════


class TestPath:
    def test_connected_pair(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/path", params={"source_id": 1, "target_id": 2})
        assert r.status_code == 200
        data = r.json()
        assert data["hops"] >= 1
        assert len(data["nodes"]) >= 2

    def test_disconnected_pair_404(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/path", params={"source_id": 1, "target_id": 6})
        assert r.status_code == 404

    def test_same_entity(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/path", params={"source_id": 1, "target_id": 1})
        assert r.status_code == 200
        assert r.json()["hops"] == 0

    def test_nonexistent_entity_404(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/path", params={"source_id": 1, "target_id": 999})
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/neighbors/{entity_id}
# ═══════════════════════════════════════════════════════════════════


class TestNeighbors:
    def test_with_data(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/neighbors/1")
        assert r.status_code == 200
        data = r.json()
        assert data["entity_id"] == 1
        assert len(data["neighbors"]) > 0

    def test_hops_param(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/neighbors/1", params={"hops": 2})
        assert r.status_code == 200
        assert len(r.json()["neighbors"]) > 2

    def test_nonexistent_404(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/neighbors/999")
        assert r.status_code == 404

    def test_isolated_returns_empty(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/neighbors/6")
        assert r.status_code == 200
        assert r.json()["neighbors"] == []


# ═══════════════════════════════════════════════════════════════════
# GET /api/graph/subgraph
# ═══════════════════════════════════════════════════════════════════


class TestSubgraph:
    def test_valid_ids(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/subgraph", params={"entity_ids": "1,2,3"})
        assert r.status_code == 200
        data = r.json()
        assert len(data["nodes"]) == 3
        assert len(data["edges"]) == 2

    def test_empty_list(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/subgraph", params={"entity_ids": ""})
        assert r.status_code == 200
        assert r.json() == {"nodes": [], "edges": []}

    def test_no_param(self, seeded_graph_client):
        r = seeded_graph_client.get("/api/graph/subgraph")
        assert r.status_code == 200
        assert r.json() == {"nodes": [], "edges": []}
