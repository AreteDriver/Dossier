"""Tests for dossier.core.api_resolver — Entity Resolver API endpoints."""

import sqlite3

import pytest


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def seeded_resolver_client(client):
    """Client with entities seeded for resolution testing."""
    import dossier.db.database as db_mod

    db_path = db_mod.DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Insert entities that should resolve to each other
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        ("John Smith", "person", "john smith"),
    )
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        ("Smith, John", "person", "smith, john"),
    )
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        ("New York", "place", "new york"),
    )
    conn.commit()
    conn.close()

    return client


# ═══════════════════════════════════════════════════════════════════
# POST /api/resolver/resolve
# ═══════════════════════════════════════════════════════════════════


class TestResolveAll:
    def test_empty_db(self, client):
        r = client.post("/api/resolver/resolve")
        assert r.status_code == 200
        data = r.json()
        assert data["entities_scanned"] == 0
        assert data["auto_merged"] == 0

    def test_with_entities(self, seeded_resolver_client):
        r = seeded_resolver_client.post("/api/resolver/resolve")
        assert r.status_code == 200
        data = r.json()
        assert data["entities_scanned"] >= 2
        assert data["auto_merged"] >= 1

    def test_by_type_filter(self, seeded_resolver_client):
        r = seeded_resolver_client.post("/api/resolver/resolve", params={"entity_type": "place"})
        assert r.status_code == 200
        data = r.json()
        # Only 1 place entity, no pairs to resolve
        assert data["entities_scanned"] == 1
        assert data["auto_merged"] == 0


# ═══════════════════════════════════════════════════════════════════
# POST /api/resolver/resolve/{entity_id}
# ═══════════════════════════════════════════════════════════════════


class TestResolveSingle:
    def test_resolve_one_entity(self, seeded_resolver_client):
        r = seeded_resolver_client.post("/api/resolver/resolve/1")
        assert r.status_code == 200
        data = r.json()
        assert data["entity_id"] == 1
        assert len(data["matches"]) >= 1


# ═══════════════════════════════════════════════════════════════════
# GET /api/resolver/duplicates
# ═══════════════════════════════════════════════════════════════════


class TestDuplicates:
    def test_empty(self, client):
        r = client.get("/api/resolver/duplicates")
        assert r.status_code == 200
        assert r.json()["duplicates"] == []

    def test_after_resolve(self, seeded_resolver_client):
        # First resolve to create merges
        seeded_resolver_client.post("/api/resolver/resolve")
        r = seeded_resolver_client.get("/api/resolver/duplicates")
        assert r.status_code == 200
        assert len(r.json()["duplicates"]) >= 1


# ═══════════════════════════════════════════════════════════════════
# GET /api/resolver/queue + POST /api/resolver/queue/{id}/review
# ═══════════════════════════════════════════════════════════════════


class TestReviewQueue:
    def test_empty(self, client):
        r = client.get("/api/resolver/queue")
        assert r.status_code == 200
        assert r.json()["queue"] == []

    def test_review_nonexistent(self, client):
        r = client.post("/api/resolver/queue/9999/review", params={"approve": True})
        assert r.status_code == 404

    def test_review_approve(self, seeded_resolver_client):
        """Approve a queue item via API."""
        import dossier.db.database as db_mod

        db_path = db_mod.DB_PATH
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        # Seed the queue directly
        conn.execute(
            "INSERT INTO resolution_queue (source_entity_id, target_entity_id, confidence, strategy) VALUES (1, 2, 0.70, 'initial_match')"
        )
        conn.commit()
        queue_id = conn.execute("SELECT id FROM resolution_queue").fetchone()["id"]
        conn.close()

        r = seeded_resolver_client.post(
            f"/api/resolver/queue/{queue_id}/review", params={"approve": True}
        )
        assert r.status_code == 200
        assert r.json()["approved"] is True


# ═══════════════════════════════════════════════════════════════════
# POST /api/resolver/merge
# ═══════════════════════════════════════════════════════════════════


class TestMerge:
    def test_merge_success(self, seeded_resolver_client):
        r = seeded_resolver_client.post(
            "/api/resolver/merge", params={"source_id": 1, "target_id": 2}
        )
        assert r.status_code == 200
        assert r.json()["merged"] is True

    def test_merge_nonexistent(self, client):
        r = client.post("/api/resolver/merge", params={"source_id": 999, "target_id": 888})
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════
# POST /api/resolver/split
# ═══════════════════════════════════════════════════════════════════


class TestSplit:
    def test_split_after_merge(self, seeded_resolver_client):
        # Merge first
        seeded_resolver_client.post("/api/resolver/merge", params={"source_id": 1, "target_id": 2})
        # Then split
        r = seeded_resolver_client.post(
            "/api/resolver/split", params={"source_id": 1, "target_id": 2}
        )
        assert r.status_code == 200
        assert r.json()["split"] is True

    def test_split_nonexistent(self, client):
        r = client.post("/api/resolver/split", params={"source_id": 999, "target_id": 888})
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════
# GET /api/resolver/aliases/{entity_id}
# ═══════════════════════════════════════════════════════════════════


class TestAliases:
    def test_empty(self, seeded_resolver_client):
        r = seeded_resolver_client.get("/api/resolver/aliases/1")
        assert r.status_code == 200
        assert r.json()["aliases"] == []

    def test_after_merge(self, seeded_resolver_client):
        # Merge first
        seeded_resolver_client.post("/api/resolver/merge", params={"source_id": 1, "target_id": 2})
        r = seeded_resolver_client.get("/api/resolver/aliases/2")
        assert r.status_code == 200
        aliases = r.json()["aliases"]
        assert len(aliases) >= 2
        assert "John Smith" in aliases
