"""Tests for dossier.api.routes_entities — tags, aliases, profiles, timeline, merge, export."""

from tests.conftest import upload_sample


def _get_entity_id(client):
    """Upload sample and return the first entity ID."""
    upload_sample(client)
    r = client.get("/api/entities")
    entities = r.json()["entities"]
    assert len(entities) > 0
    return entities[0]["id"]


class TestEntitySearch:
    def test_search_empty(self, client):
        r = client.get("/api/entities/search", params={"q": "nonexistent"})
        assert r.status_code == 200
        assert r.json()["entities"] == []

    def test_search_match(self, client):
        upload_sample(client)
        r = client.get("/api/entities/search", params={"q": "Epstein"})
        assert r.status_code == 200
        assert len(r.json()["entities"]) >= 1


class TestEntityTags:
    def test_get_tags_empty(self, client):
        eid = _get_entity_id(client)
        r = client.get(f"/api/entities/{eid}/tags")
        assert r.status_code == 200
        assert r.json()["tags"] == []

    def test_add_tag(self, client):
        eid = _get_entity_id(client)
        r = client.post(f"/api/entities/{eid}/tags", json={"tag": "suspect"})
        assert r.status_code == 200
        assert r.json()["added"] is True

        r = client.get(f"/api/entities/{eid}/tags")
        assert "suspect" in r.json()["tags"]

    def test_add_tag_entity_not_found(self, client):
        r = client.post("/api/entities/999999/tags", json={"tag": "test"})
        assert r.status_code == 404

    def test_add_tag_empty(self, client):
        eid = _get_entity_id(client)
        r = client.post(f"/api/entities/{eid}/tags", json={"tag": ""})
        assert r.status_code == 400

    def test_remove_tag(self, client):
        eid = _get_entity_id(client)
        client.post(f"/api/entities/{eid}/tags", json={"tag": "remove_me"})
        r = client.delete(f"/api/entities/{eid}/tags/remove_me")
        assert r.status_code == 200
        assert r.json()["removed"] is True

    def test_entities_by_tag(self, client):
        eid = _get_entity_id(client)
        client.post(f"/api/entities/{eid}/tags", json={"tag": "vip"})
        r = client.get("/api/entities/by-tag", params={"tag": "vip"})
        assert r.status_code == 200
        assert len(r.json()["entities"]) >= 1

    def test_list_all_tags(self, client):
        eid = _get_entity_id(client)
        client.post(f"/api/entities/{eid}/tags", json={"tag": "tagged"})
        r = client.get("/api/tags")
        assert r.status_code == 200
        assert len(r.json()["tags"]) >= 1


class TestEntityAliases:
    def test_get_aliases_empty(self, client):
        eid = _get_entity_id(client)
        r = client.get(f"/api/entities/{eid}/aliases")
        assert r.status_code == 200
        assert r.json()["aliases"] == []

    def test_add_alias(self, client):
        eid = _get_entity_id(client)
        r = client.post(f"/api/entities/{eid}/aliases", json={"alias": "Jeff E."})
        assert r.status_code == 200
        assert r.json()["added"] is True

    def test_add_alias_empty(self, client):
        eid = _get_entity_id(client)
        r = client.post(f"/api/entities/{eid}/aliases", json={"alias": ""})
        assert r.status_code == 400

    def test_add_alias_entity_not_found(self, client):
        r = client.post("/api/entities/999999/aliases", json={"alias": "test"})
        assert r.status_code == 404

    def test_delete_alias(self, client):
        eid = _get_entity_id(client)
        client.post(f"/api/entities/{eid}/aliases", json={"alias": "Del Me"})
        aliases = client.get(f"/api/entities/{eid}/aliases").json()["aliases"]
        alias_id = aliases[0]["id"]
        r = client.delete(f"/api/aliases/{alias_id}")
        assert r.status_code == 200

    def test_resolve_alias(self, client):
        eid = _get_entity_id(client)
        client.post(f"/api/entities/{eid}/aliases", json={"alias": "JE"})
        r = client.get("/api/aliases/resolve", params={"name": "JE"})
        assert r.status_code == 200
        assert r.json()["resolved"] is True
        assert r.json()["via"] == "alias"

    def test_resolve_direct_name(self, client):
        upload_sample(client)
        r = client.get("/api/aliases/resolve", params={"name": "Jeffrey Epstein"})
        assert r.status_code == 200
        assert r.json()["resolved"] is True
        assert r.json()["via"] == "direct"

    def test_resolve_not_found(self, client):
        r = client.get("/api/aliases/resolve", params={"name": "Nobody McNothing"})
        assert r.status_code == 200
        assert r.json()["resolved"] is False


class TestEntityProfile:
    def test_profile_success(self, client):
        eid = _get_entity_id(client)
        r = client.get(f"/api/entities/{eid}/profile")
        assert r.status_code == 200
        data = r.json()
        assert "entity" in data
        assert "documents" in data
        assert "risk_exposure" in data

    def test_profile_404(self, client):
        r = client.get("/api/entities/999999/profile")
        assert r.status_code == 404


class TestEntityTimeline:
    def test_timeline_success(self, client):
        eid = _get_entity_id(client)
        r = client.get(f"/api/entities/{eid}/timeline")
        assert r.status_code == 200
        data = r.json()
        assert "entity" in data
        assert "events" in data
        assert "documents" in data

    def test_timeline_404(self, client):
        r = client.get("/api/entities/999999/timeline")
        assert r.status_code == 404


class TestEntityMerge:
    def test_merge_preview(self, client):
        upload_sample(client)
        entities = client.get("/api/entities").json()["entities"]
        if len(entities) >= 2:
            r = client.get(
                "/api/entities/merge-preview",
                params={"source_id": entities[0]["id"], "target_id": entities[1]["id"]},
            )
            assert r.status_code == 200
            assert "source" in r.json()
            assert "target" in r.json()

    def test_merge_preview_404(self, client):
        r = client.get("/api/entities/merge-preview", params={"source_id": 999, "target_id": 998})
        assert r.status_code == 404

    def test_merge_entities(self, client):
        upload_sample(client)
        entities = client.get("/api/entities").json()["entities"]
        if len(entities) >= 2:
            src, tgt = entities[-1]["id"], entities[-2]["id"]
            r = client.post("/api/entities/merge", json={"source_id": src, "target_id": tgt})
            assert r.status_code == 200
            assert r.json()["merged"] is True

    def test_merge_same_id(self, client):
        r = client.post("/api/entities/merge", json={"source_id": 1, "target_id": 1})
        assert r.status_code == 400

    def test_merge_not_found(self, client):
        r = client.post("/api/entities/merge", json={"source_id": 999, "target_id": 998})
        assert r.status_code == 404


class TestDossierExport:
    def test_export_html(self, client):
        eid = _get_entity_id(client)
        # Ensure tags table exists by adding a tag first
        client.post(f"/api/entities/{eid}/tags", json={"tag": "export_test"})
        r = client.get(f"/api/entities/{eid}/dossier-export")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]

    def test_export_404(self, client):
        r = client.get("/api/entities/999999/dossier-export")
        assert r.status_code == 404
