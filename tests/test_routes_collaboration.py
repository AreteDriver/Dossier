"""Tests for dossier.api.routes_collaboration — annotations, audit, watchlist, alerts, notes."""

from tests.conftest import upload_sample


class TestAnnotations:
    def test_get_annotations_empty(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/annotations")
        assert r.status_code == 200
        assert r.json()["annotations"] == []

    def test_add_annotation(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/annotations",
            json={"start_offset": 0, "end_offset": 10, "text": "sample", "note": "key passage"},
        )
        assert r.status_code == 200
        assert r.json()["added"] is True

    def test_add_annotation_missing_offsets(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(f"/api/documents/{doc_id}/annotations", json={"text": "no offsets"})
        assert r.status_code == 400

    def test_add_annotation_doc_not_found(self, client):
        r = client.post(
            "/api/documents/999/annotations",
            json={"start_offset": 0, "end_offset": 5, "text": "x"},
        )
        assert r.status_code == 404

    def test_delete_annotation(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/annotations",
            json={"start_offset": 0, "end_offset": 5, "text": "del"},
        )
        ann_id = r.json()["id"]
        r = client.delete(f"/api/annotations/{ann_id}")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    def test_search_annotations(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/annotations",
            json={"start_offset": 0, "end_offset": 10, "text": "searchable", "note": "findme"},
        )
        r = client.get("/api/annotations/search", params={"q": "findme"})
        assert r.status_code == 200
        assert len(r.json()["annotations"]) >= 1


class TestAuditLog:
    def test_get_audit_empty(self, client):
        r = client.get("/api/audit")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    def test_add_audit_entry(self, client):
        r = client.post("/api/audit", json={"action": "manual_review", "details": "test"})
        assert r.status_code == 200
        assert r.json()["logged"] is True
        r = client.get("/api/audit")
        assert r.json()["total"] >= 1

    def test_add_audit_no_action(self, client):
        r = client.post("/api/audit", json={})
        assert r.status_code == 400

    def test_audit_filter_by_action(self, client):
        client.post("/api/audit", json={"action": "test_action"})
        r = client.get("/api/audit", params={"action": "test_action"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1


class TestWatchlist:
    def test_get_watchlist_empty(self, client):
        r = client.get("/api/watchlist")
        assert r.status_code == 200
        assert r.json()["watchlist"] == []

    def test_add_to_watchlist(self, client):
        upload_sample(client)
        entities = client.get("/api/entities").json()["entities"]
        eid = entities[0]["id"]
        r = client.post("/api/watchlist", json={"entity_id": eid, "notes": "monitoring"})
        assert r.status_code == 200
        assert r.json()["added"] is True

        r = client.get("/api/watchlist")
        assert len(r.json()["watchlist"]) >= 1

    def test_add_watchlist_entity_not_found(self, client):
        r = client.post("/api/watchlist", json={"entity_id": 999999})
        assert r.status_code == 404

    def test_add_watchlist_no_entity_id(self, client):
        r = client.post("/api/watchlist", json={})
        assert r.status_code == 400

    def test_remove_from_watchlist(self, client):
        upload_sample(client)
        entities = client.get("/api/entities").json()["entities"]
        eid = entities[0]["id"]
        client.post("/api/watchlist", json={"entity_id": eid})
        r = client.delete(f"/api/watchlist/{eid}")
        assert r.status_code == 200
        assert r.json()["removed"] is True


class TestSavedQueries:
    def test_saved_queries_crud(self, client):
        r = client.get("/api/saved-queries")
        assert r.status_code == 200
        assert r.json()["queries"] == []

        r = client.post("/api/saved-queries", json={"name": "My Query", "query_text": "Epstein"})
        assert r.status_code == 200
        query_id = r.json()["id"]

        r = client.get("/api/saved-queries")
        assert len(r.json()["queries"]) >= 1

        r = client.delete(f"/api/saved-queries/{query_id}")
        assert r.status_code == 200

    def test_saved_query_no_name(self, client):
        r = client.post("/api/saved-queries", json={"query_text": "x"})
        assert r.status_code == 400


class TestKeywordAlerts:
    def test_keyword_alerts_crud(self, client):
        upload_sample(client)
        r = client.post("/api/keyword-alerts", json={"keyword": "Epstein"})
        assert r.status_code == 200
        assert r.json()["created"] is True

        r = client.get("/api/keyword-alerts")
        assert r.status_code == 200
        alerts = r.json()["alerts"]
        assert len(alerts) >= 1
        assert alerts[0]["match_count"] >= 0

        alert_id = alerts[0]["id"]
        r = client.delete(f"/api/keyword-alerts/{alert_id}")
        assert r.status_code == 200

    def test_keyword_alert_no_keyword(self, client):
        r = client.post("/api/keyword-alerts", json={})
        assert r.status_code == 400


class TestAnalystNotes:
    def test_get_notes_empty(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/analyst-notes")
        assert r.status_code == 200
        assert r.json()["notes"] == []

    def test_add_note(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/analyst-notes",
            json={"note": "Suspicious activity pattern"},
        )
        assert r.status_code == 200
        assert r.json()["note"] == "Suspicious activity pattern"

    def test_add_note_doc_not_found(self, client):
        r = client.post("/api/documents/999/analyst-notes", json={"note": "x"})
        assert r.status_code == 404

    def test_add_note_empty(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(f"/api/documents/{doc_id}/analyst-notes", json={"note": ""})
        assert r.status_code == 400

    def test_delete_note(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(f"/api/documents/{doc_id}/analyst-notes", json={"note": "delete me"})
        note_id = r.json()["id"]
        r = client.delete(f"/api/notes/{note_id}")
        assert r.status_code == 200


class TestSearchHistory:
    def test_history_empty(self, client):
        r = client.get("/api/search-history")
        assert r.status_code == 200
        assert r.json()["history"] == []

    def test_record_and_get(self, client):
        r = client.post("/api/search-history", json={"query": "Epstein", "result_count": 5})
        assert r.status_code == 200
        assert r.json()["status"] == "recorded"

        r = client.get("/api/search-history")
        assert len(r.json()["history"]) >= 1

    def test_record_short_query_skipped(self, client):
        r = client.post("/api/search-history", json={"query": "x"})
        assert r.json()["status"] == "skipped"

    def test_clear_history(self, client):
        client.post("/api/search-history", json={"query": "test", "result_count": 1})
        r = client.delete("/api/search-history")
        assert r.status_code == 200
        assert r.json()["status"] == "cleared"


class TestTagAnalytics:
    def test_tag_analytics_empty(self, client):
        r = client.get("/api/tags/analytics")
        assert r.status_code == 200
        assert r.json()["tags"] == []

    def test_tag_analytics_with_data(self, client):
        upload_sample(client)
        entities = client.get("/api/entities").json()["entities"]
        eid = entities[0]["id"]
        client.post(f"/api/entities/{eid}/tags", json={"tag": "analytics_test"})
        r = client.get("/api/tags/analytics")
        assert r.status_code == 200
        assert len(r.json()["tags"]) >= 1


class TestBulkTag:
    def test_bulk_tag_entities(self, client):
        upload_sample(client)
        r = client.post("/api/tags/bulk", json={"tag": "bulk_tagged", "entity_type": "person"})
        assert r.status_code == 200
        assert r.json()["tagged_count"] >= 0

    def test_bulk_tag_no_tag(self, client):
        r = client.post("/api/tags/bulk", json={})
        assert r.status_code == 400

    def test_bulk_tag_documents(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/bulk-tag", json={"doc_ids": [doc_id], "category": "evidence"})
        assert r.status_code == 200
        assert r.json()["updated"] >= 1

    def test_bulk_tag_documents_no_ids(self, client):
        r = client.post("/api/bulk-tag", json={"doc_ids": [], "tag": "x"})
        assert r.status_code == 400

    def test_bulk_tag_documents_no_tag_or_category(self, client):
        r = client.post("/api/bulk-tag", json={"doc_ids": [1]})
        assert r.status_code == 400


class TestBulkTagSuggestions:
    def test_suggestions(self, client):
        upload_sample(client)
        r = client.get("/api/bulk-tag-suggestions")
        assert r.status_code == 200
        assert "categories" in r.json()
        assert "tags" in r.json()


class TestSourceCredibility:
    def test_source_credibility(self, client):
        upload_sample(client)
        r = client.get("/api/source-credibility")
        assert r.status_code == 200
        assert len(r.json()["sources"]) >= 1

    def test_rate_source(self, client):
        upload_sample(client)
        r = client.post(
            "/api/source-credibility/Test Upload/rate",
            json={"rating": "A", "notes": "Highly reliable"},
        )
        assert r.status_code == 200
        assert r.json()["rating"] == "A"

    def test_rate_source_invalid(self, client):
        r = client.post("/api/source-credibility/x/rate", json={"rating": "X"})
        assert r.status_code == 400
