"""Tests for dossier.api.routes_search — dashboard, advanced search."""

from tests.conftest import upload_sample


class TestDashboard:
    def test_dashboard_empty(self, client):
        r = client.get("/api/dashboard")
        assert r.status_code == 200
        data = r.json()
        assert data["documents"] == 0
        assert data["entities"] == 0
        assert data["recent_documents"] == []

    def test_dashboard_with_data(self, client):
        upload_sample(client)
        r = client.get("/api/dashboard")
        assert r.status_code == 200
        data = r.json()
        assert data["documents"] >= 1
        assert len(data["recent_documents"]) >= 1
        assert "sources" in data
        assert "categories" in data


class TestAdvancedSearch:
    def test_no_filters(self, client):
        upload_sample(client)
        r = client.get("/api/search/advanced")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 1
        assert len(data["results"]) >= 1

    def test_category_filter(self, client):
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"category": "nonexistent"})
        assert r.status_code == 200
        assert r.json()["total"] == 0

    def test_fts_query(self, client):
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"q": "Epstein"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_flagged_only_filter(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(f"/api/documents/{doc_id}/flag")
        r = client.get("/api/search/advanced", params={"flagged_only": True})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_sort_by_date(self, client):
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"sort_by": "date"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_entity_name_filter(self, client):
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"entity_name": "Epstein"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_date_from_filter(self, client):
        """Cover date_from condition (lines 293-295)."""
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/search/advanced", params={"date_from": "2010-01-01"})
        assert r.status_code == 200
        # Should only get docs from 2015+
        assert r.json()["total"] >= 1

    def test_date_to_filter(self, client):
        """Cover date_to condition (lines 296-298)."""
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/search/advanced", params={"date_to": "2003-01-01"})
        assert r.status_code == 200

    def test_min_risk_filter(self, client):
        """Cover min_risk join + condition (lines 310-316)."""
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/search/advanced", params={"min_risk": 0.5})
        assert r.status_code == 200

    def test_sort_by_pages(self, client):
        """Cover sort_by=pages branch (line 322)."""
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"sort_by": "pages"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_sort_by_relevance_with_query(self, client):
        """Cover sort_by=relevance + query branch (line 323-324)."""
        upload_sample(client)
        r = client.get("/api/search/advanced", params={"q": "Epstein", "sort_by": "relevance"})
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_source_filter(self, client):
        """Cover source condition (lines 291-292)."""
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/search/advanced", params={"source": "FBI"})
        assert r.status_code == 200

    def test_dashboard_event_count(self, client):
        """Cover events table count (lines 231-233)."""
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/dashboard")
        assert r.status_code == 200
        data = r.json()
        assert "timeline_events" in data
