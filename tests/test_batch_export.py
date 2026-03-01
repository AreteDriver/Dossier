"""Tests for batch export endpoints (CSV/JSON)."""

import pytest

from tests.conftest import seed_analytics_data, upload_sample


@pytest.fixture
def analytics_client(client):
    doc_ids = seed_analytics_data(client)
    return client, doc_ids


class TestCaseFileExportCSV:
    def test_export_csv(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/case-files", json={"name": "CSV Export Test"})
        case_id = r.json()["id"]
        client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "document", "item_id": doc_id, "note": "Key evidence"},
        )
        r = client.get(f"/api/case-files/{case_id}/export/csv")
        assert r.status_code == 200
        data = r.json()
        assert data["count"] == 1
        assert "case_file" in data
        assert "item_type,item_id" in data["csv"]
        assert "Key evidence" in data["csv"]

    def test_export_csv_entity_item(self, analytics_client):
        client, _ = analytics_client
        from dossier.db.database import get_db

        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities LIMIT 1").fetchone()
        if not ent:
            pytest.skip("No entities")
        r = client.post("/api/case-files", json={"name": "Entity CSV"})
        case_id = r.json()["id"]
        client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "entity", "item_id": ent["id"]},
        )
        r = client.get(f"/api/case-files/{case_id}/export/csv")
        assert r.status_code == 200
        assert r.json()["count"] == 1

    def test_export_csv_chain_item(self, client):
        upload_sample(client)
        r = client.post("/api/evidence-chains", json={"name": "Test Chain"})
        chain_id = r.json()["id"]
        r = client.post("/api/case-files", json={"name": "Chain CSV"})
        case_id = r.json()["id"]
        client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "chain", "item_id": chain_id},
        )
        r = client.get(f"/api/case-files/{case_id}/export/csv")
        assert r.status_code == 200
        assert r.json()["count"] == 1

    def test_export_csv_empty(self, client):
        r = client.post("/api/case-files", json={"name": "Empty CSV"})
        case_id = r.json()["id"]
        r = client.get(f"/api/case-files/{case_id}/export/csv")
        assert r.status_code == 200
        assert r.json()["count"] == 0
        assert r.json()["csv"] == ""

    def test_export_csv_not_found(self, client):
        r = client.get("/api/case-files/99999/export/csv")
        assert r.status_code == 404


class TestExportEntityGraph:
    def test_graph_json(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/entity-graph")
        assert r.status_code == 200
        data = r.json()
        assert "nodes" in data
        assert "edges" in data
        assert "node_count" in data
        assert "edge_count" in data
        assert data["node_count"] == len(data["nodes"])
        assert data["edge_count"] == len(data["edges"])

    def test_graph_type_filter(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/entity-graph", params={"type": "person"})
        assert r.status_code == 200
        data = r.json()
        for node in data["nodes"]:
            assert node["type"] == "person"

    def test_graph_min_weight(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/entity-graph", params={"min_weight": 5})
        assert r.status_code == 200
        data = r.json()
        for edge in data["edges"]:
            assert edge["weight"] >= 5

    def test_graph_node_structure(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/entity-graph")
        data = r.json()
        if data["nodes"]:
            node = data["nodes"][0]
            assert "id" in node
            assert "label" in node
            assert "type" in node
            assert "mentions" in node
            assert "doc_count" in node

    def test_graph_edge_structure(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/entity-graph")
        data = r.json()
        if data["edges"]:
            edge = data["edges"][0]
            assert "source" in edge
            assert "target" in edge
            assert "weight" in edge


class TestExportDocuments:
    def test_documents_json(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/documents")
        assert r.status_code == 200
        data = r.json()
        assert "documents" in data
        assert "count" in data
        assert data["count"] > 0

    def test_documents_csv(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/documents", params={"format": "csv"})
        assert r.status_code == 200
        data = r.json()
        assert "csv" in data
        assert "count" in data
        assert "filename" in data["csv"]

    def test_documents_category_filter(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/documents", params={"category": "deposition"})
        assert r.status_code == 200
        data = r.json()
        for doc in data["documents"]:
            assert doc["category"] == "deposition"

    def test_documents_limit(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/documents", params={"limit": 1})
        assert r.status_code == 200
        assert r.json()["count"] <= 1

    def test_documents_structure(self, analytics_client):
        client, _ = analytics_client
        r = client.get("/api/export/documents")
        data = r.json()
        if data["documents"]:
            doc = data["documents"][0]
            assert "id" in doc
            assert "filename" in doc
            assert "category" in doc
            assert "entity_count" in doc
