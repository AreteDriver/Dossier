"""Tests for dossier.api.routes_investigation — board, evidence chains, snapshots, case files."""

from tests.conftest import upload_sample


class TestBoard:
    def test_get_board_empty(self, client):
        r = client.get("/api/board")
        assert r.status_code == 200
        assert r.json()["items"] == []

    def test_add_board_item(self, client):
        r = client.post("/api/board", json={"title": "Key Finding", "content": "Suspicious"})
        assert r.status_code == 200
        assert r.json()["id"] >= 1

    def test_add_board_item_no_title(self, client):
        r = client.post("/api/board", json={"content": "no title"})
        assert r.status_code == 400

    def test_update_board_item(self, client):
        r = client.post("/api/board", json={"title": "Update Me"})
        item_id = r.json()["id"]
        r = client.put(f"/api/board/{item_id}", json={"title": "Updated", "x": 100})
        assert r.status_code == 200
        assert r.json()["updated"] is True

    def test_update_board_item_404(self, client):
        r = client.put("/api/board/999", json={"title": "x"})
        assert r.status_code == 404

    def test_delete_board_item(self, client):
        r = client.post("/api/board", json={"title": "Delete Me"})
        item_id = r.json()["id"]
        r = client.delete(f"/api/board/{item_id}")
        assert r.status_code == 200
        assert r.json()["deleted"] is True


class TestEvidenceChains:
    def test_list_empty(self, client):
        r = client.get("/api/evidence-chains")
        assert r.status_code == 200
        assert r.json()["chains"] == []

    def test_create_chain(self, client):
        r = client.post(
            "/api/evidence-chains",
            json={"name": "Money Trail", "description": "Follow the money"},
        )
        assert r.status_code == 200
        assert r.json()["name"] == "Money Trail"

    def test_create_chain_no_name(self, client):
        r = client.post("/api/evidence-chains", json={})
        assert r.status_code == 400

    def test_get_chain(self, client):
        r = client.post("/api/evidence-chains", json={"name": "Chain A"})
        chain_id = r.json()["id"]
        r = client.get(f"/api/evidence-chains/{chain_id}")
        assert r.status_code == 200
        assert r.json()["chain"]["name"] == "Chain A"

    def test_get_chain_404(self, client):
        r = client.get("/api/evidence-chains/999")
        assert r.status_code == 404

    def test_add_link(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/evidence-chains", json={"name": "Links"})
        chain_id = r.json()["id"]
        r = client.post(
            f"/api/evidence-chains/{chain_id}/links",
            json={"target_id": doc_id, "link_type": "document", "narrative": "Key evidence"},
        )
        assert r.status_code == 200
        assert r.json()["added"] is True

    def test_add_link_no_target(self, client):
        r = client.post("/api/evidence-chains", json={"name": "No Link"})
        chain_id = r.json()["id"]
        r = client.post(f"/api/evidence-chains/{chain_id}/links", json={"link_type": "document"})
        assert r.status_code == 400

    def test_delete_chain_link(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/evidence-chains", json={"name": "Del Link"})
        chain_id = r.json()["id"]
        client.post(
            f"/api/evidence-chains/{chain_id}/links",
            json={"target_id": doc_id, "link_type": "document"},
        )
        links = client.get(f"/api/evidence-chains/{chain_id}").json()["links"]
        r = client.delete(f"/api/evidence-chain-links/{links[0]['id']}")
        assert r.status_code == 200

    def test_delete_chain(self, client):
        r = client.post("/api/evidence-chains", json={"name": "Del Chain"})
        chain_id = r.json()["id"]
        r = client.delete(f"/api/evidence-chains/{chain_id}")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    def test_export_chain(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/evidence-chains", json={"name": "Export Test"})
        chain_id = r.json()["id"]
        client.post(
            f"/api/evidence-chains/{chain_id}/links",
            json={"target_id": doc_id, "link_type": "document", "narrative": "Primary evidence"},
        )
        r = client.get(f"/api/evidence-chains/{chain_id}/export")
        assert r.status_code == 200
        assert "html" in r.json()

    def test_export_chain_404(self, client):
        r = client.get("/api/evidence-chains/999/export")
        assert r.status_code == 404


class TestSnapshots:
    def test_list_empty(self, client):
        r = client.get("/api/snapshots")
        assert r.status_code == 200
        assert r.json()["snapshots"] == []

    def test_create_snapshot(self, client):
        r = client.post("/api/snapshots", json={"name": "Checkpoint 1"})
        assert r.status_code == 200
        assert r.json()["name"] == "Checkpoint 1"

    def test_create_snapshot_no_name(self, client):
        r = client.post("/api/snapshots", json={})
        assert r.status_code == 400

    def test_get_snapshot(self, client):
        r = client.post("/api/snapshots", json={"name": "Get Test"})
        snap_id = r.json()["id"]
        r = client.get(f"/api/snapshots/{snap_id}")
        assert r.status_code == 200
        assert "snapshot_data" in r.json()

    def test_get_snapshot_404(self, client):
        r = client.get("/api/snapshots/999")
        assert r.status_code == 404

    def test_delete_snapshot(self, client):
        r = client.post("/api/snapshots", json={"name": "Del"})
        snap_id = r.json()["id"]
        r = client.delete(f"/api/snapshots/{snap_id}")
        assert r.status_code == 200


class TestCaseFiles:
    def test_list_empty(self, client):
        r = client.get("/api/case-files")
        assert r.status_code == 200
        assert r.json()["case_files"] == []

    def test_create_case_file(self, client):
        r = client.post("/api/case-files", json={"name": "Epstein Investigation"})
        assert r.status_code == 200
        assert r.json()["name"] == "Epstein Investigation"

    def test_create_case_file_no_name(self, client):
        r = client.post("/api/case-files", json={})
        assert r.status_code == 400

    def test_get_case_file(self, client):
        r = client.post("/api/case-files", json={"name": "Get Case"})
        case_id = r.json()["id"]
        r = client.get(f"/api/case-files/{case_id}")
        assert r.status_code == 200
        assert r.json()["case_file"]["name"] == "Get Case"

    def test_get_case_file_404(self, client):
        r = client.get("/api/case-files/999")
        assert r.status_code == 404

    def test_add_case_file_items(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/case-files", json={"name": "Items Case"})
        case_id = r.json()["id"]

        # Add document item
        r = client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "document", "item_id": doc_id, "note": "Key doc"},
        )
        assert r.status_code == 200
        assert r.json()["added"] is True

        # Add entity item
        entities = client.get("/api/entities").json()["entities"]
        if entities:
            r = client.post(
                f"/api/case-files/{case_id}/items",
                json={"item_type": "entity", "item_id": entities[0]["id"]},
            )
            assert r.status_code == 200

    def test_add_item_invalid_type(self, client):
        r = client.post("/api/case-files", json={"name": "Bad Type"})
        case_id = r.json()["id"]
        r = client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "invalid", "item_id": 1},
        )
        assert r.status_code == 400

    def test_add_item_case_not_found(self, client):
        r = client.post("/api/case-files/999/items", json={"item_type": "document", "item_id": 1})
        assert r.status_code == 404

    def test_remove_case_file_item(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/case-files", json={"name": "Remove Item"})
        case_id = r.json()["id"]
        client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "document", "item_id": doc_id},
        )
        items = client.get(f"/api/case-files/{case_id}").json()["items"]
        r = client.delete(f"/api/case-file-items/{items[0]['id']}")
        assert r.status_code == 200

    def test_delete_case_file(self, client):
        r = client.post("/api/case-files", json={"name": "Delete Case"})
        case_id = r.json()["id"]
        r = client.delete(f"/api/case-files/{case_id}")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    def test_export_case_file(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post("/api/case-files", json={"name": "Export Case"})
        case_id = r.json()["id"]
        client.post(
            f"/api/case-files/{case_id}/items",
            json={"item_type": "document", "item_id": doc_id, "note": "Evidence"},
        )
        r = client.get(f"/api/case-files/{case_id}/export")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]

    def test_export_case_file_404(self, client):
        r = client.get("/api/case-files/999/export")
        assert r.status_code == 404


class TestInvestigationStats:
    def test_stats(self, client):
        upload_sample(client)
        r = client.get("/api/investigation-stats")
        assert r.status_code == 200
        data = r.json()
        assert "core" in data
        assert data["core"]["documents"] >= 1
        assert "analysis" in data
