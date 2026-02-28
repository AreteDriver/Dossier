"""Tests for dossier.api.routes_documents — text, notes, similar, tone, provenance."""

from tests.conftest import upload_sample


class TestSources:
    def test_sources_empty(self, client):
        r = client.get("/api/sources")
        assert r.status_code == 200
        assert r.json()["sources"] == []

    def test_sources_with_data(self, client):
        upload_sample(client)
        r = client.get("/api/sources")
        assert r.status_code == 200
        assert len(r.json()["sources"]) >= 1


class TestDocumentText:
    def test_get_text_success(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/text")
        assert r.status_code == 200
        data = r.json()
        assert "text" in data
        assert data["char_count"] > 0

    def test_get_text_404(self, client):
        r = client.get("/api/documents/999/text")
        assert r.status_code == 404


class TestDocumentNotes:
    def test_get_notes(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/notes")
        assert r.status_code == 200
        assert r.json()["notes"] == ""

    def test_save_notes(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/notes",
            json={"notes": "Important document"},
        )
        assert r.status_code == 200
        assert r.json()["saved"] is True

        r = client.get(f"/api/documents/{doc_id}/notes")
        assert r.json()["notes"] == "Important document"

    def test_notes_404(self, client):
        r = client.get("/api/documents/999/notes")
        assert r.status_code == 404

    def test_save_notes_404(self, client):
        r = client.post("/api/documents/999/notes", json={"notes": "x"})
        assert r.status_code == 404


class TestDocumentSimilar:
    def test_similar_success(self, client):
        upload_sample(client, filename="a.txt")
        upload_sample(
            client,
            filename="b.txt",
            content="Jeffrey Epstein case files from Palm Beach FBI office.",
        )
        r = client.get("/api/documents/1/similar")
        assert r.status_code == 200
        assert "similar" in r.json()

    def test_similar_404(self, client):
        r = client.get("/api/documents/999/similar")
        assert r.status_code == 404


class TestToneAnalysis:
    def test_tone_success(self, client):
        r = upload_sample(
            client,
            content="The defendant is guilty of criminal activity. Urgent prosecution needed immediately.",
        )
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/tone")
        assert r.status_code == 200
        data = r.json()
        assert "analysis" in data
        assert "overall_score" in data
        assert "legal_exposure" in data["analysis"]

    def test_tone_404(self, client):
        r = client.get("/api/documents/999/tone")
        assert r.status_code == 404


class TestProvenance:
    def test_get_provenance(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/provenance")
        assert r.status_code == 200
        assert r.json()["provenance_events"] == []

    def test_add_provenance(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/provenance",
            json={"event_type": "acquisition", "description": "FOIA request", "actor": "FBI"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "added"

        r = client.get(f"/api/documents/{doc_id}/provenance")
        assert len(r.json()["provenance_events"]) == 1

    def test_add_provenance_no_type(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(f"/api/documents/{doc_id}/provenance", json={})
        assert r.status_code == 400

    def test_add_provenance_404(self, client):
        r = client.post("/api/documents/999/provenance", json={"event_type": "x"})
        assert r.status_code == 404

    def test_provenance_summary(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/provenance",
            json={"event_type": "acquisition"},
        )
        r = client.get("/api/provenance-summary")
        assert r.status_code == 200
        data = r.json()
        assert data["tracked_documents"] >= 1


class TestCrossReferences:
    def test_cross_refs_with_text(self, client):
        upload_sample(client, filename="a.txt")
        upload_sample(
            client,
            filename="b.txt",
            content="Jeffrey Epstein and Goldman Sachs documents from Palm Beach.",
        )
        r = client.get(
            "/api/documents/1/cross-references", params={"text": "Jeffrey Epstein Palm Beach"}
        )
        assert r.status_code == 200
        assert "cross_references" in r.json()

    def test_cross_refs_no_text(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/cross-references")
        assert r.status_code == 200
        assert r.json()["cross_references"] == []

    def test_cross_refs_404(self, client):
        r = client.get("/api/documents/999/cross-references", params={"text": "test"})
        assert r.status_code == 404


class TestCompareDocuments:
    def test_compare_success(self, client):
        upload_sample(client, filename="a.txt")
        upload_sample(
            client,
            filename="b.txt",
            content="Jeffrey Epstein documents from Palm Beach FBI investigation.",
        )
        r = client.get("/api/compare-documents", params={"doc_a": 1, "doc_b": 2})
        assert r.status_code == 200
        data = r.json()
        assert "shared_entities" in data
        assert "stats" in data

    def test_compare_404(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get("/api/compare-documents", params={"doc_a": doc_id, "doc_b": 999})
        assert r.status_code == 404
