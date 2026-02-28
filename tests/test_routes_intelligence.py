"""Tests for dossier.api.routes_intelligence — AI, duplicates, clusters, patterns, link-analysis."""

from unittest.mock import patch

from tests.conftest import upload_sample


class TestAISummarize:
    def test_summarize_success(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        with patch("dossier.api.utils._ollama_generate", return_value="Summary of document."):
            r = client.post("/api/ai/summarize", json={"doc_id": doc_id})
        assert r.status_code == 200
        assert r.json()["summary"] == "Summary of document."

    def test_summarize_no_doc_id(self, client):
        r = client.post("/api/ai/summarize", json={})
        assert r.status_code == 400

    def test_summarize_doc_not_found(self, client):
        with patch("dossier.api.utils._ollama_generate", return_value="x"):
            r = client.post("/api/ai/summarize", json={"doc_id": 999})
        assert r.status_code == 404


class TestAIAsk:
    def test_ask_success(self, client):
        upload_sample(client)
        with patch("dossier.api.utils._ollama_generate", return_value="The answer is 42."):
            r = client.post("/api/ai/ask", json={"question": "What is the case about?"})
        assert r.status_code == 200
        assert r.json()["answer"] == "The answer is 42."
        assert "sources" in r.json()

    def test_ask_no_question(self, client):
        r = client.post("/api/ai/ask", json={})
        assert r.status_code == 400


class TestDuplicates:
    def test_duplicates(self, client):
        upload_sample(client, filename="a.txt")
        upload_sample(
            client,
            filename="b.txt",
            content="Jeffrey Epstein and Goldman Sachs documents from Palm Beach FBI.",
        )
        r = client.get("/api/duplicates", params={"threshold": 0.1})
        assert r.status_code == 200
        assert "duplicates" in r.json()

    def test_dismiss_duplicate(self, client):
        r = client.post("/api/duplicates/dismiss", json={"id_a": 1, "id_b": 2})
        assert r.status_code == 200
        assert r.json()["dismissed"] is True

    def test_dismiss_missing_ids(self, client):
        r = client.post("/api/duplicates/dismiss", json={})
        assert r.status_code == 400


class TestClusters:
    def test_clusters(self, client):
        upload_sample(client)
        r = client.get("/api/clusters", params={"min_cluster_size": 2})
        assert r.status_code == 200
        assert "clusters" in r.json()


class TestPatterns:
    def test_patterns(self, client):
        upload_sample(client)
        r = client.get("/api/patterns")
        assert r.status_code == 200
        data = r.json()
        assert "co_appearances" in data
        assert "activity_bursts" in data
        assert "cross_category" in data


class TestLinkAnalysis:
    def test_link_analysis(self, client):
        upload_sample(client)
        r = client.get("/api/link-analysis", params={"min_connections": 1})
        assert r.status_code == 200
        data = r.json()
        assert "entities" in data
        assert "edge_count" in data


class TestCommunicationFlow:
    def test_communication_flow(self, client):
        upload_sample(client)
        r = client.get("/api/communication-flow")
        assert r.status_code == 200
        assert "flows" in r.json()


class TestFinancialTrail:
    def test_financial_trail(self, client):
        from tests.conftest import seed_forensics

        seed_forensics(client)
        r = client.get("/api/financial-trail")
        assert r.status_code == 200
        data = r.json()
        assert len(data["indicators"]) >= 1


class TestWitnessIndex:
    def test_witness_index(self, client):
        upload_sample(client)
        r = client.get("/api/witness-index")
        assert r.status_code == 200
        assert "witnesses" in r.json()


class TestDocumentGaps:
    def test_gaps(self, client):
        upload_sample(client)
        r = client.get("/api/document-gaps")
        assert r.status_code == 200
        data = r.json()
        assert "gaps" in data
        assert "undated_count" in data


class TestCorroboration:
    def test_corroboration(self, client):
        upload_sample(client)
        r = client.get("/api/corroboration")
        assert r.status_code == 200
        assert "corroborated_entities" in r.json()


class TestDepositions:
    def test_depositions(self, client):
        upload_sample(client)
        r = client.get("/api/depositions")
        assert r.status_code == 200
        assert "depositions" in r.json()


def _ensure_lazy_tables(client):
    """Call endpoints that create lazy tables so later endpoints don't fail."""
    client.get("/api/evidence-chains")  # creates evidence_chains + links
    client.get("/api/board")  # creates board_items
    client.get("/api/snapshots")  # creates investigation_snapshots


class TestNarrative:
    def test_narrative(self, client):
        upload_sample(client)
        _ensure_lazy_tables(client)
        r = client.get("/api/narrative")
        assert r.status_code == 200


class TestContactNetwork:
    def test_contact_network(self, client):
        upload_sample(client)
        r = client.get("/api/contact-network")
        assert r.status_code == 200
        data = r.json()
        assert "pairs" in data
        assert "total_correspondence" in data


class TestPhraseTrends:
    def test_phrase_trends(self, client):
        from tests.conftest import seed_forensics

        seed_forensics(client)
        r = client.get("/api/phrase-trends")
        assert r.status_code == 200
        data = r.json()
        assert "phrases" in data
        assert "years" in data


class TestEntityDisambiguation:
    def test_entity_disambiguation(self, client):
        upload_sample(client)
        r = client.get("/api/entity-disambiguation")
        assert r.status_code == 200
        data = r.json()
        assert "ambiguous_pairs" in data
        assert "short_entities" in data


class TestInfluenceScores:
    def test_influence_scores(self, client):
        upload_sample(client)
        r = client.get("/api/influence-scores")
        assert r.status_code == 200
        assert "entities" in r.json()


class TestEntityClusters:
    def test_entity_clusters(self, client):
        upload_sample(client)
        r = client.get("/api/entity-clusters")
        assert r.status_code == 200
        assert "clusters" in r.json()


class TestCoverNames:
    def test_cover_names(self, client):
        upload_sample(client)
        r = client.get("/api/cover-names")
        assert r.status_code == 200
        data = r.json()
        assert "known_aliases" in data
        assert "potential_aliases" in data


class TestFlightAnalysis:
    def test_flight_analysis(self, client):
        upload_sample(client)
        r = client.get("/api/flight-analysis")
        assert r.status_code == 200
        data = r.json()
        assert "flight_documents" in data
        assert "top_destinations" in data
