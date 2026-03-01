"""Tests for dossier.api.routes_intelligence — AI, duplicates, clusters, patterns, link-analysis."""

import pytest
from unittest.mock import patch

from tests.conftest import upload_sample, seed_multi_doc_data


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


# ═══════════════════════════════════════════
# INNER-LOOP COVERAGE WITH MULTI-DOC DATA
# ═══════════════════════════════════════════


@pytest.fixture
def multi_client(client):
    """Client with 4 seeded docs across categories."""
    doc_ids = seed_multi_doc_data(client)
    return client, doc_ids


class TestClustersWithData:
    def test_clusters_with_shared_keywords(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/clusters", params={"min_cluster_size": 2})
        assert r.status_code == 200
        data = r.json()
        assert "clusters" in data
        # With 4 docs sharing entities, should get at least 1 cluster
        if data["clusters"]:
            cluster = data["clusters"][0]
            assert "keyword" in cluster
            assert "size" in cluster
            assert "documents" in cluster
            assert "shared_entities" in cluster

    def test_clusters_high_min_size_empty(self, multi_client):
        client, _ = multi_client
        # max min_cluster_size is 20 per Query(ge=2, le=20)
        r = client.get("/api/clusters", params={"min_cluster_size": 20})
        assert r.status_code == 200
        assert r.json()["clusters"] == []

    def test_clusters_limit_respected(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/clusters", params={"min_cluster_size": 2, "limit": 1})
        assert r.status_code == 200
        assert len(r.json()["clusters"]) <= 1


class TestCommunicationFlowWithData:
    def test_communication_flow_with_entities(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/communication-flow")
        assert r.status_code == 200
        data = r.json()
        assert "flows" in data
        assert "top_communicators" in data
        assert "total_flows" in data

    def test_communication_flow_with_entity_filter(self, multi_client):
        client, _ = multi_client
        from dossier.db.database import get_db
        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities WHERE type = 'person' LIMIT 1").fetchone()
        if ent:
            r = client.get("/api/communication-flow", params={"entity_id": ent["id"]})
            assert r.status_code == 200
            assert "flows" in r.json()

    def test_communication_flow_empty(self, client):
        r = client.get("/api/communication-flow")
        assert r.status_code == 200
        assert r.json()["flows"] == []


class TestWitnessIndexWithData:
    def test_witness_index_with_co_deponents(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/witness-index")
        assert r.status_code == 200
        data = r.json()
        assert "witnesses" in data
        # With deposition docs, should have witnesses
        if data["witnesses"]:
            w = data["witnesses"][0]
            assert "co_deponents" in w
            assert "doc_ids" in w

    def test_witness_index_limit(self, multi_client):
        client, _ = multi_client
        # limit has ge=10, le=200
        r = client.get("/api/witness-index", params={"limit": 10})
        assert r.status_code == 200
        assert len(r.json()["witnesses"]) <= 10


class TestDocumentGapsWithData:
    def test_gaps_detected(self, multi_client):
        client, _ = multi_client
        # Docs dated 2002, 2015, 2016, 2018 → gaps of years
        r = client.get("/api/document-gaps", params={"min_gap_days": 30})
        assert r.status_code == 200
        data = r.json()
        assert "gaps" in data
        assert "coverage" in data
        assert "total_dated" in data
        if data["gaps"]:
            gap = data["gaps"][0]
            assert "gap_days" in gap
            assert "start_date" in gap
            assert "end_date" in gap
            assert "before_doc" in gap

    def test_gaps_year_coverage(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/document-gaps")
        data = r.json()
        assert isinstance(data["coverage"], dict)
        # Should have years from our seeded dates
        if data["coverage"]:
            assert all(len(yr) == 4 for yr in data["coverage"].keys())

    def test_gaps_large_min_days(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/document-gaps", params={"min_gap_days": 99999})
        assert r.status_code == 200
        assert r.json()["gaps"] == []

    def test_gaps_few_docs_early_return(self, client):
        r = client.get("/api/document-gaps")
        assert r.status_code == 200
        assert r.json()["gaps"] == []


class TestDepositionsWithData:
    def test_depositions_people_orgs(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/depositions")
        assert r.status_code == 200
        data = r.json()
        assert "depositions" in data
        assert "deponents" in data
        if data["depositions"]:
            depo = data["depositions"][0]
            assert "people" in depo
            assert "orgs" in depo
            assert "doc_id" in depo

    def test_deponent_summary(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/depositions")
        data = r.json()
        if data["deponents"]:
            dep = data["deponents"][0]
            assert "name" in dep
            assert "deposition_count" in dep


class TestContactNetworkWithData:
    def test_contact_network_pairs(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/contact-network")
        assert r.status_code == 200
        data = r.json()
        assert "pairs" in data
        assert "total_correspondence" in data
        if data["pairs"]:
            pair = data["pairs"][0]
            assert "person_a" in pair
            assert "person_b" in pair
            assert "frequency" in pair

    def test_contact_network_empty(self, client):
        r = client.get("/api/contact-network")
        assert r.status_code == 200
        assert r.json()["pairs"] == []


class TestEntityDisambiguationWithData:
    def test_disambiguation_results(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/entity-disambiguation", params={"min_docs": 1})
        assert r.status_code == 200
        data = r.json()
        assert "ambiguous_pairs" in data
        assert "short_entities" in data
        assert "already_resolved" in data

    def test_disambiguation_high_threshold(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/entity-disambiguation", params={"min_docs": 100})
        assert r.status_code == 200
        assert r.json()["ambiguous_pairs"] == []


class TestCoverNamesWithData:
    def test_cover_names_with_data(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/cover-names")
        assert r.status_code == 200
        data = r.json()
        assert "potential_aliases" in data
        assert "known_aliases" in data
        assert "single_name_entities" in data


class TestFlightAnalysisWithData:
    def test_flight_analysis_with_data(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/flight-analysis")
        assert r.status_code == 200
        data = r.json()
        assert "flight_documents" in data
        if data["flight_documents"]:
            doc = data["flight_documents"][0]
            assert "people" in doc
            assert "places" in doc

    def test_flight_top_destinations(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/flight-analysis")
        data = r.json()
        assert "top_destinations" in data
        assert "top_passengers" in data


class TestLinkAnalysisWithData:
    def test_link_analysis_with_connections(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/link-analysis", params={"min_connections": 1})
        assert r.status_code == 200
        data = r.json()
        assert "entities" in data
        assert "edge_count" in data

    def test_link_analysis_empty(self, client):
        r = client.get("/api/link-analysis", params={"min_connections": 1})
        assert r.status_code == 200


class TestInfluenceScoresWithData:
    def test_influence_scores_computed(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/influence-scores")
        assert r.status_code == 200
        data = r.json()
        assert "entities" in data
        if data["entities"]:
            ent = data["entities"][0]
            assert "influence_score" in ent
            assert "connection_weight" in ent
            assert "event_count" in ent


class TestEntityClustersWithData:
    def test_entity_clusters_results(self, multi_client):
        client, _ = multi_client
        r = client.get("/api/entity-clusters", params={"min_shared": 2})
        assert r.status_code == 200
        data = r.json()
        assert "clusters" in data
        if data["clusters"]:
            pair = data["clusters"][0]
            assert "doc_a" in pair
            assert "doc_b" in pair
            assert "shared_count" in pair
            assert "shared_entities" in pair


class TestNarrativeWithData:
    def test_narrative_with_entity_id(self, multi_client):
        client, _ = multi_client
        from dossier.db.database import get_db
        with get_db() as conn:
            ent = conn.execute("SELECT id FROM entities LIMIT 1").fetchone()
        _ensure_lazy_tables(client)
        if ent:
            r = client.get("/api/narrative", params={"entity_id": ent["id"]})
            assert r.status_code == 200
            data = r.json()
            assert "key_people" in data
            assert "timeline_events" in data
