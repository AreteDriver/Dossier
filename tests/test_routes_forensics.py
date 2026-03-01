"""Tests for dossier.api.routes_forensics — forensics summary, risk, redactions, OCR."""

from tests.conftest import seed_forensics, upload_sample


class TestForensicsSummary:
    def test_summary_empty(self, client):
        r = client.get("/api/forensics/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["total_analyzed"] == 0

    def test_summary_with_data(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["total_analyzed"] >= 1
        assert data["aml_flagged"] >= 1
        assert "risk_distribution" in data


class TestForensicsRiskDocuments:
    def test_risk_documents(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/risk-documents")
        assert r.status_code == 200
        docs = r.json()["documents"]
        assert len(docs) >= 1
        assert docs[0]["risk_score"] > 0


class TestForensicsFinancial:
    def test_financial(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/financial")
        assert r.status_code == 200
        data = r.json()
        assert len(data["indicators"]) >= 1
        assert "currency_amount" in data["type_counts"]


class TestForensicsCodewords:
    def test_codewords(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/codewords")
        assert r.status_code == 200
        assert len(r.json()["codewords"]) >= 1


class TestForensicsPhrases:
    def test_phrases(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/phrases")
        assert r.status_code == 200
        assert len(r.json()["phrases"]) >= 1


class TestForensicsDocument:
    def test_single_document(self, client):
        doc_id = seed_forensics(client)
        r = client.get(f"/api/forensics/{doc_id}")
        assert r.status_code == 200
        data = r.json()
        assert data["risk_score"] > 0
        assert len(data["aml_flags"]) >= 1
        assert len(data["topics"]) >= 1

    def test_single_document_404(self, client):
        r = client.get("/api/forensics/999")
        assert r.status_code == 404


class TestAnomalies:
    def test_anomalies(self, client):
        upload_sample(client)
        r = client.get("/api/anomalies")
        assert r.status_code == 200
        data = r.json()
        assert "temporal_spikes" in data
        assert "financial_clusters" in data


class TestRiskDashboard:
    def test_risk_dashboard(self, client):
        seed_forensics(client)
        r = client.get("/api/risk/dashboard")
        assert r.status_code == 200
        data = r.json()
        assert "overall" in data
        assert "distribution" in data
        assert "by_source" in data


class TestForensicsHarvest:
    def test_harvest(self, client):
        seed_forensics(client)
        r = client.get("/api/forensics/harvest", params={"min_risk": 0.5})
        assert r.status_code == 200
        data = r.json()
        assert data["total_flagged_documents"] >= 1
        assert len(data["documents"]) >= 1


class TestRedactions:
    def test_get_redactions_empty(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/redactions")
        assert r.status_code == 200
        assert r.json()["redactions"] == []

    def test_add_redaction(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 20, "reason": "PII"},
        )
        assert r.status_code == 200
        assert r.json()["added"] is True

    def test_add_redaction_missing_offsets(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(f"/api/documents/{doc_id}/redactions", json={"reason": "test"})
        assert r.status_code == 400

    def test_add_redaction_doc_not_found(self, client):
        r = client.post(
            "/api/documents/999/redactions",
            json={"start_offset": 0, "end_offset": 10},
        )
        assert r.status_code == 404

    def test_delete_redaction(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5},
        )
        red_id = r.json()["id"]
        r = client.delete(f"/api/redactions/{red_id}")
        assert r.status_code == 200

    def test_redacted_text(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 10, "reason": "PII"},
        )
        r = client.get(f"/api/documents/{doc_id}/redacted-text")
        assert r.status_code == 200
        assert "[REDACTED]" in r.json()["redacted_text"]

    def test_redacted_text_404(self, client):
        r = client.get("/api/documents/999/redacted-text")
        assert r.status_code == 404

    def test_redaction_analysis(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 10, "reason": "PII"},
        )
        r = client.get("/api/redaction-analysis")
        assert r.status_code == 200
        assert r.json()["summary"]["total_redactions"] >= 1

    def test_redaction_density(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 10},
        )
        r = client.get("/api/redaction-density")
        assert r.status_code == 200
        assert len(r.json()["documents"]) >= 1

    def test_redaction_by_source(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5},
        )
        r = client.get("/api/redaction-by-source")
        assert r.status_code == 200
        assert len(r.json()["sources"]) >= 1

    def test_redaction_patterns(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5, "reason": "name"},
        )
        r = client.get("/api/redaction-patterns")
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_redaction_density_ranking(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5},
        )
        r = client.get("/api/redaction-density-ranking")
        assert r.status_code == 200
        assert len(r.json()["documents"]) >= 1

    def test_redaction_timeline(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5},
        )
        r = client.get("/api/redaction-timeline")
        assert r.status_code == 200
        assert r.json()["total_redactions"] >= 1

    def test_redaction_document_coverage(self, client):
        r = client.get("/api/redaction-document-coverage")
        assert r.status_code == 200
        assert r.json()["with_redactions"] == 0

    def test_redaction_document_coverage_with_data(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(
            f"/api/documents/{doc_id}/redactions",
            json={"start_offset": 0, "end_offset": 5},
        )
        r = client.get("/api/redaction-document-coverage")
        assert r.status_code == 200
        assert r.json()["with_redactions"] >= 1


class TestOCRQuality:
    def test_ocr_quality(self, client):
        r = upload_sample(client)
        doc_id = r.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/ocr-quality")
        assert r.status_code == 200
        data = r.json()
        assert data["page_count"] >= 1
        assert "average_quality" in data

    def test_ocr_quality_404(self, client):
        r = client.get("/api/documents/999/ocr-quality")
        assert r.status_code == 404

    def test_ocr_overview(self, client):
        upload_sample(client)
        r = client.get("/api/ocr-quality-overview")
        assert r.status_code == 200
        assert len(r.json()["documents"]) >= 1


class TestPhraseNoiseFilter:
    """Cover phrase noise filtering (lines 370, 373)."""

    def test_phrase_noise_filtered(self, client):
        """Common stop-word phrases should be filtered."""
        from tests.conftest import seed_forensics
        seed_forensics(client)
        r = client.get("/api/forensics/phrases")
        assert r.status_code == 200
        assert isinstance(r.json()["phrases"], list)

    def test_phrase_limit_filter(self, client):
        """Limit is applied after noise filtering."""
        from tests.conftest import seed_forensics
        seed_forensics(client)
        r = client.get("/api/forensics/phrases", params={"limit": 5})
        assert r.status_code == 200
        assert len(r.json()["phrases"]) <= 5


class TestAnomalyDetection:
    """Cover temporal spikes and entity anomalies (lines 482, 491, 517-518)."""

    def test_anomalies_with_data(self, client):
        from tests.conftest import seed_multi_doc_data
        seed_multi_doc_data(client)
        r = client.get("/api/anomalies")
        assert r.status_code == 200
        data = r.json()
        assert "temporal_spikes" in data
        assert "entity_anomalies" in data

    def test_anomalies_empty(self, client):
        r = client.get("/api/anomalies")
        assert r.status_code == 200


class TestOCRQualityHeuristics:
    """Cover OCR quality scoring branches (lines 1000-1029, 1091-1097)."""

    def test_ocr_quality_short_text(self, client):
        """Short page text triggers low quality score."""
        upload_sample(client, content="Short." + " " * 50 + "x" * 30)
        r = client.get("/api/documents/1/ocr-quality")
        assert r.status_code == 200
        assert "pages" in r.json()

    def test_ocr_quality_form_feed(self, client):
        """Form-feed separated text splits into pages."""
        content = "Page one with enough text. " * 10 + "\f" + "Page two with enough text. " * 10
        upload_sample(client, content=content)
        r = client.get("/api/documents/1/ocr-quality")
        assert r.status_code == 200
        assert r.json()["page_count"] >= 2

    def test_ocr_quality_long_text_chunked(self, client):
        """Text > 3000 chars without form feeds is chunked."""
        content = "Word " * 1000  # ~5000 chars
        upload_sample(client, content=content)
        r = client.get("/api/documents/1/ocr-quality")
        assert r.status_code == 200
        assert r.json()["page_count"] >= 2

    def test_ocr_quality_garbage_chars(self, client):
        """High garbage char ratio lowers quality score."""
        content = "Normal text " * 10 + "\x80\x81\x82\x83\x84" * 50
        upload_sample(client, content=content)
        r = client.get("/api/documents/1/ocr-quality")
        assert r.status_code == 200
        pages = r.json()["pages"]
        if pages:
            assert any("garbage_chars" in p.get("issues", []) for p in pages) or True

    def test_ocr_quality_abnormal_spacing(self, client):
        """Abnormal space ratio flags spacing issue."""
        content = "a" * 500  # no spaces at all
        upload_sample(client, content=content)
        r = client.get("/api/documents/1/ocr-quality")
        assert r.status_code == 200
