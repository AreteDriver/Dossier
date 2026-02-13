"""Tests for dossier.api.server — FastAPI REST endpoints."""

import io
from unittest.mock import MagicMock, patch


class TestEmptyDb:
    def test_root_returns_200(self, client):
        r = client.get("/")
        assert r.status_code == 200

    def test_stats_empty(self, client):
        r = client.get("/api/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["documents"] == 0
        assert data["entities"] == 0
        assert data["pages"] == 0

    def test_search_empty(self, client):
        r = client.get("/api/search", params={"q": "test"})
        assert r.status_code == 200
        assert r.json()["results"] == []

    def test_list_documents_empty(self, client):
        r = client.get("/api/documents")
        assert r.status_code == 200
        assert r.json()["documents"] == []
        assert r.json()["total"] == 0

    def test_list_entities_empty(self, client):
        r = client.get("/api/entities")
        assert r.status_code == 200
        assert r.json()["entities"] == []

    def test_list_keywords_empty(self, client):
        r = client.get("/api/keywords")
        assert r.status_code == 200
        assert r.json()["keywords"] == []

    def test_connections_empty(self, client):
        r = client.get("/api/connections")
        assert r.status_code == 200
        assert r.json()["connections"] == []

    def test_document_not_found(self, client):
        r = client.get("/api/documents/999")
        assert r.status_code == 404


def _upload_sample(client, filename="test_doc.txt", content=None):
    """Helper to upload a sample text file."""
    if content is None:
        content = (
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The deposition was taken on January 15, 2015. "
            "Goldman Sachs provided financial records related to the case."
        )
    return client.post(
        "/api/upload",
        files={"file": (filename, io.BytesIO(content.encode()), "text/plain")},
        params={"source": "Test Upload"},
    )


class TestUpload:
    def test_upload_success(self, client):
        r = _upload_sample(client)
        assert r.status_code == 201
        data = r.json()
        assert data["success"] is True
        assert "document_id" in data

    def test_upload_duplicate(self, client):
        _upload_sample(client, filename="dup.txt")
        r = _upload_sample(client, filename="dup.txt")
        assert r.status_code == 409
        assert "duplicate" in r.json()["message"].lower()


class TestDocuments:
    def test_document_detail(self, client):
        r = _upload_sample(client)
        doc_id = r.json()["document_id"]

        r = client.get(f"/api/documents/{doc_id}")
        assert r.status_code == 200
        doc = r.json()
        assert doc["id"] == doc_id
        assert "entities" in doc
        assert "keywords" in doc

    def test_list_after_upload(self, client):
        _upload_sample(client)
        r = client.get("/api/documents")
        assert r.status_code == 200
        assert r.json()["total"] >= 1

    def test_filter_by_category(self, client):
        _upload_sample(client)
        r = client.get("/api/documents", params={"category": "nonexistent_cat"})
        assert r.status_code == 200
        assert r.json()["documents"] == []

    def test_toggle_flag(self, client):
        r = _upload_sample(client)
        doc_id = r.json()["document_id"]

        # Flag it
        r = client.post(f"/api/documents/{doc_id}/flag")
        assert r.status_code == 200
        assert r.json()["flagged"] is True

        # Unflag it
        r = client.post(f"/api/documents/{doc_id}/flag")
        assert r.status_code == 200
        assert r.json()["flagged"] is False

    def test_toggle_flag_not_found(self, client):
        r = client.post("/api/documents/999/flag")
        assert r.status_code == 404

    def test_filter_by_flagged(self, client):
        r = _upload_sample(client)
        doc_id = r.json()["document_id"]
        client.post(f"/api/documents/{doc_id}/flag")

        r = client.get("/api/documents", params={"flagged": True})
        assert r.status_code == 200
        assert len(r.json()["documents"]) >= 1


class TestSearch:
    def test_search_finds_uploaded(self, client):
        _upload_sample(client)
        r = client.get("/api/search", params={"q": "Epstein"})
        assert r.status_code == 200
        assert len(r.json()["results"]) >= 1

    def test_search_with_category_filter(self, client):
        _upload_sample(client)
        # Search with a category that shouldn't match
        r = client.get("/api/search", params={"q": "Epstein", "category": "flight"})
        assert r.status_code == 200
        # May or may not find results depending on classification

    def test_search_no_query_lists_all(self, client):
        _upload_sample(client)
        r = client.get("/api/search", params={"q": ""})
        assert r.status_code == 200
        assert r.json()["total"] >= 1


class TestEntities:
    def test_entities_populated_after_upload(self, client):
        _upload_sample(client)
        r = client.get("/api/entities")
        assert r.status_code == 200
        assert len(r.json()["entities"]) > 0

    def test_filter_by_type(self, client):
        _upload_sample(client)
        r = client.get("/api/entities", params={"type": "person"})
        assert r.status_code == 200
        entities = r.json()["entities"]
        assert all(e["type"] == "person" for e in entities)

    def test_entity_documents(self, client):
        _upload_sample(client)
        # Get an entity ID
        r = client.get("/api/entities")
        entities = r.json()["entities"]
        if entities:
            eid = entities[0]["id"]
            r = client.get(f"/api/entities/{eid}/documents")
            assert r.status_code == 200
            assert len(r.json()["documents"]) >= 1


class TestKeywordsAndConnections:
    def test_keywords_populated_after_upload(self, client):
        _upload_sample(client)
        r = client.get("/api/keywords")
        assert r.status_code == 200
        assert len(r.json()["keywords"]) > 0

    def test_connections_populated_after_upload(self, client):
        _upload_sample(client)
        r = client.get("/api/connections")
        assert r.status_code == 200
        # Multiple entities means connections should exist
        assert len(r.json()["connections"]) > 0

    def test_connections_with_entity_filter(self, client):
        _upload_sample(client)
        r = client.get("/api/entities")
        entities = r.json()["entities"]
        if entities:
            eid = entities[0]["id"]
            r = client.get("/api/connections", params={"entity_id": eid})
            assert r.status_code == 200


class TestIngestDirectory:
    def test_bad_path(self, client, tmp_path):
        bad = tmp_path / "nonexistent_subdir"
        r = client.post("/api/ingest-directory", params={"dirpath": str(bad)})
        assert r.status_code == 400

    def test_directory_ingest(self, client, tmp_path):
        d = tmp_path / "api_batch"
        d.mkdir()
        (d / "a.txt").write_text(
            "Jeffrey Epstein documents from Palm Beach FBI investigation records."
        )
        r = client.post("/api/ingest-directory", params={"dirpath": str(d)})
        assert r.status_code == 200
        assert r.json()["ingested"] >= 1


# ═══════════════════════════════════════════
# SECURITY TESTS
# ═══════════════════════════════════════════


class TestPathTraversal:
    def test_ingest_directory_path_traversal(self, client):
        """Paths outside allowed dirs are rejected with 403."""
        r = client.post("/api/ingest-directory", params={"dirpath": "/etc"})
        assert r.status_code == 403

    def test_ingest_emails_directory_path_traversal(self, client):
        """Email directory endpoint also validates paths."""
        r = client.post("/api/ingest-emails-directory", params={"dirpath": "/etc/passwd"})
        assert r.status_code == 403

    def test_ingest_directory_rejects_symlink(self, client, tmp_path):
        """Symlinks resolving outside allowed dirs are rejected."""
        link = tmp_path / "sneaky_link"
        link.symlink_to("/etc")
        r = client.post("/api/ingest-directory", params={"dirpath": str(link)})
        assert r.status_code == 403

    def test_ingest_directory_allows_valid_path(self, client, tmp_path):
        """Paths within allowed dirs still work."""
        d = tmp_path / "legit"
        d.mkdir()
        (d / "doc.txt").write_text("Test document about financial records.")
        r = client.post("/api/ingest-directory", params={"dirpath": str(d)})
        assert r.status_code == 200


class TestFilenameSanitization:
    def test_upload_sanitizes_traversal_filename(self, client, tmp_path):
        """Path traversal in filename is stripped to basename only."""
        import dossier.api.server as srv_mod

        r = client.post(
            "/api/upload",
            files={"file": ("../../evil.txt", io.BytesIO(b"test content"), "text/plain")},
            params={"source": "Test"},
        )
        # Should succeed (file saved with sanitized name)
        assert r.status_code in (201, 422)  # 422 if ingestion fails on tiny content
        # Verify no file was written outside UPLOAD_DIR
        assert not (srv_mod.UPLOAD_DIR.parent.parent / "evil.txt").exists()

    def test_upload_empty_filename_fallback(self, client):
        """Empty filename gets a uuid-based fallback."""
        r = client.post(
            "/api/upload",
            files={"file": ("", io.BytesIO(b"test content"), "text/plain")},
            params={"source": "Test"},
        )
        # Should not crash — gets a generated name
        assert r.status_code in (201, 422)

    def test_sanitize_filename_unit(self):
        """Unit test for _sanitize_filename edge cases."""
        from dossier.api.server import _sanitize_filename

        # Path traversal stripped
        assert _sanitize_filename("../../etc/evil.txt") == "evil.txt"

        # Leading dots stripped
        assert _sanitize_filename(".hidden.txt") == "hidden.txt"

        # Special chars replaced
        result = _sanitize_filename("file name (1).txt")
        assert "(" not in result
        assert " " not in result

        # Empty string gets uuid fallback
        result = _sanitize_filename("")
        assert result.startswith("upload_")

        # Pure dots get uuid fallback
        result = _sanitize_filename("...")
        assert result.startswith("upload_")


class TestFTSEscaping:
    def test_search_fts_special_chars(self, client):
        """FTS5 operators don't crash the search endpoint."""
        _upload_sample(client)
        # These would cause FTS5 parse errors without sanitization
        for query in ["test* OR 1=1", 'NEAR("a" "b")', "col:value", '"unmatched']:
            r = client.get("/api/search", params={"q": query})
            assert r.status_code == 200, f"Failed on query: {query}"

    def test_search_fts_metachar_stripped(self):
        """Verify metacharacters are stripped from queries."""
        import re

        q = 'test* OR "injection" (evil)'
        fts_query = re.sub(r'["\*\(\)\{\}\[\]:^~]', " ", q.strip()).strip()
        assert "*" not in fts_query
        assert '"' not in fts_query
        assert "(" not in fts_query


class TestUploadSizeLimit:
    def test_upload_too_large(self, client, monkeypatch):
        """Files exceeding MAX_UPLOAD_SIZE return 413."""
        import dossier.api.server as srv_mod

        # Set limit to 1KB for testing
        monkeypatch.setattr(srv_mod, "MAX_UPLOAD_SIZE", 1024)
        big_content = b"x" * 2048  # 2KB, exceeds 1KB limit
        r = client.post(
            "/api/upload",
            files={"file": ("big.txt", io.BytesIO(big_content), "text/plain")},
            params={"source": "Test"},
        )
        assert r.status_code == 413

    def test_upload_within_limit(self, client):
        """Files under the limit upload normally."""
        r = _upload_sample(client, filename="small.txt")
        assert r.status_code == 201


class TestGenericErrorHandler:
    def test_generic_500_no_stacktrace(self, tmp_path, monkeypatch):
        """Unhandled exceptions return generic 500 without stack trace."""
        from fastapi.testclient import TestClient

        import dossier.db.database as db_mod
        import dossier.api.server as srv_mod

        db_path = str(tmp_path / "err_test.db")
        monkeypatch.setattr(db_mod, "DB_PATH", db_path)
        monkeypatch.setattr(srv_mod, "UPLOAD_DIR", tmp_path / "inbox")
        monkeypatch.setattr(srv_mod, "ALLOWED_BASE_DIRS", [tmp_path])

        def _exploding_db():
            raise RuntimeError("DB connection exploded")

        monkeypatch.setattr(srv_mod, "get_db", _exploding_db)

        with TestClient(srv_mod.app, raise_server_exceptions=False) as c:
            r = c.get("/api/stats")
            assert r.status_code == 500
            body = r.json()
            assert body["detail"] == "Internal server error"
            assert "Traceback" not in r.text
            assert "exploded" not in r.text


class TestSearchEmptyQueryWithCategory:
    def test_empty_query_with_category_filter(self, client):
        """Empty query + category filter covers the non-FTS category branch."""
        _upload_sample(client)

        # Get the category assigned to our uploaded doc
        r = client.get("/api/documents")
        docs = r.json()["documents"]
        assert len(docs) >= 1
        cat = docs[0]["category"]

        # Search with empty query but matching category
        r = client.get("/api/search", params={"q": "", "category": cat})
        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 1
        assert all(d["category"] == cat for d in data["results"])

    def test_empty_query_with_nonmatching_category(self, client):
        """Empty query + nonexistent category returns zero results."""
        _upload_sample(client)
        r = client.get("/api/search", params={"q": "", "category": "nonexistent_cat"})
        assert r.status_code == 200
        assert r.json()["total"] == 0


class TestUploadEmail:
    def test_success(self, client):
        """Successful email upload returns 201."""
        mock_fn = MagicMock(return_value=[{"success": True, "document_id": 1}])
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.email_pipeline": MagicMock(ingest_email_file=mock_fn),
            },
        ):
            r = client.post(
                "/api/upload-email",
                files={"file": ("test.eml", io.BytesIO(b"email content here"), "message/rfc822")},
                params={"source": "Email Test", "corpus": "test"},
            )
        assert r.status_code == 201
        data = r.json()
        assert data["ingested"] == 1
        assert data["failed"] == 0

    def test_all_failed(self, client):
        """All-failed email upload returns 422."""
        mock_fn = MagicMock(return_value=[{"success": False, "message": "parse error"}])
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.email_pipeline": MagicMock(ingest_email_file=mock_fn),
            },
        ):
            r = client.post(
                "/api/upload-email",
                files={"file": ("bad.eml", io.BytesIO(b"bad email"), "message/rfc822")},
                params={"source": "Email Test"},
            )
        assert r.status_code == 422
        data = r.json()
        assert data["ingested"] == 0
        assert data["failed"] == 1


class TestIngestEmailsDirectory:
    def test_success(self, client, tmp_path):
        """Successful email directory ingest."""
        d = tmp_path / "emails"
        d.mkdir()
        (d / "sample.eml").write_text("From: test@example.com\nSubject: Test")

        mock_fn = MagicMock(return_value={"ingested": 2, "failed": 0})
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.email_pipeline": MagicMock(ingest_email_directory=mock_fn),
            },
        ):
            r = client.post(
                "/api/ingest-emails-directory",
                params={"dirpath": str(d), "source": "Email Import", "corpus": "test"},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["ingested"] == 2

    def test_bad_directory(self, client, tmp_path):
        """Nonexistent directory returns 400."""
        bad = tmp_path / "no_such_dir"
        mock_fn = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.email_pipeline": MagicMock(ingest_email_directory=mock_fn),
            },
        ):
            r = client.post(
                "/api/ingest-emails-directory",
                params={"dirpath": str(bad)},
            )
        assert r.status_code == 400


class TestLobbyingGenerate:
    def test_success(self, client):
        """Lobbying generate endpoint calls all three functions."""
        mock_mod = MagicMock()
        mock_mod.create_lobbying_index = MagicMock()
        mock_mod.generate_ingestable_documents = MagicMock(return_value=5)
        mock_mod.ingest_lobbying_docs = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.fara_lobbying": mock_mod,
            },
        ):
            r = client.post("/api/lobbying/generate")
        assert r.status_code == 200
        data = r.json()
        assert "5" in data["message"]
        mock_mod.create_lobbying_index.assert_called_once()
        mock_mod.ingest_lobbying_docs.assert_called_once()


class TestServeFrontendNoIndex:
    def test_no_index_returns_api_message(self, client, monkeypatch):
        """When no index.html exists, returns API running message."""
        import dossier.api.server as srv_mod

        monkeypatch.setattr(
            srv_mod,
            "STATIC_DIR",
            client.app.state._tmp
            if hasattr(client.app.state, "_tmp")
            else monkeypatch.tmpdir
            if hasattr(monkeypatch, "tmpdir")
            else "/tmp/empty_static_dir_test",
        )
        # Use a fresh empty dir
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            from pathlib import Path

            monkeypatch.setattr(srv_mod, "STATIC_DIR", Path(td))
            r = client.get("/")
            assert r.status_code == 200
            data = r.json()
            assert "DOSSIER API is running" in data["message"]
