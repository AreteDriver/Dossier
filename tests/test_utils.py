"""Tests for dossier.api.utils — _ollama_generate, _log_audit."""

import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from dossier.api.utils import _log_audit, _ollama_generate


class TestOllamaGenerate:
    def test_success(self):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"response": "Hello world"}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("dossier.api.utils.urllib.request.urlopen", return_value=mock_resp):
            result = _ollama_generate("test prompt")
        assert result == "Hello world"

    def test_error_raises_503(self):
        import urllib.error

        with patch(
            "dossier.api.utils.urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with pytest.raises(HTTPException) as exc_info:
                _ollama_generate("test prompt")
            assert exc_info.value.status_code == 503


class TestLogAudit:
    def test_creates_table_and_inserts(self, tmp_path):
        db_path = str(tmp_path / "audit_test.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _log_audit(conn, "test_action", "entity", 42, "details here")
        row = conn.execute("SELECT * FROM audit_log").fetchone()
        assert row["action"] == "test_action"
        assert row["target_id"] == 42
        conn.close()
