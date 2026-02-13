"""Tests for dossier.__main__ — CLI entry point."""

import sys

import pytest

import dossier.db.database as db_mod
import dossier.ingestion.pipeline as pipe_mod
from dossier.__main__ import main


@pytest.fixture(autouse=True)
def cli_env(tmp_path, monkeypatch):
    """Redirect DB and processed dir for all CLI tests."""
    db_path = str(tmp_path / "cli_test.db")
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    monkeypatch.setattr(pipe_mod, "PROCESSED_DIR", tmp_path / "processed")
    return tmp_path


class TestCliBasic:
    def test_no_args_exits(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_unknown_command_exits(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "foobar"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_init_command(self, monkeypatch, cli_env):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        # DB should now exist
        import sqlite3
        conn = sqlite3.connect(str(cli_env / "cli_test.db"))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "documents" in tables

    def test_stats_command(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "stats"])
        main()
        output = capsys.readouterr().out
        assert "Documents" in output

    def test_entities_command(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "entities"])
        main()
        output = capsys.readouterr().out
        assert "No entities found" in output or "Top Entities" in output

    def test_search_command(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "search", "nothing"])
        main()
        output = capsys.readouterr().out
        assert "No results" in output


class TestCliMissingArgs:
    def test_ingest_no_path(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_ingest_dir_no_path(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest-dir"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_ingest_emails_no_path(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest-emails"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


class TestCliIngest:
    def test_ingest_file(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "ingest_test.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f), "--source", "CLI Test"])
        main()
        output = capsys.readouterr().out
        assert "Ingested" in output
