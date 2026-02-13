"""Tests for dossier.__main__ — CLI entry point."""

import runpy
import sys
from unittest.mock import patch, MagicMock

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
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
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

    def test_search_no_query(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "search"])
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

    def test_ingest_with_date(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "dated.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(
            sys, "argv", ["dossier", "ingest", str(f), "--source", "Test", "--date", "2023-06-15"]
        )
        main()
        output = capsys.readouterr().out
        assert "Ingested" in output

    def test_ingest_failure(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", "/nonexistent/file.txt"])
        main()
        output = capsys.readouterr().out
        assert "Failed" in output


class TestCliStatsWithData:
    def test_stats_shows_categories(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "categorized.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()  # clear

        monkeypatch.setattr(sys, "argv", ["dossier", "stats"])
        main()
        output = capsys.readouterr().out
        assert "Documents:" in output
        assert "Categories:" in output


class TestCliServe:
    def test_serve_default_port(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "serve"])
        mock_uvicorn = MagicMock()
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            main()
        mock_uvicorn.run.assert_called_once_with(
            "dossier.api.server:app", host="0.0.0.0", port=8000, reload=True
        )

    def test_serve_custom_port(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "serve", "9090"])
        mock_uvicorn = MagicMock()
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            main()
        mock_uvicorn.run.assert_called_once_with(
            "dossier.api.server:app", host="0.0.0.0", port=9090, reload=True
        )


class TestCliIngestDir:
    def test_ingest_directory(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        d = cli_env / "batch_dir"
        d.mkdir()
        (d / "doc.txt").write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest-dir", str(d)])
        main()
        output = capsys.readouterr().out
        assert "Ingested: 1" in output

    def test_ingest_dir_with_source(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        d = cli_env / "sourced_dir"
        d.mkdir()
        (d / "report.txt").write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest-dir", str(d), "--source", "FOIA"])
        main()
        output = capsys.readouterr().out
        assert "Ingested: 1" in output


class TestCliIngestEmails:
    def test_ingest_emails(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        mock_fn = MagicMock(return_value={"ingested": 3, "failed": 0})
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.email_pipeline": MagicMock(ingest_email_directory=mock_fn),
            },
        ):
            monkeypatch.setattr(
                sys,
                "argv",
                [
                    "dossier",
                    "ingest-emails",
                    "/some/dir",
                    "--source",
                    "TestSrc",
                    "--corpus",
                    "TestCorpus",
                ],
            )
            main()

        mock_fn.assert_called_once_with("/some/dir", source="TestSrc", corpus="TestCorpus")
        output = capsys.readouterr().out
        assert "Ingested: 3" in output


class TestCliPodesta:
    def test_podesta_download(self, monkeypatch):
        mock_download = MagicMock()
        mock_mod = MagicMock(download_range=mock_download)
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.wikileaks_podesta": mock_mod,
            },
        ):
            monkeypatch.setattr(
                sys,
                "argv",
                ["dossier", "podesta-download", "--range", "10", "50", "--delay", "0.5"],
            )
            main()

        mock_download.assert_called_once_with(10, 50, delay=0.5)

    def test_podesta_ingest(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        mock_ingest = MagicMock()
        mock_mod = MagicMock(ingest_downloaded=mock_ingest)
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.wikileaks_podesta": mock_mod,
            },
        ):
            monkeypatch.setattr(sys, "argv", ["dossier", "podesta-ingest", "--limit", "25"])
            main()

        mock_ingest.assert_called_once_with(limit=25)


class TestCliLobbying:
    def _mock_fara(self):
        """Return mock fara_lobbying module with 3 mock functions."""
        mock_mod = MagicMock()
        mock_mod.create_lobbying_index = MagicMock()
        mock_mod.generate_ingestable_documents = MagicMock()
        mock_mod.ingest_lobbying_docs = MagicMock()
        return mock_mod

    def test_lobbying_all(self, monkeypatch):
        mock_mod = self._mock_fara()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.fara_lobbying": mock_mod,
            },
        ):
            monkeypatch.setattr(sys, "argv", ["dossier", "lobbying", "--all"])
            main()

        mock_mod.create_lobbying_index.assert_called_once()
        mock_mod.generate_ingestable_documents.assert_called_once()
        mock_mod.ingest_lobbying_docs.assert_called_once()

    def test_lobbying_create_index(self, monkeypatch):
        mock_mod = self._mock_fara()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.fara_lobbying": mock_mod,
            },
        ):
            monkeypatch.setattr(sys, "argv", ["dossier", "lobbying", "--create-index"])
            main()

        mock_mod.create_lobbying_index.assert_called_once()

    def test_lobbying_generate_docs(self, monkeypatch):
        mock_mod = self._mock_fara()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.fara_lobbying": mock_mod,
            },
        ):
            monkeypatch.setattr(sys, "argv", ["dossier", "lobbying", "--generate-docs"])
            main()

        mock_mod.generate_ingestable_documents.assert_called_once()

    def test_lobbying_ingest(self, monkeypatch):
        mock_mod = self._mock_fara()
        with patch.dict(
            "sys.modules",
            {
                "dossier.ingestion.scrapers": MagicMock(),
                "dossier.ingestion.scrapers.fara_lobbying": mock_mod,
            },
        ):
            monkeypatch.setattr(sys, "argv", ["dossier", "lobbying", "--ingest"])
            main()

        mock_mod.ingest_lobbying_docs.assert_called_once()

    def test_lobbying_no_flag_shows_usage(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "lobbying"])
        main()
        output = capsys.readouterr().out
        assert "Usage:" in output or "--all" in output


class TestCliSearchWithResults:
    def test_search_with_results(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "searchable.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()  # clear

        monkeypatch.setattr(sys, "argv", ["dossier", "search", "Epstein"])
        main()
        output = capsys.readouterr().out
        assert "Results for" in output


class TestMainGuard:
    def test_main_module_guard(self, monkeypatch, cli_env):
        """Running dossier.__main__ as __main__ calls main()."""
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        runpy.run_module("dossier.__main__", run_name="__main__")


class TestCliEntitiesWithResults:
    def test_entities_with_type_filter(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "entities_test.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The investigation uncovered significant evidence."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()  # clear

        monkeypatch.setattr(sys, "argv", ["dossier", "entities", "person"])
        main()
        output = capsys.readouterr().out
        assert "Top Entities" in output
        assert "person" in output
