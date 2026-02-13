"""Tests for dossier.__main__ — CLI entry point."""

import runpy
import sqlite3
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


class TestCliTimeline:
    def test_timeline_empty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "timeline"])
        main()
        output = capsys.readouterr().out
        assert "No timeline events" in output

    def test_timeline_with_events(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "timeline_test.txt"
        f.write_text(
            "On March 14, 2009, Jane Doe testified in the Southern District. "
            "Jeffrey Epstein was arrested on July 6, 2019 at Teterboro Airport."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()  # clear

        monkeypatch.setattr(sys, "argv", ["dossier", "timeline"])
        main()
        output = capsys.readouterr().out
        assert "Timeline" in output
        assert "2009" in output

    def test_timeline_with_filters(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "timeline_filter.txt"
        f.write_text(
            "On March 14, 2009, Jane Doe testified. Jeffrey Epstein was arrested on July 6, 2019."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()  # clear

        monkeypatch.setattr(
            sys, "argv", ["dossier", "timeline", "--start", "2015-01-01", "--end", "2020-12-31"]
        )
        main()
        output = capsys.readouterr().out
        assert "Timeline" in output


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


class TestCliResolve:
    def test_resolve_empty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve"])
        main()
        output = capsys.readouterr().out
        assert "Entity Resolution" in output
        assert "Entities scanned:  0" in output

    def test_resolve_with_data(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "resolve_test.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The investigation uncovered significant evidence."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve"])
        main()
        output = capsys.readouterr().out
        assert "Entity Resolution" in output

    def test_resolve_with_type_filter(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "resolve_type.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The investigation uncovered significant evidence."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve", "--type", "person"])
        main()
        output = capsys.readouterr().out
        assert "Entity Resolution" in output

    def test_resolve_dry_run(self, monkeypatch, cli_env, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()

        f = cli_env / "resolve_dry.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The investigation uncovered significant evidence."
        )
        monkeypatch.setattr(sys, "argv", ["dossier", "ingest", str(f)])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve", "--dry-run"])
        main()
        output = capsys.readouterr().out
        assert "Dry Run" in output

    def test_resolve_dry_run_no_candidates(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve", "--dry-run"])
        main()
        output = capsys.readouterr().out
        assert "No candidates found" in output

    def test_resolve_dry_run_with_candidates(self, monkeypatch, cli_env, capsys):
        """Dry run shows candidates when duplicate entities exist."""
        import sqlite3
        import dossier.db.database as db_mod

        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        # Manually insert entities that would resolve
        conn = sqlite3.connect(db_mod.DB_PATH)
        conn.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("John Smith", "person", "john smith"),
        )
        conn.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("Smith, John", "person", "smith, john"),
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve", "--dry-run"])
        main()
        output = capsys.readouterr().out
        assert "Total candidates:" in output
        assert "John Smith" in output

    def test_resolve_with_matches(self, monkeypatch, cli_env, capsys):
        """Full resolve shows match details when entities merge."""
        import sqlite3
        import dossier.db.database as db_mod

        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        conn = sqlite3.connect(db_mod.DB_PATH)
        conn.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("John Smith", "person", "john smith"),
        )
        conn.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("Smith, John", "person", "smith, john"),
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(sys, "argv", ["dossier", "resolve"])
        main()
        output = capsys.readouterr().out
        assert "Auto-merged:" in output
        assert "Matches:" in output


class TestCliGraph:
    def _seed_graph(self):
        """Insert entities + connections into the CLI test DB."""
        import dossier.db.database as db_mod

        conn = sqlite3.connect(db_mod.DB_PATH)
        conn.execute("PRAGMA foreign_keys=ON")
        entities = [
            (1, "Alice", "person", "alice"),
            (2, "Bob", "person", "bob"),
            (3, "Acme Corp", "org", "acme corp"),
        ]
        conn.executemany(
            "INSERT INTO entities (id, name, type, canonical) VALUES (?, ?, ?, ?)", entities
        )
        connections = [(1, 2, 5), (1, 3, 2), (2, 3, 3)]
        conn.executemany(
            "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
            connections,
        )
        conn.commit()
        conn.close()

    def test_graph_no_subcommand(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "graph"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_graph_unknown_subcommand(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "foobar"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_graph_stats_empty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "stats"])
        main()
        output = capsys.readouterr().out
        assert "Network Stats" in output
        assert "Nodes:" in output

    def test_graph_stats_with_data(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "stats"])
        main()
        output = capsys.readouterr().out
        assert "Nodes:" in output

    def test_graph_stats_type_filter(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "stats", "--type", "person"])
        main()
        output = capsys.readouterr().out
        assert "Nodes:" in output

    def test_graph_centrality_empty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "centrality"])
        main()
        output = capsys.readouterr().out
        assert "No entities found" in output

    def test_graph_centrality_with_data(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "dossier",
                "graph",
                "centrality",
                "--metric",
                "degree",
                "--type",
                "person",
                "--limit",
                "2",
            ],
        )
        main()
        output = capsys.readouterr().out
        assert "degree centrality" in output

    def test_graph_communities_empty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "communities"])
        main()
        output = capsys.readouterr().out
        assert "No communities found" in output

    def test_graph_communities_with_data(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(
            sys,
            "argv",
            ["dossier", "graph", "communities", "--type", "person", "--min-size", "2"],
        )
        main()
        output = capsys.readouterr().out
        assert "communities" in output.lower()

    def test_graph_path_missing_args(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "path"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_graph_path_with_data(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "path", "1", "2"])
        main()
        output = capsys.readouterr().out
        assert "Path" in output

    def test_graph_path_no_path(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "path", "1", "999"])
        main()
        output = capsys.readouterr().out
        assert "No path found" in output

    def test_graph_neighbors_missing_args(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "neighbors"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_graph_neighbors_with_data(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        self._seed_graph()
        capsys.readouterr()

        monkeypatch.setattr(
            sys, "argv", ["dossier", "graph", "neighbors", "1", "--hops", "1", "--min-weight", "1"]
        )
        main()
        output = capsys.readouterr().out
        assert "Neighbors" in output

    def test_graph_neighbors_no_neighbors(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["dossier", "init"])
        main()
        capsys.readouterr()

        monkeypatch.setattr(sys, "argv", ["dossier", "graph", "neighbors", "999"])
        main()
        output = capsys.readouterr().out
        assert "No neighbors found" in output
