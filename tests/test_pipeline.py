"""Tests for dossier.ingestion.pipeline — file ingestion and directory processing."""

import sqlite3

import pytest

import dossier.db.database as db_mod
import dossier.ingestion.pipeline as pipe_mod
from dossier.ingestion.pipeline import ingest_file, ingest_directory


@pytest.fixture
def pipeline_env(tmp_path, monkeypatch):
    """Set up a complete pipeline environment with temp DB and dirs."""
    db_path = str(tmp_path / "pipeline_test.db")
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    monkeypatch.setattr(pipe_mod, "PROCESSED_DIR", tmp_path / "processed")
    db_mod.init_db()
    return tmp_path


class TestIngestFile:
    def test_success(self, pipeline_env):
        f = pipeline_env / "doc.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The deposition was taken on January 15, 2015. "
            "Goldman Sachs provided financial records related to the case."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True
        assert "document_id" in result
        assert result["stats"]["people"] > 0
        assert result["stats"]["timeline_events"] > 0

    def test_file_not_found(self, pipeline_env):
        result = ingest_file("/nonexistent/file.txt")
        assert result["success"] is False
        assert "not found" in result["message"].lower()

    def test_duplicate_detection(self, pipeline_env):
        f = pipeline_env / "doc.txt"
        f.write_text("Jeffrey Epstein was investigated by the FBI in Palm Beach for many years.")
        result1 = ingest_file(str(f), source="Test")
        assert result1["success"] is True

        result2 = ingest_file(str(f), source="Test")
        assert result2["success"] is False
        assert "duplicate" in result2["message"].lower()

    def test_empty_text_fails(self, pipeline_env):
        f = pipeline_env / "empty.txt"
        f.write_text("tiny")  # < 20 chars after extraction
        result = ingest_file(str(f))
        assert result["success"] is False
        assert "no text" in result["message"].lower()

    def test_entity_storage(self, pipeline_env):
        f = pipeline_env / "entities.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell met at Palm Beach. "
            "The FBI investigation began in 2005. The Clinton Foundation was mentioned."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True

        conn = sqlite3.connect(str(pipeline_env / "pipeline_test.db"))
        conn.row_factory = sqlite3.Row
        entities = conn.execute("SELECT name, type FROM entities").fetchall()
        conn.close()

        entity_names = {r["name"].lower() for r in entities}
        assert "jeffrey epstein" in entity_names
        assert "ghislaine maxwell" in entity_names

    def test_keyword_storage(self, pipeline_env):
        f = pipeline_env / "keywords.txt"
        f.write_text(
            "The investigation investigation investigation into flight logs "
            "revealed significant evidence. Investigation continued for years."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True

        conn = sqlite3.connect(str(pipeline_env / "pipeline_test.db"))
        conn.row_factory = sqlite3.Row
        kws = conn.execute("SELECT word FROM keywords").fetchall()
        conn.close()
        words = {r["word"] for r in kws}
        assert "investigation" in words

    def test_cooccurrence(self, pipeline_env):
        f = pipeline_env / "cooccur.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were both present in Palm Beach. "
            "The FBI filed the report in 2005."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True

        conn = sqlite3.connect(str(pipeline_env / "pipeline_test.db"))
        connections = conn.execute("SELECT COUNT(*) as c FROM entity_connections").fetchone()[0]
        conn.close()
        assert connections > 0

    def test_copies_to_processed_dir(self, pipeline_env):
        f = pipeline_env / "copy_test.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach over many years. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True

        processed = pipeline_env / "processed"
        assert processed.exists()
        # File should be in a category subdirectory
        files = list(processed.rglob("copy_test.txt"))
        assert len(files) == 1

    def test_custom_source_and_date(self, pipeline_env):
        f = pipeline_env / "sourced.txt"
        f.write_text(
            "Jeffrey Epstein documents released under FOIA. "
            "The FBI provided these records after extensive litigation."
        )
        result = ingest_file(str(f), source="FOIA Release", date="2023-01-15")
        assert result["success"] is True

        conn = sqlite3.connect(str(pipeline_env / "pipeline_test.db"))
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT source, date FROM documents WHERE id = ?", (result["document_id"],)
        ).fetchone()
        conn.close()
        assert doc["source"] == "FOIA Release"
        assert doc["date"] == "2023-01-15"

    def test_file_copy_collision(self, pipeline_env):
        """Same filename but different content gets hash suffix in processed dir."""
        f = pipeline_env / "collision.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The investigation uncovered significant evidence of wrongdoing."
        )
        result1 = ingest_file(str(f), source="Test")
        assert result1["success"] is True

        # Overwrite with different content (different hash, same name)
        f.write_text(
            "Ghislaine Maxwell was investigated by the FBI in New York. "
            "The investigation revealed a complex network of associates."
        )
        result2 = ingest_file(str(f), source="Test")
        assert result2["success"] is True

        # Both files should exist in processed dir
        processed = pipeline_env / "processed"
        all_files = list(processed.rglob("collision*"))
        assert len(all_files) == 2
        # One should have the hash suffix
        names = [p.name for p in all_files]
        assert any("_" in n and n != "collision.txt" for n in names)


class TestResolverIntegration:
    def test_resolver_runs_on_ingest(self, pipeline_env):
        """Entity resolution runs automatically after ingestion."""
        f = pipeline_env / "resolver_test.txt"
        f.write_text(
            "Jeffrey Epstein was investigated by the FBI in Palm Beach. "
            "The deposition was taken on January 15, 2015. "
            "Goldman Sachs provided financial records related to the case."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True
        assert "resolved_entities" in result["stats"]
        assert "suggested_merges" in result["stats"]

    def test_resolver_stats_in_response(self, pipeline_env):
        """Resolution stats are included in the ingest response."""
        f = pipeline_env / "resolver_stats.txt"
        f.write_text(
            "Jeffrey Epstein and Ghislaine Maxwell were both present in Palm Beach. "
            "The FBI filed the report in 2005. Goldman Sachs reviewed the financials."
        )
        result = ingest_file(str(f), source="Test")
        assert result["success"] is True
        stats = result["stats"]
        assert isinstance(stats["resolved_entities"], int)
        assert isinstance(stats["suggested_merges"], int)


class TestIngestDirectory:
    def test_ingests_supported_files(self, pipeline_env):
        d = pipeline_env / "batch"
        d.mkdir()
        (d / "doc1.txt").write_text(
            "Jeffrey Epstein document one from Palm Beach investigation by the FBI."
        )
        (d / "doc2.txt").write_text(
            "Ghislaine Maxwell document two from the New York investigation by the FBI."
        )

        results = ingest_directory(str(d), source="Batch")
        assert len(results) == 2
        assert all(r["success"] for r in results)

    def test_skips_unsupported_files(self, pipeline_env):
        d = pipeline_env / "mixed"
        d.mkdir()
        (d / "good.txt").write_text(
            "Jeffrey Epstein investigation documents from Palm Beach FBI office records."
        )
        (d / "bad.xyz").write_text("unsupported format")

        results = ingest_directory(str(d), source="Mixed")
        assert len(results) == 1  # only .txt processed
