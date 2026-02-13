"""Tests for dossier.db.database — schema, context manager, FTS triggers."""

import runpy
import sqlite3

import pytest

import dossier.db.database as db_mod
from dossier.db.database import get_db, get_connection, init_db


EXPECTED_TABLES = {
    "documents",
    "entities",
    "document_entities",
    "keywords",
    "document_keywords",
    "entity_connections",
}


class TestInitDb:
    def test_creates_all_tables(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        conn.close()
        assert EXPECTED_TABLES.issubset(tables)

    def test_creates_fts_table(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        conn.close()
        assert "documents_fts" in tables

    def test_idempotent(self, tmp_db, monkeypatch):
        """Calling init_db twice should not raise."""
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        init_db()  # second call
        conn = sqlite3.connect(tmp_db)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        conn.close()
        assert EXPECTED_TABLES.issubset(tables)


class TestGetConnection:
    def test_wal_mode(self, tmp_db, monkeypatch):
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        conn = get_connection()
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "wal"

    def test_foreign_keys_enabled(self, tmp_db, monkeypatch):
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        conn = get_connection()
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        conn.close()
        assert fk == 1

    def test_row_factory(self, tmp_db, monkeypatch):
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        conn = get_connection()
        conn.execute("INSERT INTO keywords (word, total_count, doc_count) VALUES ('test', 1, 1)")
        row = conn.execute("SELECT word FROM keywords WHERE word='test'").fetchone()
        conn.close()
        assert row["word"] == "test"


class TestGetDb:
    def test_commits_on_success(self, tmp_db, monkeypatch):
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        with get_db() as conn:
            conn.execute(
                "INSERT INTO keywords (word, total_count, doc_count) VALUES ('committed', 5, 1)"
            )

        # Verify persisted
        check = sqlite3.connect(tmp_db)
        row = check.execute("SELECT word FROM keywords WHERE word='committed'").fetchone()
        check.close()
        assert row is not None

    def test_rolls_back_on_error(self, tmp_db, monkeypatch):
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_db)
        with pytest.raises(ValueError):
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO keywords (word, total_count, doc_count) VALUES ('rollback_test', 1, 1)"
                )
                raise ValueError("intentional")

        # Verify not persisted
        check = sqlite3.connect(tmp_db)
        row = check.execute("SELECT word FROM keywords WHERE word='rollback_test'").fetchone()
        check.close()
        assert row is None


class TestFtsTriggers:
    def _insert_doc(self, conn, title="Test Doc", raw_text="Some test content"):
        conn.execute(
            "INSERT INTO documents (filename, filepath, title, category, source, raw_text, file_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("test.txt", "/tmp/test.txt", title, "report", "test", raw_text, f"hash_{title}"),
        )

    def test_insert_populates_fts(self, db_conn):
        self._insert_doc(db_conn, title="FTS Insert Test", raw_text="searchable content here")
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH '\"searchable content\"'"
        ).fetchall()
        assert len(rows) == 1

    def test_delete_removes_from_fts(self, db_conn):
        self._insert_doc(db_conn, title="FTS Delete Test", raw_text="delete me later")
        db_conn.commit()
        doc_id = db_conn.execute(
            "SELECT id FROM documents WHERE title='FTS Delete Test'"
        ).fetchone()[0]
        db_conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH '\"delete me later\"'"
        ).fetchall()
        assert len(rows) == 0

    def test_update_refreshes_fts(self, db_conn):
        self._insert_doc(db_conn, title="FTS Update Test", raw_text="original text")
        db_conn.commit()
        doc_id = db_conn.execute(
            "SELECT id FROM documents WHERE title='FTS Update Test'"
        ).fetchone()[0]
        db_conn.execute(
            "UPDATE documents SET raw_text = 'replacement text' WHERE id = ?", (doc_id,)
        )
        db_conn.commit()
        # Old text gone
        old = db_conn.execute(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH '\"original text\"'"
        ).fetchall()
        assert len(old) == 0
        # New text present
        new = db_conn.execute(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH '\"replacement text\"'"
        ).fetchall()
        assert len(new) == 1


class TestForeignKeyCascade:
    def test_delete_document_cascades_to_document_entities(self, db_conn):
        db_conn.execute(
            "INSERT INTO documents (filename, filepath, title, file_hash, raw_text) "
            "VALUES ('x.txt', '/x.txt', 'X', 'hashx', 'text')"
        )
        doc_id = db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        db_conn.execute(
            "INSERT INTO entities (name, type, canonical) VALUES ('Test Person', 'person', 'test person')"
        )
        ent_id = db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        db_conn.execute(
            "INSERT INTO document_entities (document_id, entity_id, count) VALUES (?, ?, 1)",
            (doc_id, ent_id),
        )
        db_conn.commit()

        # Delete document — junction row should cascade
        db_conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT * FROM document_entities WHERE document_id = ?", (doc_id,)
        ).fetchall()
        assert len(rows) == 0


class TestMainBlock:
    def test_main_guard(self, tmp_path, monkeypatch):
        """Running database.py as __main__ calls init_db()."""
        db_path = str(tmp_path / "main_test.db")
        monkeypatch.setenv("DOSSIER_DB", db_path)
        runpy.run_module("dossier.db.database", run_name="__main__")
        # Verify the DB was actually created
        conn = sqlite3.connect(db_path)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        conn.close()
        assert "documents" in tables
