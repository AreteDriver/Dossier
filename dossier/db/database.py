"""
DOSSIER — Database Layer
SQLite schema with FTS5 full-text search, entity tables, and keyword frequency tracking.
"""

import sqlite3
import os
from pathlib import Path
from contextlib import contextmanager

DB_PATH = os.environ.get("DOSSIER_DB", str(Path(__file__).parent.parent / "data" / "dossier.db"))


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables and indexes. Safe to call multiple times."""
    with get_db() as conn:
        conn.executescript("""
        -- ═══ DOCUMENTS ═══
        CREATE TABLE IF NOT EXISTS documents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename    TEXT NOT NULL,
            filepath    TEXT NOT NULL UNIQUE,
            title       TEXT,
            category    TEXT DEFAULT 'other',
            source      TEXT,
            date        TEXT,
            pages       INTEGER DEFAULT 0,
            file_hash   TEXT UNIQUE,
            raw_text    TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            flagged     BOOLEAN DEFAULT 0,
            notes       TEXT
        );

        -- ═══ FTS5 FULL-TEXT SEARCH INDEX ═══
        CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
            title,
            raw_text,
            category,
            source,
            content='documents',
            content_rowid='id',
            tokenize='porter unicode61'
        );

        -- Triggers to keep FTS in sync
        CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
            INSERT INTO documents_fts(rowid, title, raw_text, category, source)
            VALUES (new.id, new.title, new.raw_text, new.category, new.source);
        END;

        CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
            INSERT INTO documents_fts(documents_fts, rowid, title, raw_text, category, source)
            VALUES ('delete', old.id, old.title, old.raw_text, old.category, old.source);
        END;

        CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
            INSERT INTO documents_fts(documents_fts, rowid, title, raw_text, category, source)
            VALUES ('delete', old.id, old.title, old.raw_text, old.category, old.source);
            INSERT INTO documents_fts(rowid, title, raw_text, category, source)
            VALUES (new.id, new.title, new.raw_text, new.category, new.source);
        END;

        -- ═══ ENTITIES ═══
        CREATE TABLE IF NOT EXISTS entities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,  -- person, place, org, date
            canonical   TEXT,           -- normalized form for dedup
            UNIQUE(canonical, type)
        );

        -- ═══ DOCUMENT-ENTITY JUNCTION ═══
        CREATE TABLE IF NOT EXISTS document_entities (
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            entity_id   INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            count       INTEGER DEFAULT 1,  -- occurrences in this doc
            PRIMARY KEY (document_id, entity_id)
        );

        -- ═══ KEYWORDS ═══
        CREATE TABLE IF NOT EXISTS keywords (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            word        TEXT NOT NULL UNIQUE,
            total_count INTEGER DEFAULT 0,
            doc_count   INTEGER DEFAULT 0  -- how many docs contain this word
        );

        -- ═══ DOCUMENT-KEYWORD JUNCTION ═══
        CREATE TABLE IF NOT EXISTS document_keywords (
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            keyword_id  INTEGER NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,
            count       INTEGER DEFAULT 1,
            PRIMARY KEY (document_id, keyword_id)
        );

        -- ═══ ENTITY CONNECTIONS (co-occurrence) ═══
        CREATE TABLE IF NOT EXISTS entity_connections (
            entity_a_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            entity_b_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            weight      INTEGER DEFAULT 1,  -- number of docs both appear in
            PRIMARY KEY (entity_a_id, entity_b_id)
        );

        -- ═══ INDEXES ═══
        CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
        CREATE INDEX IF NOT EXISTS idx_entities_canonical ON entities(canonical);
        CREATE INDEX IF NOT EXISTS idx_doc_entities_doc ON document_entities(document_id);
        CREATE INDEX IF NOT EXISTS idx_doc_entities_entity ON document_entities(entity_id);
        CREATE INDEX IF NOT EXISTS idx_doc_keywords_doc ON document_keywords(document_id);
        CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category);
        CREATE INDEX IF NOT EXISTS idx_documents_date ON documents(date);
        CREATE INDEX IF NOT EXISTS idx_keywords_total ON keywords(total_count DESC);
        """)
        # Initialize forensics tables
        from dossier.forensics.timeline import init_timeline_tables

        init_timeline_tables(conn)
    print(f"[DB] Initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
