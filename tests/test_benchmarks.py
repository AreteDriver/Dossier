"""Performance benchmarks for Dossier core operations.

Run: pytest tests/test_benchmarks.py --benchmark-only
"""

import sqlite3

import pytest

from dossier.core.graph_analysis import GraphAnalyzer
from dossier.core.ner import extract_entities
from dossier.core.resolver import EntityResolver, init_resolver_tables


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def sample_5k_text():
    """Generate a ~5K-word document with mixed entity types."""
    paragraphs = [
        (
            "Jeffrey Epstein met with Ghislaine Maxwell at the Palm Beach residence "
            "on January 15, 2008. The FBI launched an investigation into the matter "
            "while the Department of Justice reviewed the case. Representatives from "
            "JPMorgan Chase and Deutsche Bank were called to testify before Congress."
        ),
        (
            "The investigation expanded to include New York City, London, and the "
            "US Virgin Islands. Bill Clinton, Prince Andrew, and Alan Dershowitz "
            "were named in court documents filed in the Southern District of New York."
        ),
        (
            "According to the CIA report dated March 2019, the operation involved "
            "multiple agencies including Interpol and MI6. The United Nations Human "
            "Rights Council issued a statement condemning the actions."
        ),
        (
            "Senator John Smith met with Governor Jane Doe in Washington DC to discuss "
            "the implications. The National Security Agency provided surveillance data "
            "to the Federal Bureau of Investigation for the ongoing probe."
        ),
        (
            "Documents recovered from the Manhattan townhouse revealed connections to "
            "Deutsche Bank AG, Barclays, and Goldman Sachs. Financial records spanning "
            "from 2005 to 2019 were analyzed by forensic accountants in Miami."
        ),
    ]
    # Repeat to reach ~5K words
    return " ".join(paragraphs * 10)


@pytest.fixture
def bench_db(tmp_path):
    """In-memory SQLite database with full Dossier schema for benchmarking."""
    db_path = str(tmp_path / "bench.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL UNIQUE,
            title TEXT,
            category TEXT DEFAULT 'other',
            source TEXT,
            date TEXT,
            pages INTEGER DEFAULT 0,
            file_hash TEXT UNIQUE,
            raw_text TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            flagged BOOLEAN DEFAULT 0,
            notes TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
            title, raw_text, category, source,
            content='documents', content_rowid='id',
            tokenize='porter unicode61'
        );

        CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
            INSERT INTO documents_fts(rowid, title, raw_text, category, source)
            VALUES (new.id, new.title, new.raw_text, new.category, new.source);
        END;

        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            canonical TEXT,
            UNIQUE(canonical, type)
        );

        CREATE TABLE IF NOT EXISTS document_entities (
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            count INTEGER DEFAULT 1,
            PRIMARY KEY (document_id, entity_id)
        );

        CREATE TABLE IF NOT EXISTS entity_connections (
            entity_a_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            entity_b_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            weight INTEGER DEFAULT 1,
            PRIMARY KEY (entity_a_id, entity_b_id)
        );

        CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
        CREATE INDEX IF NOT EXISTS idx_entities_canonical ON entities(canonical);
    """)

    init_resolver_tables(conn)
    yield conn
    conn.close()


def _seed_documents(conn, count):
    """Insert N documents with FTS5 sync."""
    for i in range(count):
        conn.execute(
            "INSERT INTO documents (filename, filepath, title, raw_text, category, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                f"doc_{i}.txt",
                f"/data/doc_{i}.txt",
                f"Document {i} about financial investigation",
                f"This document discusses Jeffrey Epstein investigation number {i}. "
                f"The FBI and CIA coordinated with New York authorities. "
                f"Goldman Sachs and JPMorgan provided records for case {i}.",
                "legal" if i % 3 == 0 else "financial",
                f"source_{i % 10}",
            ),
        )
    conn.commit()


def _seed_entities(conn, count):
    """Insert N entities with connections forming a network."""
    people = [f"Person_{i}" for i in range(count // 3)]
    orgs = [f"Org_{i}" for i in range(count // 3)]
    places = [f"Place_{i}" for i in range(count - 2 * (count // 3))]

    for name in people:
        conn.execute(
            "INSERT OR IGNORE INTO entities (name, type, canonical) VALUES (?, 'person', ?)",
            (name, name.lower()),
        )
    for name in orgs:
        conn.execute(
            "INSERT OR IGNORE INTO entities (name, type, canonical) VALUES (?, 'org', ?)",
            (name, name.lower()),
        )
    for name in places:
        conn.execute(
            "INSERT OR IGNORE INTO entities (name, type, canonical) VALUES (?, 'place', ?)",
            (name, name.lower()),
        )

    # Create connections (star topology + some cross-links)
    entities = conn.execute("SELECT id FROM entities ORDER BY id").fetchall()
    entity_ids = [e["id"] for e in entities]
    for i in range(len(entity_ids) - 1):
        a, b = entity_ids[i], entity_ids[i + 1]
        conn.execute(
            "INSERT OR IGNORE INTO entity_connections (entity_a_id, entity_b_id, weight) "
            "VALUES (?, ?, ?)",
            (min(a, b), max(a, b), (i % 5) + 1),
        )
    # Add some cross-links for community structure
    for i in range(0, len(entity_ids) - 3, 3):
        a, b = entity_ids[i], entity_ids[i + 2]
        conn.execute(
            "INSERT OR IGNORE INTO entity_connections (entity_a_id, entity_b_id, weight) "
            "VALUES (?, ?, ?)",
            (min(a, b), max(a, b), 2),
        )
    conn.commit()


# ═══════════════════════════════════════════════════════════════════
# Benchmarks
# ═══════════════════════════════════════════════════════════════════


class TestNERBenchmark:
    """NER extraction performance on realistic document sizes."""

    def test_extract_entities_5k(self, benchmark, sample_5k_text):
        """Benchmark: extract_entities on a ~5K-word document."""
        result = benchmark(extract_entities, sample_5k_text)
        assert len(result["people"]) > 0
        assert len(result["orgs"]) > 0


class TestFTS5Benchmark:
    """FTS5 full-text search latency."""

    def test_fts5_search_500_docs(self, benchmark, bench_db):
        """Benchmark: FTS5 search against 500-doc index."""
        _seed_documents(bench_db, 500)

        def search():
            return bench_db.execute(
                "SELECT d.id, d.title, d.category "
                "FROM documents_fts fts "
                "JOIN documents d ON d.id = fts.rowid "
                "WHERE documents_fts MATCH ? "
                "ORDER BY rank LIMIT 20",
                ("investigation FBI",),
            ).fetchall()

        results = benchmark(search)
        assert len(results) > 0


class TestResolverBenchmark:
    """Entity resolver pair comparison performance."""

    def test_resolve_all_200_entities(self, benchmark, bench_db):
        """Benchmark: resolve_all with 200 entities (O(n^2) pairs)."""
        _seed_entities(bench_db, 200)
        resolver = EntityResolver(bench_db)
        result = benchmark(resolver.resolve_all)
        assert result.entities_scanned == 200


class TestGraphBenchmark:
    """Graph analysis computation benchmarks."""

    def test_centrality_betweenness_100(self, benchmark, bench_db):
        """Benchmark: betweenness centrality on 100-node graph."""
        _seed_entities(bench_db, 100)
        analyzer = GraphAnalyzer(bench_db)

        result = benchmark(analyzer.get_centrality, metric="betweenness", limit=10)
        assert len(result) > 0

    def test_communities_100(self, benchmark, bench_db):
        """Benchmark: community detection on 100-node graph."""
        _seed_entities(bench_db, 100)
        analyzer = GraphAnalyzer(bench_db)

        result = benchmark(analyzer.get_communities)
        assert len(result) > 0


class TestBulkInsertBenchmark:
    """Document ingestion throughput."""

    def test_bulk_insert_100_docs(self, benchmark, tmp_path):
        """Benchmark: 100 document inserts with FTS5 sync."""
        db_path = str(tmp_path / "bulk.db")

        def bulk_insert():
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL UNIQUE,
                    title TEXT,
                    category TEXT DEFAULT 'other',
                    source TEXT,
                    raw_text TEXT,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                    title, raw_text, category, source,
                    content='documents', content_rowid='id',
                    tokenize='porter unicode61'
                );
                CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
                    INSERT INTO documents_fts(rowid, title, raw_text, category, source)
                    VALUES (new.id, new.title, new.raw_text, new.category, new.source);
                END;
                DROP TABLE IF EXISTS documents;
                DROP TABLE IF EXISTS documents_fts;
            """)
            # Recreate fresh
            conn.executescript("""
                CREATE TABLE documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL UNIQUE,
                    title TEXT,
                    category TEXT DEFAULT 'other',
                    source TEXT,
                    raw_text TEXT,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE VIRTUAL TABLE documents_fts USING fts5(
                    title, raw_text, category, source,
                    content='documents', content_rowid='id',
                    tokenize='porter unicode61'
                );
                CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
                    INSERT INTO documents_fts(rowid, title, raw_text, category, source)
                    VALUES (new.id, new.title, new.raw_text, new.category, new.source);
                END;
            """)
            for i in range(100):
                conn.execute(
                    "INSERT INTO documents (filename, filepath, title, raw_text, category, source) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        f"d{i}.txt",
                        f"/d{i}.txt",
                        f"Doc {i}",
                        f"Content for document {i} with searchable text.",
                        "legal",
                        "test",
                    ),
                )
            conn.commit()
            conn.close()

        benchmark(bulk_insert)
