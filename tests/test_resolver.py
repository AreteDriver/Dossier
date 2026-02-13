"""
Tests for DOSSIER Entity Resolver Module.

Run: pytest tests/test_resolver.py -v
"""

import sqlite3

import pytest

from dossier.core.resolver import (
    EntityResolver,
    normalize_name,
    jaccard_similarity,
    initial_match,
    edit_distance_match,
    init_resolver_tables,
    HAS_RAPIDFUZZ,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def memory_db():
    """In-memory SQLite database with resolver tables + minimal entity schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            raw_text TEXT DEFAULT '',
            category TEXT DEFAULT '',
            source TEXT DEFAULT '',
            date TEXT DEFAULT ''
        );

        CREATE TABLE entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            canonical TEXT,
            UNIQUE(canonical, type)
        );

        CREATE TABLE document_entities (
            document_id INTEGER NOT NULL REFERENCES documents(id),
            entity_id INTEGER NOT NULL REFERENCES entities(id),
            count INTEGER DEFAULT 1,
            PRIMARY KEY (document_id, entity_id)
        );
    """)

    init_resolver_tables(conn)
    conn.commit()
    return conn


def _insert_entity(conn, name, etype="person", canonical=None):
    """Helper to insert an entity and return its ID.

    Uses the raw name as canonical by default (preserving case) to avoid
    UNIQUE(canonical, type) collisions in tests that need distinct rows
    for the same logical name.
    """
    canon = canonical if canonical is not None else name
    conn.execute(
        "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
        (name, etype, canon),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ═══════════════════════════════════════════════════════════════════
# Normalization
# ═══════════════════════════════════════════════════════════════════


class TestNormalization:
    def test_strip_titles(self):
        assert normalize_name("Dr. John Smith") == "john smith"
        assert normalize_name("Mr. James Bond") == "james bond"
        assert normalize_name("Mrs. Jane Doe") == "jane doe"

    def test_strip_suffixes(self):
        assert normalize_name("John Smith Jr.") == "john smith"
        assert normalize_name("James Bond III") == "james bond"
        assert normalize_name("Jane Doe, Esq.") == "jane doe"

    def test_last_first_format(self):
        assert normalize_name("Smith, John") == "john smith"
        assert normalize_name("Doe, Jane") == "jane doe"

    def test_punctuation_and_whitespace(self):
        assert normalize_name("  John   Smith  ") == "john smith"
        assert normalize_name("John.Smith") == "johnsmith"

    def test_combined(self):
        assert normalize_name("Dr. Smith, John Jr.") == "john smith"

    def test_empty_string(self):
        assert normalize_name("") == ""

    def test_single_name(self):
        assert normalize_name("Madonna") == "madonna"


# ═══════════════════════════════════════════════════════════════════
# Jaccard Similarity
# ═══════════════════════════════════════════════════════════════════


class TestJaccardSimilarity:
    def test_identical(self):
        assert jaccard_similarity("john smith", "john smith") == 1.0

    def test_no_overlap(self):
        assert jaccard_similarity("john smith", "jane doe") == 0.0

    def test_partial_overlap(self):
        score = jaccard_similarity("john smith", "john doe")
        assert 0.0 < score < 1.0
        # "john" shared, "smith"/"doe" not → 1/3
        assert abs(score - 1 / 3) < 0.01

    def test_empty(self):
        assert jaccard_similarity("", "john") == 0.0
        assert jaccard_similarity("john", "") == 0.0
        assert jaccard_similarity("", "") == 0.0


# ═══════════════════════════════════════════════════════════════════
# Initial Matching
# ═══════════════════════════════════════════════════════════════════


class TestInitialMatch:
    def test_initial_to_full(self):
        assert initial_match("J. Smith", "John Smith") is True

    def test_full_to_initial(self):
        assert initial_match("John Smith", "J Smith") is True

    def test_different_last_names(self):
        assert initial_match("J. Smith", "John Doe") is False

    def test_single_word(self):
        assert initial_match("John", "John Smith") is False

    def test_both_full(self):
        assert initial_match("John Smith", "James Smith") is False

    def test_both_initials_same(self):
        # J. Smith vs J. Smith — both have single-letter first token
        # Function returns True since "j" starts with "j"
        assert initial_match("J. Smith", "J. Smith") is True

    def test_different_initials(self):
        # J. Smith vs R. Smith — different initials
        assert initial_match("J. Smith", "R. Smith") is False


# ═══════════════════════════════════════════════════════════════════
# Edit Distance
# ═══════════════════════════════════════════════════════════════════


class TestEditDistance:
    @pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_typo_detection(self):
        result = edit_distance_match("john smithe", "john smithx")
        assert result is not None
        assert result > 0.5

    @pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_short_name_ignored(self):
        # Names ≤ 8 chars are skipped
        assert edit_distance_match("john", "jonh") is None

    @pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_too_distant(self):
        assert edit_distance_match("abcdefghij", "zyxwvutsrq") is None

    @pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_exact_match_long_name(self):
        result = edit_distance_match("john smithson", "john smithson")
        assert result is not None
        assert result == 0.80

    @pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_one_edit(self):
        result = edit_distance_match("john smithe", "john smithx")
        # distance=2 → 0.80 - 0.20 = 0.60
        # Let's check it's distance 2 actually
        from rapidfuzz.distance import Levenshtein

        dist = Levenshtein.distance("john smithe", "john smithx")
        expected = 0.80 - dist * 0.10
        assert result == pytest.approx(expected)


# ═══════════════════════════════════════════════════════════════════
# Entity Resolver
# ═══════════════════════════════════════════════════════════════════


class TestEntityResolver:
    def test_exact_match(self, memory_db):
        """'Smith, John' normalizes to 'john smith' — same as 'John Smith'."""
        _insert_entity(memory_db, "John Smith")
        _insert_entity(memory_db, "Smith, John")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        result = resolver.resolve_all()
        assert result.auto_merged >= 1

    def test_typo_match(self, memory_db):
        _insert_entity(memory_db, "John Alexander Smith")
        _insert_entity(memory_db, "John Alexannder Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        result = resolver.resolve_all()
        # Should find a match (edit distance or jaccard)
        assert len(result.matches) >= 1

    def test_no_cross_type(self, memory_db):
        """Entities of different types should not be matched."""
        _insert_entity(memory_db, "Palm Beach", "place")
        _insert_entity(memory_db, "Palm Beach", "org")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        result = resolver.resolve_all()
        # Same name but different type → resolver only compares same-type
        assert result.auto_merged == 0

    def test_resolve_all_empty(self, memory_db):
        resolver = EntityResolver(memory_db)
        result = resolver.resolve_all()
        assert result.entities_scanned == 0
        assert result.auto_merged == 0

    def test_resolve_all_by_type(self, memory_db):
        _insert_entity(memory_db, "John Smith", "person")
        _insert_entity(memory_db, "Smith, John", "person")
        _insert_entity(memory_db, "New York", "place")
        _insert_entity(memory_db, "NYC New York", "place")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        result = resolver.resolve_all(entity_type="person")
        # Only scanned person entities
        assert result.entities_scanned == 2
        assert result.auto_merged >= 1

    def test_resolve_entity_nonexistent(self, memory_db):
        resolver = EntityResolver(memory_db)
        matches = resolver.resolve_entity(9999)
        assert matches == []

    def test_resolve_single_entity(self, memory_db):
        id1 = _insert_entity(memory_db, "John Smith")
        _insert_entity(memory_db, "Smith, John")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        matches = resolver.resolve_entity(id1)
        assert len(matches) >= 1
        assert matches[0].strategy == "exact_canonical"


# ═══════════════════════════════════════════════════════════════════
# Merge and Split
# ═══════════════════════════════════════════════════════════════════


class TestMergeAndSplit:
    def test_manual_merge(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        success = resolver.merge_entities(id1, id2)
        assert success is True

        # Check resolution mapping
        canonical = resolver.get_canonical_id(id1)
        assert canonical == id2

    def test_alias_creation_on_merge(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        resolver.merge_entities(id1, id2)

        aliases = resolver.get_aliases(id2)
        assert "J. Smith" in aliases
        assert "John Smith" in aliases

    def test_split_removes_resolution(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        resolver.merge_entities(id1, id2)

        # Now split
        success = resolver.split_entity(id1, id2)
        assert success is True

        # Should be back to self
        canonical = resolver.get_canonical_id(id1)
        assert canonical == id1

    def test_split_nonexistent(self, memory_db):
        resolver = EntityResolver(memory_db)
        assert resolver.split_entity(999, 888) is False

    def test_audit_log(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        resolver.merge_entities(id1, id2)
        resolver.split_entity(id1, id2)

        logs = memory_db.execute("SELECT action FROM resolution_log ORDER BY id").fetchall()
        actions = [r["action"] for r in logs]
        assert "merge" in actions
        assert "split" in actions

    def test_merge_nonexistent(self, memory_db):
        resolver = EntityResolver(memory_db)
        assert resolver.merge_entities(999, 888) is False

    def test_get_duplicates_empty(self, memory_db):
        resolver = EntityResolver(memory_db)
        assert resolver.get_duplicates() == []

    def test_get_duplicates_after_merge(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        resolver.merge_entities(id1, id2)

        dupes = resolver.get_duplicates()
        assert len(dupes) == 1
        assert dupes[0]["source_entity_id"] == id1
        assert dupes[0]["canonical_entity_id"] == id2


# ═══════════════════════════════════════════════════════════════════
# Review Queue
# ═══════════════════════════════════════════════════════════════════


class TestReviewQueue:
    def test_approve(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.execute(
            "INSERT INTO resolution_queue (source_entity_id, target_entity_id, confidence, strategy) VALUES (?, ?, 0.70, 'initial_match')",
            (id1, id2),
        )
        memory_db.commit()

        queue_id = memory_db.execute("SELECT id FROM resolution_queue").fetchone()["id"]
        resolver = EntityResolver(memory_db)
        success = resolver.review_queue_item(queue_id, approve=True)
        assert success is True

        # Should be merged now
        canonical = resolver.get_canonical_id(id1)
        assert canonical == id2

        # Queue should be empty
        count = memory_db.execute("SELECT COUNT(*) as c FROM resolution_queue").fetchone()["c"]
        assert count == 0

    def test_reject(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Doe")
        memory_db.execute(
            "INSERT INTO resolution_queue (source_entity_id, target_entity_id, confidence, strategy) VALUES (?, ?, 0.65, 'jaccard')",
            (id1, id2),
        )
        memory_db.commit()

        queue_id = memory_db.execute("SELECT id FROM resolution_queue").fetchone()["id"]
        resolver = EntityResolver(memory_db)
        success = resolver.review_queue_item(queue_id, approve=False)
        assert success is True

        # Should NOT be merged
        canonical = resolver.get_canonical_id(id1)
        assert canonical == id1

        # Audit log should have reject action
        log = memory_db.execute(
            "SELECT action FROM resolution_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert log["action"] == "reject"

    def test_nonexistent(self, memory_db):
        resolver = EntityResolver(memory_db)
        assert resolver.review_queue_item(9999, approve=True) is False


# ═══════════════════════════════════════════════════════════════════
# Get Canonical ID
# ═══════════════════════════════════════════════════════════════════


class TestGetCanonicalId:
    def test_unresolved_returns_self(self, memory_db):
        id1 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        assert resolver.get_canonical_id(id1) == id1

    def test_resolved_returns_canonical(self, memory_db):
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        resolver.merge_entities(id1, id2)
        assert resolver.get_canonical_id(id1) == id2


# ═══════════════════════════════════════════════════════════════════
# Co-Occurrence Boost
# ═══════════════════════════════════════════════════════════════════


class TestCoOccurrence:
    def test_cooccurring_entities_get_boost(self, memory_db):
        """Entities in the same document should get a confidence boost."""
        # Create a document
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES ('Test Doc', 'test content')"
        )
        doc_id = memory_db.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create two entities with similar (not identical) names
        id1 = _insert_entity(memory_db, "J. Smith")
        id2 = _insert_entity(memory_db, "John Smith")

        # Link both to the same document
        memory_db.execute(
            "INSERT INTO document_entities (document_id, entity_id, count) VALUES (?, ?, 1)",
            (doc_id, id1),
        )
        memory_db.execute(
            "INSERT INTO document_entities (document_id, entity_id, count) VALUES (?, ?, 1)",
            (doc_id, id2),
        )
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        matches = resolver.resolve_entity(id1)
        assert len(matches) >= 1
        # The co-occurrence boost should increase confidence
        # Initial match gives 0.70 + type_match 0.10 + co-occur 0.10 = 0.90
        assert matches[0].confidence >= 0.85


# ═══════════════════════════════════════════════════════════════════
# Init Tables
# ═══════════════════════════════════════════════════════════════════


class TestInitResolverTables:
    def test_creates_tables(self, memory_db):
        tables = {
            r[0]
            for r in memory_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "entity_resolutions" in tables
        assert "entity_aliases" in tables
        assert "resolution_log" in tables
        assert "resolution_queue" in tables

    def test_idempotent(self, memory_db):
        """Calling init_resolver_tables twice should not raise."""
        init_resolver_tables(memory_db)
        tables = {
            r[0]
            for r in memory_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "entity_resolutions" in tables


# ═══════════════════════════════════════════════════════════════════
# Import Guard (rapidfuzz unavailable)
# ═══════════════════════════════════════════════════════════════════


class TestImportGuard:
    def test_edit_distance_without_rapidfuzz(self):
        """edit_distance_match returns None when rapidfuzz is unavailable."""
        import importlib
        from unittest.mock import patch

        import dossier.core.resolver as resolver_mod

        with patch.dict("sys.modules", {"rapidfuzz": None, "rapidfuzz.distance": None}):
            importlib.reload(resolver_mod)
            assert resolver_mod.HAS_RAPIDFUZZ is False
            assert resolver_mod.edit_distance_match("john smithe", "john smithx") is None

        # Restore
        importlib.reload(resolver_mod)
        assert resolver_mod.HAS_RAPIDFUZZ is True


# ═══════════════════════════════════════════════════════════════════
# Below-Threshold Returns None
# ═══════════════════════════════════════════════════════════════════


class TestBelowThreshold:
    def test_completely_different_names_return_no_match(self, memory_db):
        """Entities with completely different names produce no candidates."""
        _insert_entity(memory_db, "John Smith")
        _insert_entity(memory_db, "Jane Doe")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        matches = resolver.resolve_entity(1)
        assert matches == []


# ═══════════════════════════════════════════════════════════════════
# Jaccard Strategy
# ═══════════════════════════════════════════════════════════════════


class TestJaccardStrategy:
    def test_jaccard_hit(self, memory_db):
        """Jaccard strategy triggers for partial name overlap > 0.5."""
        # "Robert Smith" → normalized: {robert, smith}
        # "Robert James Smith" → normalized: {robert, james, smith}
        # Jaccard: {robert, smith} ∩ {robert, james, smith} = 2/3 = 0.67 > 0.5 ✓
        # Not exact canonical (2 vs 3 tokens), not initial match (both full first tokens,
        # last tokens match but first tokens are multi-char and identical → no initial)
        _insert_entity(memory_db, "Robert Smith")
        _insert_entity(memory_db, "Robert James Smith")
        memory_db.commit()

        resolver = EntityResolver(memory_db)
        matches = resolver.resolve_entity(1)
        assert len(matches) >= 1
        assert matches[0].strategy == "jaccard"
