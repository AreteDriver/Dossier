"""
Tests for DOSSIER Timeline Reconstruction Module.

Run: pytest tests/test_timeline.py -v
"""

import sqlite3
import pytest
from dossier.forensics.timeline import (
    TimelineExtractor,
    DatePrecision,
    split_sentences,
    init_timeline_tables,
    store_events,
    query_timeline,
    get_timeline_stats,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def extractor():
    """Basic extractor with no entity linking."""
    return TimelineExtractor()


@pytest.fixture
def extractor_with_entities():
    """Extractor with known entity names for linking."""
    return TimelineExtractor(
        entity_names=[
            "Jeffrey Epstein",
            "Ghislaine Maxwell",
            "Palm Beach",
            "New York",
            "Jane Doe",
        ]
    )


@pytest.fixture
def memory_db():
    """In-memory SQLite database with timeline tables + minimal document schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Minimal schema matching DOSSIER's documents + entities tables
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

    init_timeline_tables(conn)
    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════════════
# Date Extraction Tests
# ═══════════════════════════════════════════════════════════════════


class TestDateExtraction:
    """Test that various date formats are correctly identified."""

    def test_full_date_month_day_year(self, extractor):
        dates = extractor.extract_dates("The meeting occurred on March 14, 2009.")
        assert len(dates) == 1
        assert dates[0].iso_date == "2009-03-14"
        assert dates[0].precision == DatePrecision.DAY
        assert dates[0].confidence >= 0.9

    def test_full_date_with_ordinal(self, extractor):
        dates = extractor.extract_dates("She arrived on January 3rd, 2005.")
        assert len(dates) == 1
        assert dates[0].iso_date == "2005-01-03"

    def test_european_date_format(self, extractor):
        dates = extractor.extract_dates("On 14 March 2009 the deposition began.")
        assert len(dates) == 1
        assert dates[0].iso_date == "2009-03-14"

    def test_numeric_date_slash(self, extractor):
        dates = extractor.extract_dates("Filed on 03/14/2009.")
        assert len(dates) == 1
        assert dates[0].iso_date == "2009-03-14"

    def test_iso_format(self, extractor):
        dates = extractor.extract_dates("Record dated 2009-03-14.")
        assert len(dates) == 1
        assert dates[0].iso_date == "2009-03-14"

    def test_two_digit_year(self, extractor):
        dates = extractor.extract_dates("Logged on 3/14/09.")
        assert len(dates) == 1
        assert dates[0].confidence < 0.9  # Lower confidence for ambiguous year

    def test_month_year_only(self, extractor):
        dates = extractor.extract_dates("In approximately March 2003, he traveled to London.")
        # Should get the approximate match, not just month
        assert len(dates) >= 1
        found_month = any(
            d.precision in (DatePrecision.MONTH, DatePrecision.APPROXIMATE) for d in dates
        )
        assert found_month

    def test_season_date(self, extractor):
        dates = extractor.extract_dates("She first visited in Spring 2001.")
        assert len(dates) >= 1
        season_dates = [d for d in dates if d.precision == DatePrecision.SEASON]
        assert len(season_dates) == 1
        assert "2001" in season_dates[0].iso_date

    def test_year_only_with_context(self, extractor):
        dates = extractor.extract_dates("He was employed there since 2003.")
        assert len(dates) >= 1
        year_dates = [d for d in dates if d.precision == DatePrecision.YEAR]
        assert len(year_dates) == 1
        assert year_dates[0].iso_date == "2003"

    def test_approximate_date(self, extractor):
        dates = extractor.extract_dates("I met him approximately 2001.")
        assert len(dates) >= 1
        approx = [d for d in dates if d.precision == DatePrecision.APPROXIMATE]
        assert len(approx) == 1
        assert approx[0].confidence <= 0.6

    def test_multiple_dates(self, extractor):
        text = "From January 5, 2003 through March 14, 2009, the investigation continued."
        dates = extractor.extract_dates(text)
        assert len(dates) == 2
        iso_dates = [d.iso_date for d in dates]
        assert "2003-01-05" in iso_dates
        assert "2009-03-14" in iso_dates

    def test_no_false_positive_on_numbers(self, extractor):
        """Should not match random 4-digit numbers as years."""
        dates = extractor.extract_dates("The total was 4500 units at warehouse 2100.")
        # These should NOT be matched since they lack context words
        year_dates = [d for d in dates if d.precision == DatePrecision.YEAR]
        assert len(year_dates) == 0


# ═══════════════════════════════════════════════════════════════════
# Relative Date Tests
# ═══════════════════════════════════════════════════════════════════


class TestRelativeDates:
    """Test that relative dates are flagged but not resolved."""

    def test_following_day(self, extractor):
        dates = extractor.extract_dates("The following Tuesday she returned.")
        relative = [d for d in dates if d.precision == DatePrecision.RELATIVE]
        assert len(relative) == 1
        assert relative[0].iso_date is None

    def test_weeks_later(self, extractor):
        dates = extractor.extract_dates("Two weeks later the charges were filed.")
        relative = [d for d in dates if d.precision == DatePrecision.RELATIVE]
        assert len(relative) == 1

    def test_shortly_after(self, extractor):
        dates = extractor.extract_dates("Shortly after, the documents were sealed.")
        relative = [d for d in dates if d.precision == DatePrecision.RELATIVE]
        assert len(relative) == 1

    def test_same_day(self, extractor):
        dates = extractor.extract_dates("That same day, the warrant was issued.")
        relative = [d for d in dates if d.precision == DatePrecision.RELATIVE]
        assert len(relative) == 1


# ═══════════════════════════════════════════════════════════════════
# Event Extraction Tests
# ═══════════════════════════════════════════════════════════════════


class TestEventExtraction:
    """Test full event construction with context and entities."""

    def test_event_has_context(self, extractor):
        text = "On March 14, 2009, the deposition of Jane Doe 3 was recorded."
        events = extractor.extract_events(text, document_id=1)
        assert len(events) == 1
        assert "deposition" in events[0].context.lower()
        assert events[0].document_id == 1

    def test_event_links_entities(self, extractor_with_entities):
        text = "Jeffrey Epstein met Ghislaine Maxwell in Palm Beach on March 5, 2003."
        events = extractor_with_entities.extract_events(text, document_id=1)
        assert len(events) >= 1
        entities = events[0].entities
        assert "Jeffrey Epstein" in entities
        assert "Ghislaine Maxwell" in entities
        assert "Palm Beach" in entities

    def test_multiple_events_from_paragraph(self, extractor_with_entities):
        text = (
            "On January 5, 2003, Ghislaine Maxwell arranged the first meeting. "
            "Jeffrey Epstein arrived in Palm Beach on January 7, 2003. "
            "The following week, they traveled to New York."
        )
        events = extractor_with_entities.extract_events(text, document_id=1)
        resolved = [e for e in events if e.is_resolved]
        unresolved = [e for e in events if not e.is_resolved]
        assert len(resolved) >= 2
        assert len(unresolved) >= 1  # "the following week"

    def test_event_confidence_propagates(self, extractor):
        text = "It happened around 2003. On March 14, 2009, the record was filed."
        events = extractor.extract_events(text, document_id=1)
        # The exact date should have higher confidence than the approximate one
        confidences = {e.date: e.confidence for e in events if e.date}
        if "2003" in confidences and "2009-03-14" in confidences:
            assert confidences["2009-03-14"] > confidences["2003"]


# ═══════════════════════════════════════════════════════════════════
# Sentence Splitting Tests
# ═══════════════════════════════════════════════════════════════════


class TestSentenceSplitter:
    def test_basic_split(self):
        sents = split_sentences("First sentence. Second sentence. Third.")
        assert len(sents) == 3

    def test_preserves_abbreviations(self):
        sents = split_sentences("Dr. Smith arrived. Mr. Jones was already there.")
        # Should not split on "Dr." or "Mr."
        texts = [s[0] for s in sents]
        assert any("Dr." in t and "Smith" in t for t in texts)

    def test_returns_offsets(self):
        text = "Hello world. Goodbye world."
        sents = split_sentences(text)
        for sent_text, start, end in sents:
            assert text[start:end].strip() == sent_text or sent_text in text[start:end]


# ═══════════════════════════════════════════════════════════════════
# Database Integration Tests
# ═══════════════════════════════════════════════════════════════════


class TestDatabaseOperations:
    def test_store_and_query_events(self, memory_db):
        # Insert a test document
        memory_db.execute(
            "INSERT INTO documents (title, raw_text, category, source) VALUES (?, ?, ?, ?)",
            ("Test Doc", "Sample text", "report", "test"),
        )

        # Insert a test entity
        memory_db.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("John Smith", "person", "john smith"),
        )
        memory_db.commit()

        # Extract and store
        extractor = TimelineExtractor(entity_names=["John Smith"])
        text = "John Smith met with officials on March 14, 2009 in Washington."
        events = extractor.extract_events(text, document_id=1)
        event_ids = store_events(memory_db, events)
        memory_db.commit()

        assert len(event_ids) >= 1

        # Query back
        results = query_timeline(memory_db, start_date="2009-01-01", end_date="2009-12-31")
        assert len(results) >= 1
        assert results[0]["event_date"] == "2009-03-14"

    def test_query_by_entity(self, memory_db):
        # Setup
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES (?, ?)", ("Doc 1", "Text")
        )
        memory_db.execute(
            "INSERT INTO entities (name, type, canonical) VALUES (?, ?, ?)",
            ("Jane Doe", "person", "jane doe"),
        )
        memory_db.commit()

        extractor = TimelineExtractor(entity_names=["Jane Doe"])
        events = extractor.extract_events("Jane Doe testified on April 5, 2010.", document_id=1)
        store_events(memory_db, events)
        memory_db.commit()

        # Query filtered by entity
        results = query_timeline(memory_db, entity_name="Jane Doe")
        assert len(results) >= 1

    def test_timeline_stats(self, memory_db):
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES (?, ?)", ("Doc 1", "Text")
        )
        memory_db.commit()

        extractor = TimelineExtractor()
        events = extractor.extract_events(
            "Meeting on March 14, 2009. Follow-up in June 2009. The following week was busy.",
            document_id=1,
        )
        store_events(memory_db, events)
        memory_db.commit()

        stats = get_timeline_stats(memory_db)
        assert stats["total_events"] >= 2
        assert stats["unresolved_events"] >= 1
        assert "day" in stats["by_precision"] or "month" in stats["by_precision"]


# ═══════════════════════════════════════════════════════════════════
# Edge Cases
# ═══════════════════════════════════════════════════════════════════


class TestEdgeCases:
    def test_empty_text(self, extractor):
        events = extractor.extract_events("", document_id=1)
        assert events == []

    def test_no_dates(self, extractor):
        events = extractor.extract_events(
            "The witness declined to answer any further questions.", document_id=1
        )
        assert events == []

    def test_overlapping_patterns_dedup(self, extractor):
        """March 14, 2009 should match once, not as both 'March 14, 2009' and 'March 2009'."""
        dates = extractor.extract_dates("On March 14, 2009 the hearing began.")
        # Should have exactly 1 match for the full date
        day_dates = [d for d in dates if d.precision == DatePrecision.DAY]
        assert len(day_dates) == 1

    def test_deposition_header_extraction(self, extractor_with_entities):
        """Real-world test: extract dates from a deposition header."""
        text = """
        DEPOSITION OF JANE DOE 3
        Case No. 08-80736-CIV
        Southern District of New York
        March 14, 2009

        Q. When did you first meet Jeffrey Epstein?
        A. In approximately 2001, Ghislaine Maxwell introduced me
           at his Palm Beach residence.
        """
        events = extractor_with_entities.extract_events(text, document_id=1)
        assert len(events) >= 2  # March 14, 2009 + approximately 2001

        # Entities should be linked when they appear in the same sentence as a date
        all_entities = set()
        for e in events:
            all_entities.update(e.entities)
        # "approximately 2001" is in the same sentence as Ghislaine Maxwell and Palm Beach
        assert "Ghislaine Maxwell" in all_entities or "Palm Beach" in all_entities


class TestRelativeDateOverlap:
    """Line 229: relative date overlapping with an already-seen explicit date span."""

    def test_relative_overlapping_explicit_is_skipped(self, extractor):
        """'the following March 5, 2003' — 'the following' overlaps the full date span."""
        dates = extractor.extract_dates("He returned the following day on March 5, 2003.")
        day_dates = [d for d in dates if d.precision == DatePrecision.DAY]
        assert len(day_dates) == 1
        # The key invariant: no duplicate spans
        spans = [(d.char_start, d.char_end) for d in dates]
        assert len(spans) == len(set(spans))


class TestSentenceNotFound:
    """Line 294: _find_containing_sentence returns None."""

    def test_date_outside_all_sentences(self, extractor):
        """Date char offsets that don't fall within any sentence boundary."""
        events = extractor.extract_events("March 2003", document_id=1)
        # Should still extract the date even if sentence lookup fails
        assert len(events) >= 1


class TestNormalizationFallback:
    """Lines 348-353: ParserError fallback extracts year."""

    def test_unparseable_day_date_falls_back_to_year(self, extractor):
        """A date matching DAY pattern but dateutil can't parse — falls back to year."""
        from dossier.forensics.timeline import DatePrecision

        # Directly test _normalize_to_iso with an unparseable DAY-precision string
        result = extractor._normalize_to_iso("32/13/2007", DatePrecision.DAY)
        # dateutil will raise ParserError, fallback extracts "2007"
        assert result == "2007"

    def test_normalize_no_year_returns_none(self, extractor):
        """If ParserError and no year in string, returns None."""
        result = extractor._normalize_to_iso("not a date at all", DatePrecision.DAY)
        assert result is None


class TestRelativeDateOverlapSkip:
    """Line 228: relative date overlapping an already-captured explicit date is skipped."""

    def test_relative_overlap_with_explicit_skipped(self, extractor):
        # "in 2003" matches YEAR pattern (spans ~0-7).
        # "2003 years later" matches relative pattern (spans ~3-19) — overlaps.
        # The relative match must be skipped via the continue on line 228.
        dates = extractor.extract_dates("in 2003 years later")
        year_dates = [d for d in dates if d.precision == DatePrecision.YEAR]
        assert len(year_dates) == 1
        assert year_dates[0].iso_date == "2003"
        relative_dates = [d for d in dates if d.precision == DatePrecision.RELATIVE]
        assert len(relative_dates) == 0


class TestFindContainingSentenceNone:
    """Line 298: _find_containing_sentence returns None for out-of-range offsets."""

    def test_date_outside_sentence_boundaries(self):
        from dossier.forensics.timeline import ExtractedDate

        date = ExtractedDate(
            raw_text="March 2003",
            iso_date="2003-03",
            precision=DatePrecision.MONTH,
            confidence=0.8,
            char_start=100,
            char_end=110,
        )
        sentences = [("First sentence.", 0, 15), ("Second sentence.", 20, 36)]
        result = TimelineExtractor._find_containing_sentence(date, sentences)
        assert result is None


class TestNormalizeEdgeCases:
    """Lines 315, 335: _normalize_to_iso edge cases."""

    def test_relative_precision_returns_none(self, extractor):
        """Line 315: RELATIVE precision short-circuits to None."""
        result = extractor._normalize_to_iso("the following day", DatePrecision.RELATIVE)
        assert result is None

    def test_season_no_regex_match_returns_none(self, extractor):
        """Line 335: SEASON precision with no season word returns None."""
        result = extractor._normalize_to_iso("no season here", DatePrecision.SEASON)
        assert result is None


class TestQueryFilters:
    """Lines 475-476, 487-488: min_confidence and document_id query filters."""

    def test_query_min_confidence_filter(self, memory_db):
        """Events below confidence threshold are excluded."""
        init_timeline_tables(memory_db)
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES (?, ?)",
            ("Test doc", "Some text"),
        )
        memory_db.execute("""
            INSERT INTO events (document_id, event_date, date_raw, precision,
                              confidence, context, is_resolved, source_start, source_end)
            VALUES (1, '2009-03-14', 'March 14, 2009', 'day', 0.95, 'context', 1, 0, 10)
        """)
        memory_db.execute("""
            INSERT INTO events (document_id, event_date, date_raw, precision,
                              confidence, context, is_resolved, source_start, source_end)
            VALUES (1, '2005', 'approximately 2005', 'approx', 0.40, 'context', 1, 0, 10)
        """)
        memory_db.commit()

        results = query_timeline(memory_db, min_confidence=0.8)
        assert len(results) == 1
        assert results[0]["confidence"] >= 0.8

    def test_query_document_id_filter(self, memory_db):
        """Events from other documents are excluded."""
        init_timeline_tables(memory_db)
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES (?, ?)",
            ("Doc A", "text"),
        )
        memory_db.execute(
            "INSERT INTO documents (title, raw_text) VALUES (?, ?)",
            ("Doc B", "text"),
        )
        memory_db.execute("""
            INSERT INTO events (document_id, event_date, date_raw, precision,
                              confidence, context, is_resolved, source_start, source_end)
            VALUES (1, '2009-03-14', 'March 14', 'day', 0.95, 'ctx', 1, 0, 10)
        """)
        memory_db.execute("""
            INSERT INTO events (document_id, event_date, date_raw, precision,
                              confidence, context, is_resolved, source_start, source_end)
            VALUES (2, '2019-07-06', 'July 6', 'day', 0.95, 'ctx', 1, 0, 10)
        """)
        memory_db.commit()

        results = query_timeline(memory_db, document_id=1)
        assert len(results) == 1
        assert results[0]["document_id"] == 1
