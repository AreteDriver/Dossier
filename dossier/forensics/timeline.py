"""
DOSSIER — Timeline Reconstruction Module

Extracts dates from document text, normalizes to ISO format, constructs
structured events with entity linkage and confidence scoring.

Usage:
    from dossier.forensics.timeline import TimelineExtractor

    extractor = TimelineExtractor()
    events = extractor.extract_events(document_text, document_id=42)

Architecture notes:
    - Explicit dates (March 5, 2003) are extracted with high confidence.
    - Partial dates (March 2003, Spring 2001) are stored with reduced confidence
      and a precision indicator (day/month/year/season).
    - Relative dates ("two weeks later", "the following Tuesday") are flagged
      but NOT auto-resolved — that requires document-level context and is
      error-prone. They're stored as unresolved references for manual review.
    - Each event captures: the normalized date, the source sentence, confidence,
      precision, and any co-occurring entities from the same sentence.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from dateutil import parser as dateutil_parser
from dateutil.parser import ParserError


# ═══════════════════════════════════════════════════════════════════
# Data Structures
# ═══════════════════════════════════════════════════════════════════


class DatePrecision(Enum):
    """How specific the extracted date is."""

    DAY = "day"  # March 5, 2003
    MONTH = "month"  # March 2003
    YEAR = "year"  # 2003
    SEASON = "season"  # Spring 2001
    APPROXIMATE = "approx"  # "approximately 2001", "around 2003"
    RELATIVE = "relative"  # "two weeks later", "the following day"


@dataclass
class ExtractedDate:
    """A single date mention found in text."""

    raw_text: str  # Original text matched
    iso_date: Optional[str]  # Normalized ISO date (None for relative)
    precision: DatePrecision
    confidence: float  # 0.0 - 1.0
    char_start: int  # Position in source text
    char_end: int


@dataclass
class TimelineEvent:
    """A structured event reconstructed from a document."""

    document_id: int
    date: Optional[str]  # ISO date string (None for unresolved relative)
    date_raw: str  # Original date text
    precision: DatePrecision
    confidence: float
    context: str  # The sentence containing the date
    entities: list[str] = field(default_factory=list)  # Co-occurring entities
    is_resolved: bool = True  # False for relative dates needing manual review
    source_char_start: int = 0
    source_char_end: int = 0


# ═══════════════════════════════════════════════════════════════════
# Date Extraction Patterns
# ═══════════════════════════════════════════════════════════════════

# Months for regex
_MONTHS = (
    r"(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
)

_ORDINALS = r"(?:st|nd|rd|th)"

# Ordered by specificity — more specific patterns first
DATE_PATTERNS: list[tuple[str, DatePrecision, float]] = [
    # ── Full dates (high confidence) ──
    # March 14, 2009 / March 14th, 2009
    (rf"({_MONTHS})\s+(\d{{1,2}}){_ORDINALS}?,?\s+(\d{{4}})", DatePrecision.DAY, 0.95),
    # 14 March 2009 / 14th March 2009
    (rf"(\d{{1,2}}){_ORDINALS}?\s+({_MONTHS}),?\s+(\d{{4}})", DatePrecision.DAY, 0.95),
    # 03/14/2009, 3/14/2009, 03-14-2009
    (r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", DatePrecision.DAY, 0.85),
    # 2009-03-14 (ISO format)
    (r"(\d{4})-(\d{2})-(\d{2})", DatePrecision.DAY, 0.95),
    # 03/14/09, 3/14/09 (two-digit year — lower confidence)
    (r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2})(?!\d)", DatePrecision.DAY, 0.70),
    # ── Partial dates (medium confidence) ──
    # March 2009
    (rf"({_MONTHS}),?\s+(\d{{4}})", DatePrecision.MONTH, 0.80),
    # Spring/Summer/Fall/Winter 2009
    (r"(Spring|Summer|Fall|Autumn|Winter)\s+(?:of\s+)?(\d{4})", DatePrecision.SEASON, 0.70),
    # ── Year only ──
    # "in 2003", "during 2003" — only match with context words to avoid false positives
    (r"(?:in|during|around|circa|since|before|after|by)\s+(\d{4})(?!\d)", DatePrecision.YEAR, 0.60),
    # ── Approximate dates ──
    # "approximately 2001", "around March 2003"
    (
        rf"(?:approximately|approx\.?|around|circa|roughly)\s+(?:({_MONTHS})\s+)?(\d{{4}})",
        DatePrecision.APPROXIMATE,
        0.50,
    ),
]

# Relative date patterns — flagged, not resolved
RELATIVE_PATTERNS: list[str] = [
    r"(?:the\s+)?(?:following|next|previous|prior)\s+(?:day|week|month|year|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)",
    r"(?:\d+|two|three|four|five|six|seven|eight|nine|ten)\s+(?:days?|weeks?|months?|years?)\s+(?:later|earlier|before|after|prior)",
    r"(?:the\s+)?(?:same|that)\s+(?:day|week|month|year|evening|morning|afternoon|night)",
    r"(?:shortly|soon|immediately)\s+(?:after|before|thereafter|prior)",
]


# ═══════════════════════════════════════════════════════════════════
# Sentence Splitter
# ═══════════════════════════════════════════════════════════════════


def split_sentences(text: str) -> list[tuple[str, int, int]]:
    """
    Split text into sentences with character offsets.
    Returns list of (sentence_text, start_char, end_char).

    Uses a simple but effective regex approach. Not perfect for
    abbreviations (Dr., Mr., etc.) but good enough for legal/investigative
    documents which tend to have clean sentence boundaries.
    """
    # Handle common abbreviations that shouldn't trigger splits
    protected = text
    abbrevs = ["Mr.", "Mrs.", "Ms.", "Dr.", "Jr.", "Sr.", "St.", "vs.", "No.", "Vol."]
    for abbr in abbrevs:
        protected = protected.replace(abbr, abbr.replace(".", "§"))

    sentences = []
    for match in re.finditer(r"[^.!?\n]+[.!?\n]+\s*|[^.!?\n]+$", protected):
        sent = match.group().replace("§", ".")
        start = match.start()
        # Map back to original text position
        sent_original = text[start : start + len(sent)]
        sentences.append((sent_original.strip(), start, start + len(sent)))

    return sentences if sentences else [(text.strip(), 0, len(text))]


# ═══════════════════════════════════════════════════════════════════
# Core Extractor
# ═══════════════════════════════════════════════════════════════════


class TimelineExtractor:
    """
    Extracts timeline events from document text.

    Flow:
        1. Split text into sentences
        2. Run date patterns against each sentence
        3. Normalize matched dates to ISO format
        4. Detect relative date references
        5. Build TimelineEvent objects with context and entity linkage
    """

    def __init__(self, entity_names: Optional[list[str]] = None):
        """
        Args:
            entity_names: Optional list of known entity names to link
                         when found in the same sentence as a date.
                         If None, entity linking is skipped.
        """
        self.entity_names = entity_names or []
        # Pre-compile patterns for performance
        self._date_patterns = [
            (re.compile(pattern, re.IGNORECASE), precision, confidence)
            for pattern, precision, confidence in DATE_PATTERNS
        ]
        self._relative_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in RELATIVE_PATTERNS
        ]

    def extract_dates(self, text: str) -> list[ExtractedDate]:
        """
        Extract all date mentions from text.
        Returns dates ordered by position in text.
        """
        dates: list[ExtractedDate] = []
        seen_spans: set[tuple[int, int]] = set()  # Prevent overlapping matches

        for regex, precision, confidence in self._date_patterns:
            for match in regex.finditer(text):
                span = (match.start(), match.end())

                # Skip if this span overlaps with an already-captured date
                if any(self._spans_overlap(span, s) for s in seen_spans):
                    continue

                raw = match.group()
                iso = self._normalize_to_iso(raw, precision)

                dates.append(
                    ExtractedDate(
                        raw_text=raw.strip(),
                        iso_date=iso,
                        precision=precision,
                        confidence=confidence,
                        char_start=match.start(),
                        char_end=match.end(),
                    )
                )
                seen_spans.add(span)

        # Check for relative dates
        for regex in self._relative_patterns:
            for match in regex.finditer(text):
                span = (match.start(), match.end())
                if any(self._spans_overlap(span, s) for s in seen_spans):
                    continue

                dates.append(
                    ExtractedDate(
                        raw_text=match.group().strip(),
                        iso_date=None,
                        precision=DatePrecision.RELATIVE,
                        confidence=0.30,
                        char_start=match.start(),
                        char_end=match.end(),
                    )
                )
                seen_spans.add(span)

        return sorted(dates, key=lambda d: d.char_start)

    def extract_events(self, text: str, document_id: int = 0) -> list[TimelineEvent]:
        """
        Extract structured timeline events from document text.

        Each event links a date to its surrounding sentence context
        and any co-occurring entities.
        """
        sentences = split_sentences(text)
        dates = self.extract_dates(text)
        events: list[TimelineEvent] = []

        for date in dates:
            # Find the sentence containing this date
            context_sent = self._find_containing_sentence(date, sentences)
            context_text = (
                context_sent[0]
                if context_sent
                else text[max(0, date.char_start - 100) : date.char_end + 100]
            )

            # Find entities in the same sentence
            cooccurring = self._find_entities_in_text(context_text)

            events.append(
                TimelineEvent(
                    document_id=document_id,
                    date=date.iso_date,
                    date_raw=date.raw_text,
                    precision=date.precision,
                    confidence=date.confidence,
                    context=context_text.strip(),
                    entities=cooccurring,
                    is_resolved=(date.precision != DatePrecision.RELATIVE),
                    source_char_start=date.char_start,
                    source_char_end=date.char_end,
                )
            )

        return events

    # ── Internal helpers ──

    @staticmethod
    def _spans_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
        return a[0] < b[1] and b[0] < a[1]

    @staticmethod
    def _find_containing_sentence(
        date: ExtractedDate, sentences: list[tuple[str, int, int]]
    ) -> Optional[tuple[str, int, int]]:
        """Find the sentence that contains the given date mention."""
        for sent_text, start, end in sentences:
            if start <= date.char_start and date.char_end <= end:
                return (sent_text, start, end)
        return None

    def _find_entities_in_text(self, text: str) -> list[str]:
        """Find known entity names that appear in the given text."""
        text_lower = text.lower()
        return [name for name in self.entity_names if name.lower() in text_lower]

    def _normalize_to_iso(self, raw: str, precision: DatePrecision) -> Optional[str]:
        """
        Normalize a date string to ISO 8601 format.

        For DAY precision: YYYY-MM-DD
        For MONTH precision: YYYY-MM
        For YEAR/SEASON/APPROXIMATE: YYYY
        For RELATIVE: None (cannot resolve without context)
        """
        if precision == DatePrecision.RELATIVE:
            return None

        # Season mapping
        season_months = {
            "spring": "03",
            "summer": "06",
            "fall": "09",
            "autumn": "09",
            "winter": "12",
        }

        try:
            if precision == DatePrecision.SEASON:
                match = re.search(
                    r"(Spring|Summer|Fall|Autumn|Winter)\s+(?:of\s+)?(\d{4})", raw, re.IGNORECASE
                )
                if match:
                    season = match.group(1).lower()
                    year = match.group(2)
                    return f"{year}-{season_months[season]}"
                return None

            if precision == DatePrecision.YEAR:
                match = re.search(r"(\d{4})", raw)
                return match.group(1) if match else None

            if precision == DatePrecision.APPROXIMATE:
                match = re.search(r"(\d{4})", raw)
                return match.group(1) if match else None

            if precision == DatePrecision.MONTH:
                # dateutil handles "March 2009" well
                parsed = dateutil_parser.parse(raw, default=None, fuzzy=True)
                return parsed.strftime("%Y-%m") if parsed else None

            # DAY precision — full parse
            parsed = dateutil_parser.parse(raw, fuzzy=True)
            return parsed.strftime("%Y-%m-%d")

        except (ParserError, ValueError, OverflowError):
            # Fallback: try to extract any year at minimum
            year_match = re.search(r"(\d{4})", raw)
            if year_match:
                return year_match.group(1)
            return None


# ═══════════════════════════════════════════════════════════════════
# Database Schema Extension
# ═══════════════════════════════════════════════════════════════════

TIMELINE_SCHEMA = """
    -- ═══ TIMELINE EVENTS ═══
    CREATE TABLE IF NOT EXISTS events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        event_date      TEXT,               -- ISO date (NULL for unresolved relative)
        date_raw        TEXT NOT NULL,       -- Original text matched
        precision       TEXT NOT NULL,       -- day/month/year/season/approx/relative
        confidence      REAL NOT NULL,       -- 0.0 - 1.0
        context         TEXT NOT NULL,       -- Sentence containing the date
        is_resolved     INTEGER DEFAULT 1,   -- 0 = relative date needing manual review
        source_start    INTEGER DEFAULT 0,   -- Char offset in original document
        source_end      INTEGER DEFAULT 0,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ═══ EVENT-ENTITY JUNCTION ═══
    -- Links entities to events they co-occur with
    CREATE TABLE IF NOT EXISTS event_entities (
        event_id    INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
        entity_id   INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
        role        TEXT DEFAULT 'mentioned',  -- mentioned/subject/location/org
        PRIMARY KEY (event_id, entity_id)
    );

    -- ═══ INDEXES ═══
    CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);
    CREATE INDEX IF NOT EXISTS idx_events_document ON events(document_id);
    CREATE INDEX IF NOT EXISTS idx_events_precision ON events(precision);
    CREATE INDEX IF NOT EXISTS idx_events_unresolved ON events(is_resolved) WHERE is_resolved = 0;
"""


def init_timeline_tables(conn):
    """Add timeline tables to an existing DOSSIER database."""
    conn.executescript(TIMELINE_SCHEMA)
    conn.commit()


# ═══════════════════════════════════════════════════════════════════
# Database Operations
# ═══════════════════════════════════════════════════════════════════


def store_events(conn, events: list[TimelineEvent]) -> list[int]:
    """
    Store extracted timeline events in the database.
    Returns list of inserted event IDs.

    Also links co-occurring entities if they exist in the entities table.
    """
    event_ids = []

    for event in events:
        cursor = conn.execute(
            """
            INSERT INTO events (document_id, event_date, date_raw, precision,
                              confidence, context, is_resolved, source_start, source_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                event.document_id,
                event.date,
                event.date_raw,
                event.precision.value,
                event.confidence,
                event.context,
                1 if event.is_resolved else 0,
                event.source_char_start,
                event.source_char_end,
            ),
        )
        event_id = cursor.lastrowid
        event_ids.append(event_id)

        # Link co-occurring entities
        for entity_name in event.entities:
            row = conn.execute(
                "SELECT id FROM entities WHERE canonical = ? OR name = ?",
                (entity_name.lower(), entity_name),
            ).fetchone()
            if row:
                conn.execute(
                    "INSERT OR IGNORE INTO event_entities (event_id, entity_id) VALUES (?, ?)",
                    (event_id, row["id"]),
                )

    return event_ids


def query_timeline(
    conn,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    entity_name: Optional[str] = None,
    document_id: Optional[int] = None,
    min_confidence: float = 0.0,
    include_unresolved: bool = False,
    limit: int = 200,
) -> list[dict]:
    """
    Query the timeline with filters.

    Returns events ordered chronologically, with linked entities and document info.
    """
    sql = """
        SELECT e.id, e.document_id, e.event_date, e.date_raw, e.precision,
               e.confidence, e.context, e.is_resolved,
               d.title as doc_title, d.category as doc_category, d.source as doc_source
        FROM events e
        JOIN documents d ON d.id = e.document_id
        WHERE 1=1
    """
    params: list = []

    if not include_unresolved:
        sql += " AND e.is_resolved = 1"

    if min_confidence > 0:
        sql += " AND e.confidence >= ?"
        params.append(min_confidence)

    if start_date:
        sql += " AND e.event_date >= ?"
        params.append(start_date)

    if end_date:
        sql += " AND e.event_date <= ?"
        params.append(end_date)

    if document_id is not None:
        sql += " AND e.document_id = ?"
        params.append(document_id)

    if entity_name:
        sql += """
            AND e.id IN (
                SELECT ee.event_id FROM event_entities ee
                JOIN entities ent ON ent.id = ee.entity_id
                WHERE ent.name LIKE ? OR ent.canonical LIKE ?
            )
        """
        params.extend([f"%{entity_name}%", f"%{entity_name.lower()}%"])

    sql += " ORDER BY e.event_date ASC NULLS LAST LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        event = dict(row)
        # Fetch linked entities for this event
        entity_rows = conn.execute(
            """
            SELECT ent.name, ent.type, ee.role
            FROM event_entities ee
            JOIN entities ent ON ent.id = ee.entity_id
            WHERE ee.event_id = ?
        """,
            (row["id"],),
        ).fetchall()
        event["entities"] = [dict(er) for er in entity_rows]
        results.append(event)

    return results


def get_timeline_stats(conn) -> dict:
    """Get summary statistics for the timeline."""
    stats = {}

    row = conn.execute("SELECT COUNT(*) as c FROM events").fetchone()
    stats["total_events"] = row["c"]

    row = conn.execute("SELECT COUNT(*) as c FROM events WHERE is_resolved = 0").fetchone()
    stats["unresolved_events"] = row["c"]

    row = conn.execute(
        "SELECT MIN(event_date) as earliest, MAX(event_date) as latest FROM events WHERE event_date IS NOT NULL"
    ).fetchone()
    stats["date_range"] = {"earliest": row["earliest"], "latest": row["latest"]}

    precision_rows = conn.execute(
        "SELECT precision, COUNT(*) as c FROM events GROUP BY precision ORDER BY c DESC"
    ).fetchall()
    stats["by_precision"] = {r["precision"]: r["c"] for r in precision_rows}

    return stats
