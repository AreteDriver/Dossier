"""
DOSSIER — Entity Resolver

Consolidates entity *mentions* into single identities with aliases,
confidence scores, and an audit trail. Takes existing NER output from
the entities table and resolves "J. Smith", "John Smith", "Smith, John"
into the same canonical identity.

Architecture:
    - Read-time resolution layer via entity_resolutions mapping table.
    - No ALTER TABLE on entities — resolution is non-destructive.
    - Merges are instant, reversible, and audit-logged.

Usage:
    from dossier.core.resolver import EntityResolver, init_resolver_tables

    resolver = EntityResolver(conn)
    results = resolver.resolve_all()
"""

import re
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

try:
    from rapidfuzz.distance import Levenshtein

    HAS_RAPIDFUZZ = True
except ImportError:  # pragma: no cover
    HAS_RAPIDFUZZ = False


# ═══════════════════════════════════════════════════════════════════
# Data Structures
# ═══════════════════════════════════════════════════════════════════


class MergeAction(Enum):
    """Resolution action for a candidate pair."""

    AUTO_MERGE = "auto_merge"
    SUGGEST_MERGE = "suggest_merge"
    NO_MERGE = "no_merge"


@dataclass
class CandidateMatch:
    """A potential match between two entities."""

    source_id: int
    source_name: str
    target_id: int
    target_name: str
    confidence: float
    strategy: str  # which strategy produced this match
    action: MergeAction


@dataclass
class ResolutionResult:
    """Summary of a resolution run."""

    entities_scanned: int
    auto_merged: int
    suggested: int
    skipped: int
    matches: list[CandidateMatch] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
# Name Normalization
# ═══════════════════════════════════════════════════════════════════

_TITLES = re.compile(
    r"^(?:mr\.?|mrs\.?|ms\.?|dr\.?|prof\.?|rev\.?|hon\.?|sir|dame|lord|lady)\s+",
    re.IGNORECASE,
)
_SUFFIXES = re.compile(
    r",?\s+(?:jr\.?|sr\.?|ii|iii|iv|esq\.?|ph\.?d\.?|md|m\.d\.)$",
    re.IGNORECASE,
)


def normalize_name(name: str) -> str:
    """Normalize a name for comparison.

    - Strip titles (Mr., Dr., etc.) and suffixes (Jr., III, etc.)
    - Handle "Last, First" → "first last"
    - Lowercase, collapse whitespace, strip punctuation
    """
    s = name.strip()
    # Strip titles
    s = _TITLES.sub("", s)
    # Strip suffixes
    s = _SUFFIXES.sub("", s)
    # Handle "Last, First" format
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            s = f"{parts[1]} {parts[0]}"
    # Lowercase, collapse whitespace, strip non-alpha chars except spaces
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ═══════════════════════════════════════════════════════════════════
# Similarity Strategies
# ═══════════════════════════════════════════════════════════════════


def jaccard_similarity(a: str, b: str) -> float:
    """Token-level Jaccard similarity between two names."""
    tokens_a = set(a.lower().split())
    tokens_b = set(b.lower().split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def initial_match(a: str, b: str) -> bool:
    """Check if one name is an initial-form of the other.

    "J. Smith" matches "John Smith" — first token of one is a single letter
    matching the first letter of the other's first token, and last tokens match.
    """
    tokens_a = a.lower().replace(".", "").split()
    tokens_b = b.lower().replace(".", "").split()

    if len(tokens_a) < 2 or len(tokens_b) < 2:
        return False

    # Last tokens must match
    if tokens_a[-1] != tokens_b[-1]:
        return False

    # Check if first token of either is an initial
    first_a, first_b = tokens_a[0], tokens_b[0]
    if len(first_a) == 1 and first_b.startswith(first_a):
        return True
    if len(first_b) == 1 and first_a.startswith(first_b):
        return True

    return False


def edit_distance_match(a: str, b: str) -> Optional[float]:
    """Check edit distance for typo detection. Returns confidence or None.

    Only considers names > 8 chars and distance ≤ 2.
    Requires rapidfuzz.
    """
    if not HAS_RAPIDFUZZ:
        return None
    if len(a) <= 8 or len(b) <= 8:
        return None
    dist = Levenshtein.distance(a.lower(), b.lower())
    if dist <= 2:
        return 0.80 - dist * 0.10
    return None


# ═══════════════════════════════════════════════════════════════════
# Entity Resolver Engine
# ═══════════════════════════════════════════════════════════════════

# Thresholds
AUTO_MERGE_THRESHOLD = 0.85
SUGGEST_MERGE_THRESHOLD = 0.60
CO_OCCURRENCE_BOOST = 0.10
TYPE_MATCH_BOOST = 0.10


class EntityResolver:
    """Resolves duplicate entities into canonical identities."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def resolve_entity(self, entity_id: int) -> list[CandidateMatch]:
        """Find resolution candidates for a single entity."""
        row = self.conn.execute(
            "SELECT id, name, type FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not row:
            return []

        name = row["name"]
        etype = row["type"]
        norm = normalize_name(name)

        # Get all other entities of the same type
        others = self.conn.execute(
            "SELECT id, name, type FROM entities WHERE id != ? AND type = ?",
            (entity_id, etype),
        ).fetchall()

        matches = []
        for other in others:
            match = self._compare_entities(
                entity_id, name, norm, etype, other["id"], other["name"], other["type"]
            )
            if match:
                matches.append(match)

        return matches

    def resolve_all(self, entity_type: Optional[str] = None) -> ResolutionResult:
        """Run resolution across all entities (or filtered by type)."""
        sql = "SELECT id, name, type FROM entities"
        params: list = []
        if entity_type:
            sql += " WHERE type = ?"
            params.append(entity_type)
        sql += " ORDER BY id"

        entities = self.conn.execute(sql, params).fetchall()
        result = ResolutionResult(
            entities_scanned=len(entities), auto_merged=0, suggested=0, skipped=0
        )

        # Build normalized lookup for exact canonical match
        seen_pairs: set[tuple[int, int]] = set()

        for entity in entities:
            eid = entity["id"]
            name = entity["name"]
            etype = entity["type"]
            norm = normalize_name(name)

            # Compare against all other entities of the same type
            others = self.conn.execute(
                "SELECT id, name, type FROM entities WHERE id != ? AND type = ?",
                (eid, etype),
            ).fetchall()

            for other in others:
                pair = (min(eid, other["id"]), max(eid, other["id"]))
                if pair in seen_pairs:
                    continue

                match = self._compare_entities(
                    eid, name, norm, etype, other["id"], other["name"], other["type"]
                )
                if match:
                    seen_pairs.add(pair)
                    result.matches.append(match)

                    if match.action == MergeAction.AUTO_MERGE:
                        self.merge_entities(match.source_id, match.target_id)
                        result.auto_merged += 1
                    else:
                        self._add_to_queue(match)
                        result.suggested += 1

        return result

    def merge_entities(self, source_id: int, target_id: int) -> bool:
        """Merge source entity into target (target becomes canonical).

        Returns True if merge succeeded, False if entities don't exist.
        """
        source = self.conn.execute(
            "SELECT id, name FROM entities WHERE id = ?", (source_id,)
        ).fetchone()
        target = self.conn.execute(
            "SELECT id, name FROM entities WHERE id = ?", (target_id,)
        ).fetchone()
        if not source or not target:
            return False

        # Create resolution mapping
        self.conn.execute(
            "INSERT OR REPLACE INTO entity_resolutions (source_entity_id, canonical_entity_id) VALUES (?, ?)",
            (source_id, target_id),
        )

        # Add both names as aliases of the canonical
        self.conn.execute(
            "INSERT OR IGNORE INTO entity_aliases (entity_id, alias_name) VALUES (?, ?)",
            (target_id, source["name"]),
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO entity_aliases (entity_id, alias_name) VALUES (?, ?)",
            (target_id, target["name"]),
        )

        # Audit log
        self.conn.execute(
            "INSERT INTO resolution_log (source_entity_id, canonical_entity_id, action, detail) VALUES (?, ?, 'merge', ?)",
            (source_id, target_id, f"Merged '{source['name']}' into '{target['name']}'"),
        )

        return True

    def split_entity(self, source_id: int, target_id: int) -> bool:
        """Undo a merge — remove resolution mapping.

        Returns True if split succeeded, False if no resolution existed.
        """
        existing = self.conn.execute(
            "SELECT 1 FROM entity_resolutions WHERE source_entity_id = ? AND canonical_entity_id = ?",
            (source_id, target_id),
        ).fetchone()
        if not existing:
            return False

        self.conn.execute(
            "DELETE FROM entity_resolutions WHERE source_entity_id = ? AND canonical_entity_id = ?",
            (source_id, target_id),
        )

        # Audit log
        self.conn.execute(
            "INSERT INTO resolution_log (source_entity_id, canonical_entity_id, action, detail) VALUES (?, ?, 'split', ?)",
            (source_id, target_id, f"Split entity {source_id} from canonical {target_id}"),
        )

        return True

    def get_canonical_id(self, entity_id: int) -> int:
        """Get the canonical ID for an entity. Returns self if unresolved."""
        row = self.conn.execute(
            "SELECT canonical_entity_id FROM entity_resolutions WHERE source_entity_id = ?",
            (entity_id,),
        ).fetchone()
        return row["canonical_entity_id"] if row else entity_id

    def get_aliases(self, entity_id: int) -> list[str]:
        """Get all known aliases for an entity."""
        rows = self.conn.execute(
            "SELECT alias_name FROM entity_aliases WHERE entity_id = ? ORDER BY alias_name",
            (entity_id,),
        ).fetchall()
        return [r["alias_name"] for r in rows]

    def get_duplicates(self) -> list[dict]:
        """Get all resolved duplicate pairs."""
        rows = self.conn.execute(
            """
            SELECT er.source_entity_id, e1.name as source_name,
                   er.canonical_entity_id, e2.name as canonical_name
            FROM entity_resolutions er
            JOIN entities e1 ON e1.id = er.source_entity_id
            JOIN entities e2 ON e2.id = er.canonical_entity_id
            ORDER BY er.canonical_entity_id
        """
        ).fetchall()
        return [dict(r) for r in rows]

    def review_queue_item(self, queue_id: int, approve: bool) -> bool:
        """Approve or reject a suggestion from the review queue.

        Returns True if item existed, False otherwise.
        """
        item = self.conn.execute(
            "SELECT source_entity_id, target_entity_id FROM resolution_queue WHERE id = ?",
            (queue_id,),
        ).fetchone()
        if not item:
            return False

        if approve:
            self.merge_entities(item["source_entity_id"], item["target_entity_id"])

        # Log the decision
        action = "approve" if approve else "reject"
        self.conn.execute(
            "INSERT INTO resolution_log (source_entity_id, canonical_entity_id, action, detail) VALUES (?, ?, ?, ?)",
            (
                item["source_entity_id"],
                item["target_entity_id"],
                action,
                f"Queue item {queue_id} {'approved' if approve else 'rejected'}",
            ),
        )

        # Remove from queue
        self.conn.execute("DELETE FROM resolution_queue WHERE id = ?", (queue_id,))
        return True

    # ── Internal helpers ──

    def _compare_entities(
        self,
        eid: int,
        name: str,
        norm: str,
        etype: str,
        other_id: int,
        other_name: str,
        other_type: str,
    ) -> Optional[CandidateMatch]:
        """Compare two entities and return a CandidateMatch if similar enough."""
        other_norm = normalize_name(other_name)
        confidence = 0.0
        strategy = ""

        # Strategy 1: Exact canonical match
        if norm == other_norm and norm:
            confidence = 0.95
            strategy = "exact_canonical"
        # Strategy 2: Initial matching ("J. Smith" ↔ "John Smith")
        elif initial_match(norm, other_norm):
            confidence = 0.70
            strategy = "initial_match"
        else:
            # Strategy 3: Jaccard token similarity
            jac = jaccard_similarity(norm, other_norm)
            if jac > 0.5:
                confidence = jac
                strategy = "jaccard"

            # Strategy 4: Edit distance (rapidfuzz)
            edit_conf = edit_distance_match(norm, other_norm)
            if edit_conf and edit_conf > confidence:
                confidence = edit_conf
                strategy = "edit_distance"

        if confidence < SUGGEST_MERGE_THRESHOLD:
            return None

        # Context boosters
        if etype == other_type:
            confidence = min(1.0, confidence + TYPE_MATCH_BOOST)

        # Co-occurrence boost: entities appearing in the same document
        cooccur = self.conn.execute(
            """
            SELECT COUNT(*) as c FROM document_entities de1
            JOIN document_entities de2 ON de1.document_id = de2.document_id
            WHERE de1.entity_id = ? AND de2.entity_id = ?
        """,
            (eid, other_id),
        ).fetchone()
        if cooccur and cooccur["c"] > 0:
            confidence = min(1.0, confidence + CO_OCCURRENCE_BOOST)

        # Determine action — confidence is guaranteed >= SUGGEST_MERGE_THRESHOLD
        # here because we returned None above for lower values, and boosters
        # only increase confidence.
        if confidence >= AUTO_MERGE_THRESHOLD:
            action = MergeAction.AUTO_MERGE
        else:
            action = MergeAction.SUGGEST_MERGE

        return CandidateMatch(
            source_id=eid,
            source_name=name,
            target_id=other_id,
            target_name=other_name,
            confidence=confidence,
            strategy=strategy,
            action=action,
        )

    def _add_to_queue(self, match: CandidateMatch) -> None:
        """Add a suggested merge to the review queue."""
        self.conn.execute(
            "INSERT OR IGNORE INTO resolution_queue (source_entity_id, target_entity_id, confidence, strategy) VALUES (?, ?, ?, ?)",
            (match.source_id, match.target_id, match.confidence, match.strategy),
        )


# ═══════════════════════════════════════════════════════════════════
# Database Schema
# ═══════════════════════════════════════════════════════════════════

RESOLVER_SCHEMA = """
    -- ═══ ENTITY RESOLUTIONS ═══
    -- Maps source entities to their canonical identity
    CREATE TABLE IF NOT EXISTS entity_resolutions (
        source_entity_id    INTEGER PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
        canonical_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE
    );

    -- ═══ ENTITY ALIASES ═══
    -- All known name variants per entity
    CREATE TABLE IF NOT EXISTS entity_aliases (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id   INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
        alias_name  TEXT NOT NULL,
        UNIQUE(entity_id, alias_name)
    );

    -- ═══ RESOLUTION LOG ═══
    -- Audit trail for merge/split/review actions
    CREATE TABLE IF NOT EXISTS resolution_log (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        source_entity_id    INTEGER NOT NULL,
        canonical_entity_id INTEGER NOT NULL,
        action              TEXT NOT NULL,  -- merge/split/approve/reject
        detail              TEXT,
        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ═══ RESOLUTION QUEUE ═══
    -- Human review queue for suggested merges
    CREATE TABLE IF NOT EXISTS resolution_queue (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        source_entity_id    INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
        target_entity_id    INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
        confidence          REAL NOT NULL,
        strategy            TEXT NOT NULL,
        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_entity_id, target_entity_id)
    );

    -- ═══ INDEXES ═══
    CREATE INDEX IF NOT EXISTS idx_resolutions_canonical ON entity_resolutions(canonical_entity_id);
    CREATE INDEX IF NOT EXISTS idx_aliases_entity ON entity_aliases(entity_id);
    CREATE INDEX IF NOT EXISTS idx_queue_confidence ON resolution_queue(confidence DESC);
"""


def init_resolver_tables(conn):
    """Add resolver tables to an existing DOSSIER database."""
    conn.executescript(RESOLVER_SCHEMA)
    conn.commit()
