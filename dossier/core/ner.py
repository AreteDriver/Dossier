"""
DOSSIER — Named Entity Recognition Engine
Custom NER using regex patterns, heuristics, and gazetteers.

Why not spaCy's pretrained model?
  1. Domain-specific: legal documents have patterns generic NER misses
  2. Controllable: we can add known entities and patterns easily
  3. No model download required — runs anywhere
  4. For this corpus, precision > recall. We add entities as we find them.

The system works in layers:
  Layer 1: Gazetteer lookup (known entities)
  Layer 2: Pattern-based extraction (dates, addresses, case numbers)
  Layer 3: Heuristic NER (capitalized multi-word sequences, title patterns)
  Layer 4: Keyword extraction (TF-based frequency analysis)
"""

import re
from collections import Counter, defaultdict
from pathlib import Path


# ═══════════════════════════════════════════
# GAZETTEERS — Known entities (user-extensible)
# ═══════════════════════════════════════════

KNOWN_PEOPLE = {
    # ─── Epstein Network ───
    "jeffrey epstein",
    "ghislaine maxwell",
    "virginia giuffre",
    "virginia roberts",
    "sarah kellen",
    "nadia marcinkova",
    "jean-luc brunel",
    "alan dershowitz",
    "alexander acosta",
    "kenneth starr",
    "jay lefkowitz",
    "leslie wexner",
    "prince andrew",
    "bill clinton",
    "donald trump",
    "kevin spacey",
    "bill richardson",
    "george mitchell",
    "glenn dubin",
    "eva dubin",
    "adriana ross",
    "lesley groff",
    "joseph recarey",
    "michael reiter",
    "courtney wild",
    "annie farmer",
    "maria farmer",
    # ─── Podesta Network ───
    "john podesta",
    "tony podesta",
    "heather podesta",
    "mary podesta",
    "hillary clinton",
    "huma abedin",
    "cheryl mills",
    "jake sullivan",
    "robby mook",
    "john sullivan",
    "jennifer palmieri",
    "brian fallon",
    "neera tanden",
    "joel benenson",
    "jim margolis",
    "mandy grunwald",
    "philippe reines",
    "sid blumenthal",
    "sidney blumenthal",
    "donna brazile",
    "debbie wasserman schultz",
    "bernie sanders",
    "barack obama",
    "joe biden",
    "tim kaine",
    "elizabeth warren",
    "harry reid",
    "nancy pelosi",
    "chuck schumer",
    # ─── Podesta Group Lobbying ───
    "doug band",
    "ira magaziner",
    "laura graham",
    "dennis cheng",
    "craig minassian",
    "amitabh desai",
    # ─── Media / Journalists in emails ───
    "glenn thrush",
    "maggie haberman",
    "john harwood",
    "dana milbank",
    "brent budowsky",
    "tina flournoy",
}

KNOWN_PLACES = {
    # ─── Epstein locations ───
    "palm beach",
    "new york",
    "manhattan",
    "little st. james",
    "little st james",
    "great st. james",
    "great st james",
    "u.s. virgin islands",
    "usvi",
    "paris",
    "london",
    "new mexico",
    "zorro ranch",
    "teterboro",
    "358 el brillo way",
    "el brillo way",
    "9 east 71st street",
    "les wexner",
    "columbus ohio",
    "saint thomas",
    "st. thomas",
    "le bourget",
    "miami",
    "washington d.c.",
    "washington dc",
    # ─── Podesta / Political locations ───
    "capitol hill",
    "foggy bottom",
    "k street",
    "brooklyn",
    "benghazi",
    "libya",
    "syria",
    "iraq",
    "saudi arabia",
    "qatar",
    "haiti",
    "ukraine",
    "russia",
    "china",
    "iran",
    "israel",
    "turkey",
    "egypt",
    "martha's vineyard",
    "chappaqua",
    "camp david",
    "des moines",
    "cedar rapids",
    "las vegas",
    "philadelphia",
    "charlotte",
    "cleveland",
    "milwaukee",
    # ─── Offshore / Financial jurisdictions ───
    "cayman islands",
    "british virgin islands",
    "bermuda",
    "panama",
    "liechtenstein",
    "monaco",
    "jersey",
    "guernsey",
    "isle of man",
    "seychelles",
    "bahamas",
    "zurich",
    "geneva",
    "switzerland",
    "luxembourg",
    "cyprus",
    "malta",
    "dubai",
    "singapore",
    "hong kong",
    "st. kitts",
    "nevis",
    "belize",
    "vanuatu",
    # ─── General ───
    "florida",
    "new jersey",
    "connecticut",
    "brussels",
    "belgium",
}

KNOWN_ORGS = {
    # ─── Epstein orgs ───
    "fbi",
    "doj",
    "department of justice",
    "sdny",
    "palm beach police",
    "palm beach pd",
    "sec",
    "jpmorgan",
    "jp morgan",
    "deutsche bank",
    "citibank",
    "harvard",
    "mit",
    "ohio state",
    "victoria's secret",
    "l brands",
    "mc2 model management",
    "faa",
    "u.s. attorney",
    "metropolitan correctional center",
    # ─── Podesta / Political orgs ───
    "podesta group",
    "clinton foundation",
    "clinton global initiative",
    "center for american progress",
    "hillaryclinton.com",
    "democratic national committee",
    "dnc",
    "dccc",
    "dscc",
    "super pac",
    "priorities usa",
    "white house",
    "state department",
    "department of state",
    "cia",
    "nsa",
    "pentagon",
    "treasury department",
    "uranium one",
    "joule unlimited",
    "joule energy",
    "sberbank",
    "troika dialog",
    "rusnano",
    # ─── Lobbying / FARA ───
    "european centre for a modern ukraine",
    "republic of iraq",
    "republic of egypt",
    "kingdom of saudi arabia",
    "government of qatar",
    # ─── Media ───
    "new york times",
    "washington post",
    "politico",
    "cnn",
    "fox news",
    "msnbc",
    "associated press",
    "reuters",
    "wall street journal",
    "huffington post",
    # ─── Financial ───
    "goldman sachs",
    "morgan stanley",
    "citigroup",
    "bank of america",
    "wells fargo",
}

# ═══════════════════════════════════════════
# PRECOMPILED GAZETTEER REGEX (single-pass matching)
# Sort by length descending so longer matches take priority
# ═══════════════════════════════════════════


def _compile_gazetteer(names: set) -> re.Pattern:
    """Compile a set of names into a single alternation regex, longest-first."""
    sorted_names = sorted(names, key=len, reverse=True)
    escaped = [re.escape(n) for n in sorted_names]
    return re.compile("|".join(escaped))


_PEOPLE_RE = _compile_gazetteer(KNOWN_PEOPLE)
_PLACES_RE = _compile_gazetteer(KNOWN_PLACES)
_ORGS_RE = _compile_gazetteer(KNOWN_ORGS)


# Title patterns that precede names
TITLE_PATTERNS = r"(?:Mr\.|Mrs\.|Ms\.|Dr\.|Judge|Det\.|Detective|Agent|Senator|Governor|President|Prince|Professor|Atty\.|Attorney)"


# ═══════════════════════════════════════════
# PATTERN EXTRACTORS
# ═══════════════════════════════════════════

# Dates
DATE_PATTERNS = [
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}\b",
    r"\b(?:spring|summer|fall|winter|early|late|mid-?)\s*\d{4}\b",
    r"\b(?:19|20)\d{2}\b",  # standalone years
]

# Case numbers
CASE_PATTERN = r"\b(?:Case\s+)?(?:No\.?\s*)?(?:\d{2,4}-(?:cv|cr|mc|mj)-\d{3,6}(?:-[A-Z]+)?)\b"

# Address patterns
ADDRESS_PATTERN = r"\b\d{1,5}\s+(?:[A-Z][a-z]+\s+){1,3}(?:Street|St\.|Avenue|Ave\.|Boulevard|Blvd\.|Road|Rd\.|Drive|Dr\.|Lane|Ln\.|Way|Place|Pl\.)\b"


# ═══════════════════════════════════════════
# STOP WORDS for keyword extraction
# ═══════════════════════════════════════════

STOP_WORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "is",
    "was",
    "were",
    "are",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "shall",
    "can",
    "that",
    "this",
    "these",
    "those",
    "it",
    "its",
    "he",
    "she",
    "they",
    "we",
    "you",
    "his",
    "her",
    "their",
    "our",
    "your",
    "my",
    "him",
    "them",
    "us",
    "not",
    "no",
    "nor",
    "as",
    "if",
    "then",
    "than",
    "so",
    "up",
    "out",
    "about",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "between",
    "same",
    "each",
    "every",
    "all",
    "both",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "only",
    "own",
    "just",
    "also",
    "very",
    "often",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "what",
    "which",
    "who",
    "whom",
    "whose",
    "any",
    "many",
    "much",
    "over",
    "under",
    "again",
    "further",
    "once",
    "said",
    "one",
    "two",
    "three",
    "four",
    "five",
    "first",
    "second",
    "third",
    "new",
    "old",
    "see",
    "page",
    "document",
    "file",
    "exhibit",
    "yes",
    "no",
    "q",
    "a",
    "mr",
    "ms",
    "mrs",
    "dr",
    "re",
    "cc",
    "per",
    "via",
    "i",
    "me",
    "we",
    "don",
    "doesn",
    "didn",
    "won",
    "wouldn",
    "couldn",
    "shouldn",
    "isn",
    "aren",
    "wasn",
    "weren",
    "hadn",
    "hasn",
    "haven",
}


# ═══════════════════════════════════════════
# MAIN EXTRACTION FUNCTION
# ═══════════════════════════════════════════


def extract_entities(text: str) -> dict:
    """
    Extract all entities from text. Returns:
    {
        "people": [{"name": str, "count": int}, ...],
        "places": [{"name": str, "count": int}, ...],
        "orgs":   [{"name": str, "count": int}, ...],
        "dates":  [{"name": str, "count": int}, ...],
        "keywords": [{"word": str, "count": int}, ...],
    }
    """
    if not text:
        return {"people": [], "places": [], "orgs": [], "dates": [], "keywords": []}

    # Normalize whitespace for matching (collapse newlines/tabs to spaces)
    text_normalized = re.sub(r"\s+", " ", text)
    text_lower = text_normalized.lower()

    people = Counter()
    places = Counter()
    orgs = Counter()
    dates = Counter()

    # ─── Layer 1: Gazetteer lookup (single-pass regex) ───
    for match in _PEOPLE_RE.finditer(text_lower):
        name = match.group()
        people[name.title()] += 1

    for match in _PLACES_RE.finditer(text_lower):
        name = match.group()
        places[_capitalize_place(name)] += 1

    for match in _ORGS_RE.finditer(text_lower):
        name = match.group()
        orgs[_capitalize_org(name)] += 1

    # ─── Layer 2: Pattern-based extraction ───
    # Dates
    for pattern in DATE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            dates[match.group().strip()] += 1

    # ─── Layer 3: Heuristic NER ───
    # Find capitalized multi-word sequences (likely proper nouns)
    # Pattern: 2-4 capitalized words in sequence, not at sentence start
    # Uses text_normalized to avoid newline-spanning false matches
    for match in re.finditer(
        r"(?<!\.\s)(?<!\n)(?<!^)\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,}){1,3})\b", text_normalized
    ):
        candidate = match.group().strip()
        candidate_lower = candidate.lower()

        # Skip if already in gazetteers
        if (
            candidate_lower in KNOWN_PEOPLE
            or candidate_lower in KNOWN_PLACES
            or candidate_lower in KNOWN_ORGS
        ):
            continue

        # Skip common false positives
        false_positives = {
            "the united states",
            "united states",
            "pursuant to",
            "direct examination",
            "cross examination",
            "southern district",
            "palm beach international",
            "model management",
            "aircraft",
            "le bourget",
            "ground transport",
            "flight log",
            "manifest records",
            "summary total",
            # Legal/business terms that look like names
            "legal counsel",
            "registered agent",
            "nominee director",
            "nominee shareholder",
            "beneficial owner",
            "managing director",
            "general counsel",
            "chief executive",
            "executive director",
            "outside counsel",
            "special counsel",
            "independent counsel",
            "corporate counsel",
            "senior counsel",
            "associate counsel",
            "deputy counsel",
            "foreign principal",
            "political activity",
            "government relations",
            "public affairs",
            "public relations",
            # Document heading phrases
            "investigation update",
            "case summary",
            "executive summary",
            "supplemental report",
            "witness statements",
            "next steps",
            "financial evidence",
            "banking relationships",
            "corporate entities",
            "corporate structure",
            "lobbying activities",
            "political contributions",
            "related entities",
            # CSV/JSON header terms
            "tail number",
            "supplemental statement",
            "filing type",
            "indicator type",
            "content type",
            "message type",
        }
        if candidate_lower in false_positives:
            continue
        # Skip if it's a known place or contains common non-person words
        skip_words = {
            # Geographic/address terms
            "international",
            "island",
            "islands",
            "county",
            "country",
            "boulevard",
            "avenue",
            "street",
            "route",
            "way",
            "drive",
            "lane",
            "road",
            "place",
            "airport",
            "beach",
            # Organization terms
            "management",
            "department",
            "district",
            "corporation",
            "company",
            "holdings",
            "holding",
            "limited",
            "ltd",
            "inc",
            "llc",
            "llp",
            "corp",
            "incorporated",
            "foundation",
            "stiftung",
            "anstalt",
            "institute",
            "university",
            "committee",
            "commission",
            "association",
            "trust",
            "group",
            "partners",
            "services",
            "solutions",
            "industries",
            "enterprises",
            "consulting",
            "advisors",
            "advisory",
            "capital",
            "ventures",
            "media",
            "labs",
            # Document/record terms
            "records",
            "aircraft",
            "transport",
            "period",
            "details",
            "compensation",
            "centre",
            "center",
            "principal",
            "registrant",
            "filing",
            "disclosure",
            "profile",
            "engagement",
            "various",
            "status",
            "notes",
            "source",
            "bourget",
            # Miscellaneous false positive triggers
            "modern",
            "general",
            "special",
            "national",
            "federal",
            "regional",
            "annual",
            "total",
            "summary",
            "report",
            "section",
            "chapter",
            "article",
            "schedule",
            "exhibit",
            "appendix",
        }
        if any(w in candidate_lower for w in skip_words):
            continue
        # Skip known countries/regions/places being misidentified as people
        if candidate_lower in KNOWN_PLACES:
            continue
        # Skip if any individual word is a known single-word place
        # (catches "Qatar Date", "Panama City" fragments, etc.)
        candidate_words = candidate_lower.split()
        if any(w in KNOWN_PLACES for w in candidate_words):
            continue
        known_countries = {
            "ukraine",
            "russia",
            "azerbaijan",
            "iraq",
            "iran",
            "saudi arabia",
            "qatar",
            "egypt",
            "libya",
            "syria",
            "china",
            "israel",
            "turkey",
            "canada",
            "india",
            "cayman islands",
            "british virgin islands",
            "virgin islands",
            "panama",
            "liechtenstein",
            "switzerland",
            "bermuda",
            "bahamas",
            "belgium",
            "brussels",
            "zurich",
            "geneva",
            "cyprus",
            "malta",
            "dubai",
            "singapore",
            "hong kong",
            "new jersey",
            "new mexico",
            "new york",
            "florida",
            "connecticut",
            "palm beach",
        }
        if candidate_lower in known_countries:
            continue

        # Heuristic: if preceded by a title, it's a person
        pre_context = text_normalized[max(0, match.start() - 15) : match.start()]
        if re.search(TITLE_PATTERNS, pre_context):
            people[candidate] += 1
            continue

        # If 2 words and both capitalized, likely a person name
        words = candidate.split()
        if len(words) == 2 and all(w[0].isupper() for w in words):
            # Skip if this is a substring of a known gazetteer person
            if not _is_substring_of_known(candidate_lower, KNOWN_PEOPLE):
                people[candidate] += 1

    # Titled names: "Mr. Smith", "Detective Recarey", etc.
    titled_pattern = rf"({TITLE_PATTERNS})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"
    for match in re.finditer(titled_pattern, text_normalized):
        title_word = match.group(1).strip().rstrip(".").lower()
        name = match.group(2).strip()
        name_lower = name.lower()
        if name_lower not in STOP_WORDS:
            # Skip single-word fragments when title+name is already a known person
            # e.g., "Prince Andrew" → "prince andrew" is in KNOWN_PEOPLE, skip "Andrew"
            if len(name.split()) == 1:
                full = f"{title_word} {name_lower}"
                if full in KNOWN_PEOPLE:
                    continue
            people[name] += 1

    # ─── Layer 4: Keyword extraction ───
    keywords = _extract_keywords(text)

    return {
        "people": [{"name": k, "count": v} for k, v in people.most_common(100)],
        "places": [{"name": k, "count": v} for k, v in places.most_common(50)],
        "orgs": [{"name": k, "count": v} for k, v in orgs.most_common(50)],
        "dates": [{"name": k, "count": v} for k, v in dates.most_common(50)],
        "keywords": keywords,
    }


def _is_substring_of_known(candidate: str, known_set: set) -> bool:
    """Check if candidate is a substring of any entry in the known set.

    Catches fragments like 'Luc Brunel' (substring of 'jean-luc brunel')
    or 'Andrew' (substring of 'prince andrew').
    """
    for known in known_set:
        if candidate != known and candidate in known:
            return True
    return False


def _extract_keywords(text: str, top_n: int = 50) -> list[dict]:
    """Extract significant keywords using term frequency."""
    # Tokenize: lowercase, alpha-only, 3+ chars
    words = re.findall(r"\b[a-z]{3,}\b", text.lower())

    # Filter stop words
    filtered = [w for w in words if w not in STOP_WORDS]

    # Count and return top N
    counts = Counter(filtered)

    # Boost multi-word phrases (bigrams)
    for i in range(len(filtered) - 1):
        bigram = f"{filtered[i]} {filtered[i + 1]}"
        if filtered[i] not in STOP_WORDS and filtered[i + 1] not in STOP_WORDS:
            counts[bigram] += 1

    return [{"word": k, "count": v} for k, v in counts.most_common(top_n)]


def _capitalize_place(name: str) -> str:
    """Proper capitalization for places."""
    special = {"usvi": "USVI", "u.s. virgin islands": "U.S. Virgin Islands"}
    if name in special:
        return special[name]
    return name.title()


def _capitalize_org(name: str) -> str:
    """Proper capitalization for organizations."""
    acronyms = {"fbi", "doj", "sdny", "sec", "faa"}
    if name in acronyms:
        return name.upper()
    special = {
        "palm beach police": "Palm Beach Police",
        "palm beach pd": "Palm Beach PD",
        "u.s. attorney": "U.S. Attorney",
    }
    if name in special:
        return special[name]
    return name.title()


# ═══════════════════════════════════════════
# CATEGORY CLASSIFIER
# ═══════════════════════════════════════════

CATEGORY_SIGNALS = {
    "deposition": [
        "deposition",
        "deposed",
        "sworn testimony",
        "q.",
        "a.",
        "direct examination",
        "cross examination",
        "the witness",
        "under oath",
        "do you swear",
    ],
    "flight": [
        "flight log",
        "manifest",
        "passenger",
        "tail number",
        "departure",
        "arrival",
        "aircraft",
        "gulfstream",
        "boeing",
        "n908je",
        "n212jl",
        "teterboro",
    ],
    "correspondence": [
        "dear",
        "sincerely",
        "regards",
        "re:",
        "from:",
        "to:",
        "cc:",
        "memorandum",
        "memo",
        "letter",
    ],
    "report": [
        "incident report",
        "case number",
        "reporting officer",
        "investigation",
        "detective",
        "fbi",
        "police report",
        "supplemental report",
        "field report",
    ],
    "legal": [
        "plaintiff",
        "defendant",
        "motion",
        "court order",
        "filed",
        "docket",
        "case no",
        "civil action",
        "complaint",
        "indictment",
        "plea agreement",
        "non-prosecution agreement",
        "npa",
    ],
    "email": [
        "subject:",
        "from:",
        "to:",
        "cc:",
        "date:",
        "message-id:",
        "mime-version",
        "content-type",
        "sent from my",
        "forwarded message",
        "on behalf of",
        "original message",
    ],
    "lobbying": [
        "fara",
        "foreign agent",
        "lobbying disclosure",
        "lobbying activity",
        "registrant",
        "client",
        "lobbying firm",
        "government relations",
        "foreign principal",
        "political activity",
        "supplemental statement",
    ],
}


def classify_document(text: str, filename: str = "") -> str:
    """Classify a document into a category based on content signals."""
    text_lower = text[:5000].lower()  # Check first 5000 chars for speed
    filename_lower = filename.lower()

    scores = defaultdict(int)

    for category, signals in CATEGORY_SIGNALS.items():
        for signal in signals:
            count = text_lower.count(signal)
            scores[category] += count
            # Also check filename
            if signal in filename_lower:
                scores[category] += 5  # Filename match gets heavy weight

    if not scores:
        return "other"

    best = max(scores, key=scores.get)
    return best if scores[best] >= 2 else "other"


def generate_title(text: str, filename: str) -> str:
    """Generate a descriptive title from document content."""
    # Try to find a title-like line at the start
    lines = text.strip().split("\n")
    for line in lines[:10]:
        line = line.strip()
        # Skip blank lines and very short/long lines
        if 10 < len(line) < 120 and not line.startswith(("page", "Page", "#")):
            # If it looks like a title (mostly caps, or short)
            if len(line) < 80:
                return line

    # Fall back to filename
    stem = Path(filename).stem if filename else "Untitled"
    return stem.replace("_", " ").replace("-", " ").title()
