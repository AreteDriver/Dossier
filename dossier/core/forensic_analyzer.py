"""
DOSSIER — Forensic Document Analyzer

Deep analysis layer for investigative document intelligence:
- Repeated phrases (n-gram cross-document analysis)
- Codeword detection (anomalous word usage patterns)
- Topic classification (keyword-cluster topic tagging)
- Intent analysis (transactional, evasive, coordinating, threatening)
- Money laundering indicators (structuring, layering, shell companies, offshore)
- Financial pattern extraction (amounts, accounts, thresholds)
"""

import re
from collections import Counter, defaultdict


# ═══════════════════════════════════════════
# MONEY LAUNDERING SIGNALS
# ═══════════════════════════════════════════

# BSA/AML reporting threshold is $10,000
CTR_THRESHOLD = 10_000

# Structuring: amounts just under reporting thresholds
STRUCTURING_PATTERNS = [
    r"\$\s*9[,.]?[0-9]{3}(?:\.\d{2})?",  # $9,000-$9,999
    r"\$\s*4[,.]?[0-9]{3}(?:\.\d{2})?",  # $4,000-$4,999 (half-structuring)
]

# Round number pattern (suspicious in aggregate)
ROUND_AMOUNT_PATTERN = r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.00)?)\b"

STRUCTURING_LANGUAGE = [
    "split the payment",
    "break it up",
    "break up the",
    "multiple transactions",
    "several payments",
    "under the limit",
    "below the threshold",
    "keep it under",
    "reporting requirement",
    "avoid report",
    "structured",
    "structuring",
    "smurfing",
    "cash deposit",
    "cash deposits",
    "money order",
    "money orders",
    "cashier's check",
    "cashier check",
    "traveler's check",
]

SHELL_COMPANY_INDICATORS = [
    "llc",
    "holding company",
    "holdings",
    "offshore",
    "registered agent",
    "nominee director",
    "nominee shareholder",
    "bearer share",
    "shelf company",
    "shell company",
    "special purpose vehicle",
    "spv",
    "special purpose entity",
    "beneficial owner",
    "ultimate beneficial owner",
    "ubo",
    "trust account",
    "blind trust",
    "irrevocable trust",
    "foundation",
    "stiftung",
    "anstalt",
]

LAYERING_INDICATORS = [
    "wire transfer",
    "wire from",
    "wire to",
    "wired funds",
    "convert to",
    "convert the",
    "exchange for",
    "cryptocurrency",
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "usdt",
    "tether",
    "monero",
    "crypto wallet",
    "wallet address",
    "mixer",
    "tumbler",
    "correspondent bank",
    "intermediary bank",
    "nostro account",
    "vostro account",
    "back-to-back loan",
    "round-tripping",
    "trade-based",
    "over-invoice",
    "under-invoice",
    "mispricing",
    "hawala",
    "hundi",
    "fei ch'ien",
]

# High-risk jurisdictions (FATF grey/black list + known secrecy havens)
HIGH_RISK_JURISDICTIONS = [
    "cayman islands",
    "british virgin islands",
    "bvi",
    "panama",
    "bermuda",
    "jersey",
    "guernsey",
    "isle of man",
    "liechtenstein",
    "monaco",
    "andorra",
    "seychelles",
    "mauritius",
    "vanuatu",
    "samoa",
    "marshall islands",
    "belize",
    "nevis",
    "st. kitts",
    "turks and caicos",
    "bahamas",
    "luxembourg",
    "switzerland",
    "dubai",
    "uae",
    "singapore",
    "hong kong",
    "cyprus",
    "malta",
    "labuan",
    "curacao",
    "aruba",
    "cook islands",
    "niue",
    "nauru",
    "myanmar",
    "north korea",
    "iran",
    "syria",
    "yemen",
    "afghanistan",
    "somalia",
]

SECRECY_LANGUAGE = [
    "off the record",
    "off the books",
    "keep this quiet",
    "don't tell",
    "do not tell",
    "delete this",
    "burn after reading",
    "destroy this",
    "confidential",
    "eyes only",
    "not for distribution",
    "private channel",
    "secure line",
    "encrypted",
    "use signal",
    "use telegram",
    "burner",
    "prepaid phone",
    "no paper trail",
    "plausible deniability",
    "clean",
    "discreet",
    "discretion",
    "hush money",
    "quiet payment",
    "under the table",
    "off-book",
    "black money",
    "slush fund",
]

# ═══════════════════════════════════════════
# INTENT CLASSIFICATION SIGNALS
# ═══════════════════════════════════════════

INTENT_SIGNALS = {
    "transactional": [
        "invoice",
        "payment",
        "receipt",
        "purchase",
        "sale",
        "transfer",
        "amount due",
        "balance",
        "fee",
        "commission",
        "disbursement",
        "remittance",
        "payable",
        "receivable",
        "billing",
        "credit",
        "debit",
    ],
    "coordinating": [
        "meeting",
        "schedule",
        "arrange",
        "coordinate",
        "confirm",
        "agenda",
        "action item",
        "follow up",
        "next steps",
        "call me",
        "let's discuss",
        "set up",
        "plan for",
        "deadline",
        "deliverable",
    ],
    "evasive": [
        "hypothetically",
        "in theory",
        "if someone were to",
        "not saying",
        "i'm not sure",
        "can't recall",
        "don't remember",
        "no comment",
        "plead the fifth",
        "on advice of counsel",
        "decline to answer",
        "i would have to check",
        "that depends",
        "it's complicated",
        "you know what i mean",
        "between us",
        "off the record",
    ],
    "threatening": [
        "or else",
        "consequence",
        "you will regret",
        "final warning",
        "last chance",
        "demand",
        "ultimatum",
        "expose",
        "go public",
        "authorities",
        "lawyer up",
        "legal action",
        "lawsuit",
        "subpoena",
        "leverage",
        "insurance policy",
        "dead man's switch",
    ],
    "informational": [
        "report",
        "update",
        "summary",
        "analysis",
        "findings",
        "attached",
        "enclosed",
        "for your review",
        "fyi",
        "please note",
        "per our discussion",
        "as discussed",
        "background",
        "briefing",
        "memo",
    ],
    "directive": [
        "make sure",
        "ensure that",
        "i need you to",
        "handle this",
        "take care of",
        "get it done",
        "asap",
        "immediately",
        "urgent",
        "priority",
        "do not delay",
        "expedite",
        "move on this",
    ],
}

# ═══════════════════════════════════════════
# TOPIC KEYWORDS
# ═══════════════════════════════════════════

TOPIC_KEYWORDS = {
    "financial": [
        "bank",
        "account",
        "transfer",
        "payment",
        "money",
        "fund",
        "invest",
        "capital",
        "asset",
        "loan",
        "mortgage",
        "interest",
        "dividend",
        "revenue",
        "profit",
        "loss",
        "tax",
        "audit",
        "compliance",
        "fiduciary",
    ],
    "legal": [
        "attorney",
        "lawyer",
        "court",
        "judge",
        "plaintiff",
        "defendant",
        "motion",
        "filing",
        "statute",
        "regulation",
        "indictment",
        "prosecution",
        "defense",
        "testimony",
        "deposition",
        "discovery",
        "subpoena",
        "verdict",
        "settlement",
    ],
    "political": [
        "campaign",
        "election",
        "vote",
        "delegate",
        "caucus",
        "primary",
        "donor",
        "fundrais",
        "pac",
        "lobby",
        "legislation",
        "senator",
        "congress",
        "governor",
        "cabinet",
        "policy",
        "diplomat",
        "ambassador",
        "sanction",
    ],
    "intelligence": [
        "surveillance",
        "classified",
        "clearance",
        "operative",
        "asset",
        "handler",
        "intelligence",
        "counterintelligence",
        "intercept",
        "sigint",
        "humint",
        "reconnaissance",
        "briefing",
        "debriefing",
        "informant",
        "source",
    ],
    "real_estate": [
        "property",
        "deed",
        "title",
        "parcel",
        "acreage",
        "zoning",
        "tenant",
        "lease",
        "rental",
        "mortgage",
        "escrow",
        "closing",
        "appraisal",
        "renovation",
        "construction",
        "development",
    ],
    "travel": [
        "flight",
        "airport",
        "passenger",
        "itinerary",
        "travel",
        "visa",
        "passport",
        "customs",
        "border",
        "hotel",
        "yacht",
        "charter",
        "manifest",
        "departure",
        "arrival",
        "destination",
    ],
    "communication": [
        "email",
        "phone",
        "call",
        "text",
        "message",
        "correspondence",
        "memo",
        "letter",
        "fax",
        "meeting",
        "conference",
        "briefing",
    ],
    "criminal": [
        "trafficking",
        "smuggling",
        "conspiracy",
        "fraud",
        "embezzlement",
        "bribery",
        "extortion",
        "racketeering",
        "obstruction",
        "perjury",
        "forgery",
        "counterfeit",
        "launder",
        "kickback",
        "corruption",
    ],
}


# ═══════════════════════════════════════════
# CODEWORD DETECTION
# ═══════════════════════════════════════════

# Words commonly used as substitutes in coded communications
KNOWN_CODE_SUBSTITUTES = {
    # Drug trafficking codes (also used in other criminal contexts)
    "pizza": "potential codeword",
    "pasta": "potential codeword",
    "cheese": "potential codeword",
    "ice cream": "potential codeword",
    "candy": "potential codeword",
    "party": "potential codeword",
    "package": "potential codeword",
    "shipment": "potential codeword",
    "delivery": "potential codeword",
    "sample": "potential codeword",
    "product": "potential codeword",
    "material": "potential codeword",
    "stuff": "potential codeword",
    "thing": "potential codeword",
    "item": "potential codeword",
    "gift": "potential codeword",
    "donation": "potential codeword",
    "contribution": "potential codeword",
    "consulting fee": "potential codeword",
    "consulting": "potential codeword",
    "advisory fee": "potential codeword",
    "entertainment": "potential codeword",
    "hospitality": "potential codeword",
    "favor": "potential codeword",
    "arrangement": "potential codeword",
    "understanding": "potential codeword",
    "our friend": "potential codeword",
    "the guy": "potential codeword",
    "the man": "potential codeword",
    "the client": "potential codeword",
    "the project": "potential codeword",
    "the situation": "potential codeword",
    "the matter": "potential codeword",
    "the issue": "potential codeword",
    "special project": "potential codeword",
    "side project": "potential codeword",
}


# ═══════════════════════════════════════════
# FINANCIAL PATTERN EXTRACTION
# ═══════════════════════════════════════════

CURRENCY_PATTERN = r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:million|billion|thousand|[MBKmk])?"
ACCOUNT_PATTERN = r"\b(?:account\s*(?:#|no\.?|number)?\s*[:.]?\s*)(\d{4,20})\b"
ROUTING_PATTERN = r"\b(?:routing\s*(?:#|no\.?|number)?\s*[:.]?\s*)(\d{9})\b"
SWIFT_PATTERN = r"\b([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b"
EIN_PATTERN = r"\b(\d{2}-\d{7})\b"


# ═══════════════════════════════════════════
# MAIN ANALYSIS FUNCTIONS
# ═══════════════════════════════════════════


def analyze_document(text: str, filename: str = "") -> dict:
    """
    Run full forensic analysis on document text.

    Returns:
    {
        "intents": [{"label": str, "score": float, "evidence": [str]}],
        "topics": [{"label": str, "score": float}],
        "aml_flags": [{"flag": str, "severity": str, "evidence": [str]}],
        "codewords": [{"word": str, "context": str, "count": int}],
        "phrases": [{"phrase": str, "count": int}],
        "financial_indicators": [{"type": str, "value": str, "context": str, "risk_score": float}],
        "risk_score": float,  # 0.0 - 1.0 overall document risk
    }
    """
    if not text or len(text.strip()) < 50:
        return _empty_result()

    text_lower = text.lower()

    intents = _classify_intent(text_lower)
    topics = _classify_topics(text_lower)
    aml_flags = _detect_aml_flags(text, text_lower)
    codewords = _detect_codewords(text, text_lower)
    phrases = _extract_repeated_phrases(text_lower)
    financial = _extract_financial_indicators(text, text_lower)

    # Compute overall risk score
    risk_score = _compute_risk_score(aml_flags, financial, codewords, intents)

    return {
        "intents": intents,
        "topics": topics,
        "aml_flags": aml_flags,
        "codewords": codewords,
        "phrases": phrases,
        "financial_indicators": financial,
        "risk_score": risk_score,
    }


def _empty_result() -> dict:
    return {
        "intents": [],
        "topics": [],
        "aml_flags": [],
        "codewords": [],
        "phrases": [],
        "financial_indicators": [],
        "risk_score": 0.0,
    }


# ═══════════════════════════════════════════
# INTENT CLASSIFICATION
# ═══════════════════════════════════════════


def _classify_intent(text_lower: str) -> list[dict]:
    """Score document against intent categories."""
    results = []
    total_words = max(len(text_lower.split()), 1)

    for intent, signals in INTENT_SIGNALS.items():
        hits = []
        score = 0
        for signal in signals:
            count = text_lower.count(signal)
            if count > 0:
                score += count
                # Extract context around first occurrence
                idx = text_lower.find(signal)
                start = max(0, idx - 40)
                end = min(len(text_lower), idx + len(signal) + 40)
                context = text_lower[start:end].strip()
                hits.append(f"...{context}...")

        if score > 0:
            # Normalize: score relative to document length
            normalized = min(1.0, score / (total_words * 0.01))
            results.append({
                "label": intent,
                "score": round(normalized, 3),
                "evidence": hits[:5],
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ═══════════════════════════════════════════
# TOPIC CLASSIFICATION
# ═══════════════════════════════════════════


def _classify_topics(text_lower: str) -> list[dict]:
    """Score document against topic categories."""
    results = []
    total_words = max(len(text_lower.split()), 1)

    for topic, keywords in TOPIC_KEYWORDS.items():
        score = 0
        for kw in keywords:
            # Use word boundary for short keywords to avoid false matches
            if len(kw) <= 4:
                score += len(re.findall(rf"\b{re.escape(kw)}\b", text_lower))
            else:
                score += text_lower.count(kw)

        if score > 0:
            normalized = min(1.0, score / (total_words * 0.005))
            results.append({"label": topic, "score": round(normalized, 3)})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:5]  # Top 5 topics


# ═══════════════════════════════════════════
# AML FLAG DETECTION
# ═══════════════════════════════════════════


def _detect_aml_flags(text: str, text_lower: str) -> list[dict]:
    """Detect money laundering indicators."""
    flags = []

    # Structuring detection
    structuring_evidence = []
    for signal in STRUCTURING_LANGUAGE:
        if signal in text_lower:
            idx = text_lower.find(signal)
            start = max(0, idx - 50)
            end = min(len(text_lower), idx + len(signal) + 50)
            structuring_evidence.append(text_lower[start:end].strip())
    # Check for amounts just under $10k
    for pattern in STRUCTURING_PATTERNS:
        matches = re.findall(pattern, text)
        if matches:
            structuring_evidence.extend(matches[:3])
    if structuring_evidence:
        flags.append({
            "flag": "structuring",
            "severity": "high",
            "evidence": structuring_evidence[:5],
        })

    # Shell company indicators
    shell_evidence = []
    for signal in SHELL_COMPANY_INDICATORS:
        if signal in text_lower:
            idx = text_lower.find(signal)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(signal) + 40)
            shell_evidence.append(text_lower[start:end].strip())
    if len(shell_evidence) >= 2:  # Need multiple signals
        flags.append({
            "flag": "shell_company",
            "severity": "high" if len(shell_evidence) >= 4 else "medium",
            "evidence": shell_evidence[:5],
        })

    # Layering indicators
    layering_evidence = []
    for signal in LAYERING_INDICATORS:
        if signal in text_lower:
            idx = text_lower.find(signal)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(signal) + 40)
            layering_evidence.append(text_lower[start:end].strip())
    if layering_evidence:
        flags.append({
            "flag": "layering",
            "severity": "high" if len(layering_evidence) >= 3 else "medium",
            "evidence": layering_evidence[:5],
        })

    # High-risk jurisdictions
    jurisdiction_evidence = []
    for jurisdiction in HIGH_RISK_JURISDICTIONS:
        if jurisdiction in text_lower:
            idx = text_lower.find(jurisdiction)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(jurisdiction) + 40)
            jurisdiction_evidence.append(text_lower[start:end].strip())
    if jurisdiction_evidence:
        flags.append({
            "flag": "high_risk_jurisdiction",
            "severity": "medium",
            "evidence": jurisdiction_evidence[:5],
        })

    # Secrecy/concealment language
    secrecy_evidence = []
    for signal in SECRECY_LANGUAGE:
        if signal in text_lower:
            idx = text_lower.find(signal)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(signal) + 40)
            secrecy_evidence.append(text_lower[start:end].strip())
    if secrecy_evidence:
        flags.append({
            "flag": "secrecy_concealment",
            "severity": "high" if len(secrecy_evidence) >= 3 else "medium",
            "evidence": secrecy_evidence[:5],
        })

    # Round-number transactions (suspicious pattern)
    round_amounts = []
    for match in re.finditer(ROUND_AMOUNT_PATTERN, text):
        amount_str = match.group(1).replace(",", "")
        try:
            amount = float(amount_str)
            if amount >= 1000 and amount % 1000 == 0:
                round_amounts.append(f"${match.group(1)}")
        except ValueError:
            continue
    if len(round_amounts) >= 3:
        flags.append({
            "flag": "round_number_transactions",
            "severity": "low",
            "evidence": round_amounts[:5],
        })

    return flags


# ═══════════════════════════════════════════
# CODEWORD DETECTION
# ═══════════════════════════════════════════


def _detect_codewords(text: str, text_lower: str) -> list[dict]:
    """Detect potential codewords and coded language."""
    found = []

    # Check known code substitutes
    for word, note in KNOWN_CODE_SUBSTITUTES.items():
        count = text_lower.count(word)
        if count > 0:
            idx = text_lower.find(word)
            start = max(0, idx - 50)
            end = min(len(text_lower), idx + len(word) + 50)
            context = text_lower[start:end].strip()
            found.append({
                "word": word,
                "context": f"...{context}...",
                "count": count,
                "note": note,
            })

    # Detect quoted ordinary words (e.g., "pizza", "delivery")
    # These suggest words being used with special meaning
    quoted_pattern = r'["\u201c\u201d](\w+(?:\s+\w+)?)["\u201c\u201d]'
    for match in re.finditer(quoted_pattern, text):
        word = match.group(1).lower()
        if len(word) >= 3 and word not in {"said", "told", "asked", "the", "and", "but"}:
            idx = match.start()
            start = max(0, idx - 40)
            end = min(len(text), match.end() + 40)
            context = text[start:end].strip()
            # Only flag if it's a common word being quoted (unusual usage)
            if word in KNOWN_CODE_SUBSTITUTES or _is_common_word(word):
                found.append({
                    "word": f'"{word}"',
                    "context": f"...{context}...",
                    "count": 1,
                    "note": "quoted ordinary word",
                })

    # Deduplicate by word
    seen = set()
    deduped = []
    for item in found:
        if item["word"] not in seen:
            seen.add(item["word"])
            deduped.append(item)

    deduped.sort(key=lambda x: x["count"], reverse=True)
    return deduped[:30]


def _is_common_word(word: str) -> bool:
    """Check if a word is common English (thus suspicious when quoted)."""
    common = {
        "food", "dinner", "lunch", "breakfast", "party", "meeting", "game",
        "play", "fun", "trip", "visit", "friend", "help", "work", "job",
        "business", "deal", "trade", "exchange", "gift", "present", "favor",
        "service", "ticket", "pass", "green", "white", "snow", "ice",
        "rock", "sugar", "candy", "chocolate", "coffee", "tea",
    }
    return word in common


# ═══════════════════════════════════════════
# REPEATED PHRASE EXTRACTION
# ═══════════════════════════════════════════


def _extract_repeated_phrases(text_lower: str, min_count: int = 2) -> list[dict]:
    """Extract repeated multi-word phrases (3-5 word n-grams)."""
    # Tokenize
    words = re.findall(r"\b[a-z]{2,}\b", text_lower)

    phrases = Counter()

    # 3-grams
    for i in range(len(words) - 2):
        trigram = f"{words[i]} {words[i+1]} {words[i+2]}"
        phrases[trigram] += 1

    # 4-grams
    for i in range(len(words) - 3):
        fourgram = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]}"
        phrases[fourgram] += 1

    # 5-grams
    for i in range(len(words) - 4):
        fivegram = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]} {words[i+4]}"
        phrases[fivegram] += 1

    # Filter: must appear at least min_count times, skip boring phrases
    stop_phrases = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "is", "was", "were", "are", "be",
    }

    results = []
    for phrase, count in phrases.most_common(200):
        if count < min_count:
            continue
        # Skip phrases that are mostly stop words
        phrase_words = phrase.split()
        non_stop = [w for w in phrase_words if w not in stop_phrases]
        if len(non_stop) < 2:
            continue
        results.append({"phrase": phrase, "count": count})

    return results[:50]


# ═══════════════════════════════════════════
# FINANCIAL INDICATOR EXTRACTION
# ═══════════════════════════════════════════


def _extract_financial_indicators(text: str, text_lower: str) -> list[dict]:
    """Extract financial patterns: amounts, accounts, SWIFT codes."""
    indicators = []

    # Currency amounts
    for match in re.finditer(CURRENCY_PATTERN, text):
        amount_str = match.group(1).replace(",", "")
        try:
            amount = float(amount_str)
        except ValueError:
            continue

        full_match = match.group(0)
        idx = match.start()
        start = max(0, idx - 40)
        end = min(len(text), match.end() + 40)
        context = text[start:end].strip()

        # Risk scoring based on amount
        risk = 0.0
        if 9000 <= amount < 10000:
            risk = 0.9  # Structuring range
        elif amount >= 10000:
            risk = 0.5
        elif amount >= 1000 and amount % 1000 == 0:
            risk = 0.3  # Round numbers

        # Boost risk if near suspicious language
        near_text = text_lower[max(0, idx - 200):min(len(text_lower), match.end() + 200)]
        for signal in STRUCTURING_LANGUAGE + SECRECY_LANGUAGE:
            if signal in near_text:
                risk = min(1.0, risk + 0.2)
                break

        if amount >= 500:  # Only track amounts $500+
            indicators.append({
                "type": "currency_amount",
                "value": full_match.strip(),
                "context": f"...{context}...",
                "risk_score": round(risk, 2),
            })

    # Account numbers
    for match in re.finditer(ACCOUNT_PATTERN, text, re.IGNORECASE):
        idx = match.start()
        start = max(0, idx - 40)
        end = min(len(text), match.end() + 40)
        context = text[start:end].strip()
        indicators.append({
            "type": "account_number",
            "value": match.group(1),
            "context": f"...{context}...",
            "risk_score": 0.4,
        })

    # Routing numbers
    for match in re.finditer(ROUTING_PATTERN, text, re.IGNORECASE):
        idx = match.start()
        start = max(0, idx - 40)
        end = min(len(text), match.end() + 40)
        context = text[start:end].strip()
        indicators.append({
            "type": "routing_number",
            "value": match.group(1),
            "context": f"...{context}...",
            "risk_score": 0.4,
        })

    # SWIFT/BIC codes (8 or 11 alphanumeric, starts with 4 letters)
    for match in re.finditer(SWIFT_PATTERN, text):
        candidate = match.group(1)
        # Must start with 4 letters (bank code) + 2 letters (country)
        if re.match(r"^[A-Z]{4}[A-Z]{2}", candidate):
            idx = match.start()
            start = max(0, idx - 40)
            end = min(len(text), match.end() + 40)
            context = text[start:end].strip()
            indicators.append({
                "type": "swift_code",
                "value": candidate,
                "context": f"...{context}...",
                "risk_score": 0.5,
            })

    # EIN (employer identification number)
    for match in re.finditer(EIN_PATTERN, text):
        idx = match.start()
        start = max(0, idx - 40)
        end = min(len(text), match.end() + 40)
        context = text[start:end].strip()
        indicators.append({
            "type": "ein",
            "value": match.group(1),
            "context": f"...{context}...",
            "risk_score": 0.2,
        })

    # Deduplicate by type+value
    seen = set()
    deduped = []
    for ind in indicators:
        key = (ind["type"], ind["value"])
        if key not in seen:
            seen.add(key)
            deduped.append(ind)

    deduped.sort(key=lambda x: x["risk_score"], reverse=True)
    return deduped[:50]


# ═══════════════════════════════════════════
# RISK SCORING
# ═══════════════════════════════════════════


def _compute_risk_score(
    aml_flags: list[dict],
    financial: list[dict],
    codewords: list[dict],
    intents: list[dict],
) -> float:
    """Compute overall document risk score (0.0 - 1.0)."""
    score = 0.0

    # AML flags are heavy
    for flag in aml_flags:
        if flag["severity"] == "high":
            score += 0.25
        elif flag["severity"] == "medium":
            score += 0.15
        elif flag["severity"] == "low":
            score += 0.05

    # High-risk financial indicators
    high_risk_financial = [f for f in financial if f["risk_score"] >= 0.7]
    score += len(high_risk_financial) * 0.1

    # Codewords
    score += min(0.2, len(codewords) * 0.02)

    # Evasive or threatening intent boosts risk
    for intent in intents:
        if intent["label"] == "evasive" and intent["score"] > 0.3:
            score += 0.1
        elif intent["label"] == "threatening" and intent["score"] > 0.3:
            score += 0.1

    return round(min(1.0, score), 3)
