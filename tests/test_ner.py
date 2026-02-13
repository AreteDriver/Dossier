"""Tests for dossier.core.ner — entity extraction, classification, title generation."""

from unittest.mock import patch

import pytest

from dossier.core.ner import (
    extract_entities,
    classify_document,
    generate_title,
    _extract_keywords,
    _capitalize_place,
    _capitalize_org,
)


# ── Gazetteer lookup ──


class TestGazetteerLookup:
    def test_known_person_found(self):
        result = extract_entities("Jeffrey Epstein was arrested in 2019.")
        names = [p["name"] for p in result["people"]]
        assert "Jeffrey Epstein" in names

    def test_known_person_count(self):
        text = "Jeffrey Epstein met with Jeffrey Epstein's lawyers."
        result = extract_entities(text)
        person = next(p for p in result["people"] if p["name"] == "Jeffrey Epstein")
        assert person["count"] >= 2

    def test_known_place_found(self):
        result = extract_entities("The investigation centered on Palm Beach.")
        names = [p["name"] for p in result["places"]]
        assert "Palm Beach" in names

    def test_known_org_found(self):
        result = extract_entities("The FBI opened a case.")
        names = [o["name"] for o in result["orgs"]]
        assert "FBI" in names

    def test_multiple_entity_types(self, sample_text):
        result = extract_entities(sample_text)
        assert len(result["people"]) > 0
        assert len(result["places"]) > 0
        assert len(result["orgs"]) > 0


# ── Date extraction ──


class TestDateExtraction:
    def test_full_date(self):
        result = extract_entities("On January 15, 2015, the deposition was taken.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("January" in d and "2015" in d for d in date_names)

    def test_slash_date(self):
        result = extract_entities("Filed on 03/15/2016.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("03/15/2016" in d for d in date_names)

    def test_iso_date(self):
        result = extract_entities("Date: 2023-07-01.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("2023-07-01" in d for d in date_names)

    def test_abbreviated_date(self):
        result = extract_entities("On Sep. 5, 2020, the hearing was held.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("Sep" in d and "2020" in d for d in date_names)

    def test_seasonal_date(self):
        result = extract_entities("During summer 2005, events occurred.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("summer 2005" in d for d in date_names)

    def test_standalone_year(self):
        result = extract_entities("Events in 2003 were significant.")
        date_names = [d["name"] for d in result["dates"]]
        assert any("2003" in d for d in date_names)


# ── Heuristic NER ──


class TestHeuristicNER:
    def test_titled_name(self):
        result = extract_entities("The testimony of Mr. Smith was recorded.")
        names = [p["name"] for p in result["people"]]
        assert "Smith" in names

    def test_titled_name_detective(self):
        result = extract_entities("Then Detective Recarey filed the report.")
        names = [p["name"] for p in result["people"]]
        assert "Recarey" in names

    def test_two_word_capitalized_name(self):
        # Must not be at sentence start to be detected by heuristic
        result = extract_entities("The lawyer contacted Robert Anderson about the case.")
        names = [p["name"] for p in result["people"]]
        assert "Robert Anderson" in names


# ── Keyword extraction ──


class TestKeywordExtraction:
    def test_stop_words_filtered(self):
        keywords = _extract_keywords("the quick brown fox jumps over the lazy dog")
        words = [kw["word"] for kw in keywords]
        assert "the" not in words
        assert "over" not in words

    def test_short_words_filtered(self):
        keywords = _extract_keywords("an ox is big and has no ax to grind")
        words = [kw["word"] for kw in keywords]
        # All words under 3 chars should be excluded
        assert all(len(w.split()[0]) >= 3 for w in words)

    def test_bigrams_present(self):
        text = "flight log flight log flight log analysis"
        keywords = _extract_keywords(text)
        words = [kw["word"] for kw in keywords]
        assert "flight log" in words

    def test_top_n_respected(self):
        text = " ".join(f"word{i}" * (100 - i) for i in range(60))
        keywords = _extract_keywords(text, top_n=10)
        assert len(keywords) <= 10

    def test_empty_text_returns_empty(self):
        assert _extract_keywords("") == []


# ── Document classifier ──


class TestClassifyDocument:
    @pytest.mark.parametrize(
        "category,signal_text",
        [
            ("deposition", "The witness was deposed under oath. Q. State your name. A. Virginia."),
            ("flight", "Flight log manifest showing passenger departure from Teterboro on N908JE."),
            ("correspondence", "Dear Sir, Re: our previous memorandum. Sincerely, John."),
            (
                "report",
                "Incident report filed by reporting officer. Case number 05-123. FBI investigation.",
            ),
            ("legal", "The plaintiff filed a motion against the defendant. Case No. 08-cv-123."),
            (
                "email",
                "Subject: Meeting\nFrom: test@example.com\nTo: other@example.com\nDate: today",
            ),
            ("lobbying", "FARA foreign agent lobbying disclosure for the registrant and client."),
        ],
    )
    def test_category_detection(self, category, signal_text):
        assert classify_document(signal_text) == category

    def test_other_fallback(self):
        assert classify_document("This is generic text with nothing specific.") == "other"

    def test_filename_boost(self):
        # Weak text signal but strong filename match — signal must be exact substring
        result = classify_document("Some general text.", filename="passenger manifest.pdf")
        assert result == "flight"

    def test_empty_text(self):
        assert classify_document("") == "other"


# ── Title generation ──


class TestGenerateTitle:
    def test_extracts_first_suitable_line(self):
        text = "\n\nDeposition of Virginia Giuffre\n\nMore text follows."
        assert generate_title(text, "doc.pdf") == "Deposition of Virginia Giuffre"

    def test_skips_short_lines(self):
        text = "Hi\n\nThis is a proper title for the document\nMore text."
        assert generate_title(text, "doc.pdf") == "This is a proper title for the document"

    def test_falls_back_to_filename(self):
        text = "a\nb\nc\n"  # All lines too short
        assert generate_title(text, "epstein_deposition_2009.pdf") == "Epstein Deposition 2009"

    def test_empty_text_uses_filename(self):
        assert generate_title("", "some_report.txt") == "Some Report"


# ── Capitalization helpers ──


class TestCapitalizePlace:
    def test_usvi(self):
        assert _capitalize_place("usvi") == "USVI"

    def test_us_virgin_islands(self):
        assert _capitalize_place("u.s. virgin islands") == "U.S. Virgin Islands"

    def test_default_title_case(self):
        assert _capitalize_place("palm beach") == "Palm Beach"


class TestCapitalizeOrg:
    def test_fbi_uppercase(self):
        assert _capitalize_org("fbi") == "FBI"

    def test_sdny_uppercase(self):
        assert _capitalize_org("sdny") == "SDNY"

    def test_palm_beach_police(self):
        assert _capitalize_org("palm beach police") == "Palm Beach Police"

    def test_us_attorney(self):
        assert _capitalize_org("u.s. attorney") == "U.S. Attorney"

    def test_default_title_case(self):
        assert _capitalize_org("goldman sachs") == "Goldman Sachs"


# ── Heuristic NER filters ──


class TestHeuristicNERFilters:
    def test_false_positive_filtered(self):
        """'United States' mid-sentence is filtered as a false positive."""
        result = extract_entities("The case was filed in the United States district court.")
        names = [p["name"] for p in result["people"]]
        assert "United States" not in names

    def test_skip_words_filtered(self):
        """Words containing skip_words like 'department' are filtered."""
        result = extract_entities("The employee visited the Aviation Department for review.")
        names = [p["name"] for p in result["people"]]
        assert all("Department" not in n for n in names)

    def test_known_countries_filtered(self):
        """Multi-word known country (not in gazetteers) is filtered from people (line 237).

        'Saudi Arabia' is in both known_countries and KNOWN_PLACES, so the gazetteer
        check catches it first. We patch KNOWN_PLACES to exclude it, forcing the
        known_countries filter to be the one that catches it.
        """
        import dossier.core.ner as ner_mod

        patched_places = ner_mod.KNOWN_PLACES - {"saudi arabia"}
        with patch.object(ner_mod, "KNOWN_PLACES", patched_places):
            result = extract_entities(
                "The diplomat visited offices in Saudi Arabia for negotiations with officials."
            )
        names = [p["name"] for p in result["people"]]
        assert "Saudi Arabia" not in names

    def test_titled_multi_word_name_via_pre_context(self):
        r"""Title like 'Det.' in pre_context triggers heuristic person detection (lines 242-243).

        The title must NOT be followed by a space (otherwise (?<!\.\s) lookbehind blocks).
        'Det.Frederick Hamilton' makes 'Frederick Hamilton' match the regex, with 'Det.'
        in the 15-char pre_context window matching TITLE_PATTERNS.
        """
        result = extract_entities(
            "Evidence from Det.Frederick Hamilton confirmed the allegations against the defendant."
        )
        names = [p["name"] for p in result["people"]]
        assert "Frederick Hamilton" in names

    def test_three_word_capitalized_not_person(self):
        """3+ word capitalized sequences with skip words are excluded."""
        result = extract_entities(
            "She worked at the Aviation Management Center during the project."
        )
        names = [p["name"] for p in result["people"]]
        assert all("Aviation" not in n for n in names)


class TestClassifyDocumentEdge:
    def test_empty_category_signals(self):
        """When CATEGORY_SIGNALS is empty, classify returns 'other'."""
        with patch.dict("dossier.core.ner.CATEGORY_SIGNALS", {}, clear=True):
            result = classify_document("deposition under oath Q. A. sworn testimony")
        assert result == "other"


# ── Edge cases ──


class TestEdgeCases:
    def test_empty_text(self):
        result = extract_entities("")
        assert result == {"people": [], "places": [], "orgs": [], "dates": [], "keywords": []}

    def test_no_entities_found(self):
        result = extract_entities("just some plain boring text with nothing notable")
        # Should return structure with possibly empty or keyword-only results
        assert "people" in result
        assert "places" in result
        assert "orgs" in result
