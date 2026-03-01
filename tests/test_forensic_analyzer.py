"""Tests for dossier.core.forensic_analyzer — pure function unit tests."""

import pytest

from dossier.core.forensic_analyzer import (
    analyze_document,
    _classify_intent,
    _classify_topics,
    _compute_risk_score,
    _detect_aml_flags,
    _detect_codewords,
    _extract_financial_indicators,
    _extract_repeated_phrases,
    _is_common_word,
)


# ═══════════════════════════════════════════
# analyze_document
# ═══════════════════════════════════════════


class TestAnalyzeDocument:
    def test_empty_text_returns_empty_result(self):
        result = analyze_document("")
        assert result["risk_score"] == 0.0
        assert result["intents"] == []
        assert result["aml_flags"] == []

    def test_short_text_returns_empty_result(self):
        result = analyze_document("too short")
        assert result["risk_score"] == 0.0

    def test_whitespace_only_returns_empty(self):
        result = analyze_document("   \n\t   ")
        assert result["risk_score"] == 0.0

    def test_all_keys_present(self):
        text = "This is a longer document about payments and transfers " * 10
        result = analyze_document(text)
        expected_keys = {
            "intents", "topics", "aml_flags", "codewords",
            "phrases", "financial_indicators", "risk_score",
        }
        assert set(result.keys()) == expected_keys

    def test_risk_score_in_range(self):
        text = (
            "Wire transfer of $9,500 to the shell company LLC in the "
            "Cayman Islands. Split the payment to keep it under the limit. "
            "Off the record, delete this message. The package delivery is "
            "arranged. Bitcoin mixer tumbler cryptocurrency. "
        ) * 5
        result = analyze_document(text)
        assert 0.0 <= result["risk_score"] <= 1.0


# ═══════════════════════════════════════════
# _classify_intent
# ═══════════════════════════════════════════


class TestClassifyIntent:
    def test_transactional_intent(self):
        text = "invoice payment receipt purchase sale transfer amount due balance"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "transactional" in labels

    def test_coordinating_intent(self):
        text = "meeting schedule arrange coordinate confirm agenda action item follow up"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "coordinating" in labels

    def test_evasive_intent(self):
        text = "hypothetically in theory can't recall don't remember no comment plead the fifth"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "evasive" in labels

    def test_threatening_intent(self):
        text = "or else consequence you will regret final warning ultimatum demand expose"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "threatening" in labels

    def test_informational_intent(self):
        text = "report update summary analysis findings attached enclosed for your review fyi"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "informational" in labels

    def test_directive_intent(self):
        text = "make sure ensure that i need you to handle this take care of get it done asap"
        results = _classify_intent(text)
        labels = [r["label"] for r in results]
        assert "directive" in labels

    def test_evidence_truncated_to_five(self):
        # Many signals → evidence list capped at 5
        text = ("invoice payment receipt purchase sale transfer amount due "
                "balance fee commission disbursement remittance payable")
        results = _classify_intent(text)
        transactional = [r for r in results if r["label"] == "transactional"][0]
        assert len(transactional["evidence"]) <= 5

    def test_no_match_returns_empty(self):
        text = "the quick brown fox jumps over the lazy dog"
        results = _classify_intent(text)
        assert results == []

    def test_score_normalized(self):
        text = "payment " * 100
        results = _classify_intent(text)
        for r in results:
            assert 0.0 <= r["score"] <= 1.0


# ═══════════════════════════════════════════
# _classify_topics
# ═══════════════════════════════════════════


class TestClassifyTopics:
    def test_financial_topic(self):
        text = "bank account transfer payment money fund invest capital asset loan"
        results = _classify_topics(text)
        labels = [r["label"] for r in results]
        assert "financial" in labels

    def test_legal_topic(self):
        text = "attorney lawyer court judge plaintiff defendant deposition testimony"
        results = _classify_topics(text)
        labels = [r["label"] for r in results]
        assert "legal" in labels

    def test_travel_topic(self):
        text = "flight airport passenger itinerary travel visa passport customs"
        results = _classify_topics(text)
        labels = [r["label"] for r in results]
        assert "travel" in labels

    def test_criminal_topic(self):
        text = "trafficking smuggling conspiracy fraud embezzlement bribery extortion"
        results = _classify_topics(text)
        labels = [r["label"] for r in results]
        assert "criminal" in labels

    def test_max_five_topics(self):
        # Text that triggers many topics — result capped at 5
        text = ("bank attorney flight trafficking surveillance campaign "
                "property email " * 20)
        results = _classify_topics(text)
        assert len(results) <= 5

    def test_score_normalized(self):
        text = "bank account transfer " * 50
        results = _classify_topics(text)
        for r in results:
            assert 0.0 <= r["score"] <= 1.0


# ═══════════════════════════════════════════
# _detect_aml_flags
# ═══════════════════════════════════════════


class TestDetectAmlFlags:
    def test_structuring_language(self):
        text = "We need to split the payment and keep it under the limit."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "structuring" in flag_types

    def test_structuring_amount_pattern(self):
        text = "Transfer $9,500 and then another $9,800 in cash deposits."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "structuring" in flag_types

    def test_structuring_severity_high(self):
        text = "Split the payment of $9,500 to avoid reporting."
        flags = _detect_aml_flags(text, text.lower())
        structuring = [f for f in flags if f["flag"] == "structuring"][0]
        assert structuring["severity"] == "high"

    def test_shell_company_needs_multiple_signals(self):
        # Single signal not enough
        text = "The LLC was registered."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "shell_company" not in flag_types

    def test_shell_company_two_signals(self):
        text = "The LLC was a holding company with a registered agent."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "shell_company" in flag_types

    def test_shell_company_severity_threshold(self):
        # 4+ signals → high, otherwise medium
        text = "LLC holding company registered agent nominee director bearer share offshore"
        flags = _detect_aml_flags(text, text.lower())
        shell = [f for f in flags if f["flag"] == "shell_company"][0]
        assert shell["severity"] == "high"

    def test_shell_company_medium_severity(self):
        text = "The LLC is a holding company." + " " * 100
        flags = _detect_aml_flags(text, text.lower())
        shell = [f for f in flags if f["flag"] == "shell_company"][0]
        assert shell["severity"] == "medium"

    def test_layering_single_signal(self):
        text = "We completed a wire transfer to the intermediary bank."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "layering" in flag_types

    def test_layering_severity_high(self):
        text = "Wire transfer through cryptocurrency mixer using bitcoin tumbler"
        flags = _detect_aml_flags(text, text.lower())
        layering = [f for f in flags if f["flag"] == "layering"][0]
        assert layering["severity"] == "high"

    def test_layering_severity_medium(self):
        text = "A single wire transfer was sent."
        flags = _detect_aml_flags(text, text.lower())
        layering = [f for f in flags if f["flag"] == "layering"][0]
        assert layering["severity"] == "medium"

    def test_jurisdiction_detection(self):
        text = "Funds were routed through the Cayman Islands and Panama."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "high_risk_jurisdiction" in flag_types

    def test_secrecy_language(self):
        text = "Off the record, delete this, burn after reading, eyes only."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "secrecy_concealment" in flag_types

    def test_secrecy_severity_high(self):
        text = "Off the record, delete this, burn after reading, eyes only."
        flags = _detect_aml_flags(text, text.lower())
        secrecy = [f for f in flags if f["flag"] == "secrecy_concealment"][0]
        assert secrecy["severity"] == "high"

    def test_secrecy_severity_medium(self):
        text = "This is confidential information. " + " " * 200
        flags = _detect_aml_flags(text, text.lower())
        secrecy = [f for f in flags if f["flag"] == "secrecy_concealment"][0]
        assert secrecy["severity"] == "medium"

    def test_round_number_transactions(self):
        text = "Payments of $5,000 and $10,000 and $50,000 were deposited."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "round_number_transactions" in flag_types

    def test_round_numbers_need_three(self):
        text = "Only $5,000 and $10,000 were sent."
        flags = _detect_aml_flags(text, text.lower())
        flag_types = [f["flag"] for f in flags]
        assert "round_number_transactions" not in flag_types

    def test_clean_document_no_flags(self):
        text = "The weather is nice today and the garden looks beautiful."
        flags = _detect_aml_flags(text, text.lower())
        assert flags == []

    def test_evidence_capped_at_five(self):
        signals = " ".join(f"word{i} wire transfer word{i}" for i in range(20))
        flags = _detect_aml_flags(signals, signals.lower())
        for flag in flags:
            assert len(flag["evidence"]) <= 5


# ═══════════════════════════════════════════
# _detect_codewords
# ═══════════════════════════════════════════


class TestDetectCodewords:
    def test_known_substitute_detected(self):
        text = "The pizza delivery is scheduled for tonight."
        results = _detect_codewords(text, text.lower())
        words = [r["word"] for r in results]
        assert "pizza" in words
        assert "delivery" in words

    def test_context_extraction(self):
        text = "Please send the package to the warehouse tomorrow morning."
        results = _detect_codewords(text, text.lower())
        pkg = [r for r in results if r["word"] == "package"][0]
        assert "package" in pkg["context"]

    def test_dedup_by_word(self):
        text = "pizza pizza pizza pizza pizza delivery"
        results = _detect_codewords(text, text.lower())
        pizza_entries = [r for r in results if r["word"] == "pizza"]
        assert len(pizza_entries) == 1
        assert pizza_entries[0]["count"] == 5

    def test_quoted_common_word(self):
        text = 'He said the "dinner" was ready and the "party" was arranged.'
        results = _detect_codewords(text, text.lower())
        words = [r["word"] for r in results]
        assert any('"dinner"' in w for w in words) or any('"party"' in w for w in words)

    def test_cap_at_thirty(self):
        # Create text with 40+ distinct known substitutes
        from dossier.core.forensic_analyzer import KNOWN_CODE_SUBSTITUTES
        words = list(KNOWN_CODE_SUBSTITUTES.keys())[:35]
        text = " ".join(words)
        results = _detect_codewords(text, text.lower())
        assert len(results) <= 30

    def test_sorted_by_count(self):
        text = "pizza " * 10 + "delivery " * 5 + "package " * 2
        results = _detect_codewords(text, text.lower())
        counts = [r["count"] for r in results]
        assert counts == sorted(counts, reverse=True)

    def test_no_codewords_in_clean_text(self):
        text = "mathematical theorem proves convergence analysis"
        results = _detect_codewords(text, text.lower())
        assert len(results) == 0


# ═══════════════════════════════════════════
# _is_common_word
# ═══════════════════════════════════════════


class TestIsCommonWord:
    def test_common_words(self):
        assert _is_common_word("dinner") is True
        assert _is_common_word("party") is True
        assert _is_common_word("candy") is True

    def test_uncommon_words(self):
        assert _is_common_word("algorithm") is False
        assert _is_common_word("jurisprudence") is False


# ═══════════════════════════════════════════
# _extract_repeated_phrases
# ═══════════════════════════════════════════


class TestExtractRepeatedPhrases:
    def test_trigrams_detected(self):
        text = "the quick brown fox jumped over the quick brown fox again"
        results = _extract_repeated_phrases(text, min_count=2)
        phrases = [r["phrase"] for r in results]
        assert any("quick brown fox" in p for p in phrases)

    def test_fourgrams_detected(self):
        text = "jumped over the fence " * 3 + " and then something else happened"
        results = _extract_repeated_phrases(text, min_count=2)
        phrases = [r["phrase"] for r in results]
        assert any(len(p.split()) == 4 for p in phrases)

    def test_stop_word_filtering(self):
        # Phrases that are mostly stop words should be filtered
        text = "the and the or the and the or the and"
        results = _extract_repeated_phrases(text, min_count=2)
        # All phrases would be stop words → filtered
        assert len(results) == 0

    def test_min_count_threshold(self):
        text = "unique phrase here once and another unique phrase here once"
        results = _extract_repeated_phrases(text, min_count=3)
        # Nothing appears 3+ times
        assert all(r["count"] >= 3 for r in results)

    def test_max_fifty_results(self):
        # Generate text with many repeated phrases
        phrases_text = " ".join(
            f"important meeting topic{i} " * 3 for i in range(100)
        )
        results = _extract_repeated_phrases(phrases_text, min_count=2)
        assert len(results) <= 50


# ═══════════════════════════════════════════
# _extract_financial_indicators
# ═══════════════════════════════════════════


class TestExtractFinancialIndicators:
    def test_currency_amount_detected(self):
        text = "The transfer was $500,000 to the account."
        results = _extract_financial_indicators(text, text.lower())
        types = [r["type"] for r in results]
        assert "currency_amount" in types

    def test_structuring_range_high_risk(self):
        text = "Deposited $9,500 in cash at the bank."
        results = _extract_financial_indicators(text, text.lower())
        amounts = [r for r in results if r["type"] == "currency_amount"]
        assert any(r["risk_score"] >= 0.9 for r in amounts)

    def test_round_number_risk(self):
        text = "Payment of $5,000 received."
        results = _extract_financial_indicators(text, text.lower())
        amounts = [r for r in results if r["type"] == "currency_amount"]
        assert any(r["risk_score"] == 0.3 for r in amounts)

    def test_large_amount_medium_risk(self):
        text = "Transfer of $15,000 completed."
        results = _extract_financial_indicators(text, text.lower())
        amounts = [r for r in results if r["type"] == "currency_amount"]
        assert any(r["risk_score"] == 0.5 for r in amounts)

    def test_risk_boost_near_suspicious_language(self):
        text = "Split the payment of $15,000 to keep it under the limit."
        results = _extract_financial_indicators(text, text.lower())
        amounts = [r for r in results if r["type"] == "currency_amount"]
        # Base 0.5 + 0.2 boost = 0.7
        assert any(r["risk_score"] >= 0.7 for r in amounts)

    def test_account_number(self):
        text = "Account number: 12345678901234 was used."
        results = _extract_financial_indicators(text, text.lower())
        types = [r["type"] for r in results]
        assert "account_number" in types

    def test_routing_number(self):
        text = "Routing number: 123456789 for the bank."
        results = _extract_financial_indicators(text, text.lower())
        types = [r["type"] for r in results]
        assert "routing_number" in types

    def test_swift_code(self):
        text = "SWIFT code DEUTDEFF was used for the wire transfer."
        results = _extract_financial_indicators(text, text.lower())
        types = [r["type"] for r in results]
        assert "swift_code" in types
        swift = [r for r in results if r["type"] == "swift_code"][0]
        assert swift["value"] == "DEUTDEFF"

    def test_ein_detected(self):
        text = "The EIN is 12-3456789 for the holding company."
        results = _extract_financial_indicators(text, text.lower())
        types = [r["type"] for r in results]
        assert "ein" in types
        ein = [r for r in results if r["type"] == "ein"][0]
        assert ein["value"] == "12-3456789"

    def test_dedup_by_type_value(self):
        text = "Transfer $500,000 first, then another $500,000 later."
        results = _extract_financial_indicators(text, text.lower())
        amounts = [r for r in results if r["type"] == "currency_amount"]
        values = [r["value"] for r in amounts]
        # Should be deduped
        assert len(values) == len(set(values))

    def test_amounts_under_500_excluded(self):
        text = "Only $100 was paid."
        results = _extract_financial_indicators(text, text.lower())
        assert len(results) == 0

    def test_max_fifty_indicators(self):
        # Many amounts
        lines = [f"Transfer ${i},000 completed." for i in range(1, 60)]
        text = " ".join(lines)
        results = _extract_financial_indicators(text, text.lower())
        assert len(results) <= 50


# ═══════════════════════════════════════════
# _compute_risk_score
# ═══════════════════════════════════════════


class TestComputeRiskScore:
    def test_no_signals_zero_score(self):
        score = _compute_risk_score([], [], [], [])
        assert score == 0.0

    def test_high_severity_aml(self):
        flags = [{"flag": "structuring", "severity": "high", "evidence": []}]
        score = _compute_risk_score(flags, [], [], [])
        assert score == 0.25

    def test_medium_severity_aml(self):
        flags = [{"flag": "layering", "severity": "medium", "evidence": []}]
        score = _compute_risk_score(flags, [], [], [])
        assert score == 0.15

    def test_low_severity_aml(self):
        flags = [{"flag": "round_numbers", "severity": "low", "evidence": []}]
        score = _compute_risk_score(flags, [], [], [])
        assert score == 0.05

    def test_high_risk_financial(self):
        financial = [{"type": "currency", "value": "$9,500", "context": "", "risk_score": 0.9}]
        score = _compute_risk_score([], financial, [], [])
        assert score == 0.1

    def test_codeword_contribution_capped(self):
        codewords = [{"word": f"word{i}", "count": 1} for i in range(20)]
        score = _compute_risk_score([], [], codewords, [])
        assert score == 0.2  # Capped at 0.2

    def test_evasive_intent_boost(self):
        intents = [{"label": "evasive", "score": 0.5, "evidence": []}]
        score = _compute_risk_score([], [], [], intents)
        assert score == 0.1

    def test_threatening_intent_boost(self):
        intents = [{"label": "threatening", "score": 0.5, "evidence": []}]
        score = _compute_risk_score([], [], [], intents)
        assert score == 0.1

    def test_intent_below_threshold_no_boost(self):
        intents = [{"label": "evasive", "score": 0.2, "evidence": []}]
        score = _compute_risk_score([], [], [], intents)
        assert score == 0.0

    def test_score_capped_at_one(self):
        flags = [
            {"flag": "structuring", "severity": "high", "evidence": []},
            {"flag": "layering", "severity": "high", "evidence": []},
            {"flag": "shell", "severity": "high", "evidence": []},
            {"flag": "secrecy", "severity": "high", "evidence": []},
            {"flag": "jurisdiction", "severity": "high", "evidence": []},
        ]
        financial = [{"type": "c", "value": "v", "context": "", "risk_score": 0.9}] * 5
        codewords = [{"word": f"w{i}", "count": 1} for i in range(20)]
        intents = [
            {"label": "evasive", "score": 0.5, "evidence": []},
            {"label": "threatening", "score": 0.5, "evidence": []},
        ]
        score = _compute_risk_score(flags, financial, codewords, intents)
        assert score == 1.0

    def test_multiple_flag_accumulation(self):
        flags = [
            {"flag": "structuring", "severity": "high", "evidence": []},
            {"flag": "layering", "severity": "medium", "evidence": []},
        ]
        score = _compute_risk_score(flags, [], [], [])
        assert score == 0.4  # 0.25 + 0.15
