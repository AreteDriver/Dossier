"""Tests for the anomaly detection module."""

from dossier.forensics.anomaly import (
    detect_activity_bursts,
    detect_ingestion_anomalies,
    detect_isolation_anomalies,
    detect_missing_metadata,
    detect_page_outliers,
    detect_sudden_appearances,
    detect_temporal_gaps,
)


class TestDetectTemporalGaps:
    def test_finds_gap(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2015-06-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert len(result) == 1
        assert result[0]["type"] == "temporal_gap"
        assert result[0]["evidence"]["gap_days"] == 151

    def test_no_gap(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2015-01-15"},
        ]
        assert detect_temporal_gaps(events, min_gap_days=90) == []

    def test_severity_high_over_year(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2016-06-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert result[0]["severity"] == "high"

    def test_severity_medium(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2015-08-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert result[0]["severity"] == "medium"

    def test_severity_low(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2015-05-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert result[0]["severity"] == "low"

    def test_empty_events(self):
        assert detect_temporal_gaps([]) == []

    def test_single_event(self):
        assert detect_temporal_gaps([{"id": 1, "event_date": "2015-01-01"}]) == []

    def test_invalid_date_skipped(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "not-a-date"},
            {"id": 3, "event_date": "2015-06-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert len(result) == 1

    def test_uses_date_key(self):
        events = [
            {"id": 1, "date": "2015-01-01"},
            {"id": 2, "date": "2015-06-01"},
        ]
        result = detect_temporal_gaps(events, min_gap_days=90)
        assert len(result) == 1


class TestDetectActivityBursts:
    def test_finds_burst(self):
        events = [{"id": i, "event_date": "2015-01-15"} for i in range(20)] + [
            {"id": 100 + i, "event_date": f"2015-{m:02d}-15"} for i, m in enumerate(range(2, 12))
        ]
        result = detect_activity_bursts(events, std_threshold=2.0)
        assert len(result) >= 1
        assert result[0]["type"] == "activity_burst"
        assert result[0]["evidence"]["month"] == "2015-01"

    def test_no_burst(self):
        events = [{"id": i, "event_date": f"2015-{(i % 12) + 1:02d}-15"} for i in range(12)]
        result = detect_activity_bursts(events, std_threshold=2.0)
        assert result == []

    def test_too_few_months(self):
        events = [
            {"id": 1, "event_date": "2015-01-01"},
            {"id": 2, "event_date": "2015-01-05"},
        ]
        assert detect_activity_bursts(events) == []

    def test_zero_stdev(self):
        # All months have exactly the same count
        events = [{"id": i, "event_date": f"2015-{(i % 6) + 1:02d}-15"} for i in range(6)]
        assert detect_activity_bursts(events) == []


class TestDetectPageOutliers:
    def test_finds_outlier(self):
        docs = [{"id": i, "pages": 10} for i in range(20)]
        docs.append({"id": 99, "pages": 500})
        result = detect_page_outliers(docs)
        assert len(result) == 1
        assert result[0]["affected_ids"] == [99]

    def test_no_outlier(self):
        docs = [{"id": i, "pages": 10} for i in range(5)]
        assert detect_page_outliers(docs) == []

    def test_too_few_docs(self):
        docs = [{"id": 1, "pages": 100}]
        assert detect_page_outliers(docs) == []

    def test_zero_stdev(self):
        docs = [{"id": i, "pages": 10} for i in range(5)]
        assert detect_page_outliers(docs) == []

    def test_zero_pages_excluded(self):
        docs = [{"id": i, "pages": 0} for i in range(5)]
        assert detect_page_outliers(docs) == []


class TestDetectIngestionAnomalies:
    def test_bulk_dump(self):
        base = "2025-01-15T10:00:00"
        docs = [{"id": i, "ingested_at": base} for i in range(15)]
        result = detect_ingestion_anomalies(docs)
        bulk = [a for a in result if a["type"] == "bulk_dump"]
        assert len(bulk) == 1
        assert bulk[0]["evidence"]["count"] == 15

    def test_ingestion_gap(self):
        docs = [
            {"id": 1, "ingested_at": "2025-01-01T10:00:00"},
            {"id": 2, "ingested_at": "2025-02-15T10:00:00"},
        ]
        result = detect_ingestion_anomalies(docs, gap_hours=168)
        gaps = [a for a in result if a["type"] == "ingestion_gap"]
        assert len(gaps) == 1

    def test_no_anomalies(self):
        docs = [
            {"id": 1, "ingested_at": "2025-01-01T10:00:00"},
            {"id": 2, "ingested_at": "2025-01-02T10:00:00"},
        ]
        assert detect_ingestion_anomalies(docs) == []

    def test_empty_docs(self):
        assert detect_ingestion_anomalies([]) == []

    def test_single_doc(self):
        assert detect_ingestion_anomalies([{"id": 1, "ingested_at": "2025-01-01"}]) == []

    def test_invalid_timestamp_skipped(self):
        docs = [
            {"id": 1, "ingested_at": "2025-01-01T10:00:00"},
            {"id": 2, "ingested_at": "bad-date"},
            {"id": 3, "ingested_at": "2025-06-01T10:00:00"},
        ]
        result = detect_ingestion_anomalies(docs, gap_hours=168)
        assert any(a["type"] == "ingestion_gap" for a in result)


class TestDetectMissingMetadata:
    def test_finds_missing(self):
        docs = [{"id": 1, "date": None, "source": "test", "category": "report"}]
        result = detect_missing_metadata(docs)
        assert len(result) == 1
        assert "date" in result[0]["evidence"]["missing_fields"]
        assert result[0]["severity"] == "low"

    def test_multiple_missing(self):
        docs = [{"id": 1, "date": None, "source": "", "category": "report"}]
        result = detect_missing_metadata(docs)
        assert result[0]["severity"] == "medium"
        assert len(result[0]["evidence"]["missing_fields"]) == 2

    def test_all_present(self):
        docs = [{"id": 1, "date": "2025-01-01", "source": "test", "category": "report"}]
        assert detect_missing_metadata(docs) == []

    def test_empty_docs(self):
        assert detect_missing_metadata([]) == []


class TestDetectIsolationAnomalies:
    def test_finds_isolated(self):
        entities = [{"id": 1, "name": "John Doe", "type": "person", "total_mentions": 15}]
        connections = []  # No connections at all
        result = detect_isolation_anomalies(entities, connections)
        assert len(result) == 1
        assert result[0]["type"] == "isolation_anomaly"

    def test_connected_entity_ok(self):
        entities = [{"id": 1, "name": "John Doe", "type": "person", "total_mentions": 15}]
        connections = [{"entity_a_id": 1, "entity_b_id": 2}]
        assert detect_isolation_anomalies(entities, connections) == []

    def test_low_mentions_ok(self):
        entities = [{"id": 1, "name": "John Doe", "type": "person", "total_mentions": 3}]
        connections = []
        assert detect_isolation_anomalies(entities, connections) == []

    def test_uses_mentions_key(self):
        entities = [{"id": 1, "name": "Corp Inc", "type": "org", "mentions": 20}]
        connections = []
        result = detect_isolation_anomalies(entities, connections)
        assert len(result) == 1


class TestDetectSuddenAppearances:
    def test_finds_sudden(self):
        entities = [{"id": 1, "name": "New Person"}]
        events = [{"entity_id": 1, "event_date": f"2015-01-{d:02d}"} for d in range(1, 10)]
        result = detect_sudden_appearances(entities, events)
        assert len(result) == 1
        assert result[0]["type"] == "sudden_appearance"
        assert result[0]["severity"] == "high"

    def test_spread_out_ok(self):
        entities = [{"id": 1, "name": "Regular Person"}]
        events = [{"entity_id": 1, "event_date": f"2015-{m:02d}-01"} for m in range(1, 7)]
        result = detect_sudden_appearances(entities, events)
        assert result == []

    def test_too_few_events(self):
        entities = [{"id": 1, "name": "Minor Person"}]
        events = [{"entity_id": 1, "event_date": "2015-01-01"}]
        assert detect_sudden_appearances(entities, events) == []

    def test_no_entity_id(self):
        entities = [{"id": 1, "name": "X"}]
        events = [{"event_date": "2015-01-01"}]  # no entity_id
        assert detect_sudden_appearances(entities, events) == []

    def test_unknown_entity(self):
        entities = []
        events = [{"entity_id": 99, "event_date": f"2015-01-{d:02d}"} for d in range(1, 10)]
        result = detect_sudden_appearances(entities, events)
        assert len(result) == 1
        assert "Entity 99" in result[0]["description"]
