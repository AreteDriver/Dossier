"""Shared fixtures for Dossier test suite."""

import io
import sqlite3
import textwrap

import pytest

import dossier.db.database as db_mod


SAMPLE_TEXT = textwrap.dedent("""\
    DEPOSITION OF VIRGINIA GIUFFRE

    On January 15, 2015, the deposition of Virginia Giuffre was taken at
    the offices of the FBI in Palm Beach, Florida.

    Ms. Giuffre stated that Jeffrey Epstein and Ghislaine Maxwell recruited
    her from Mar-a-Lago when she was working as a towel girl. She described
    being flown on Epstein's aircraft from Teterboro to Little St. James
    in the U.S. Virgin Islands.

    Detective Recarey of the Palm Beach Police confirmed that the investigation
    began in 2005 after a complaint was filed.

    The Clinton Foundation was mentioned in connection with fundraising events.
    Goldman Sachs provided financial advisory services.

    Subject: Re: Meeting notes
    From: john.podesta@example.com
    Date: 03/15/2016

    Dear Mr. Sullivan,
    Please review the attached flight log manifest from 2002-2003.
    The passenger list includes trips to New York, London, and Paris.
    Regards,
    John Podesta
""")


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Provide a temporary SQLite database for tests."""
    db_path = str(tmp_path / "test_dossier.db")
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    db_mod.init_db()
    return db_path


@pytest.fixture
def db_conn(tmp_db):
    """Provide an open connection to the temp database."""
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    yield conn
    conn.close()


@pytest.fixture
def sample_text():
    """Return realistic sample text containing known entities."""
    return SAMPLE_TEXT


@pytest.fixture
def sample_file(tmp_path, sample_text):
    """Write sample text to a .txt file and return the path."""
    p = tmp_path / "deposition_giuffre.txt"
    p.write_text(sample_text)
    return p


@pytest.fixture
def client(tmp_path, monkeypatch):
    """FastAPI TestClient backed by a temp database."""
    from pathlib import Path
    from fastapi.testclient import TestClient

    db_path = str(tmp_path / "api_test.db")
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    # Redirect upload and processed dirs to tmp_path
    import dossier.api.utils as utils_mod
    import dossier.ingestion.pipeline as pipe_mod

    monkeypatch.setattr(utils_mod, "UPLOAD_DIR", tmp_path / "inbox")
    monkeypatch.setattr(pipe_mod, "PROCESSED_DIR", tmp_path / "processed")

    # Allow tmp_path for directory ingest in tests
    monkeypatch.setattr(utils_mod, "ALLOWED_BASE_DIRS", [tmp_path, Path.home()])

    from dossier.api.server import app

    with TestClient(app) as c:
        yield c


def upload_sample(client, filename="test_doc.txt", content=None):
    """Upload a sample text file and return the response."""
    if content is None:
        content = (
            "Jeffrey Epstein and Ghislaine Maxwell were investigated by the FBI "
            "in Palm Beach. The deposition was taken on January 15, 2015. "
            "Goldman Sachs provided financial records related to the case."
        )
    return client.post(
        "/api/upload",
        files={"file": (filename, io.BytesIO(content.encode()), "text/plain")},
        params={"source": "Test Upload"},
    )


def seed_forensics(client):
    """Upload a sample doc and seed forensics tables for testing.

    Returns the document ID of the uploaded document.
    """
    r = upload_sample(client)
    doc_id = r.json()["document_id"]

    with db_mod.get_db() as conn:
        # Seed document_forensics
        conn.execute(
            "INSERT INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, "risk_score", "risk_score", 0.85, "high", "High risk indicators"),
        )
        conn.execute(
            "INSERT INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, "aml_flag", "shell_company", 0.9, "high", "Shell company detected"),
        )
        conn.execute(
            "INSERT INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, "topic", "financial_crime", 0.75, None, "Topic classification"),
        )
        conn.execute(
            "INSERT INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, "intent", "concealment", 0.65, None, "Intent classification"),
        )
        conn.execute(
            "INSERT INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_id, "codeword", "package", 0.5, None, "Suspicious term in context"),
        )

        # Seed financial_indicators
        conn.execute(
            "INSERT INTO financial_indicators (document_id, indicator_type, value, context, risk_score) VALUES (?, ?, ?, ?, ?)",
            (doc_id, "currency_amount", "$500,000", "Wire transfer of $500,000", 0.8),
        )

        # Seed phrases table
        conn.execute(
            "INSERT OR IGNORE INTO phrases (phrase, doc_count, total_count) VALUES (?, ?, ?)",
            ("financial records", 1, 3),
        )
        phrase_id = conn.execute(
            "SELECT id FROM phrases WHERE phrase = ?", ("financial records",)
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO document_phrases (document_id, phrase_id, count) VALUES (?, ?, ?)",
            (doc_id, phrase_id, 3),
        )

        conn.commit()

    return doc_id


def seed_multi_doc_data(client):
    """Upload multiple docs with shared entities for intelligence endpoint testing.

    Creates 4 docs across categories (deposition, correspondence, flight, report)
    with overlapping person/org/place entities so inner loops execute.
    Returns list of document IDs.
    """
    docs = [
        (
            "deposition_witness.txt",
            "Jeffrey Epstein and Ghislaine Maxwell were named in the deposition. "
            "The FBI investigated the case in Palm Beach Florida. "
            "Goldman Sachs provided financial records. Bill Clinton attended events. "
            "Testimony was given under oath in New York.",
        ),
        (
            "correspondence_memo.txt",
            "From: John Podesta. To: Bill Clinton. "
            "Regarding the meeting with Jeffrey Epstein at the office. "
            "Goldman Sachs prepared advisory materials. Palm Beach visit confirmed. "
            "Please review the attached flight log manifest.",
        ),
        (
            "flight_manifest_2002.txt",
            "Flight log manifest passenger list. Jeffrey Epstein and Bill Clinton "
            "flew from New York to Palm Beach via Teterboro. "
            "Ghislaine Maxwell was also a passenger. Flight date 2002-03-15.",
        ),
        (
            "financial_report.txt",
            "Goldman Sachs report on wire transfers. Jeffrey Epstein account "
            "showed $9,500 structured deposits through the Cayman Islands LLC. "
            "Split the payment to keep it under the limit. Off the record.",
        ),
    ]

    doc_ids = []
    for filename, content in docs:
        r = upload_sample(client, filename=filename, content=content)
        doc_ids.append(r.json()["document_id"])

    # Seed additional metadata for richer endpoint testing
    with db_mod.get_db() as conn:
        # Set categories and dates
        conn.execute(
            "UPDATE documents SET category = 'deposition', date = '2015-01-15', source = 'FBI' WHERE id = ?",
            (doc_ids[0],),
        )
        conn.execute(
            "UPDATE documents SET category = 'correspondence', date = '2016-03-15', source = 'FOIA' WHERE id = ?",
            (doc_ids[1],),
        )
        conn.execute(
            "UPDATE documents SET category = 'flight', date = '2002-03-15', source = 'FAA' WHERE id = ?",
            (doc_ids[2],),
        )
        conn.execute(
            "UPDATE documents SET category = 'report', date = '2018-06-01', source = 'SEC' WHERE id = ?",
            (doc_ids[3],),
        )

        # Seed financial_indicators for influence scores
        conn.execute(
            "INSERT OR IGNORE INTO financial_indicators (document_id, indicator_type, value, context, risk_score) VALUES (?, ?, ?, ?, ?)",
            (doc_ids[3], "currency_amount", "$9,500", "Structured deposit", 0.9),
        )

        # Seed events for timeline/narrative
        conn.execute(
            "INSERT INTO events (document_id, event_date, date_raw, context, confidence, precision) VALUES (?, ?, ?, ?, ?, ?)",
            (
                doc_ids[0],
                "2015-01-15",
                "January 15, 2015",
                "Deposition taken at FBI offices",
                0.9,
                "day",
            ),
        )
        conn.execute(
            "INSERT INTO events (document_id, event_date, date_raw, context, confidence, precision) VALUES (?, ?, ?, ?, ?, ?)",
            (doc_ids[2], "2002-03-15", "2002-03-15", "Flight from NY to Palm Beach", 0.8, "day"),
        )

        # Seed entity_connections for link analysis
        entities = conn.execute("SELECT id, name FROM entities").fetchall()
        ent_map = {e["name"]: e["id"] for e in entities}
        epstein_id = None
        maxwell_id = None
        for name, eid in ent_map.items():
            if "epstein" in name.lower():
                epstein_id = eid
            if "maxwell" in name.lower():
                maxwell_id = eid
        if epstein_id and maxwell_id:
            conn.execute(
                "INSERT OR IGNORE INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
                (min(epstein_id, maxwell_id), max(epstein_id, maxwell_id), 4),
            )

        conn.commit()

    return doc_ids


def seed_analytics_data(client):
    """Seed comprehensive data for analytics endpoint testing.

    Builds on seed_multi_doc_data with additional entity_connections,
    financial_indicators, forensics data, and events for full analytics coverage.
    Returns list of document IDs.
    """
    doc_ids = seed_multi_doc_data(client)

    with db_mod.get_db() as conn:
        entities = conn.execute("SELECT id, name, type FROM entities").fetchall()

        # Add more entity connections for graph/matrix endpoints
        person_ids = [e["id"] for e in entities if e["type"] == "person"]
        for i in range(len(person_ids)):
            for j in range(i + 1, min(i + 3, len(person_ids))):
                conn.execute(
                    "INSERT OR IGNORE INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
                    (
                        min(person_ids[i], person_ids[j]),
                        max(person_ids[i], person_ids[j]),
                        i + j + 1,
                    ),
                )

        # Add more forensics data across document types
        for doc_id in doc_ids:
            conn.execute(
                "INSERT OR IGNORE INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                (doc_id, "risk_score", "risk_score", 0.6 + (doc_id * 0.05), "medium", "Analysis"),
            )
            conn.execute(
                "INSERT OR IGNORE INTO document_forensics (document_id, analysis_type, label, score, severity, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                (doc_id, "topic", "financial", 0.7, None, "Financial topic"),
            )

        # Add entity aliases for alias-network testing
        if person_ids:
            conn.execute(
                "INSERT OR IGNORE INTO entity_aliases (entity_id, alias_name) VALUES (?, ?)",
                (person_ids[0], "Jeff E"),
            )

        # Add entity resolution records
        if len(person_ids) >= 2:
            conn.execute(
                "INSERT OR IGNORE INTO entity_resolutions (source_entity_id, canonical_entity_id) VALUES (?, ?)",
                (person_ids[0], person_ids[1]),
            )

        conn.commit()

    return doc_ids
