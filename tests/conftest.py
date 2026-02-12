"""Shared fixtures for Dossier test suite."""

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
    import dossier.api.server as srv_mod
    import dossier.ingestion.pipeline as pipe_mod

    monkeypatch.setattr(srv_mod, "UPLOAD_DIR", tmp_path / "inbox")
    monkeypatch.setattr(pipe_mod, "PROCESSED_DIR", tmp_path / "processed")

    # Allow tmp_path for directory ingest in tests
    monkeypatch.setattr(srv_mod, "ALLOWED_BASE_DIRS", [tmp_path, Path.home()])

    from dossier.api.server import app

    with TestClient(app) as c:
        yield c
