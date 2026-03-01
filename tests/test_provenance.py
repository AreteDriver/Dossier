"""Tests for the PDF metadata provenance module."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from dossier.forensics.provenance import (
    PDFMetadata,
    _ensure_pdf_metadata_table,
    _parse_pdf_date,
    extract_pdf_metadata,
    get_corpus_metadata_stats,
    get_metadata_timeline,
    get_pdf_metadata,
    search_pdf_metadata,
    store_pdf_metadata,
)


# ── Fixtures ────────────────────────────────────────────────────


@pytest.fixture
def memory_db():
    """In-memory SQLite database with required tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT DEFAULT '',
            filename TEXT DEFAULT '',
            category TEXT DEFAULT '',
            source TEXT DEFAULT '',
            raw_text TEXT DEFAULT ''
        );
    """)
    _ensure_pdf_metadata_table(conn)
    conn.commit()
    return conn


@pytest.fixture
def sample_metadata():
    """Return a sample PDFMetadata instance."""
    return PDFMetadata(
        document_id=1,
        author="John Smith",
        creator="Microsoft Word",
        producer="Adobe PDF Library 15.0",
        title="Confidential Report",
        subject="Financial Analysis",
        keywords="finance, audit, compliance",
        creation_date="2020-03-15T10:30:00",
        modification_date="2020-06-20T14:45:00",
        encrypted=False,
        page_count=42,
        file_size=1048576,
    )


# ── Date Parsing ────────────────────────────────────────────────


class TestParsePdfDate:
    def test_full_format_with_timezone(self):
        result = _parse_pdf_date("D:20030305120000Z")
        assert result == "2003-03-05T12:00:00"

    def test_full_format_with_offset(self):
        result = _parse_pdf_date("D:20030305120000+05'00'")
        assert result == "2003-03-05T12:00:00"

    def test_date_only(self):
        result = _parse_pdf_date("D:20030305")
        assert result == "2003-03-05T00:00:00"

    def test_year_month_only(self):
        result = _parse_pdf_date("D:200303")
        assert result == "2003-03-01T00:00:00"

    def test_year_only(self):
        result = _parse_pdf_date("D:2003")
        assert result == "2003-01-01T00:00:00"

    def test_no_prefix(self):
        result = _parse_pdf_date("20030305120000")
        assert result == "2003-03-05T12:00:00"

    def test_none_input(self):
        assert _parse_pdf_date(None) is None

    def test_empty_string(self):
        assert _parse_pdf_date("") is None

    def test_garbage_input(self):
        assert _parse_pdf_date("not-a-date") is None

    def test_non_string_input(self):
        assert _parse_pdf_date(12345) is None

    def test_negative_timezone(self):
        result = _parse_pdf_date("D:20200101080000-05'00'")
        assert result == "2020-01-01T08:00:00"

    def test_with_hour_minute(self):
        result = _parse_pdf_date("D:200303051430")
        assert result == "2003-03-05T14:30:00"


# ── Extraction ──────────────────────────────────────────────────


class TestExtractPdfMetadata:
    def test_non_pdf_returns_none(self, tmp_path):
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("hello")
        assert extract_pdf_metadata(str(txt_file), document_id=1) is None

    def test_nonexistent_file_returns_none(self):
        assert extract_pdf_metadata("/no/such/file.pdf", document_id=1) is None

    def test_successful_extraction(self, tmp_path):
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"fake-pdf-content")

        mock_pdf = MagicMock()
        mock_pdf.metadata = {
            "Author": "Jane Doe",
            "Creator": "LibreOffice",
            "Producer": "LibreOffice PDF",
            "Title": "Test Report",
            "Subject": "Testing",
            "Keywords": "test, pdf",
            "CreationDate": "D:20200315103000Z",
            "ModDate": "D:20200620144500Z",
        }
        mock_pdf.pages = [MagicMock()] * 5
        mock_pdf.is_encrypted = False
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        with patch("pdfplumber.open", return_value=mock_pdf):
            result = extract_pdf_metadata(str(pdf_file), document_id=42)

        assert result is not None
        assert result.document_id == 42
        assert result.author == "Jane Doe"
        assert result.creator == "LibreOffice"
        assert result.producer == "LibreOffice PDF"
        assert result.title == "Test Report"
        assert result.creation_date == "2020-03-15T10:30:00"
        assert result.page_count == 5
        assert result.encrypted is False

    def test_empty_metadata(self, tmp_path):
        pdf_file = tmp_path / "empty.pdf"
        pdf_file.write_bytes(b"fake-pdf")

        mock_pdf = MagicMock()
        mock_pdf.metadata = {}
        mock_pdf.pages = [MagicMock()] * 2
        mock_pdf.is_encrypted = False
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        with patch("pdfplumber.open", return_value=mock_pdf):
            result = extract_pdf_metadata(str(pdf_file), document_id=1)

        assert result is not None
        assert result.author is None
        assert result.creator is None
        assert result.page_count == 2

    def test_encrypted_pdf(self, tmp_path):
        pdf_file = tmp_path / "encrypted.pdf"
        pdf_file.write_bytes(b"fake-pdf")

        mock_pdf = MagicMock()
        mock_pdf.metadata = {"Author": "Secret"}
        mock_pdf.pages = [MagicMock()]
        mock_pdf.is_encrypted = True
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        with patch("pdfplumber.open", return_value=mock_pdf):
            result = extract_pdf_metadata(str(pdf_file), document_id=1)

        assert result is not None
        assert result.encrypted is True

    def test_extraction_exception_returns_none(self, tmp_path):
        pdf_file = tmp_path / "bad.pdf"
        pdf_file.write_bytes(b"corrupted")

        with patch("pdfplumber.open", side_effect=Exception("corrupt")):
            result = extract_pdf_metadata(str(pdf_file), document_id=1)

        assert result is None

    def test_encrypt_flag_from_metadata(self, tmp_path):
        """Fallback encryption detection from metadata dict."""
        pdf_file = tmp_path / "enc2.pdf"
        pdf_file.write_bytes(b"fake-pdf")

        mock_pdf = MagicMock()
        mock_pdf.metadata = {"Author": "Test", "Encrypt": "Standard"}
        mock_pdf.pages = [MagicMock()]
        mock_pdf.is_encrypted = False
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        with patch("pdfplumber.open", return_value=mock_pdf):
            result = extract_pdf_metadata(str(pdf_file), document_id=1)

        assert result is not None
        assert result.encrypted is True


# ── Database Storage ────────────────────────────────────────────


class TestDatabaseOperations:
    def test_store_and_retrieve(self, memory_db, sample_metadata):
        memory_db.execute("INSERT INTO documents (title) VALUES (?)", ("Test Doc",))
        memory_db.commit()

        store_pdf_metadata(memory_db, sample_metadata)
        memory_db.commit()

        result = get_pdf_metadata(memory_db, document_id=1)
        assert result is not None
        assert result["author"] == "John Smith"
        assert result["creator"] == "Microsoft Word"
        assert result["producer"] == "Adobe PDF Library 15.0"
        assert result["page_count"] == 42
        assert result["file_size"] == 1048576

    def test_get_nonexistent_returns_none(self, memory_db):
        result = get_pdf_metadata(memory_db, document_id=999)
        assert result is None

    def test_store_replaces_on_conflict(self, memory_db, sample_metadata):
        memory_db.execute("INSERT INTO documents (title) VALUES (?)", ("Test Doc",))
        memory_db.commit()

        store_pdf_metadata(memory_db, sample_metadata)
        memory_db.commit()

        updated = PDFMetadata(
            document_id=1,
            author="Updated Author",
            creator=sample_metadata.creator,
            producer=sample_metadata.producer,
            title=sample_metadata.title,
            subject=sample_metadata.subject,
            keywords=sample_metadata.keywords,
            creation_date=sample_metadata.creation_date,
            modification_date=sample_metadata.modification_date,
            encrypted=True,
            page_count=100,
            file_size=2000000,
        )
        store_pdf_metadata(memory_db, updated)
        memory_db.commit()

        result = get_pdf_metadata(memory_db, document_id=1)
        assert result["author"] == "Updated Author"
        assert result["page_count"] == 100
        assert result["encrypted"] == 1


# ── Corpus Stats ────────────────────────────────────────────────


class TestCorpusStats:
    def test_empty_corpus(self, memory_db):
        stats = get_corpus_metadata_stats(memory_db)
        assert stats["total_pdfs"] == 0
        assert stats["top_authors"] == []
        assert stats["encrypted_count"] == 0

    def test_with_data(self, memory_db):
        for i in range(1, 4):
            memory_db.execute("INSERT INTO documents (title) VALUES (?)", (f"Doc {i}",))
        memory_db.commit()

        metas = [
            PDFMetadata(
                1,
                "Alice",
                "Word",
                "Adobe",
                None,
                None,
                None,
                "2020-01-01T00:00:00",
                None,
                False,
                10,
                1000,
            ),
            PDFMetadata(
                2,
                "Alice",
                "Word",
                "LibreOffice",
                None,
                None,
                None,
                "2021-06-15T00:00:00",
                None,
                True,
                20,
                2000,
            ),
            PDFMetadata(
                3,
                "Bob",
                "LaTeX",
                "pdfTeX",
                None,
                None,
                None,
                "2019-03-01T00:00:00",
                None,
                False,
                5,
                500,
            ),
        ]
        for m in metas:
            store_pdf_metadata(memory_db, m)
        memory_db.commit()

        stats = get_corpus_metadata_stats(memory_db)
        assert stats["total_pdfs"] == 3
        assert stats["encrypted_count"] == 1
        assert stats["total_pages"] == 35
        assert stats["total_size_bytes"] == 3500
        assert stats["date_range"]["earliest"] == "2019-03-01T00:00:00"
        assert stats["date_range"]["latest"] == "2021-06-15T00:00:00"

        # Alice should be top author (2 docs)
        assert stats["top_authors"][0]["author"] == "Alice"
        assert stats["top_authors"][0]["count"] == 2


# ── Search ──────────────────────────────────────────────────────


class TestSearch:
    def test_search_by_author(self, memory_db):
        for i in range(1, 3):
            memory_db.execute(
                "INSERT INTO documents (title, filename, category) VALUES (?, ?, ?)",
                (f"Doc {i}", f"doc{i}.pdf", "report"),
            )
        memory_db.commit()

        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                1,
                "Alice Smith",
                "Word",
                "Adobe",
                None,
                None,
                None,
                None,
                None,
                False,
                10,
                1000,
            ),
        )
        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                2,
                "Bob Jones",
                "Word",
                "Adobe",
                None,
                None,
                None,
                None,
                None,
                False,
                5,
                500,
            ),
        )
        memory_db.commit()

        results = search_pdf_metadata(memory_db, author="Alice")
        assert len(results) == 1
        assert results[0]["author"] == "Alice Smith"

    def test_search_by_producer(self, memory_db):
        memory_db.execute(
            "INSERT INTO documents (title, filename, category) VALUES (?, ?, ?)",
            ("Doc 1", "doc1.pdf", "report"),
        )
        memory_db.commit()

        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                1,
                None,
                None,
                "LibreOffice 7.0",
                None,
                None,
                None,
                None,
                None,
                False,
                3,
                300,
            ),
        )
        memory_db.commit()

        results = search_pdf_metadata(memory_db, producer="LibreOffice")
        assert len(results) == 1

    def test_search_no_filters(self, memory_db):
        """No filters returns all entries."""
        memory_db.execute(
            "INSERT INTO documents (title, filename, category) VALUES (?, ?, ?)",
            ("Doc 1", "doc1.pdf", "report"),
        )
        memory_db.commit()

        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                1,
                "Alice",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                False,
                1,
                100,
            ),
        )
        memory_db.commit()

        results = search_pdf_metadata(memory_db)
        assert len(results) == 1


# ── Timeline ────────────────────────────────────────────────────


class TestMetadataTimeline:
    def test_empty_timeline(self, memory_db):
        entries = get_metadata_timeline(memory_db)
        assert entries == []

    def test_timeline_with_dates(self, memory_db):
        for i in range(1, 3):
            memory_db.execute(
                "INSERT INTO documents (title, filename) VALUES (?, ?)",
                (f"Doc {i}", f"doc{i}.pdf"),
            )
        memory_db.commit()

        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                1,
                "Alice",
                None,
                None,
                None,
                None,
                None,
                "2020-01-01T00:00:00",
                "2020-02-01T00:00:00",
                False,
                10,
                1000,
            ),
        )
        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                2,
                "Bob",
                None,
                None,
                None,
                None,
                None,
                "2019-06-15T00:00:00",
                None,
                False,
                5,
                500,
            ),
        )
        memory_db.commit()

        entries = get_metadata_timeline(memory_db)
        assert len(entries) == 2
        # Should be sorted by creation_date
        assert entries[0]["creation_date"] == "2019-06-15T00:00:00"
        assert entries[1]["creation_date"] == "2020-01-01T00:00:00"

    def test_timeline_excludes_no_dates(self, memory_db):
        memory_db.execute(
            "INSERT INTO documents (title, filename) VALUES (?, ?)",
            ("Doc 1", "doc1.pdf"),
        )
        memory_db.commit()

        store_pdf_metadata(
            memory_db,
            PDFMetadata(
                1,
                "Alice",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                False,
                10,
                1000,
            ),
        )
        memory_db.commit()

        entries = get_metadata_timeline(memory_db)
        assert entries == []


# ── API Endpoint Integration Tests ──────────────────────────────


class TestAPIEndpoints:
    def test_get_pdf_metadata_not_found(self, client):
        r = client.get("/api/documents/9999/pdf-metadata")
        assert r.status_code == 404

    def test_get_pdf_metadata_no_metadata(self, client):
        from tests.conftest import upload_sample

        resp = upload_sample(client)
        doc_id = resp.json()["document_id"]
        r = client.get(f"/api/documents/{doc_id}/pdf-metadata")
        assert r.status_code == 200
        data = r.json()
        assert data["document_id"] == doc_id
        assert data["pdf_metadata"] is None

    def test_get_pdf_metadata_with_data(self, client):
        from tests.conftest import upload_sample

        resp = upload_sample(client)
        doc_id = resp.json()["document_id"]

        # Manually store metadata
        from dossier.db.database import get_db
        from dossier.forensics.provenance import _ensure_pdf_metadata_table, store_pdf_metadata

        with get_db() as conn:
            _ensure_pdf_metadata_table(conn)
            store_pdf_metadata(
                conn,
                PDFMetadata(
                    doc_id,
                    "Test Author",
                    "Word",
                    "Adobe",
                    "Title",
                    None,
                    None,
                    "2020-01-01T00:00:00",
                    None,
                    False,
                    10,
                    5000,
                ),
            )
            conn.commit()

        r = client.get(f"/api/documents/{doc_id}/pdf-metadata")
        assert r.status_code == 200
        meta = r.json()["pdf_metadata"]
        assert meta["author"] == "Test Author"
        assert meta["page_count"] == 10

    def test_pdf_metadata_stats_empty(self, client):
        r = client.get("/api/pdf-metadata/stats")
        assert r.status_code == 200
        assert r.json()["total_pdfs"] == 0

    def test_pdf_metadata_search_empty(self, client):
        r = client.get("/api/pdf-metadata/search", params={"author": "nobody"})
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_pdf_metadata_timeline_empty(self, client):
        r = client.get("/api/pdf-metadata/timeline")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_pdf_metadata_stats_with_data(self, client):
        from tests.conftest import upload_sample

        resp = upload_sample(client)
        doc_id = resp.json()["document_id"]

        from dossier.db.database import get_db
        from dossier.forensics.provenance import _ensure_pdf_metadata_table, store_pdf_metadata

        with get_db() as conn:
            _ensure_pdf_metadata_table(conn)
            store_pdf_metadata(
                conn,
                PDFMetadata(
                    doc_id,
                    "Analyst",
                    "Word",
                    "Adobe",
                    None,
                    None,
                    None,
                    "2021-01-01T00:00:00",
                    None,
                    False,
                    25,
                    10000,
                ),
            )
            conn.commit()

        r = client.get("/api/pdf-metadata/stats")
        data = r.json()
        assert data["total_pdfs"] == 1
        assert data["top_authors"][0]["author"] == "Analyst"

    def test_pdf_metadata_search_with_data(self, client):
        from tests.conftest import upload_sample

        resp = upload_sample(client)
        doc_id = resp.json()["document_id"]

        from dossier.db.database import get_db
        from dossier.forensics.provenance import _ensure_pdf_metadata_table, store_pdf_metadata

        with get_db() as conn:
            _ensure_pdf_metadata_table(conn)
            store_pdf_metadata(
                conn,
                PDFMetadata(
                    doc_id,
                    "Jane Forensic",
                    "Acrobat",
                    "Adobe",
                    None,
                    None,
                    None,
                    None,
                    None,
                    False,
                    5,
                    2000,
                ),
            )
            conn.commit()

        r = client.get("/api/pdf-metadata/search", params={"author": "Jane"})
        data = r.json()
        assert data["count"] == 1
        assert data["results"][0]["author"] == "Jane Forensic"

    def test_extract_all_no_pdfs(self, client):
        """No PDFs in corpus — nothing to extract."""
        r = client.post("/api/pdf-metadata/extract-all")
        assert r.status_code == 200
        data = r.json()
        assert data["total_pdfs"] == 0
        assert data["extracted"] == 0

    def test_extract_all_skips_non_pdf(self, client):
        """Text files should not appear in extract-all results."""
        from tests.conftest import upload_sample

        upload_sample(client)
        r = client.post("/api/pdf-metadata/extract-all")
        assert r.status_code == 200
        assert r.json()["total_pdfs"] == 0

    def test_extract_all_with_pdf_doc(self, client):
        """PDF filepath in DB triggers extraction (even if file doesn't exist)."""
        from dossier.db.database import get_db

        # Insert a doc with a .pdf filepath (file won't exist, so extraction skips)
        with get_db() as conn:
            conn.execute(
                "INSERT INTO documents (filename, filepath, title, category, source, raw_text) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("test.pdf", "/nonexistent/test.pdf", "Test PDF", "report", "test", "content"),
            )
            conn.commit()

        r = client.post("/api/pdf-metadata/extract-all")
        assert r.status_code == 200
        data = r.json()
        assert data["total_pdfs"] == 1
        # File doesn't exist so extraction fails gracefully
        assert data["skipped"] == 1
        assert data["extracted"] == 0

    def test_extract_all_skips_existing(self, client):
        """Default mode skips docs that already have metadata."""
        from dossier.db.database import get_db
        from dossier.forensics.provenance import _ensure_pdf_metadata_table, store_pdf_metadata

        with get_db() as conn:
            conn.execute(
                "INSERT INTO documents (filename, filepath, title, category, source, raw_text) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("test.pdf", "/nonexistent/test.pdf", "Test PDF", "report", "test", "content"),
            )
            _ensure_pdf_metadata_table(conn)
            store_pdf_metadata(
                conn,
                PDFMetadata(1, "Author", None, None, None, None, None, None, None, False, 5, 1000),
            )
            conn.commit()

        r = client.post("/api/pdf-metadata/extract-all")
        assert r.status_code == 200
        # Already has metadata, so not included in extraction list
        assert r.json()["total_pdfs"] == 0

    def test_extract_all_force_reextracts(self, client):
        """Force mode includes docs that already have metadata."""
        from dossier.db.database import get_db
        from dossier.forensics.provenance import _ensure_pdf_metadata_table, store_pdf_metadata

        with get_db() as conn:
            conn.execute(
                "INSERT INTO documents (filename, filepath, title, category, source, raw_text) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("test.pdf", "/nonexistent/test.pdf", "Test PDF", "report", "test", "content"),
            )
            _ensure_pdf_metadata_table(conn)
            store_pdf_metadata(
                conn,
                PDFMetadata(1, "Author", None, None, None, None, None, None, None, False, 5, 1000),
            )
            conn.commit()

        r = client.post("/api/pdf-metadata/extract-all", params={"force": True})
        assert r.status_code == 200
        # Force includes existing — file doesn't exist so it skips
        assert r.json()["total_pdfs"] == 1


class TestAnomaliesIncludesProvenance:
    def test_anomalies_includes_provenance(self, client):
        r = client.get("/api/anomalies")
        assert r.status_code == 200
        data = r.json()
        assert "provenance_anomalies" in data
        assert isinstance(data["provenance_anomalies"], list)
