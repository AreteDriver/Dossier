"""Tests for dossier.ingestion.extractor — text extraction and file hashing."""

from unittest.mock import patch, MagicMock

from dossier.ingestion.extractor import (
    file_hash,
    extract_text,
    _extract_text,
    _extract_html,
)


class TestFileHash:
    def test_deterministic(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        assert file_hash(str(f)) == file_hash(str(f))

    def test_different_files_different_hash(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("content A")
        b.write_text("content B")
        assert file_hash(str(a)) != file_hash(str(b))

    def test_returns_hex_string(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("test")
        h = file_hash(str(f))
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest


class TestExtractTextPlaintext:
    def test_txt_file(self, tmp_path):
        f = tmp_path / "doc.txt"
        f.write_text("This is plain text content.")
        result = extract_text(str(f))
        assert result["text"] == "This is plain text content."
        assert result["method"] == "plaintext"
        assert result["pages"] >= 1

    def test_markdown_file(self, tmp_path):
        f = tmp_path / "doc.md"
        f.write_text("# Heading\n\nSome markdown content.")
        result = extract_text(str(f))
        assert "Heading" in result["text"]
        assert result["method"] == "plaintext"

    def test_page_estimation(self, tmp_path):
        f = tmp_path / "long.txt"
        f.write_text("x" * 9000)
        result = _extract_text(str(f))
        assert result["pages"] == 3  # 9000 / 3000

    def test_minimum_one_page(self, tmp_path):
        f = tmp_path / "short.txt"
        f.write_text("short")
        result = _extract_text(str(f))
        assert result["pages"] == 1


class TestExtractTextHtml:
    def test_strips_tags(self, tmp_path):
        f = tmp_path / "doc.html"
        f.write_text("<html><body><p>Hello <b>world</b></p></body></html>")
        result = extract_text(str(f))
        assert "Hello" in result["text"]
        assert "world" in result["text"]
        assert "<b>" not in result["text"]
        assert result["method"] == "html"

    def test_removes_script_blocks(self, tmp_path):
        f = tmp_path / "doc.html"
        f.write_text("<html><script>alert('xss')</script><body>Content</body></html>")
        result = _extract_html(str(f))
        assert "alert" not in result["text"]
        assert "Content" in result["text"]

    def test_removes_style_blocks(self, tmp_path):
        f = tmp_path / "doc.htm"
        f.write_text("<html><style>.foo{color:red}</style><body>Visible</body></html>")
        result = extract_text(str(f))
        assert "color" not in result["text"]
        assert "Visible" in result["text"]


class TestExtractTextUnsupported:
    def test_unsupported_extension(self, tmp_path):
        f = tmp_path / "doc.xyz"
        f.write_text("data")
        result = extract_text(str(f))
        assert result["text"] == ""
        assert result["method"] == "unsupported"
        assert result["pages"] == 0


class TestExtractPdf:
    def test_native_extraction(self, tmp_path):
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"fake pdf")

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Extracted PDF text content for testing purposes."
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        with patch("dossier.ingestion.extractor.pdfplumber", create=True) as mock_pdfplumber:
            # pdfplumber is imported inside the function
            import dossier.ingestion.extractor as ext_mod

            with patch.dict("sys.modules", {"pdfplumber": mock_pdfplumber}):
                mock_pdfplumber.open.return_value = mock_pdf
                result = ext_mod._extract_pdf(str(f))

        assert "Extracted PDF text" in result["text"]
        assert result["method"] == "pdf_native"
        assert result["pages"] == 1

    def test_ocr_fallback_on_low_text(self, tmp_path):
        f = tmp_path / "scanned.pdf"
        f.write_bytes(b"fake pdf")

        # pdfplumber returns very little text
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "ab"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        import dossier.ingestion.extractor as ext_mod

        with patch.dict("sys.modules", {"pdfplumber": MagicMock()}):
            import sys

            pdfplumber_mock = sys.modules["pdfplumber"]
            pdfplumber_mock.open.return_value = mock_pdf

            with patch.object(
                ext_mod, "_ocr_pdf", return_value="OCR extracted text is much longer than native"
            ):
                result = ext_mod._extract_pdf(str(f))

        assert result["method"] == "pdf_ocr"
        assert "OCR extracted" in result["text"]


class TestExtractPdfErrors:
    def test_pdfplumber_exception(self, tmp_path):
        """pdfplumber.open raises → returns empty text with pdf_native method."""
        f = tmp_path / "corrupt.pdf"
        f.write_bytes(b"not a real pdf")

        import dossier.ingestion.extractor as ext_mod

        mock_pdfplumber = MagicMock()
        mock_pdfplumber.open.side_effect = Exception("Corrupt PDF")
        with patch.dict("sys.modules", {"pdfplumber": mock_pdfplumber}):
            result = ext_mod._extract_pdf(str(f))

        assert result["text"] == ""
        assert result["method"] == "pdf_native"
        assert result["pages"] == 0


class TestOcrPdf:
    def test_ocr_pipeline(self, tmp_path):
        """Full OCR pipeline: pdftoppm creates PNGs, tesseract reads them."""
        f = tmp_path / "scanned.pdf"
        f.write_bytes(b"fake pdf")

        def mock_subprocess_run(cmd, **kwargs):
            result = MagicMock()
            if "pdftoppm" in cmd:
                # Create fake PNG files in the tmpdir
                import os

                tmpdir = (
                    cmd[-1].rsplit("/page", 1)[0]
                    if "/page" in cmd[-1]
                    else os.path.dirname(cmd[-1])
                )
                prefix = os.path.basename(cmd[-1])
                for i in range(2):
                    png = os.path.join(tmpdir, f"{prefix}-{i + 1:03d}.png")
                    with open(png, "wb") as pf:
                        pf.write(b"fake png")
                result.returncode = 0
            elif "tesseract" in cmd:
                result.stdout = "OCR text from page"
                result.returncode = 0
            return result

        from dossier.ingestion.extractor import _ocr_pdf

        with patch("subprocess.run", side_effect=mock_subprocess_run):
            result = _ocr_pdf(str(f))

        assert "OCR text from page" in result

    def test_ocr_exception(self, tmp_path):
        """OCR exception returns empty string."""
        f = tmp_path / "bad.pdf"
        f.write_bytes(b"fake pdf")

        from dossier.ingestion.extractor import _ocr_pdf

        with patch("subprocess.run", side_effect=Exception("pdftoppm not found")):
            result = _ocr_pdf(str(f))

        assert result == ""


class TestExtractTextErrors:
    def test_plaintext_read_error(self, tmp_path):
        """File read failure returns plaintext_error method."""
        f = tmp_path / "unreadable.txt"
        f.write_text("content")

        from dossier.ingestion.extractor import _extract_text

        with patch("builtins.open", side_effect=PermissionError("Access denied")):
            result = _extract_text(str(f))

        assert result["text"] == ""
        assert result["method"] == "plaintext_error"

    def test_html_parse_error(self, tmp_path):
        """HTML read failure returns html_error method."""
        f = tmp_path / "broken.html"
        f.write_text("<html>content</html>")

        from dossier.ingestion.extractor import _extract_html

        with patch("builtins.open", side_effect=PermissionError("Access denied")):
            result = _extract_html(str(f))

        assert result["text"] == ""
        assert result["method"] == "html_error"


class TestExtractImageOcr:
    def test_successful_ocr(self, tmp_path):
        f = tmp_path / "scan.png"
        f.write_bytes(b"fake image")

        mock_result = MagicMock()
        mock_result.stdout = "OCR text from image"

        with patch("subprocess.run", return_value=mock_result):
            from dossier.ingestion.extractor import _extract_image_ocr

            result = _extract_image_ocr(str(f))

        assert result["text"] == "OCR text from image"
        assert result["method"] == "image_ocr"
        assert result["pages"] == 1

    def test_ocr_failure(self, tmp_path):
        f = tmp_path / "bad.png"
        f.write_bytes(b"fake image")

        with patch("subprocess.run", side_effect=Exception("tesseract not found")):
            from dossier.ingestion.extractor import _extract_image_ocr

            result = _extract_image_ocr(str(f))

        assert result["text"] == ""
        assert result["method"] == "image_ocr_error"
