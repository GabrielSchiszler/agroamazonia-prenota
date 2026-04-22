"""
Tests for backend/lambdas/extract_documents/handler.py

Covers:
- Filtering: skips .xml files, skips metadata-only items (no FILE_KEY)
- Extension gating: rejects unsupported formats (e.g. .docx)
- Happy path: calls Textract and persists results
- Empty process: 0 non-XML files → extracted_count = 0
"""

import io
import json
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")


def _file_item(name, key=None, doc_type="ADDITIONAL"):
    return {
        "PK": "PROCESS#p1",
        "SK": f"FILE#{name}",
        "FILE_NAME": name,
        "FILE_KEY": key or f"processes/p1/docs/{name}",
        "DOC_TYPE": doc_type,
    }


def _pdf_body(size: int = 500_000) -> io.BytesIO:
    return io.BytesIO(b"%PDF-1.4\n" + b"x" * max(1, size - 10))


def _metadata_only_item(name):
    """FILE# item without FILE_KEY (e.g. metadata-only virtual entry)."""
    return {
        "PK": "PROCESS#p1",
        "SK": f"FILE#{name}",
        "FILE_NAME": name,
        "DOC_TYPE": "ADDITIONAL",
        "METADATA_ONLY": True,
    }


class TestExtractDocumentsFiltering:
    """Verify which files get selected for Textract processing."""

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_skips_xml_files(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {
            "Items": [
                _file_item("nota.xml", doc_type="DANFE"),
                _file_item("config.XML"),
            ]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 0
        assert result["rejected"] == []
        mock_textract.analyze_document.assert_not_called()

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_skips_metadata_only_items(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {
            "Items": [_metadata_only_item("pedido_metadata")]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 0
        mock_textract.analyze_document.assert_not_called()

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_rejects_docx(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {
            "Items": [_file_item("relatorio.docx")]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 0
        assert "relatorio.docx" in result["rejected"]
        mock_textract.analyze_document.assert_not_called()

        mock_table.update_item.assert_called_once()
        call_kwargs = mock_table.update_item.call_args[1]
        assert call_kwargs["ExpressionAttributeValues"][":st"] == "REJECTED"

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_empty_process_no_files(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {"Items": []}

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 0
        assert result["rejected"] == []


class TestExtractDocumentsHappyPath:
    """Verify Textract is called for supported files and results are persisted."""

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_processes_pdf_sync(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {
            "Items": [_file_item("fatura.pdf")]
        }
        mock_s3.head_object.return_value = {"ContentLength": 500_000}
        mock_s3.get_object.return_value = {"Body": _pdf_body(500_000)}
        mock_textract.analyze_document.return_value = {
            "Blocks": [
                {"BlockType": "LINE", "Id": "l1", "Text": "Fatura #12345"},
                {"BlockType": "LINE", "Id": "l2", "Text": "Total: R$ 1.000,00"},
            ]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 1
        assert result["rejected"] == []

        mock_textract.analyze_document.assert_called_once()
        ad_kw = mock_textract.analyze_document.call_args[1]
        assert "Bytes" in ad_kw["Document"]
        assert mock_table.put_item.call_count == 1
        saved = mock_table.put_item.call_args[1]["Item"]
        assert saved["SK"] == "TEXTRACT#fatura.pdf"
        assert "Fatura #12345" in saved["RAW_TEXT"]

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_processes_multiple_pdfs_and_images(self, mock_table, mock_s3, mock_textract):
        from extract_documents.handler import handler

        mock_table.query.return_value = {
            "Items": [
                _file_item("fatura.pdf"),
                _file_item("foto.jpg"),
                _file_item("nota.xml"),
            ]
        }
        mock_s3.head_object.return_value = {"ContentLength": 100_000}
        mock_s3.get_object.return_value = {"Body": _pdf_body(100_000)}
        mock_textract.analyze_document.return_value = {
            "Blocks": [{"BlockType": "LINE", "Id": "x", "Text": "text"}]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["extracted_count"] == 2
        assert mock_textract.analyze_document.call_count == 2

    @patch("extract_documents.handler.textract")
    @patch("extract_documents.handler.s3")
    @patch("extract_documents.handler.table")
    def test_single_file_map_mode(self, mock_table, mock_s3, mock_textract):
        """Modo Map: um PDF por invocação (não consulta query batch)."""
        from extract_documents.handler import handler

        mock_s3.head_object.return_value = {"ContentLength": 100_000}
        mock_s3.get_object.return_value = {"Body": _pdf_body(100_000)}
        mock_textract.analyze_document.return_value = {
            "Blocks": [{"BlockType": "LINE", "Id": "x", "Text": "Linha"}]
        }

        result = handler(
            {
                "process_id": "p1",
                "file_name": "doc.pdf",
                "file_key": "processes/p1/docs/doc.pdf",
                "file_sk": "FILE#doc.pdf",
            },
            None,
        )
        assert result["extracted_count"] == 1
        mock_table.query.assert_not_called()
        mock_textract.analyze_document.assert_called_once()
        assert mock_table.put_item.call_args[1]["Item"]["SK"] == "TEXTRACT#doc.pdf"
