"""
Tests for backend/lambdas/merge_extractions/handler.py

Covers:
- XML-only process (old flow): MERGED has nfe_xml, no textract_documents
- Textract-only process: MERGED has null nfe_xml, populated textract_documents
- Both XML + Textract: all fields populated
- Empty process: both null/empty
- PARSED_OCR backfill created only when textract results exist
"""

import json
import pytest
from unittest.mock import patch, MagicMock, call


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")


def _make_items(*items):
    """Helper: returns Items list for DynamoDB query mock."""
    return {"Items": list(items)}


def _parsed_xml_item(file_name="nota.xml"):
    return {
        "PK": "PROCESS#p1",
        "SK": f"PARSED_XML={file_name}",
        "FILE_NAME": file_name,
        "PARSED_DATA": json.dumps({"numero_nota": "123", "emitente": {"cnpj": "111"}}),
    }


def _textract_item(file_name="fatura.pdf", raw_text="Fatura #12345 R$1000", hints=None):
    row = {
        "PK": "PROCESS#p1",
        "SK": f"TEXTRACT#{file_name}",
        "FILE_NAME": file_name,
        "RAW_TEXT": raw_text,
        "TABLES_DATA": json.dumps([{"rows": [["A", "B"]]}]),
        "JOB_ID": "sync",
    }
    if hints is not None:
        row["PROTHEUS_HINTS"] = json.dumps(hints)
    return row


class TestMergeExtractions:

    @patch("merge_extractions.handler.table")
    def test_xml_only_flow(self, mock_table):
        """Old flow: only PARSED_XML, no TEXTRACT → merged has nfe_xml, empty textract."""
        from merge_extractions.handler import handler

        mock_table.query.return_value = _make_items(_parsed_xml_item())

        result = handler({"process_id": "p1"}, None)
        assert result == {"process_id": "p1"}

        calls = mock_table.put_item.call_args_list
        assert len(calls) == 1

        merged_saved = calls[0][1]["Item"]
        assert merged_saved["SK"] == "MERGED_EXTRACTION"
        merged = json.loads(merged_saved["MERGED_DATA"])
        assert merged["schema_version"] == 2
        assert merged["nfe_xml"]["numero_nota"] == "123"
        assert merged["nfe_file"] == "nota.xml"
        assert len(merged["xml_documents"]) == 1
        assert merged["xml_documents"][0]["file_name"] == "nota.xml"
        assert merged["textract_documents"] == []

    @patch("merge_extractions.handler.table")
    def test_textract_only_flow(self, mock_table):
        """Multi-anexo without XML: only TEXTRACT → merged has null nfe_xml."""
        from merge_extractions.handler import handler

        mock_table.query.return_value = _make_items(
            _textract_item("fatura.pdf"),
            _textract_item("boleto.pdf", "Boleto 999"),
        )

        result = handler({"process_id": "p1"}, None)
        assert result == {"process_id": "p1"}

        calls = mock_table.put_item.call_args_list
        assert len(calls) == 2

        merged = json.loads(calls[0][1]["Item"]["MERGED_DATA"])
        assert merged["schema_version"] == 2
        assert merged["nfe_xml"] is None
        assert merged["nfe_file"] is None
        assert merged["xml_documents"] == []
        assert len(merged["textract_documents"]) == 2

        ocr_backfill = calls[1][1]["Item"]
        assert ocr_backfill["SK"] == "PARSED_OCR=textract_merged"
        assert ocr_backfill["SOURCE"] == "TEXTRACT"
        ocr_pd = json.loads(ocr_backfill["PARSED_DATA"])
        assert "per_document" in ocr_pd
        assert len(ocr_pd["per_document"]) == 2
        assert {d["file_name"] for d in ocr_pd["per_document"]} == {"fatura.pdf", "boleto.pdf"}

    @patch("merge_extractions.handler.table")
    def test_xml_plus_textract_flow(self, mock_table):
        """Full multi-anexo: XML + Textract → both present in merged."""
        from merge_extractions.handler import handler

        mock_table.query.return_value = _make_items(
            _parsed_xml_item(),
            _textract_item("comprovante.pdf"),
        )

        handler({"process_id": "p1"}, None)

        calls = mock_table.put_item.call_args_list
        assert len(calls) == 2

        merged = json.loads(calls[0][1]["Item"]["MERGED_DATA"])
        assert merged["schema_version"] == 2
        assert merged["nfe_xml"] is not None
        assert len(merged["xml_documents"]) == 1
        assert len(merged["textract_documents"]) == 1
        assert merged["textract_documents"][0]["file_name"] == "comprovante.pdf"

    @patch("merge_extractions.handler.table")
    def test_multiple_xml_sources_listed_in_xml_documents(self, mock_table):
        second = {
            "PK": "PROCESS#p1",
            "SK": "PARSED_XML=extra.xml",
            "FILE_NAME": "extra.xml",
            "PARSED_DATA": json.dumps({"_kind": "generic_xml", "root_tag": "root"}),
        }
        mock_table.query.return_value = _make_items(_parsed_xml_item(), second)

        from merge_extractions.handler import handler

        handler({"process_id": "p1"}, None)
        merged = json.loads(mock_table.put_item.call_args_list[0][1]["Item"]["MERGED_DATA"])
        assert len(merged["xml_documents"]) == 2
        assert merged["nfe_xml"]["numero_nota"] == "123"

    @patch("merge_extractions.handler.table")
    def test_textract_protheus_hints_in_merged_doc(self, mock_table):
        hints = {"chaveAcesso": "3" * 44, "cnpjEmitente": "12345678000195"}
        mock_table.query.return_value = _make_items(
            _textract_item("doc.pdf", "texto", hints=hints),
        )
        from merge_extractions.handler import handler

        handler({"process_id": "p1"}, None)
        merged = json.loads(mock_table.put_item.call_args_list[0][1]["Item"]["MERGED_DATA"])
        assert merged["textract_documents"][0]["protheus_hints"]["chaveAcesso"] == "3" * 44

    @patch("merge_extractions.handler.table")
    def test_empty_process(self, mock_table):
        """No PARSED_XML and no TEXTRACT → merged with nulls, no OCR backfill."""
        from merge_extractions.handler import handler

        mock_table.query.return_value = _make_items(
            {"PK": "PROCESS#p1", "SK": "METADATA", "STATUS": "PROCESSING"},
        )

        handler({"process_id": "p1"}, None)

        calls = mock_table.put_item.call_args_list
        assert len(calls) == 1

        merged = json.loads(calls[0][1]["Item"]["MERGED_DATA"])
        assert merged["schema_version"] == 2
        assert merged["nfe_xml"] is None
        assert merged["xml_documents"] == []
        assert merged["textract_documents"] == []

    @patch("merge_extractions.handler.table")
    def test_ocr_backfill_not_created_without_textract(self, mock_table):
        """OCR compat record should only be created when Textract results exist."""
        from merge_extractions.handler import handler

        mock_table.query.return_value = _make_items(_parsed_xml_item())
        handler({"process_id": "p1"}, None)

        saved_sks = [c[1]["Item"]["SK"] for c in mock_table.put_item.call_args_list]
        assert "PARSED_OCR=textract_merged" not in saved_sks
