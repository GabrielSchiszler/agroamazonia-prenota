"""
Tests for backend/lambdas/parse_xml/handler.py

Covers:
- _select_nfe_xml: priority logic among multiple XML files
- handler: graceful skip when no XML exists
- handler: full parse when NF-e XML exists
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import SAMPLE_NFE_XML, SAMPLE_GENERIC_XML


# We need to mock boto3 BEFORE importing the handler (module-level clients)
@pytest.fixture(autouse=True)
def _mock_boto(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")


def _make_file_item(name, key=None, doc_type="DANFE"):
    return {
        "PK": "PROCESS#test-123",
        "SK": f"FILE#{name}",
        "FILE_NAME": name,
        "FILE_KEY": key or f"processes/test-123/danfe/{name}",
        "DOC_TYPE": doc_type,
    }


class TestSelectNfeXml:
    """Test _select_nfe_xml logic in isolation (mock S3 only)."""

    @patch("parse_xml.handler.s3")
    def test_selects_nfe_xml_among_multiple(self, mock_s3):
        from parse_xml.handler import _select_nfe_xml

        mock_s3.get_object.side_effect = [
            {"Body": MagicMock(read=lambda: SAMPLE_GENERIC_XML)},
            {"Body": MagicMock(read=lambda: SAMPLE_NFE_XML)},
        ]

        items = [
            _make_file_item("generic.xml"),
            _make_file_item("nota_fiscal.xml"),
        ]

        item, raw = _select_nfe_xml(items, "test-bucket")
        assert item["FILE_NAME"] == "nota_fiscal.xml"
        assert raw == SAMPLE_NFE_XML

    @patch("parse_xml.handler.s3")
    def test_falls_back_to_first_xml_when_no_nfe(self, mock_s3):
        from parse_xml.handler import _select_nfe_xml

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: SAMPLE_GENERIC_XML)
        }

        items = [_make_file_item("config.xml")]
        item, raw = _select_nfe_xml(items, "test-bucket")
        assert item["FILE_NAME"] == "config.xml"
        assert raw == SAMPLE_GENERIC_XML

    @patch("parse_xml.handler.s3")
    def test_returns_none_when_no_xml_files(self, mock_s3):
        from parse_xml.handler import _select_nfe_xml

        items = [
            {"PK": "PROCESS#test-123", "SK": "FILE#doc.pdf", "FILE_NAME": "doc.pdf", "FILE_KEY": "k"},
        ]
        item, raw = _select_nfe_xml(items, "test-bucket")
        assert item is None
        assert raw is None

    @patch("parse_xml.handler.s3")
    def test_returns_none_when_no_files_at_all(self, mock_s3):
        from parse_xml.handler import _select_nfe_xml

        item, raw = _select_nfe_xml([], "test-bucket")
        assert item is None
        assert raw is None


class TestParseXmlHandler:
    """Test the handler itself with mocked DynamoDB + S3."""

    @patch("parse_xml.handler.table")
    @patch("parse_xml.handler.s3")
    def test_single_xml_attachment_map_mode(self, mock_s3, mock_table):
        """Modo Step Functions Map: um arquivo por invocação (sem batch)."""
        from parse_xml.handler import handler

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: SAMPLE_NFE_XML)
        }

        result = handler(
            {
                "process_id": "abc",
                "file_name": "nota.xml",
                "file_key": "processes/abc/danfe/nota.xml",
            },
            None,
        )
        assert result["status"] == "parsed_xml"
        mock_table.query.assert_not_called()
        mock_table.put_item.assert_called_once()
        saved = mock_table.put_item.call_args[1]["Item"]
        assert saved["SK"] == "PARSED_XML=nota.xml"
        assert "IS_PRIMARY" not in saved

    @patch("parse_xml.handler.table")
    @patch("parse_xml.handler.s3")
    def test_graceful_skip_when_no_xml(self, mock_s3, mock_table):
        """No XML files → handler returns process_id without raising."""
        from parse_xml.handler import handler

        mock_table.query.return_value = {
            "Items": [
                {"PK": "PROCESS#abc", "SK": "METADATA", "STATUS": "PROCESSING"},
                {"PK": "PROCESS#abc", "SK": "FILE#foto.pdf", "FILE_NAME": "foto.pdf", "FILE_KEY": "k"},
            ]
        }

        result = handler({"process_id": "abc"}, None)
        assert result["process_id"] == "abc"
        assert result["xml_files_parsed"] == 0
        mock_table.put_item.assert_not_called()

    @patch("parse_xml.handler.table")
    @patch("parse_xml.handler.s3")
    def test_parses_nfe_xml_successfully(self, mock_s3, mock_table):
        """Valid NF-e XML → parses and saves PARSED_XML to DynamoDB."""
        from parse_xml.handler import handler

        mock_table.query.return_value = {
            "Items": [
                {"PK": "PROCESS#abc", "SK": "METADATA", "STATUS": "PROCESSING"},
                {
                    "PK": "PROCESS#abc",
                    "SK": "FILE#nota.xml",
                    "FILE_NAME": "nota.xml",
                    "FILE_KEY": "processes/abc/danfe/nota.xml",
                    "DOC_TYPE": "DANFE",
                },
            ]
        }
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: SAMPLE_NFE_XML)
        }

        result = handler({"process_id": "abc"}, None)
        assert result["process_id"] == "abc"
        assert result["xml_files_parsed"] == 1

        mock_table.put_item.assert_called_once()
        saved = mock_table.put_item.call_args[1]["Item"]
        assert saved["SK"] == "PARSED_XML=nota.xml"
        assert saved["SOURCE"] == "XML"
        assert saved.get("IS_PRIMARY") is True

        parsed = json.loads(saved["PARSED_DATA"])
        assert parsed["numero_nota"] == "1"
        assert parsed["emitente"]["cnpj"] == "12345678000195"

    @patch("parse_xml.handler.table")
    @patch("parse_xml.handler.s3")
    def test_parses_secondary_xml_after_primary_nfe(self, mock_s3, mock_table):
        """Dois XMLs: NF-e + genérico → dois PARSED_XML (secundário com _kind generic_xml)."""
        from parse_xml.handler import handler

        mock_table.query.return_value = {
            "Items": [
                {"PK": "PROCESS#abc", "SK": "METADATA"},
                {
                    "PK": "PROCESS#abc",
                    "SK": "FILE#extra.xml",
                    "FILE_NAME": "extra.xml",
                    "FILE_KEY": "processes/abc/extra.xml",
                },
                {
                    "PK": "PROCESS#abc",
                    "SK": "FILE#nota.xml",
                    "FILE_NAME": "nota.xml",
                    "FILE_KEY": "processes/abc/nota.xml",
                },
            ]
        }
        mock_s3.get_object.side_effect = [
            {"Body": MagicMock(read=lambda: SAMPLE_GENERIC_XML)},
            {"Body": MagicMock(read=lambda: SAMPLE_NFE_XML)},
            {"Body": MagicMock(read=lambda: SAMPLE_GENERIC_XML)},
        ]

        result = handler({"process_id": "abc"}, None)
        assert result["xml_files_parsed"] == 2

        puts = mock_table.put_item.call_args_list
        assert len(puts) == 2
        sks = [p[1]["Item"]["SK"] for p in puts]
        assert "PARSED_XML=nota.xml" in sks
        assert "PARSED_XML=extra.xml" in sks
        extra_saved = next(p[1]["Item"] for p in puts if p[1]["Item"]["SK"] == "PARSED_XML=extra.xml")
        extra_data = json.loads(extra_saved["PARSED_DATA"])
        assert extra_data.get("_kind") == "generic_xml"

    @patch("parse_xml.handler.table")
    @patch("parse_xml.handler.s3")
    def test_metadata_only_file_items_ignored(self, mock_s3, mock_table):
        """FILE# sem FILE_KEY não entra no loop de XML secundário."""
        from parse_xml.handler import handler

        mock_table.query.return_value = {
            "Items": [
                {"PK": "PROCESS#abc", "SK": "METADATA"},
                {"PK": "PROCESS#abc", "SK": "FILE#nota.xml", "FILE_NAME": "nota.xml",
                 "FILE_KEY": "processes/abc/danfe/nota.xml"},
                {"PK": "PROCESS#abc", "SK": "FILE#pedido.json", "FILE_NAME": "pedido.json"},
            ]
        }
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: SAMPLE_NFE_XML)
        }

        result = handler({"process_id": "abc"}, None)
        assert result["xml_files_parsed"] == 1
