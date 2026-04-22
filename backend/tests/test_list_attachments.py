"""Tests for list_attachments Lambda."""

import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")


def _file(sk, name, key):
    return {
        "PK": "PROCESS#p1",
        "SK": sk,
        "FILE_NAME": name,
        "FILE_KEY": key or f"k/{name}",
    }


@patch("list_attachments.handler.table")
def test_handlers_xml_textract_skip(mock_table):
    from list_attachments.handler import handler

    mock_table.query.return_value = {
        "Items": [
            _file("FILE#a.xml", "nota.xml", "k/nota.xml"),
            _file("FILE#b.pdf", "boleto.pdf", "k/b.pdf"),
            _file("FILE#c.docx", "x.docx", "k/x.docx"),
            {"PK": "PROCESS#p1", "SK": "METADATA"},
        ]
    }
    out = handler({"process_id": "p1"}, None)
    assert out["attachment_count"] == 3
    kinds = {a["file_name"]: a["handler"] for a in out["attachments"]}
    assert kinds["nota.xml"] == "xml"
    assert kinds["boleto.pdf"] == "textract"
    assert kinds["x.docx"] == "skip"


@patch("list_attachments.handler.table")
def test_ignores_file_without_key(mock_table):
    from list_attachments.handler import handler

    mock_table.query.return_value = {
        "Items": [
            {"PK": "PROCESS#p1", "SK": "FILE#orphan.json", "FILE_NAME": "orphan.json"},
        ]
    }
    out = handler({"process_id": "p1"}, None)
    assert out["attachment_count"] == 0
