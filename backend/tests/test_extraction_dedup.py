"""Testes: limpeza de extrações, dedup de itens/Textract e hash de upload."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.extraction_dedup import (  # noqa: E402
    clear_process_extractions,
    dedupe_extraction_itens,
    dedupe_file_items_by_content_hash,
    dedupe_textract_documents,
    is_ephemeral_extraction_sk,
    normalize_content_sha256,
    sum_itens_total,
)

_SAMPLE_HASH = "a" * 64


def test_normalize_content_sha256_accepts_lowercase_hex():
    assert normalize_content_sha256(_SAMPLE_HASH) == _SAMPLE_HASH
    assert normalize_content_sha256(_SAMPLE_HASH.upper()) == _SAMPLE_HASH


def test_normalize_content_sha256_rejects_invalid():
    assert normalize_content_sha256("") == ""
    assert normalize_content_sha256("abc") == ""
    assert normalize_content_sha256("g" * 64) == ""


def test_is_ephemeral_extraction_sk():
    assert is_ephemeral_extraction_sk("TEXTRACT#abc")
    assert is_ephemeral_extraction_sk("BEDROCK_EXTRACTION#1")
    assert is_ephemeral_extraction_sk("MERGED_EXTRACTION")
    assert is_ephemeral_extraction_sk("VALIDATION#x")
    assert not is_ephemeral_extraction_sk("FILE#upload-1")
    assert not is_ephemeral_extraction_sk("METADATA")


def test_dedupe_extraction_itens_collapses_retries():
    item = {
        "codigoProduto": "X1",
        "produto": "PRODUTO TESTE",
        "quantidade": 1,
        "valorUnitario": 5746.2,
    }
    merged = dedupe_extraction_itens([item, item, item])
    assert len(merged) == 1
    assert merged[0]["valorUnitario"] == 5746.2


def test_sum_itens_total_does_not_triple_on_duplicate_lines():
    item = {"quantidade": 1, "valorUnitario": 5746.2}
    assert sum_itens_total([item, item, item]) == pytest.approx(5746.2)


def test_dedupe_textract_documents_keeps_latest_by_file_name():
    docs = [
        {
            "file_name": "nota.pdf",
            "file_upload_id": "u1",
            "timestamp": 100,
            "raw_text": "old",
        },
        {
            "file_name": "nota.pdf",
            "file_upload_id": "u2",
            "timestamp": 300,
            "raw_text": "new",
        },
        {
            "file_name": "nota.pdf",
            "file_upload_id": "u3",
            "timestamp": 200,
            "raw_text": "mid",
        },
        {"file_name": "boleto.pdf", "file_upload_id": "u4", "timestamp": 50},
    ]
    out = dedupe_textract_documents(docs)
    assert len(out) == 2
    by_name = {d["file_name"]: d for d in out}
    assert by_name["nota.pdf"]["raw_text"] == "new"
    assert by_name["boleto.pdf"]["file_upload_id"] == "u4"


def test_clear_process_extractions_deletes_ephemeral_only():
    table = MagicMock()
    table.query.return_value = {
        "Items": [
            {"PK": "PROCESS#p1", "SK": "FILE#x"},
            {"PK": "PROCESS#p1", "SK": "TEXTRACT#1"},
            {"PK": "PROCESS#p1", "SK": "BEDROCK_EXTRACTION#1"},
            {"PK": "PROCESS#p1", "SK": "METADATA"},
        ]
    }
    n = clear_process_extractions(table, "PROCESS#p1")
    assert n == 2
    assert table.delete_item.call_count == 2
    deleted_sks = {c.kwargs["Key"]["SK"] for c in table.delete_item.call_args_list}
    assert deleted_sks == {"TEXTRACT#1", "BEDROCK_EXTRACTION#1"}


def test_dedupe_file_items_by_content_hash_keeps_latest():
    h = _SAMPLE_HASH
    items = [
        {"SK": "FILE#a", "FILE_NAME": "a.pdf", "CONTENT_SHA256": h, "TIMESTAMP": 100},
        {"SK": "FILE#b", "FILE_NAME": "b.pdf", "CONTENT_SHA256": h, "TIMESTAMP": 300},
        {"SK": "FILE#c", "FILE_NAME": "c.pdf", "TIMESTAMP": 50},
    ]
    out = dedupe_file_items_by_content_hash(items)
    assert len(out) == 2
    by_sk = {i["SK"]: i for i in out}
    assert by_sk["FILE#b"]["FILE_NAME"] == "b.pdf"
    assert "FILE#c" in by_sk


@patch("list_attachments.handler.table")
def test_list_attachments_dedupes_same_hash(mock_table):
    import list_attachments.handler as la  # noqa: E402

    h = _SAMPLE_HASH
    mock_table.query.return_value = {
        "Items": [
            {
                "PK": "PROCESS#p1",
                "SK": "FILE#u1",
                "FILE_NAME": "nota.pdf",
                "FILE_KEY": "k/u1_nota.pdf",
                "CONTENT_SHA256": h,
                "TIMESTAMP": 100,
            },
            {
                "PK": "PROCESS#p1",
                "SK": "FILE#u2",
                "FILE_NAME": "nota.pdf",
                "FILE_KEY": "k/u2_nota.pdf",
                "CONTENT_SHA256": h,
                "TIMESTAMP": 200,
            },
        ]
    }
    out = la.handler({"process_id": "p1"}, None)
    assert out["attachment_count"] == 1
    assert out["attachments"][0]["file_sk"] == "FILE#u2"
