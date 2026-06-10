"""
Regressão: processo db93f2b — 3 execuções SFN acumularam TEXTRACT/Bedrock itens
e multiplicaram 5.746,20 → 17.238,60. Com dedup, deve permanecer 5.746,20.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas"))

from bedrock_extract_fields.handler import _merge_bedrock_extractions  # noqa: E402
from send_to_protheus.handler import (  # noqa: E402
    _valor_total_documento_nota_ou_boleto,
    _valor_unitario_payload,
)
from utils.extraction_dedup import (  # noqa: E402
    dedupe_extraction_itens,
    dedupe_textract_documents,
    sum_itens_total,
)

_ITEM = {
    "codigoProduto": "1287",
    "produto": "PRODUTO USO CONSUMO SIMPLE AGRO",
    "quantidade": 1,
    "valorUnitario": 5746.2,
}
_CORRECT = 5746.2
_WRONG = 17238.60


def test_triple_identical_itens_sum_is_not_multiplied():
    triple = [_ITEM, _ITEM, _ITEM]
    assert sum_itens_total(triple) == _CORRECT
    assert dedupe_extraction_itens(triple) == [_ITEM]


def test_bedrock_merge_dedupes_triple_per_file_extractions():
    """3 extrações idênticas (3 retries) → merge com 1 item."""
    per_file = [("NF.pdf", {"itens": [_ITEM]})] * 3
    merged = _merge_bedrock_extractions(per_file, prefer_fiscal_doc=True)
    assert len(merged.get("itens") or []) == 1
    assert merged["itens"][0]["valorUnitario"] == _CORRECT


def test_textract_dedup_three_same_filename():
    docs = [
        {"file_name": "NF_-_1287_-_SIMPLE_AGRO.pdf", "timestamp": 100, "file_upload_id": "u1"},
        {"file_name": "NF_-_1287_-_SIMPLE_AGRO.pdf", "timestamp": 200, "file_upload_id": "u2"},
        {"file_name": "NF_-_1287_-_SIMPLE_AGRO.pdf", "timestamp": 300, "file_upload_id": "u3"},
        {"file_name": "BOLETO_1287_SIMPLE_AGRO.pdf", "timestamp": 150, "file_upload_id": "b1"},
        {"file_name": "BOLETO_1287_SIMPLE_AGRO.pdf", "timestamp": 250, "file_upload_id": "b2"},
        {"file_name": "BOLETO_1287_SIMPLE_AGRO.pdf", "timestamp": 350, "file_upload_id": "b3"},
    ]
    out = dedupe_textract_documents(docs)
    assert len(out) == 2
    by_name = {d["file_name"]: d for d in out}
    assert by_name["NF_-_1287_-_SIMPLE_AGRO.pdf"]["file_upload_id"] == "u3"
    assert by_name["BOLETO_1287_SIMPLE_AGRO.pdf"]["file_upload_id"] == "b3"


def test_uso_consumo_valor_unitario_not_tripled():
    bedrock = {"itens": [_ITEM, _ITEM, _ITEM]}
    total = _valor_total_documento_nota_ou_boleto(
        xml_data={},
        ocr_data={},
        bedrock_extraction=bedrock,
    )
    assert total == _CORRECT
    vu = _valor_unitario_payload({}, 1, total, 1, uso_consumo=True)
    assert vu == _CORRECT
    assert vu != _WRONG
