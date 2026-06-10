"""Testes para prioridade Bedrock → XML → OCR no payload Protheus."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas"))

from utils.document_field_resolver import (  # noqa: E402
    normalize_documento_numero,
    resolve_protheus_document_fields,
)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_normalize_documento_ten_digits_strips_leading_zero_to_nine_chars():
    assert normalize_documento_numero("0000001287") == "000001287"
    assert normalize_documento_numero("000001287") == "000001287"
    assert normalize_documento_numero("878991") == "878991"
    assert normalize_documento_numero("1234567890") == "1234567890"
    assert normalize_documento_numero("00000001287") == "000001287"


def test_resolver_normalizes_ten_digit_bedrock_documento():
    out = resolve_protheus_document_fields(
        bedrock_extraction={"documento": "0000001287", "cnpjEmitente": "30190475000159"},
        xml_data={},
        ocr_data={},
        request_body_data={},
        bedrock_first=True,
    )
    assert out.numero_documento == "000001287"


def test_bedrock_cnpj_wins_over_boleto_ocr_hints():
    """Processo 76646d76: Bedrock tem CNPJ correto; OCR do boleto tem lixo."""
    bedrock = {
        "cnpjEmitente": "30190475000159",
        "documento": "000001287",
        "serie": "NFS",
        "dataEmissao": "20260601",
        "chaveAcesso": "52188051230190475000159000000000128726068828307410",
    }
    ocr = {
        "per_document": [
            {
                "file_name": "BOLETO_1287_SIMPLE_AGRO.pdf",
                "protheus_hints": {
                    "cnpjEmitente": "62026010010620",
                    "chaveAcesso": "12606202601001062026260620262501413409721500",
                    "parsed_xml_style": {
                        "emitente": {"cnpj": "62026010010620"},
                        "chave_acesso": "12606202601001062026260620262501413409721500",
                    },
                },
            },
            {
                "file_name": "NF_-_1287_-_SIMPLE_AGRO.pdf",
                "protheus_hints": {
                    "numeroNota": "400",
                    "parsed_xml_style": {"numero_nota": "400", "serie": "NFS"},
                },
            },
        ]
    }
    out = resolve_protheus_document_fields(
        bedrock_extraction=bedrock,
        xml_data={},
        ocr_data=ocr,
        request_body_data={},
        bedrock_first=True,
    )
    assert out.cnpj_emitente == "30190475000159"
    assert out.sources["cnpjEmitente"] == "bedrock"
    assert out.numero_documento == "000001287"
    assert out.sources["documento"] == "bedrock"
    assert out.serie_raw == "NFS"


def test_legacy_xml_beats_bedrock_for_agroquimicos():
    """AGROQUÍMICOS/BARTER: XML estruturado continua prioritário sobre Bedrock."""
    bedrock = {"cnpjEmitente": "30190475000159", "documento": "999999"}
    xml = {
        "modelo": "55",
        "numero_nota": "12345",
        "chave_acesso": "35260115219378380000120000000000005326032643827496",
        "emitente": {"cnpj": "19378380000120"},
    }
    out = resolve_protheus_document_fields(
        bedrock_extraction=bedrock,
        xml_data=xml,
        ocr_data={},
        request_body_data={},
        bedrock_first=False,
    )
    assert out.cnpj_emitente == "19378380000120"
    assert out.sources["cnpjEmitente"] == "xml"
    assert out.numero_documento == "12345"
    assert out.sources["documento"] == "xml"


def test_xml_wins_when_no_bedrock():
    xml = {
        "modelo": "55",
        "numero_nota": "12345",
        "serie": "1",
        "data_emissao": "20260115",
        "chave_acesso": "35260115219378380000120000000000005326032643827496",
        "emitente": {"cnpj": "19378380000120"},
        "transporte": {"modalidade_frete": "9"},
    }
    out = resolve_protheus_document_fields(
        bedrock_extraction={},
        xml_data=xml,
        ocr_data={},
        request_body_data={},
    )
    assert out.cnpj_emitente == "19378380000120"
    assert out.sources["cnpjEmitente"] == "xml"
    assert out.numero_documento == "12345"


def test_uc_pedido_fallback_when_ocr_cnpj_invalid():
    ocr = {
        "per_document": [
            {
                "file_name": "NFSE.pdf",
                "protheus_hints": {
                    "cnpjEmitente": "00000000000000",
                    "parsed_xml_style": {"emitente": {"cnpj": "00000000000000"}},
                },
            }
        ]
    }
    out = resolve_protheus_document_fields(
        bedrock_extraction={},
        xml_data={},
        ocr_data=ocr,
        request_body_data={"cnpjEmitente": "30190475000159"},
        bedrock_first=True,
    )
    assert out.cnpj_emitente == "30190475000159"
    assert out.sources["cnpjEmitente"] == "pedido"


def test_legacy_ocr_in_xml_beats_pedido():
    """Legado: OCR já mergeado no xml_data tem prioridade sobre pedido."""
    out = resolve_protheus_document_fields(
        bedrock_extraction={},
        xml_data={"emitente": {"cnpj": "19378380000120"}},
        ocr_data={},
        request_body_data={"cnpjEmitente": "30190475000159"},
        bedrock_first=False,
    )
    assert out.cnpj_emitente == "19378380000120"
    assert out.sources["cnpjEmitente"] == "xml"


def test_bedrock_merge_prefers_nf_over_boleto():
    from bedrock_extract_fields.handler import _merge_bedrock_extractions  # noqa: E402

    uc_merged = _merge_bedrock_extractions(
        [
            (
                "BOLETO_1287.pdf",
                {
                    "cnpjEmitente": "62026010010620",
                    "documento": "000001287",
                    "chaveAcesso": "12606202601001062026260620262501413409721500",
                },
            ),
            (
                "NF_-_1287.pdf",
                {
                    "cnpjEmitente": "30190475000159",
                    "documento": "0000001287",
                    "serie": "NFS",
                    "chaveAcesso": "52188051230190475000159000000000128726068828307410",
                },
            ),
        ],
        prefer_fiscal_doc=True,
    )
    assert uc_merged["cnpjEmitente"] == "30190475000159"
    assert uc_merged["documento"] == "0000001287"
    assert uc_merged["serie"] == "NFS"

    legacy_merged = _merge_bedrock_extractions(
        [
            ("BOLETO_1287.pdf", {"cnpjEmitente": "62026010010620", "documento": "000001287"}),
            ("NF_-_1287.pdf", {"cnpjEmitente": "30190475000159", "documento": "0000001287"}),
        ],
        prefer_fiscal_doc=False,
    )
    assert legacy_merged["cnpjEmitente"] == "62026010010620"
    assert legacy_merged["documento"] == "000001287"
