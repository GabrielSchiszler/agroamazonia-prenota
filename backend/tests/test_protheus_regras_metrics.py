import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.protheus_regras import (
    augment_failed_rules_from_metadata,
    build_failed_rules_for_metrics,
    extract_api_error_from_body,
    extract_ocr_pipeline_rule_ids,
    extract_regras_from_cause,
    extract_regras_from_protheus_body,
    merge_failed_rules_for_metrics,
    normalize_cause,
    ocr_pipeline_rule_id,
    should_skip_metrics_update,
    split_rules_for_metrics,
)


def test_nf_10199_extracts_two_divergencias():
    cause = {
        "documentoEntrada": [
            "AJUDA:VALIDAÇÃO DE NOTA FISCAL VS XML\r\n"
            "Divergência de valor total da nota fiscal digitada com valor total no XML:\r\n"
            "Valor Total NFe Digitada:     22.830,72\r\n"
            "Valor Total NFe XML:     16.544,00\r\n"
            "--------------------------------------------------\r\n"
            "Divergência de valor ICMS da nota fiscal digitada com valor ICMS no XML:\r\n"
            "Valor ICMS NFe Digitada:     913,18\r\n"
            "Valor ICMS NFe XML:     661,76\r\n",
            "Tabela SX3 02/06/2026 13:42:08",
            "Inconsistencia nos Itens",
        ],
        "preNota": ["Erro -->  Inconsistencia na Linha de Itens"],
    }
    blocks = normalize_cause(cause)
    regras = extract_regras_from_cause(blocks)
    ids = [r["regra_id"] for r in regras]
    assert "DIVERGENCIA_DE_VALOR_TOTAL_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_TOTAL_NO_XML" in ids
    assert "DIVERGENCIA_DE_VALOR_ICMS_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_ICMS_NO_XML" in ids
    assert "INCONSISTENCIA_NOS_ITENS" in ids


def test_skip_metrics_somente_operacional():
    regras = [{"regra_id": "QTD_MAIOR_PEDIDO", "mensagem": "qtd maior"}]
    skip, reason = should_skip_metrics_update("FAILED", [], regras)
    assert skip is True
    assert "operacional" in reason


def test_nao_skip_com_ocr_protheus():
    regras = [
        {
            "regra_id": "DIVERGENCIA_DE_VALOR_TOTAL_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_TOTAL_NO_XML",
            "mensagem": "divergencia total",
        }
    ]
    skip, _ = should_skip_metrics_update("FAILED", [], regras)
    assert skip is False


def test_api_error_uses_error_code_not_cause():
    body = {
        "errorCode": "CALC_QTDVAL_002",
        "message": "Unidade de medida do item 1 não é igual à unidade padrão...",
        "cause": "Produto: 31400001FR100G0, Unidade do Item: UN",
    }
    regras = extract_regras_from_protheus_body(body)
    assert len(regras) == 1
    assert regras[0]["regra_id"] == "CALC_QTDVAL_002"
    assert "Unidade de medida" in regras[0]["mensagem"]
    assert regras[0]["regra_id"] != "PRODUTO"

    only_api = extract_api_error_from_body(body)
    assert only_api[0]["regra_id"] == "CALC_QTDVAL_002"


def test_skip_metrics_api_operacional():
    api_catalog = {
        "CALC_QTDVAL_002": {"tipo": "Operacional", "mensagem_resumo": "unidade"},
    }
    regras = [
        {
            "regra_id": "CALC_QTDVAL_002",
            "mensagem": "Unidade de medida do item 1...",
        }
    ]
    skip, reason = should_skip_metrics_update(
        "FAILED", [], regras, api_catalog=api_catalog
    )
    assert skip is True
    assert "operacional" in reason


def test_ocr_pipeline_timeout_rule():
    err = {
        "lambda": "UpdateStatusBeforeError",
        "message": "Sandbox.Timedout",
        "type": "LAMBDA_ERROR",
    }
    assert ocr_pipeline_rule_id(err) == "OCR_LAMBDA_TIMEOUT"
    meta = {"STATUS": "FAILED", "error_info": err}
    assert extract_ocr_pipeline_rule_ids(meta) == ["OCR_LAMBDA_TIMEOUT"]


def test_augment_failed_rules_pipeline():
    meta = {
        "STATUS": "FAILED",
        "PK": "PROCESS#x",
        "error_info": {
            "lambda": "parse_xml",
            "message": "Erro ao parsear",
            "type": "LAMBDA_ERROR",
        },
    }
    out = augment_failed_rules_from_metadata([], meta)
    assert out == ["OCR_LAMBDA_parse_xml"]


def test_skip_metrics_not_when_pipeline_ocr():
    meta = {
        "STATUS": "FAILED",
        "error_info": {"lambda": "parse_xml", "message": "x", "type": "LAMBDA_ERROR"},
    }
    rules = augment_failed_rules_from_metadata([], meta)
    skip, _ = should_skip_metrics_update("FAILED", rules, [])
    assert skip is False


def test_validar_cnpj_somente_operacional_skip():
    api_catalog = {"SCHEMA_ITEM_011": {"tipo": "Operacional", "mensagem_resumo": "lote"}}
    ocr, op, skip, reason = build_failed_rules_for_metrics(
        None,
        {"STATUS": "FAILED", "PK": "PROCESS#x"},
        validation_failed_rules=["validar_cnpj_fornecedor"],
        api_catalog=api_catalog,
    )
    assert skip is True
    assert ocr == []
    assert "validar_cnpj_fornecedor" in op


def test_schema_item_011_operacional_nao_entra_ocr():
    api_catalog = {"SCHEMA_ITEM_011": {"tipo": "Operacional", "mensagem_resumo": "lote"}}
    regras = [{"regra_id": "SCHEMA_ITEM_011", "mensagem": "lote"}]
    ocr, op = split_rules_for_metrics([], regras, None, api_catalog=api_catalog)
    assert "SCHEMA_ITEM_011" in op
    assert "SCHEMA_ITEM_011" not in ocr


def test_misto_produtos_ocr_cnpj_processo():
    ocr, op, skip, _ = build_failed_rules_for_metrics(
        None,
        {"STATUS": "FAILED", "PK": "PROCESS#x"},
        validation_failed_rules=["validar_produtos", "validar_cnpj_fornecedor"],
    )
    assert skip is False
    assert "validar_produtos" in ocr
    assert "validar_cnpj_fornecedor" in op


def test_merge_failed_rules_adds_ocr_protheus():
    regras = [
        {
            "regra_id": "DIVERGENCIA_DE_VALOR_ICMS_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_ICMS_NO_XML",
            "mensagem": "icms",
        },
        {"regra_id": "EXISTNF", "mensagem": "dup"},
    ]
    merged = merge_failed_rules_for_metrics(["validar_serie"], regras)
    assert "validar_serie" in merged
    assert "DIVERGENCIA_DE_VALOR_ICMS_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_ICMS_NO_XML" in merged
    assert "EXISTNF" not in merged
