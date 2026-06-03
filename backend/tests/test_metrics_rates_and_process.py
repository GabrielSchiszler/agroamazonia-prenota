"""
Testes da taxa sucesso/(sucesso+falha) e do fluxo de métricas para processos novos.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.metrics_process import (  # noqa: E402
    outcome_matches_rate_counts,
    resolve_process_metrics,
)
from utils.metrics_rates import (  # noqa: E402
    metrics_outcome_for_status,
    success_rate_pct,
)
from utils.protheus_regras import build_failed_rules_for_metrics  # noqa: E402


class TestSuccessRatePct:
    def test_formula_sucesso_mais_falha(self):
        assert success_rate_pct(545, 212) == 71.99

    def test_zero_quando_sem_resultado(self):
        assert success_rate_pct(0, 0) == 0.0

    def test_só_sucesso(self):
        assert success_rate_pct(10, 0) == 100.0

    def test_só_falha(self):
        assert success_rate_pct(0, 5) == 0.0

    def test_nao_usa_total_maior(self):
        """Total 951 com 545+212: taxa não é 545/951."""
        rate = success_rate_pct(545, 212)
        assert rate != round(545 / 951 * 100, 2)
        assert rate == 71.99


class TestMetricsOutcome:
    def test_success_outcome(self):
        assert metrics_outcome_for_status("SUCCESS") == "success"

    def test_failed_outcome(self):
        assert metrics_outcome_for_status("FAILED") == "failed"

    def test_skipped_operacional(self):
        assert metrics_outcome_for_status("FAILED", skip_metrics=True) == "skipped"

    def test_processing_ignored(self):
        assert metrics_outcome_for_status("PROCESSING") is None


class TestNewProcessResolve:
    def test_sucesso_com_prenota(self):
        meta = {
            "STATUS": "SUCCESS",
            "protheus_response": json_dumps_prenota(),
        }
        r = resolve_process_metrics(meta)
        assert r["outcome"] == "success"
        assert r["is_prenota"] is True
        assert r["failed_rules"] == []

    def test_falha_validacao_conta(self):
        meta = {"STATUS": "FAILED", "PK": "PROCESS#1"}
        r = resolve_process_metrics(
            meta, validation_failed_rules=["validar_produtos"]
        )
        assert r["outcome"] == "failed"
        assert "validar_produtos" in r["failed_rules"]

    def test_falha_pipeline_ocr(self):
        meta = {
            "STATUS": "FAILED",
            "error_info": {
                "lambda": "parse_xml",
                "message": "erro",
                "type": "LAMBDA_ERROR",
            },
        }
        r = resolve_process_metrics(meta)
        assert r["outcome"] == "failed"
        assert "OCR_LAMBDA_parse_xml" in r["failed_rules"]

    def test_operacional_puro_nao_conta(self):
        meta = {
            "STATUS": "FAILED",
            "protheus_request_info": json_dumps_exec_auto_operacional(),
        }
        r = resolve_process_metrics(meta)
        assert r["outcome"] == "skipped"
        assert r["failed_rules"] == []

    def test_schema_item_011_falha_de_processo(self):
        meta = {
            "STATUS": "FAILED",
            "protheus_request_info": {
                "response_status_code": 400,
                "response_body": {
                    "errorCode": "SCHEMA_ITEM_011",
                    "message": "campo obrigatorio",
                },
            },
        }
        api = {"SCHEMA_ITEM_011": {"tipo": "OCR"}}
        r = resolve_process_metrics(meta, api_catalog=api)
        assert r["outcome"] == "skipped"
        assert r["failed_rules"] == []
        assert "SCHEMA_ITEM_011" in r.get("operacional_rules", [])

    def test_api_operacional_skip(self):
        meta = {
            "STATUS": "FAILED",
            "protheus_request_info": {
                "response_status_code": 400,
                "response_body": {
                    "errorCode": "CALC_QTDVAL_002",
                    "message": "unidade",
                },
            },
        }
        api = {"CALC_QTDVAL_002": {"tipo": "Operacional"}}
        r = resolve_process_metrics(meta, api_catalog=api)
        assert r["outcome"] == "skipped"


class TestSimulateDayCounters:
    def test_novo_sucesso_aumenta_taxa(self):
        s, f = outcome_matches_rate_counts(10, 10, new_outcome="success")
        assert success_rate_pct(s, f) == 52.38

    def test_skip_nao_altera_contadores(self):
        s, f = outcome_matches_rate_counts(10, 10, new_outcome="skipped")
        assert s == 10 and f == 10

    def test_build_failed_rules_alinhado_resolve(self):
        meta = {
            "STATUS": "FAILED",
            "error_info": {"lambda": "x", "message": "m", "type": "LAMBDA_ERROR"},
        }
        ocr_rules, op_rules, skip, _ = build_failed_rules_for_metrics(
            None, meta, validation_failed_rules=[]
        )
        resolved = resolve_process_metrics(meta)
        assert skip is False
        assert resolved["outcome"] == "failed"
        assert ocr_rules == resolved["failed_rules"]


def json_dumps_prenota() -> str:
    import json

    return json.dumps(
        {"message": "Documento de entrada criado como pré-nota no sistema"}
    )


def json_dumps_exec_auto_operacional() -> str:
    import json

    return json.dumps(
        {
            "response_status_code": 400,
            "response_body": {
                "errorCode": "EXEC_AUTO_002",
                "message": "Erro pré-nota",
                "cause": {
                    "documentoEntrada": [
                        "AJUDA:QTD MAIOR PEDIDO\r\nQuantidade maior que pedido.\r\n"
                    ]
                },
            },
        }
    )
