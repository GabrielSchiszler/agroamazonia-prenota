"""Resumo de falha exposto em get_process."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from services.process_service import (  # noqa: E402
    _failure_summary_from_metadata,
    _protheus_failure_from_metadata,
)


def test_failure_summary_validation_and_protheus():
    meta = {
        "METRICS_FAILURE_ERROR_TYPE": "PROTHEUS_FAILED",
        "METRICS_FAILED_RULES": '["validar_produtos"]',
        "protheus_request_info": '{"response_status_code": 400, "response_body": {"message": "Pedido inválido"}, "request_url": "https://api/protheus/x"}',
    }
    summary = _failure_summary_from_metadata(meta)
    assert summary["reason_code"] == "PROTHEUS_FAILED"
    assert summary["failed_rules"] == ["validar_produtos"]
    assert summary["protheus"]["response_message"] == "Pedido inválido"
    assert summary["protheus"]["response_status_code"] == 400


def test_protheus_response_fallback():
    meta = {"protheus_response": '{"message": "Pré-nota criada"}'}
    p = _protheus_failure_from_metadata(meta)
    assert p["response_message"] == "Pré-nota criada"
