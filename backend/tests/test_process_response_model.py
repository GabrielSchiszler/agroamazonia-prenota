"""ProcessResponse deve expor campos de falha (não descartar no Pydantic)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models.api import ProcessResponse  # noqa: E402


def test_process_response_keeps_failure_fields():
    raw = {
        "process_id": "abc",
        "status": "FAILED",
        "files": {"danfe": [], "additional": []},
        "created_at": "1",
        "error_info": {"message": "erro x", "type": "LAMBDA_ERROR"},
        "failure_summary": {
            "reason_code": "PROTHEUS_FAILED",
            "reason_label": "Falha Protheus",
            "protheus": {"response_message": "Pedido inválido"},
        },
        "metrics_failure_dedup_role": "primary",
    }
    resp = ProcessResponse(**raw)
    dumped = resp.model_dump()
    assert dumped["error_info"]["message"] == "erro x"
    assert dumped["failure_summary"]["reason_code"] == "PROTHEUS_FAILED"
    assert dumped["metrics_failure_dedup_role"] == "primary"
