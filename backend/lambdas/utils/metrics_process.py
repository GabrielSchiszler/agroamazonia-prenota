"""
Classificação de processo para métricas (mesma regra do update_metrics).
"""

from __future__ import annotations

from typing import Any

from utils.metrics_rates import metrics_outcome_for_status
from utils.protheus_regras import (
    build_failed_rules_for_metrics,
    load_api_regras_catalog,
    load_regras_catalog,
)

_STATUS_TO_METRICS = {
    "COMPLETED": "SUCCESS",
    "SUCCESS": "SUCCESS",
    "VALIDATED": "SUCCESS",
    "FAILED": "FAILED",
    "VALIDATION_FAILURE": "FAILED",
}


def effective_metrics_status_from_metadata(metadata: dict) -> str | None:
    """
    Status efetivo para métricas: usa o STATUS atual do processo (última execução SFN).
    METRICS_STATUS só entra se o processo ainda está CREATED/PROCESSING (sem resultado final).
    """
    raw = str(metadata.get("STATUS") or "")
    if raw in _STATUS_TO_METRICS:
        return _STATUS_TO_METRICS[raw]
    if raw in ("PROCESSING", "CREATED", ""):
        ms = metadata.get("METRICS_STATUS")
        return str(ms) if ms else None
    ms = metadata.get("METRICS_STATUS")
    return str(ms) if ms else None


def _is_prenota(metadata: dict) -> bool:
    from update_metrics.handler import protheus_response_indicates_prenota

    return protheus_response_indicates_prenota({}, metadata)


def resolve_process_metrics(
    metadata: dict,
    *,
    validation_failed_rules: list[str] | None = None,
    table: Any = None,
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> dict:
    """
    Define o que update_metrics faria para um METADATA.

    Retorno:
      outcome: success | failed | skipped | ignored
      failed_rules: regras para failed_rules no METRICS#
      is_prenota: só em success com mensagem pré-nota
      skip_reason: motivo se skipped
    """
    catalog = catalog or load_regras_catalog()
    api_catalog = api_catalog if api_catalog is not None else load_api_regras_catalog()
    metric_status = effective_metrics_status_from_metadata(metadata)

    if not metric_status:
        return {
            "outcome": "ignored",
            "failed_rules": [],
            "is_prenota": False,
            "skip_reason": "",
        }

    if metric_status == "SUCCESS":
        is_prenota = _is_prenota(metadata)
        return {
            "outcome": "success",
            "failed_rules": [],
            "is_prenota": is_prenota,
            "skip_reason": "",
        }

    ocr_rules, op_rules, skip, skip_reason = build_failed_rules_for_metrics(
        table,
        metadata,
        validation_failed_rules=validation_failed_rules or [],
        catalog=catalog,
        api_catalog=api_catalog,
    )
    if skip:
        return {
            "outcome": "skipped",
            "failed_rules": [],
            "operacional_rules": op_rules,
            "is_prenota": False,
            "skip_reason": skip_reason,
        }
    return {
        "outcome": "failed",
        "failed_rules": ocr_rules,
        "operacional_rules": op_rules,
        "is_prenota": False,
        "skip_reason": "",
    }


def outcome_matches_rate_counts(
    daily_success: int,
    daily_failed: int,
    *,
    new_outcome: str,
) -> tuple[int, int]:
    """Simula incremento de um processo novo nos contadores do dia."""
    success = int(daily_success or 0)
    failed = int(daily_failed or 0)
    if new_outcome == "success":
        success += 1
    elif new_outcome == "failed":
        failed += 1
    return success, failed
