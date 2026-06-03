"""
Taxa de acerto e classificação de contadores para métricas (lambdas).
API FastAPI usa src.utils.metrics_rates (mesma fórmula).
"""


def success_rate_pct(success_count: int, failed_count: int) -> float:
    """Taxa de acerto = sucessos / (sucessos + falhas) × 100."""
    success_count = int(success_count or 0)
    failed_count = int(failed_count or 0)
    denom = success_count + failed_count
    if denom <= 0:
        return 0.0
    return round(success_count / denom * 100, 2)


def metrics_outcome_for_status(
    status: str,
    *,
    skip_metrics: bool = False,
) -> str | None:
    """
    Como o update_metrics contabiliza o processo:
    - success → incrementa success_count
    - failed  → incrementa failed_count (quando não skip)
    - skipped → não altera métricas (operacional puro)
    - None    → não contabiliza (PROCESSING, etc.)
    """
    if skip_metrics:
        return "skipped"
    if status == "SUCCESS":
        return "success"
    if status == "FAILED":
        return "failed"
    return None
