"""Taxa de acerto do dashboard: sucessos / (sucessos + falhas). Alinhado a lambdas/utils."""


def success_rate_pct(success_count: int, failed_count: int) -> float:
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
    if skip_metrics:
        return "skipped"
    if status == "SUCCESS":
        return "success"
    if status == "FAILED":
        return "failed"
    return None
