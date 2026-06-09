"""Montagem de duplicatas no payload Protheus (uso e consumo: valorVencimento + split)."""

from __future__ import annotations

from typing import Any


def _parse_valor_dup(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    s = str(value).strip().replace(",", ".")
    if not s:
        return None
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _normalize_vencimento(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    return s[:10]


def _valor_from_dup_entry(dup: dict) -> float | None:
    for key in ("valorVencimento", "valor", "valor_vencimento"):
        v = _parse_valor_dup(dup.get(key))
        if v is not None:
            return v
    return None


def build_duplicatas_protheus_payload(
    duplicatas_raw: list | None,
    *,
    uso_consumo: bool,
    valor_total_doc: float = 0.0,
) -> list[dict]:
    """
    Uso e consumo:
    - Prioriza valorVencimento (depois valor).
    - Sem valor na parcela: usa valor total da nota.
    - Vários vencimentos distintos sem valores: divide o total da nota igualmente.
    """
    if not duplicatas_raw or not isinstance(duplicatas_raw, list):
        return []

    if not uso_consumo:
        out: list[dict] = []
        for dup in duplicatas_raw:
            if not isinstance(dup, dict):
                continue
            ven = _normalize_vencimento(dup.get("vencimento"))
            val = _valor_from_dup_entry(dup)
            if not ven or val is None:
                continue
            item: dict[str, Any] = {"vencimento": ven, "valor": float(val)}
            if dup.get("numero") is not None:
                item["numero"] = str(dup.get("numero"))
            out.append(item)
        return out

    buckets: dict[str, dict] = {}
    for dup in duplicatas_raw:
        if not isinstance(dup, dict):
            continue
        ven = _normalize_vencimento(dup.get("vencimento"))
        if not ven:
            continue
        val = _valor_from_dup_entry(dup)
        bucket = buckets.setdefault(
            ven,
            {"valor": 0.0, "has_explicit": False, "numero": None},
        )
        if val is not None:
            bucket["valor"] += val
            bucket["has_explicit"] = True
        if bucket["numero"] is None and dup.get("numero") is not None:
            bucket["numero"] = dup.get("numero")

    if not buckets:
        return []

    dates = list(buckets.keys())
    n_dates = len(dates)
    any_explicit = any(b["has_explicit"] for b in buckets.values())
    total_explicit = sum(b["valor"] for b in buckets.values())
    total_doc = float(valor_total_doc or 0)

    if n_dates > 1 and (not any_explicit or total_explicit <= 0) and total_doc > 0:
        per = round(total_doc / n_dates, 2)
        allocated = 0.0
        for i, ven in enumerate(dates):
            if i == n_dates - 1:
                buckets[ven]["valor"] = round(total_doc - allocated, 2)
            else:
                buckets[ven]["valor"] = per
                allocated += per
    elif n_dates == 1:
        ven = dates[0]
        if (not any_explicit or buckets[ven]["valor"] <= 0) and total_doc > 0:
            buckets[ven]["valor"] = total_doc

    out: list[dict] = []
    for ven, bucket in buckets.items():
        if bucket["valor"] <= 0:
            continue
        item: dict[str, Any] = {
            "vencimento": ven,
            "valor": float(bucket["valor"]),
        }
        if bucket.get("numero") is not None:
            item["numero"] = str(bucket["numero"])
        out.append(item)
    return out
