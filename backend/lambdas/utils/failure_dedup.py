"""Chaves de deduplicação de falhas no dashboard (NF + CNPJ + regra + pedido)."""

from __future__ import annotations

NAO_IDENTIFICADO_NF = "NAO_IDENTIFICADO_NF"
NAO_IDENTIFICADO_CNPJ = "NAO_IDENTIFICADO_CNPJ"
NAO_IDENTIFICADO_PEDIDO = "NAO_IDENTIFICADO_PEDIDO"


def failure_identity_fallback(field: str) -> str:
    return {
        "nf": NAO_IDENTIFICADO_NF,
        "cnpj": NAO_IDENTIFICADO_CNPJ,
        "pedido": NAO_IDENTIFICADO_PEDIDO,
    }[field]


def format_failure_key_display(key: str) -> str:
    parts = (key or "").split("|")
    if len(parts) >= 4:
        nf, cnpj, regra, pedido = parts[0], parts[1], parts[2], parts[3]
        return f"NF {nf} · CNPJ {cnpj} · {regra} · Pedido {pedido}"
    if len(parts) == 3:
        return f"NF {parts[0]} · CNPJ {parts[1]} · {parts[2]}"
    return key or ""


def dedup_badge_label(role: str | None, primary_process_id: str | None = None) -> str | None:
    if role == "duplicate":
        if primary_process_id:
            short = primary_process_id[:8] + "…" if len(primary_process_id) > 8 else primary_process_id
            return f"Duplicado (contado em {short})"
        return "Duplicado no card de falhas"
    if role == "primary":
        return "Contabilizado no card de falhas"
    return None
