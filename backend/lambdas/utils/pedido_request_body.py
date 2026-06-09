"""Leitura consistente de flags no JSON do pedido (requestBody / header / raiz)."""

from __future__ import annotations

from typing import Any


def truthy_pedido_flag(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str) and value.strip().lower() == "true":
        return True
    return False


def get_pedido_field(pedido_compra: dict, request_body: dict, field: str) -> Any:
    header = pedido_compra.get("header")
    if not isinstance(header, dict):
        header = {}
    if field in request_body:
        return request_body[field]
    if field in header:
        return header[field]
    return pedido_compra.get(field)


def uso_e_consumo_active(pedido_compra: dict | None) -> bool:
    if not isinstance(pedido_compra, dict):
        return False
    rb = pedido_compra.get("requestBody") or {}
    if not isinstance(rb, dict):
        rb = {}
    v = get_pedido_field(pedido_compra, rb, "usoEConsumo")
    return truthy_pedido_flag(v)


def any_pedido_de_compra_in_itens(pedido_compra: dict | None) -> bool:
    rb = (pedido_compra or {}).get("requestBody") or {}
    if not isinstance(rb, dict):
        return False
    for it in rb.get("itens") or []:
        p = it.get("pedidoDeCompra")
        if isinstance(p, dict) and str(p.get("pedidoErp") or "").strip():
            return True
    return False


def natureza_from_pedido(pedido_compra: dict | None) -> str:
    rb = (pedido_compra or {}).get("requestBody") or {}
    if isinstance(rb, dict):
        v = rb.get("natureza")
        if v is None:
            return ""
        return str(v).strip()
    return ""


def centro_custo_from_item_rb(item_rb: dict | None) -> str | None:
    """centroCusto do item do pedido (requestBody.itens[]) para repasse ao Protheus."""
    if not isinstance(item_rb, dict):
        return None
    for key in ("centroCusto", "centroDeCusto", "centro_custo"):
        v = item_rb.get(key)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def _percentual_rateio_value(pct: Any) -> int | float | None:
    """Converte percentual do rateio para número (Protheus espera decimal, não string)."""
    if pct is None or isinstance(pct, bool):
        return None
    if isinstance(pct, int):
        return pct if pct > 0 else None
    if isinstance(pct, float):
        return pct if pct > 0 else None
    s = str(pct).strip().replace(",", ".")
    if not s:
        return None
    try:
        val = float(s)
    except (TypeError, ValueError):
        return None
    if val <= 0:
        return None
    return int(val) if val.is_integer() else val


def rateio_centro_custo_from_request_body(request_body: dict | None) -> list[dict] | None:
    """
    Extrai rateioCentroCusto do requestBody do pedido para repasse ao Protheus.
    Retorna None se ausente, vazio ou sem itens válidos (centroDeCusto preenchido).
    """
    if not isinstance(request_body, dict):
        return None
    raw = request_body.get("rateioCentroCusto")
    if not isinstance(raw, list) or not raw:
        return None
    out: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        cc = entry.get("centroDeCusto")
        if cc is None or str(cc).strip() == "":
            continue
        item: dict[str, Any] = {"centroDeCusto": str(cc).strip()}
        pct = _percentual_rateio_value(entry.get("percentual"))
        if pct is not None:
            item["percentual"] = pct
        out.append(item)
    return out or None
