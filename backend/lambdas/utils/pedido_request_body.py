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
