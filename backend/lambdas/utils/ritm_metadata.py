"""
Extrai requestBody.ritm dos metadados do processo (PEDIDO_COMPRA_METADATA ou METADATA.INPUT_JSON).
Se o campo não existir, retorna None — chamadores não devem enviar ritm às APIs externas nesse caso.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


def get_ritm_from_request_body(request_body: Any) -> Optional[Any]:
    if not isinstance(request_body, dict) or "ritm" not in request_body:
        return None
    return request_body["ritm"]


def _parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def ritm_from_pedido_metadata_item(pedido_item: Optional[Dict]) -> Optional[Any]:
    if not pedido_item:
        return None
    meta = _parse_json_dict(pedido_item.get("METADADOS"))
    if not meta:
        return None
    rb = meta.get("requestBody")
    return get_ritm_from_request_body(rb)


def ritm_from_metadata_item(metadata_item: Optional[Dict]) -> Optional[Any]:
    if not metadata_item:
        return None
    for key in ("INPUT_JSON",):
        raw = metadata_item.get(key)
        if not raw:
            continue
        data = _parse_json_dict(raw)
        if data:
            rb = data.get("requestBody")
            v = get_ritm_from_request_body(rb)
            if v is not None:
                return v
    return None


def ritm_from_items_by_sk(items_by_sk: Dict[str, Dict]) -> Optional[Any]:
    v = ritm_from_pedido_metadata_item(items_by_sk.get("PEDIDO_COMPRA_METADATA"))
    if v is not None:
        return v
    return ritm_from_metadata_item(items_by_sk.get("METADATA"))


def load_ritm_for_process(table, process_id: str) -> Optional[Any]:
    try:
        pk = f"PROCESS#{process_id}"
        response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk},
        )
        items = {item["SK"]: item for item in response.get("Items", [])}
        return ritm_from_items_by_sk(items)
    except Exception:
        return None
