"""Escolhe o item PARSED_XML principal (NF-e) entre múltiplos anexos XML."""

from __future__ import annotations

import json
from typing import Any


def score_nfe_payload(data: dict[str, Any]) -> int:
    if data.get("_kind") == "generic_xml":
        return 0
    prods = data.get("produtos")
    emit = data.get("emitente")
    if prods and isinstance(prods, list) and len(prods) > 0 and emit:
        return 100
    if emit:
        return 50
    if prods and isinstance(prods, list) and len(prods) > 0:
        return 30
    return 10


def iter_parsed_xml_items(items: list[dict]) -> list[tuple[str, str, dict[str, Any]]]:
    out: list[tuple[str, str, dict[str, Any]]] = []
    for it in items:
        sk = it.get("SK", "")
        if not sk.startswith("PARSED_XML="):
            continue
        try:
            data = json.loads(it.get("PARSED_DATA") or "{}")
        except Exception:
            continue
        out.append((sk, it.get("FILE_NAME", ""), data))
    return out


def pick_best_parsed_xml_item(items: list[dict]) -> dict | None:
    """Prioriza IS_PRIMARY (gravado pelo parse_xml); senão, heurística NF-e vs XML genérico."""
    candidates = [it for it in items if str(it.get("SK", "")).startswith("PARSED_XML=")]
    if not candidates:
        return None
    primary_marked = [it for it in candidates if it.get("IS_PRIMARY")]
    if primary_marked:
        return primary_marked[0]

    entries = iter_parsed_xml_items(items)
    if not entries:
        return None
    best = max(entries, key=lambda e: score_nfe_payload(e[2]))
    if score_nfe_payload(best[2]) == 0:
        best = entries[0]
    sk_best = best[0]
    for it in candidates:
        if it.get("SK") == sk_best:
            return it
    return None
