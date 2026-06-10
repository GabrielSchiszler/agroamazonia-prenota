"""Limpeza de extrações entre execuções e deduplicação de itens OCR/Bedrock."""

from __future__ import annotations

from typing import Any


def is_ephemeral_extraction_sk(sk: str) -> bool:
    """SKs derivados de OCR/Bedrock/validação — recriados a cada execução do SFN."""
    if not sk:
        return False
    if sk.startswith(("TEXTRACT#", "TEXTRACT=", "PARSED_OCR", "PARSED_XML=")):
        return True
    if sk.startswith("BEDROCK_EXTRACTION"):
        return True
    if sk == "MERGED_EXTRACTION" or sk.startswith("MERGED_EXTRACTION#"):
        return True
    if sk.startswith("VALIDATION#"):
        return True
    return False


def clear_process_extractions(table: Any, pk: str) -> int:
    """Remove extrações anteriores do processo (mantém FILE#, METADATA, pedido)."""
    resp = table.query(KeyConditionExpression="PK = :pk", ExpressionAttributeValues={":pk": pk})
    deleted = 0
    for item in resp.get("Items", []):
        sk = item.get("SK", "")
        if is_ephemeral_extraction_sk(sk):
            table.delete_item(Key={"PK": pk, "SK": sk})
            deleted += 1
    return deleted


def normalize_content_sha256(value: Any) -> str:
    h = str(value or "").strip().lower()
    if len(h) == 64 and all(c in "0123456789abcdef" for c in h):
        return h
    return ""


def _file_item_sort_key(item: dict) -> tuple:
    ts = int(item.get("TIMESTAMP") or item.get("timestamp") or 0)
    sk = str(item.get("SK") or "")
    return (ts, sk)


def dedupe_file_items_by_content_hash(items: list[Any]) -> list[dict]:
    """
    Mantém um FILE# por CONTENT_SHA256 (o mais recente). Sem hash, mantém todos.
    Upload duplicado continua no Dynamo; listagem e pipeline usam só um.
    """
    no_hash: list[dict] = []
    best_by_hash: dict[str, dict] = {}
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        h = normalize_content_sha256(raw.get("CONTENT_SHA256"))
        if not h:
            no_hash.append(raw)
            continue
        prev = best_by_hash.get(h)
        if not prev or _file_item_sort_key(raw) >= _file_item_sort_key(prev):
            best_by_hash[h] = raw
    return list(best_by_hash.values()) + no_hash


def _item_float(item: dict, *keys: str, default: float = 0.0) -> float:
    for k in keys:
        v = item.get(k)
        if v is None or v == "":
            continue
        try:
            return float(str(v).replace(",", "."))
        except (TypeError, ValueError):
            continue
    return default


def dedupe_extraction_itens(itens: list[Any]) -> list[dict]:
    """
    Remove linhas repetidas (ex.: mesmo item extraído 3× após retries).
    Chave: código + descrição resumida + quantidade + valor unitário.
    """
    seen: set[tuple] = set()
    out: list[dict] = []
    for raw in itens or []:
        if not isinstance(raw, dict):
            continue
        codigo = str(raw.get("codigoProduto") or raw.get("codigo") or "").strip()
        desc = str(raw.get("produto") or raw.get("descricao") or "").strip()[:120].upper()
        q = round(_item_float(raw, "quantidade", "qCom"), 4)
        vu = round(
            _item_float(raw, "valorUnitario", "valor_unitario", "vUnCom"),
            2,
        )
        key = (codigo, desc, q, vu)
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def dedupe_textract_documents(docs: list[dict]) -> list[dict]:
    """
    Um Textract por nome de arquivo, mantendo o mais recente (retries / re-upload).
    """
    best: dict[str, dict] = {}
    for doc in docs or []:
        if not isinstance(doc, dict):
            continue
        name = str(doc.get("file_name") or "").strip().lower()
        if not name:
            continue
        ts = int(doc.get("timestamp") or doc.get("TIMESTAMP") or 0)
        prev = best.get(name)
        if not prev or ts >= int(prev.get("_ts") or 0):
            copy = dict(doc)
            copy["_ts"] = ts
            best[name] = copy
    out = []
    for doc in best.values():
        doc.pop("_ts", None)
        out.append(doc)
    return out


def sum_itens_total(itens: list[Any]) -> float:
    """Soma q×vu com dedup prévia (uso e consumo / fallback sem XML)."""
    total = 0.0
    for it in dedupe_extraction_itens(itens if isinstance(itens, list) else []):
        q = _item_float(it, "quantidade", "qCom", default=0.0)
        vu = _item_float(it, "valorUnitario", "valor_unitario", "vUnCom", default=0.0)
        if q > 0 and vu > 0:
            total += q * vu
        elif vu > 0:
            total += vu
    return total
