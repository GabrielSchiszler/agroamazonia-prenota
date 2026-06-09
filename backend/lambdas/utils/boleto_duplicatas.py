"""Extrai vencimentos/valores de qualquer anexo OCR/Bedrock para duplicatas Protheus (USO E CONSUMO)."""

from __future__ import annotations

import re
from typing import Any, Iterator

_VEN = re.compile(r"(?is)vencimento[^\d]{0,60}(\d{2}/\d{2}/\d{4})")
_VALOR_DOC = re.compile(
    r"(?is)valor\s+(?:do\s+)?documento[^\d]{0,50}([\d]{1,3}(?:\.\d{3})*,\d{2})\b",
)
_VALOR_COBRADO = re.compile(
    r"(?is)(?:=\s*\)\s*valor\s+cobrado|valor\s+cobrado)[^\d]{0,50}([\d]{1,3}(?:\.\d{3})*,\d{2})\b",
)
_VALOR_NFSE = re.compile(
    r"(?is)valor\s+l[ií]quido\s+da\s+nfs-?e[^\d]{0,60}R\$\s*([\d\.\,]+)",
)


def _parse_br_money(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("R$", "").strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _parse_br_date(value: str) -> str | None:
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", (value or "").strip())
    if not m:
        return None
    d, mo, y = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"


def _vencimentos_from_text(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in _VEN.findall(text or ""):
        iso = _parse_br_date(raw)
        if iso and iso not in seen:
            seen.add(iso)
            out.append(iso)
    return out


def _valor_parcela_from_text(text: str) -> float | None:
    for rx in (_VALOR_DOC, _VALOR_COBRADO, _VALOR_NFSE):
        m = rx.search(text or "")
        if m:
            v = _parse_br_money(m.group(1))
            if v is not None:
                return v
    return None


def extract_duplicatas_from_document_text(
    text: str,
    *,
    valor_hint: Any = None,
) -> list[dict[str, Any]]:
    """
    Procura vencimento/valor no texto — qualquer anexo (NF, boleto, PDF, etc.).
    Retorno bruto para build_duplicatas_protheus_payload.
    """
    dates = _vencimentos_from_text(text)
    if not dates:
        return []

    valor = _parse_br_money(valor_hint)
    if valor is None:
        valor = _valor_parcela_from_text(text)

    out: list[dict[str, Any]] = []
    for ven in dates:
        item: dict[str, Any] = {"vencimento": ven}
        if valor is not None and len(dates) == 1:
            item["valorVencimento"] = valor
        out.append(item)
    return out


# Alias legado (testes / imports antigos)
extract_duplicatas_from_boleto_text = extract_duplicatas_from_document_text


def _iter_per_document_texts(ocr_data: dict | None) -> Iterator[tuple[str | None, str, dict]]:
    if not isinstance(ocr_data, dict):
        return
    per = ocr_data.get("per_document") or []
    combined = str(ocr_data.get("raw_text") or "")
    parts = combined.split("\n---\n") if combined else []
    source_files = ocr_data.get("source_files") or []
    by_name: dict[str, str] = {}
    for i, fn in enumerate(source_files):
        if i < len(parts) and fn:
            by_name[str(fn)] = parts[i]

    if isinstance(per, list) and per:
        for pd in per:
            if not isinstance(pd, dict):
                continue
            fn = pd.get("file_name")
            text = str(pd.get("raw_text") or by_name.get(str(fn or ""), "") or "")
            yield fn, text, pd
        return

    for fn, text in by_name.items():
        yield fn, text, {}


def _merge_dup_entry(
    merged: dict[str, dict[str, Any]],
    dup: dict[str, Any],
) -> None:
    ven = dup.get("vencimento")
    if not ven:
        return
    bucket = merged.setdefault(str(ven), {"vencimento": ven})
    if dup.get("valorVencimento") is not None:
        bucket["valorVencimento"] = dup["valorVencimento"]
    if dup.get("numero") is not None and bucket.get("numero") is None:
        bucket["numero"] = dup["numero"]


def extract_duplicatas_from_ocr(ocr_data: dict | None) -> list[dict[str, Any]]:
    """Varre todos os anexos OCR; extrai duplicatas onde houver vencimento no texto."""
    merged: dict[str, dict[str, Any]] = {}
    for _file_name, text, pd in _iter_per_document_texts(ocr_data) or []:
        if not text.strip():
            continue
        hints = pd.get("protheus_hints") if isinstance(pd.get("protheus_hints"), dict) else {}
        valor_hint = hints.get("valorDocumento")
        for dup in extract_duplicatas_from_document_text(text, valor_hint=valor_hint):
            _merge_dup_entry(merged, dup)
    return list(merged.values())


def extract_duplicatas_from_bedrock(bedrock_extraction: dict | None) -> list[dict[str, Any]]:
    if not isinstance(bedrock_extraction, dict):
        return []
    raw = bedrock_extraction.get("duplicatas")
    if not isinstance(raw, list):
        return []
    merged: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        ven = item.get("vencimento")
        if not ven:
            continue
        iso = ven if re.match(r"^\d{4}-\d{2}-\d{2}", str(ven)) else _parse_br_date(str(ven))
        if not iso:
            continue
        dup: dict[str, Any] = {"vencimento": iso}
        for key in ("valorVencimento", "valor"):
            if item.get(key) is not None:
                dup["valorVencimento"] = item.get(key)
                break
        if item.get("numero") is not None:
            dup["numero"] = item.get("numero")
        _merge_dup_entry(merged, dup)
    return list(merged.values())


def extract_duplicatas_from_sources(
    ocr_data: dict | None,
    bedrock_extraction: dict | None = None,
    *,
    bedrock_first: bool = True,
) -> list[dict[str, Any]]:
    """
    USO E CONSUMO: monta duplicatas a partir dos anexos quando pedido/XML não trazem.
    Prioridade global: Bedrock agregado → OCR (todos os documentos, sem filtro por nome).
    """
    ocr_dups = extract_duplicatas_from_ocr(ocr_data)
    ai_dups = extract_duplicatas_from_bedrock(bedrock_extraction)
    if bedrock_first:
        return ai_dups if ai_dups else ocr_dups
    return ocr_dups if ocr_dups else ai_dups
