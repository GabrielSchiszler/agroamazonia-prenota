"""Extrai vencimentos/valores de qualquer anexo OCR/Bedrock para duplicatas Protheus (USO E CONSUMO)."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterator

_MONEY_TOL = 0.02

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


def document_source_kind(file_name: str | None) -> str:
    """Classifica anexo: boleto | nf_nfs | unknown (para prioridade de vencimento)."""
    fn = (file_name or "").lower()
    if "boleto" in fn:
        return "boleto"
    if any(tok in fn for tok in ("nfse", "nfs-e", "nfs_e", "danfe", "nota", "nf_")):
        return "nf_nfs"
    if fn.startswith("nf") or "_nf" in fn or " nf" in fn:
        return "nf_nfs"
    return "unknown"


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


def _dup_valor(dup: dict[str, Any]) -> float | None:
    for key in ("valorVencimento", "valor", "valor_vencimento"):
        v = _parse_br_money(dup.get(key))
        if v is not None:
            return v
    return None


def _money_close(a: float, b: float, tol: float = _MONEY_TOL) -> bool:
    return abs(float(a) - float(b)) <= tol


def _parse_iso_date(value: str) -> datetime | None:
    s = str(value or "").strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None


def _dates_within_days(d1: str, d2: str, days: int = 1) -> bool:
    a, b = _parse_iso_date(d1), _parse_iso_date(d2)
    if not a or not b:
        return False
    return abs((a.date() - b.date()).days) <= days


def _public_dup(dup: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in dup.items() if k != "source"}


def resolve_duplicatas_uc(
    duplicatas: list[dict[str, Any]] | None,
    *,
    valor_total_doc: float = 0.0,
) -> list[dict[str, Any]]:
    """
    USO E CONSUMO: mesma parcela em NF (ex. domingo) e boleto (dia útil) → só boleto.
    Parcelas reais (valores distintos que somam o total) ou split sem valor → mantém todas.
    """
    if not duplicatas:
        return []

    entries = [dict(d) for d in duplicatas if isinstance(d, dict) and d.get("vencimento")]
    if len(entries) <= 1:
        return [_public_dup(entries[0])] if entries else []

    total_doc = float(valor_total_doc or 0)
    vals = [_dup_valor(e) for e in entries]
    n = len(entries)

    def _pick_boleto_or_latest(cands: list[dict[str, Any]]) -> dict[str, Any]:
        boletos = [e for e in cands if e.get("source") == "boleto"]
        pool = boletos if boletos else cands
        best = max(pool, key=lambda e: str(e.get("vencimento") or ""))
        return _public_dup(best)

    # Todas com valor explícito ≈ total do documento → mesma parcela em fontes diferentes
    if total_doc > 0 and all(v is not None for v in vals):
        if all(_money_close(v, total_doc) for v in vals):
            return [_pick_boleto_or_latest(entries)]

        # Valores iguais entre si (mas não necessariamente = total): NF + boleto mesma parcela
        if n == 2 and vals[0] is not None and vals[1] is not None and _money_close(vals[0], vals[1]):
            sources = {e.get("source") for e in entries}
            if "boleto" in sources and ("nf_nfs" in sources or "unknown" in sources):
                return [_pick_boleto_or_latest(entries)]
            v0, v1 = entries[0], entries[1]
            if _dates_within_days(
                str(v0.get("vencimento")),
                str(v1.get("vencimento")),
                1,
            ):
                return [_pick_boleto_or_latest(entries)]

    # Sem valor explícito: deixa build_duplicatas_protheus_payload fazer o split
    return [_public_dup(e) for e in entries]


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
    src = dup.get("source")
    if src == "boleto":
        bucket["source"] = "boleto"
    elif src and not bucket.get("source"):
        bucket["source"] = src


def extract_duplicatas_from_ocr(ocr_data: dict | None) -> list[dict[str, Any]]:
    """Varre todos os anexos OCR; extrai duplicatas onde houver vencimento no texto."""
    merged: dict[str, dict[str, Any]] = {}
    for file_name, text, pd in _iter_per_document_texts(ocr_data) or []:
        if not text.strip():
            continue
        hints = pd.get("protheus_hints") if isinstance(pd.get("protheus_hints"), dict) else {}
        valor_hint = hints.get("valorDocumento")
        source = document_source_kind(str(file_name) if file_name else None)
        for dup in extract_duplicatas_from_document_text(text, valor_hint=valor_hint):
            dup["source"] = source
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
        if item.get("source"):
            dup["source"] = item["source"]
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
