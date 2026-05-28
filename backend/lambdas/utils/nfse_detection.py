"""Detecção heurística de NFS-e (Nota Fiscal de Serviço Eletrônica) em texto OCR/PDF."""

from __future__ import annotations

import re
from typing import Optional

NFSE_SERIE_PROTHEUS = "NFS"

_NFSE_STRONG = (
    re.compile(r"(?is)\bnfs-?e\b"),
    re.compile(r"(?is)nota\s+fiscal\s+de\s+servi[cç]o"),
)

_NFSE_MEDIUM = (
    re.compile(r"(?is)\bissqn\b"),
    re.compile(r"(?is)c[oó]digo\s+de\s+servi[cç]o"),
    re.compile(r"(?is)discrimina[cç][aã]o\s+dos\s+servi[cç]os"),
    re.compile(r"(?is)munic[ií]pio\s+de\s+incid[eê]ncia"),
    re.compile(r"(?is)prestador\s+do\s+servi[cç]o"),
    re.compile(r"(?is)tomador\s+do\s+servi[cç]o"),
    re.compile(r"(?is)valor\s+l[ií]quido\s+da\s+nfs-?e"),
    re.compile(r"(?is)\bprefeitura\b"),
)

_NFE_PRODUCT_STRONG = (
    re.compile(r"(?is)\bdanfe\b"),
    re.compile(r"(?is)documento\s+auxiliar\s+da\s+nota\s+fiscal\s+eletr[oô]nica"),
    re.compile(r"(?is)\bcfop\b"),
    re.compile(r"(?is)\bicms\b"),
    re.compile(r"(?is)modalidade\s+do\s+frete"),
)

_NUMERO_NFSE = re.compile(
    r"(?is)(?:n[úu]mero|numero|n[º°])\s*(?:da\s+)?(?:nfs-?e\s*)?[:\s]*(\d{1,9})\b",
)
_NUMERO_NFSE_ALT = re.compile(
    r"(?is)nota\s+fiscal\s+de\s+servi[cç]o[^\d]{0,40}(\d{1,9})\b",
)


def is_nfse_document_text(text: str) -> bool:
    """
    True quando o texto indica NFS-e municipal (serviço), não NF-e de produto (DANFE).

    Indícios fortes: "NFS-e", "Nota Fiscal de Serviço".
    Indícios médios: ISSQN, código de serviço, prestador/tomador, etc. (≥2 sem sinais de DANFE).
    """
    if not text or not str(text).strip():
        return False
    t = str(text)
    has_strong_nfse = any(p.search(t) for p in _NFSE_STRONG)
    has_strong_nfe = any(p.search(t) for p in _NFE_PRODUCT_STRONG)
    if has_strong_nfe and not has_strong_nfse:
        return False
    if has_strong_nfse:
        return True
    medium_hits = sum(1 for p in _NFSE_MEDIUM if p.search(t))
    return medium_hits >= 2


def extract_numero_nfse(text: str) -> Optional[str]:
    """Número da nota em layout NFS-e (ex.: Número: 27)."""
    if not text:
        return None
    for rx in (_NUMERO_NFSE, _NUMERO_NFSE_ALT):
        m = rx.search(text)
        if m:
            num = m.group(1).strip().lstrip("0") or "0"
            return num
    return None


def detect_nfse_from_text(text: str, file_name: str | None = None) -> dict:
    """
    Resultado da classificação fiscal do documento.

    Returns:
        is_nfse, serie ("NFS" se NFS-e), numero_nota (opcional), fonte
    """
    is_nfse = is_nfse_document_text(text or "")
    if not is_nfse and file_name:
        fn = file_name.lower()
        if "nfse" in fn or "nfs-e" in fn or "nfs_e" in fn:
            is_nfse = True
    out: dict = {"is_nfse": is_nfse}
    if not is_nfse:
        return out
    out["serie"] = NFSE_SERIE_PROTHEUS
    out["tipo_documento_fiscal"] = "NFSE"
    num = extract_numero_nfse(text or "")
    if num:
        out["numero_nota"] = num
    return out


def _detect_nfse_relaxed_uso_consumo(combined: str) -> dict | None:
    """
    Uso e consumo costuma vir com PDF NFS-e (serviço), sem DANFE de produto.
    Limiar mais baixo quando não há sinais de NF-e mercadoria.
    """
    if not combined or not combined.strip():
        return None
    if any(p.search(combined) for p in _NFE_PRODUCT_STRONG):
        return None
    if any(p.search(combined) for p in _NFSE_STRONG):
        return detect_nfse_from_text(combined)
    medium_hits = sum(1 for p in _NFSE_MEDIUM if p.search(combined))
    if medium_hits >= 1:
        return detect_nfse_from_text(combined)
    return None


def detect_nfse_from_sources(
    *,
    raw_texts: list[str] | None = None,
    file_names: list[str] | None = None,
    bedrock_fields: dict | None = None,
    xml_modelo: str | None = None,
    uso_e_consumo: bool = False,
) -> dict:
    """
    Agrega detecção a partir de Textract, Bedrock e modelo XML (55 = NF-e produto).

    ``uso_e_consumo``: fluxo USOCONSUMO — PDF de serviço (NFS-e) com série Protheus ``NFS``.
    """
    if xml_modelo and str(xml_modelo).strip() == "55":
        return {"is_nfse": False}

    for text in raw_texts or []:
        d = detect_nfse_from_text(text)
        if d.get("is_nfse"):
            return d

    for name in file_names or []:
        d = detect_nfse_from_text("", file_name=name)
        if d.get("is_nfse"):
            return d

    bd = bedrock_fields if isinstance(bedrock_fields, dict) else {}
    serie_bd = str(bd.get("serie") or "").strip().upper()
    tipo_bd = str(bd.get("tipoDeDocumento") or "").strip().upper()
    especie_bd = str(bd.get("especie") or "").strip().upper()
    if serie_bd == NFSE_SERIE_PROTHEUS or tipo_bd in ("NFS", "NFSE", "NFS-E") or especie_bd == "NFSE":
        out = {"is_nfse": True, "serie": NFSE_SERIE_PROTHEUS, "tipo_documento_fiscal": "NFSE"}
        if bd.get("documento"):
            out["numero_nota"] = str(bd["documento"]).strip()
        return out

    combined = "\n".join(raw_texts or [])
    if is_nfse_document_text(combined):
        return detect_nfse_from_text(combined)

    if uso_e_consumo:
        relaxed = _detect_nfse_relaxed_uso_consumo(combined)
        if relaxed and relaxed.get("is_nfse"):
            return relaxed

    return {"is_nfse": False}


def is_nfse_danfe_data(danfe_data: dict | None) -> bool:
    """Documento principal classificado como NFS-e (ex.: uso e consumo com PDF de serviço)."""
    if not isinstance(danfe_data, dict):
        return False
    if danfe_data.get("tipo_documento_fiscal") == "NFSE":
        return True
    return str(danfe_data.get("serie") or "").strip().upper() == NFSE_SERIE_PROTHEUS
