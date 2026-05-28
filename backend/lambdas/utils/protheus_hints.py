"""Heurísticas sobre texto Textract → campos únicos alinhados ao documento de entrada Protheus.

Evita arrays genéricos (vários CNPJs / chaves falsas do código de barras): prioriza rótulos
no texto, CNPJ formatado com DV, e janelas de 44 dígitos com DV de NF-e quando aplicável.
"""

from __future__ import annotations

import re
from typing import Optional

from utils.nfse_detection import detect_nfse_from_text

# UF numérico na chave de acesso (NF-e / documentos eletrônicos comuns)
_VALID_UF = {
    11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 33, 35, 41, 42, 43, 50, 51, 52, 53,
}

# Modelo na posição 18–19 (0-based) — NF-e / NFC-e; 12 aparece em alguns DPS/NFS-e nacionais
_PREFERRED_MODELS = {55, 65, 59, 57, 12, 13}

_CNPJ_FMT = re.compile(r"\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b")
_CNPJ_PLAIN_AFTER_LABEL = re.compile(
    r"(?is)(?:cnpj|cpf)\s*/?\s*cnpj?\s*:\s*(\d{14})\b",
)

_CHAVE_CTX = re.compile(
    r"(?is)chave\s+de\s+acesso(?:\s+da)?(?:\s+nfs-?e|\s+da\s+nfs-?e)?[^\d]{0,160}([\d\s\.]{44,240})",
)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s)


def _cnpj_check_digits(base12: str) -> tuple[int, int]:
    """Calcula os dois dígitos verificadores a partir dos 12 primeiros dígitos."""
    nums = [int(c) for c in base12]
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s1 = sum(n * w for n, w in zip(nums, w1))
    d1 = 0 if (s1 % 11) < 2 else 11 - (s1 % 11)
    s2 = sum(n * w for n, w in zip(nums + [d1], w2))
    d2 = 0 if (s2 % 11) < 2 else 11 - (s2 % 11)
    return d1, d2


def _cnpj_valid(d14: str) -> bool:
    if len(d14) != 14 or not d14.isdigit():
        return False
    if len(set(d14)) == 1:
        return False
    d1, d2 = _cnpj_check_digits(d14[:12])
    return d1 == int(d14[12]) and d2 == int(d14[13])


def _chave_nfe_dv_ok(chave: str) -> bool:
    """DV módulo 11 dos 43 primeiros dígitos (padrão NF-e / vários doc. eletrônicos)."""
    if len(chave) != 44 or not chave.isdigit():
        return False
    pesos = [2, 3, 4, 5, 6, 7, 8, 9]
    soma = 0
    for i, dig in enumerate(reversed(chave[:43])):
        soma += int(dig) * pesos[i % 8]
    resto = soma % 11
    esperado = 0 if resto in (0, 1) else 11 - resto
    return int(chave[43]) == esperado


def _collect_cnpjs(text: str) -> dict[str, list[int]]:
    """CNPJ 14 dígitos válido → lista de posições aproximadas (início no texto)."""
    found: dict[str, list[int]] = {}
    for m in _CNPJ_FMT.finditer(text):
        d = _digits(m.group(1))
        if _cnpj_valid(d):
            found.setdefault(d, []).append(m.start())
    for m in _CNPJ_PLAIN_AFTER_LABEL.finditer(text):
        d = m.group(1)
        if _cnpj_valid(d):
            found.setdefault(d, []).append(m.start())
    return found


def _first_valid_cnpj_after_last_marker(text: str, markers: tuple[str, ...]) -> Optional[str]:
    """Última ocorrência de um dos marcadores; depois disso, primeiro CNPJ formatado com DV ok."""
    lower = text.lower()
    best_idx = -1
    for mk in markers:
        j = lower.rfind(mk.lower())
        if j > best_idx:
            best_idx = j
    if best_idx < 0:
        return None
    tail = text[best_idx:]
    for m in _CNPJ_FMT.finditer(tail):
        d = _digits(m.group(1))
        if _cnpj_valid(d):
            return d
    return None


def _digit_blob_after_chave_label(text: str) -> Optional[str]:
    m = _CHAVE_CTX.search(text)
    if not m:
        return None
    return _digits(m.group(1))


def _score_chave_window(w: str, cnpj_set: set[str], emitente_cnpj: Optional[str]) -> int:
    if len(w) != 44 or not w.isdigit():
        return -1
    score = 0
    uf = int(w[:2])
    if uf in _VALID_UF:
        score += 40
    else:
        return -1
    mod = int(w[18:20])
    if mod in _PREFERRED_MODELS:
        score += 35
    elif 1 <= mod <= 99:
        score += 10
    if _chave_nfe_dv_ok(w):
        score += 120
    cnpj_field = w[4:18]
    if emitente_cnpj and cnpj_field == emitente_cnpj:
        score += 500
    elif cnpj_field in cnpj_set:
        score += 150
    elif _cnpj_valid(cnpj_field):
        score += 90
    return score


def _best_chave_44(text: str, cnpj_set: set[str], emitente_cnpj: Optional[str]) -> Optional[str]:
    """Escolhe uma janela de 44 dígitos; exige UF válido. Prioriza prefixo do trecho após 'Chave de acesso'."""
    blobs: list[tuple[str, bool]] = []
    b = _digit_blob_after_chave_label(text)
    full = _digits(text)
    if b and len(b) >= 44:
        blobs.append((b, True))
    elif len(full) >= 44:
        # Sem rótulo confiável: só janelas com DV da chave (evita código de barras / ruído)
        blobs.append((full, False))

    best_w: Optional[str] = None
    best_score = -1
    seen_windows: set[str] = set()
    for blob, from_label in blobs:
        for i in range(0, len(blob) - 43):
            w = blob[i : i + 44]
            if w in seen_windows:
                continue
            seen_windows.add(w)
            sc = _score_chave_window(w, cnpj_set, emitente_cnpj)
            if from_label and i == 0 and int(w[:2]) in _VALID_UF:
                sc += 280
            if not from_label and not _chave_nfe_dv_ok(w):
                continue
            if sc > best_score:
                best_score = sc
                best_w = w
    if best_w is None or best_score < 30:
        return None
    return best_w


def _valor_after_keywords(text: str, patterns: tuple[re.Pattern[str], ...]) -> Optional[str]:
    for rx in patterns:
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return None


_VALOR_DOC = re.compile(
    r"(?is)valor\s+do\s+documento[^\d]{0,40}([\d]{1,3}(?:\.\d{3})*,\d{2})\b",
)
_VALOR_LIQUIDO_NFSE = re.compile(
    r"(?is)valor\s+líquido\s+da\s+nfs-?e[^\d]{0,60}R\$\s*([\d\.\,]+)",
)


def hints_from_textract_text(text: str) -> dict[str, object]:
    if not text or not text.strip():
        return {}

    cnpj_map = _collect_cnpjs(text)
    cnpj_set = set(cnpj_map.keys())

    # Emitente antes da chave (reforça janela cuja posição 4–17 coincide com o prestador)
    cnpj_emitente = _first_valid_cnpj_after_last_marker(
        text,
        ("prestador do serviço", "prestador do servico", "emitente da nfs-e", "emitente da nfs"),
    )

    chave = _best_chave_44(text, cnpj_set, cnpj_emitente)

    if not cnpj_emitente and chave and len(chave) == 44:
        emb = chave[4:18]
        if _cnpj_valid(emb):
            cnpj_emitente = emb

    cnpj_tomador = _first_valid_cnpj_after_last_marker(
        text,
        ("tomador do serviço", "tomador do servico"),
    )

    # Valor principal: prioriza NFSe líquido, senão valor do documento (boleto)
    valor_doc = _valor_after_keywords(text, (_VALOR_LIQUIDO_NFSE, _VALOR_DOC))

    out: dict[str, object] = {}
    if chave:
        out["chaveAcesso"] = chave
    if cnpj_emitente:
        out["cnpjEmitente"] = cnpj_emitente
    if cnpj_tomador and cnpj_tomador != cnpj_emitente:
        out["cnpjTomador"] = cnpj_tomador
    if valor_doc:
        out["valorDocumento"] = valor_doc

    nfse = detect_nfse_from_text(text)
    if nfse.get("is_nfse"):
        out["tipoDocumentoFiscal"] = nfse.get("tipo_documento_fiscal", "NFSE")
        out["serie"] = nfse.get("serie", "NFS")
        if nfse.get("numero_nota"):
            out["numeroNota"] = nfse["numero_nota"]

    return enrich_hints_with_xml_style(out)


def enrich_hints_with_xml_style(hints: dict[str, object]) -> dict[str, object]:
    """
    Acrescenta `parsed_xml_style`: subconjunto no mesmo formato que `parse_xml` grava em PARSED_DATA
    (emitente/destinatario/totais/chave_acesso em snake_case), para o send_to_protheus e outras
    etapas lerem como se viessem de NF-e XML. Mantém as chaves “flat” (chaveAcesso, cnpjEmitente, …).
    """
    if not hints:
        return hints
    out = dict(hints)
    px: dict[str, object] = {}

    ch = out.get("chaveAcesso")
    if ch is not None and str(ch).strip():
        cd = _digits(str(ch))
        if len(cd) >= 44:
            px["chave_acesso"] = cd[:44]

    emit: dict[str, object] = {}
    if out.get("cnpjEmitente"):
        emit["cnpj"] = str(out["cnpjEmitente"])
    if emit:
        px["emitente"] = emit

    dest: dict[str, object] = {}
    if out.get("cnpjTomador"):
        dest["cnpj"] = str(out["cnpjTomador"])
    if dest:
        px["destinatario"] = dest

    if out.get("valorDocumento") is not None and str(out.get("valorDocumento")).strip() != "":
        px["totais"] = {"valor_nota": str(out["valorDocumento"]).strip()}

    if out.get("tipoDocumentoFiscal") == "NFSE" or out.get("serie") == "NFS":
        px["serie"] = str(out.get("serie") or "NFS")
        px["tipo_documento_fiscal"] = "NFSE"
    if out.get("numeroNota"):
        px["numero_nota"] = str(out["numeroNota"]).strip()

    if px:
        out["parsed_xml_style"] = px
    return out
