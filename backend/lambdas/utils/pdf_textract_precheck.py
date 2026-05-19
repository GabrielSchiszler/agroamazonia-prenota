"""
Heurísticas em bytes para PDFs que falham no Textract AnalyzeDocument.

Sem dependências externas — útil em Lambda antes/depois de chamar a API.
Referência: UnsupportedDocumentException (formato inaceitável, XFA, encriptação, etc.).
"""

from __future__ import annotations

import re
from typing import Any


def diagnose_pdf_bytes(body: bytes) -> dict[str, Any]:
    """
    Inspeciona o início do ficheiro e amostras para sinais que explicam
    UnsupportedDocumentException no AnalyzeDocument (TABLES+FORMS).
    """
    n = len(body)
    out: dict[str, Any] = {
        "size_bytes": n,
        "looks_like_pdf": body[:5] == b"%PDF-",
    }
    if n < 8:
        out["error"] = "too_short_for_pdf"
        return out

    first_line = body.split(b"\n", 1)[0]
    out["pdf_header_line"] = first_line[:80].decode("ascii", errors="replace")

    # Amostra: início costuma ter trailer/catalog; XFA pode estar mais adiante
    sample_len = min(n, 4_000_000)
    sample = body[:sample_len]
    low = sample.lower()

    # XFA / formulários dinâmicos (comuns em alguns PDFs de prefeitura/NFSe)
    out["marker_slash_xfa"] = b"/xfa" in low
    out["marker_xfa_template_ns"] = (
        b"http://www.xfa.org/schema/xfa-template" in low
        or b"<xfa:" in low
        or b"xfa:template" in low
    )

    # Encriptação (dicionário Encrypt no trailer/catalog)
    head = body[: min(n, 400_000)]
    out["marker_encrypt_ref"] = bool(
        re.search(br"/Encrypt\s+\d+\s+\d+\s+R", head)
        or re.search(br"/Encrypt\s*<<", head)
    )

    # Contagem grosseira de páginas (objetos /Type /Page)
    out["approx_type_page_count"] = len(re.findall(br"/Type\s*/Page\b", sample))

    out["linearized"] = b"/linearized" in low[:50_000]

    risks: list[str] = []
    if not out["looks_like_pdf"]:
        risks.append("missing_pdf_magic_header")
    if out["marker_encrypt_ref"]:
        risks.append("likely_encrypted_or_restricted_pdf")
    if out["marker_slash_xfa"] or out["marker_xfa_template_ns"]:
        risks.append("likely_xfa_or_dynamic_form_pdf_textract_often_rejects")
    if out["approx_type_page_count"] == 0 and out["looks_like_pdf"]:
        risks.append("zero_page_markers_unusual_may_be_image_only_or_custom")

    out["heuristic_risks"] = risks
    out["human_hint"] = _hint_from_risks(risks)
    return out


def _hint_from_risks(risks: list[str]) -> str:
    if "likely_encrypted_or_restricted_pdf" in risks:
        return "PDF parece encriptado/restrito — Textract não processa. Remover senha ou reexportar sem proteção."
    if "likely_xfa_or_dynamic_form_pdf_textract_often_rejects" in risks:
        return "PDF com XFA/formulário dinâmico — comum em NFSe de alguns emissores; imprimir para PDF simples ou flatten."
    if "missing_pdf_magic_header" in risks:
        return "Conteúdo não parece PDF válido (cabeçalho %PDF- ausente)."
    if risks:
        return "Ver heuristic_risks para detalhes."
    return "Nenhum sinal forte de XFA/encrypt no scan; causa pode ser PDF corrompido, JPEG2000/JBIG2 interno, ou limite do parser Textract."
