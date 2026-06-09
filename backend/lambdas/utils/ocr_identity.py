"""Escolha de hints OCR/Textract alinhados ao XML principal do processo."""

from __future__ import annotations


def _norm_digits(value: object, *, max_len: int | None = None) -> str:
    d = "".join(c for c in str(value or "") if c.isdigit())
    if max_len is not None:
        return d[:max_len]
    return d


def _norm_numero(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    stripped = s.lstrip("0")
    return stripped or "0"


def _hints_from_per_document(pd: dict) -> tuple[dict, dict]:
    hints = pd.get("protheus_hints")
    hints = hints if isinstance(hints, dict) else {}
    px = hints.get("parsed_xml_style")
    px = px if isinstance(px, dict) else {}
    return hints, px


def _chave_from_per_document(pd: dict) -> str:
    hints, px = _hints_from_per_document(pd)
    return _norm_digits(hints.get("chaveAcesso") or px.get("chave_acesso"), max_len=44)


def _numero_from_per_document(pd: dict) -> str:
    hints, px = _hints_from_per_document(pd)
    return _norm_numero(hints.get("numeroNota") or px.get("numero_nota"))


def _emitente_cnpj_from_per_document(pd: dict) -> str:
    hints, px = _hints_from_per_document(pd)
    em = px.get("emitente")
    em = em if isinstance(em, dict) else {}
    return _norm_digits(em.get("cnpj") or hints.get("cnpjEmitente"), max_len=14)


def _file_names_related(a: str, b: str) -> bool:
    a = (a or "").lower().strip()
    b = (b or "").lower().strip()
    if not a or not b:
        return False
    if a == b:
        return True
    a_stem = a.rsplit(".", 1)[0]
    b_stem = b.rsplit(".", 1)[0]
    return a_stem == b_stem or a_stem in b_stem or b_stem in a_stem


def pick_matching_ocr_per_document(
    xml_data: dict | None,
    ocr_data: dict | None,
    *,
    primary_file_name: str | None = None,
) -> dict | None:
    """
    Retorna o per_document cujos hints batem com o XML principal.
    Evita preencher NF com dados de boleto/outro anexo do mesmo processo.
    """
    if not isinstance(ocr_data, dict):
        return None
    per = ocr_data.get("per_document") or []
    if not isinstance(per, list) or not per:
        return None

    xd = xml_data if isinstance(xml_data, dict) else {}
    chave_xml = _norm_digits(xd.get("chave_acesso"), max_len=44)
    num_xml = _norm_numero(xd.get("numero_nota"))
    emit_xml = _norm_digits((xd.get("emitente") or {}).get("cnpj"), max_len=14)

    if len(chave_xml) >= 44:
        for pd in per:
            if not isinstance(pd, dict):
                continue
            if _chave_from_per_document(pd) == chave_xml:
                return pd

    if primary_file_name:
        for pd in per:
            if not isinstance(pd, dict):
                continue
            if _file_names_related(primary_file_name, pd.get("file_name") or ""):
                return pd

    if num_xml:
        for pd in per:
            if not isinstance(pd, dict):
                continue
            if _numero_from_per_document(pd) == num_xml:
                return pd

    if emit_xml and len(emit_xml) == 14:
        for pd in per:
            if not isinstance(pd, dict):
                continue
            cnpj_pd = _emitente_cnpj_from_per_document(pd)
            if cnpj_pd == emit_xml:
                return pd

    if len(per) == 1 and isinstance(per[0], dict):
        return per[0]

    return None


def find_ocr_identity_conflicts(
    xml_data: dict | None,
    ocr_data: dict | None,
    *,
    primary_file_name: str | None = None,
) -> list[dict]:
    """Lista anexos OCR cujo número/chave/emitente divergem do XML principal."""
    if not isinstance(ocr_data, dict):
        return []
    per = ocr_data.get("per_document") or []
    if not isinstance(per, list):
        return []

    xd = xml_data if isinstance(xml_data, dict) else {}
    chave_xml = _norm_digits(xd.get("chave_acesso"), max_len=44)
    num_xml = _norm_numero(xd.get("numero_nota"))
    emit_xml = _norm_digits((xd.get("emitente") or {}).get("cnpj"), max_len=14)
    matched = pick_matching_ocr_per_document(
        xd, ocr_data, primary_file_name=primary_file_name
    )
    matched_fn = (matched or {}).get("file_name")

    conflicts: list[dict] = []
    for pd in per:
        if not isinstance(pd, dict):
            continue
        fn = pd.get("file_name") or "unknown"
        if matched_fn and fn == matched_fn:
            continue
        if primary_file_name and _file_names_related(primary_file_name, fn):
            continue

        issues: list[str] = []
        ch_pd = _chave_from_per_document(pd)
        if chave_xml and len(chave_xml) >= 44 and ch_pd and ch_pd != chave_xml:
            issues.append(f"chave ({ch_pd} ≠ {chave_xml})")
        num_pd = _numero_from_per_document(pd)
        if num_xml and num_pd and num_pd != num_xml:
            issues.append(f"numero_nota ({num_pd} ≠ {num_xml})")
        cnpj_pd = _emitente_cnpj_from_per_document(pd)
        if emit_xml and len(emit_xml) == 14 and cnpj_pd and cnpj_pd != emit_xml:
            issues.append(f"emitente.cnpj ({cnpj_pd} ≠ {emit_xml})")

        if issues:
            conflicts.append({"file_name": fn, "issues": issues})
    return conflicts
