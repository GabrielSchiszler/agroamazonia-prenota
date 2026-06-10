"""Resolve campos fiscais do documento de entrada: Bedrock (IA) → XML → OCR → pedido."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from utils.ocr_identity import pick_matching_ocr_per_document
from utils.protheus_hints import _chave_nfe_dv_ok, _cnpj_valid


def _digits(value: object, *, max_len: int | None = None) -> str:
    d = "".join(c for c in str(value or "") if c.isdigit())
    if max_len is not None:
        return d[:max_len]
    return d


def _non_empty_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def normalize_documento_numero(value: object) -> str:
    """
    Número do documento Protheus: com 10+ dígitos e zeros à esquerda, reduz para 9
    removendo zeros à esquerda (ex.: 0000001287 → 000001287).
    """
    s = _non_empty_str(value) or ""
    if len(s) < 10 or not s.isdigit() or s[0] != "0":
        return s
    while len(s) > 9 and s.startswith("0"):
        s = s[1:]
    return s


def _normalize_chave(value: object, *, require_dv: bool = False) -> str | None:
    blob = _digits(value)
    if not blob:
        return None
    if len(blob) == 44:
        if require_dv and not _chave_nfe_dv_ok(blob):
            return None
        return blob
    if len(blob) > 44:
        best: str | None = None
        best_score = -1
        for i in range(0, len(blob) - 43):
            w = blob[i : i + 44]
            if not w.isdigit():
                continue
            sc = 1
            if _chave_nfe_dv_ok(w):
                sc += 100
            if sc > best_score:
                best_score = sc
                best = w
        if best and (not require_dv or _chave_nfe_dv_ok(best)):
            return best
    return None


def _normalize_cnpj(value: object, *, validate_checksum: bool = True) -> str | None:
    d = _digits(value, max_len=14)
    if len(d) != 14:
        return None
    if validate_checksum and not _cnpj_valid(d):
        return None
    return d


def _normalize_cpf(value: object) -> str | None:
    d = _digits(value, max_len=11)
    return d if len(d) == 11 else None


def _ai_fields(bedrock_extraction: dict | None, ocr_data: dict | None) -> dict:
    if isinstance(bedrock_extraction, dict) and bedrock_extraction:
        return bedrock_extraction
    if isinstance(ocr_data, dict):
        dep = ocr_data.get("documento_entrada_protheus")
        if isinstance(dep, dict) and dep:
            return dep
    return {}


def _ocr_matching_pd(
    xml_data: dict | None,
    ocr_data: dict | None,
    *,
    primary_file_name: str | None,
) -> dict | None:
    if not isinstance(ocr_data, dict):
        return None
    pd = pick_matching_ocr_per_document(
        xml_data if isinstance(xml_data, dict) else {},
        ocr_data,
        primary_file_name=primary_file_name,
    )
    return pd if isinstance(pd, dict) else None


def _hints_from_pd(pd: dict | None) -> tuple[dict, dict]:
    if not isinstance(pd, dict):
        return {}, {}
    hints = pd.get("protheus_hints")
    hints = hints if isinstance(hints, dict) else {}
    px = hints.get("parsed_xml_style")
    px = px if isinstance(px, dict) else {}
    return hints, px


def _first(
    candidates: list[tuple[object, str]],
    *,
    transform=None,
) -> tuple[Any, str | None]:
    for raw, source in candidates:
        if raw is None:
            continue
        if isinstance(raw, str) and not raw.strip():
            continue
        val = transform(raw) if transform else raw
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        return val, source
    return None, None


@dataclass
class ResolvedDocumentFields:
    numero_documento: str = ""
    serie_raw: str = ""
    data_emissao_raw: str = ""
    chave_acesso_raw: str = ""
    tipo_documento_raw: str | None = None
    tipo_frete_raw: str | None = None
    especie_raw: str | None = None
    moeda_informada: Any = None
    taxa_informada: Any = None
    cnpj_emitente: str | None = None
    cpf_emitente: str | None = None
    ie_emitente: str | None = None
    modelo: str = ""
    sources: dict[str, str] = field(default_factory=dict)


def _ocr_emitente_cnpj_legacy(
    ocr_data: dict | None,
    *,
    validate_checksum: bool = False,
) -> str | None:
    """Mesma lógica de send_to_protheus._emitente_cnpj_from_ocr_per_document (todos os anexos)."""
    if not isinstance(ocr_data, dict):
        return None
    per = ocr_data.get("per_document") or []
    if not isinstance(per, list):
        return None
    chaves: list[str] = []
    candidatos: list[str] = []
    for pd in per:
        if not isinstance(pd, dict):
            continue
        hints, px = _hints_from_pd(pd)
        em = px.get("emitente") if isinstance(px.get("emitente"), dict) else {}
        raw_cnpj = em.get("cnpj") or hints.get("cnpjEmitente")
        if raw_cnpj is None:
            continue
        d = _digits(raw_cnpj, max_len=14)
        if len(d) == 14 and (not validate_checksum or _cnpj_valid(d)):
            candidatos.append(d)
        ch = hints.get("chaveAcesso") or px.get("chave_acesso")
        if ch:
            cd = _digits(ch, max_len=44)
            if len(cd) >= 44:
                chaves.append(cd[:44])
    for c in candidatos:
        for ch in chaves:
            if len(ch) >= 18 and ch[4:18] == c:
                return c
    return candidatos[0] if candidatos else None


def _ordered(
    bedrock_first: bool,
    bedrock_val: object,
    xml_val: object,
    ocr_vals: list[tuple[object, str]],
    pedido_val: object | None = None,
) -> list[tuple[object, str]]:
    """Monta lista de candidatos conforme modo USO E CONSUMO ou legado."""
    core = [
        (bedrock_val, "bedrock"),
        (xml_val, "xml"),
        *ocr_vals,
    ]
    if pedido_val is not None:
        core.append((pedido_val, "pedido"))
    if bedrock_first:
        return core
    # Legado: XML → OCR → Bedrock → pedido
    legacy = [
        (xml_val, "xml"),
        *ocr_vals,
        (bedrock_val, "bedrock"),
    ]
    if pedido_val is not None:
        legacy.append((pedido_val, "pedido"))
    return legacy


def resolve_protheus_document_fields(
    *,
    bedrock_extraction: dict | None,
    xml_data: dict | None,
    ocr_data: dict | None,
    request_body_data: dict | None,
    primary_file_name: str | None = None,
    bedrock_first: bool = False,
) -> ResolvedDocumentFields:
    """
    USO E CONSUMO (bedrock_first=True): Bedrock → XML → OCR → pedido.
    Demais tipos (legado): XML → OCR → Bedrock → pedido.
    """
    xd = xml_data if isinstance(xml_data, dict) else {}
    rb = request_body_data if isinstance(request_body_data, dict) else {}
    ai = _ai_fields(bedrock_extraction, ocr_data)
    emit = xd.get("emitente") if isinstance(xd.get("emitente"), dict) else {}
    transporte = xd.get("transporte") if isinstance(xd.get("transporte"), dict) else {}
    pd = _ocr_matching_pd(xd, ocr_data, primary_file_name=primary_file_name)
    hints, px = _hints_from_pd(pd)

    out = ResolvedDocumentFields(modelo=_non_empty_str(xd.get("modelo")) or "")

    numero, src = _first(
        _ordered(
            bedrock_first,
            ai.get("documento"),
            xd.get("numero_nota"),
            [
                (hints.get("numeroNota"), "ocr"),
                (px.get("numero_nota"), "ocr"),
            ],
        ),
        transform=lambda v: _non_empty_str(v),
    )
    if numero:
        out.numero_documento = normalize_documento_numero(numero)
        out.sources["documento"] = src or "?"

    serie, src = _first(
        _ordered(
            bedrock_first,
            ai.get("serie"),
            xd.get("serie"),
            [(hints.get("serie"), "ocr"), (px.get("serie"), "ocr")],
        ),
        transform=lambda v: _non_empty_str(v),
    )
    if serie:
        out.serie_raw = serie
        out.sources["serie"] = src or "?"

    data, src = _first(
        _ordered(bedrock_first, ai.get("dataEmissao"), xd.get("data_emissao"), []),
        transform=lambda v: _non_empty_str(v),
    )
    if data:
        out.data_emissao_raw = data
        out.sources["dataEmissao"] = src or "?"

    chave_order = _ordered(
        bedrock_first,
        ai.get("chaveAcesso"),
        xd.get("chave_acesso"),
        [(hints.get("chaveAcesso"), "ocr"), (px.get("chave_acesso"), "ocr")],
    )
    for raw, src in chave_order:
        require_dv = src == "ocr" and bedrock_first
        ch = _normalize_chave(raw, require_dv=require_dv)
        if ch:
            out.chave_acesso_raw = ch
            out.sources["chaveAcesso"] = src
            break

    tipo_doc, src = _first(
        _ordered(bedrock_first, ai.get("tipoDeDocumento"), xd.get("modelo"), []),
        transform=lambda v: _non_empty_str(v),
    )
    if tipo_doc:
        out.tipo_documento_raw = tipo_doc
        out.sources["tipoDeDocumento"] = src or "?"

    especie, src = _first(
        _ordered(bedrock_first, ai.get("especie"), None, []),
        transform=lambda v: _non_empty_str(v),
    )
    if especie:
        out.especie_raw = especie
        out.sources["especie"] = src or "?"

    tipo_frete, src = _first(
        _ordered(
            bedrock_first,
            ai.get("tipoFrete"),
            transporte.get("modalidade_frete"),
            [],
        ),
        transform=lambda v: _non_empty_str(v),
    )
    if tipo_frete is not None:
        out.tipo_frete_raw = tipo_frete
        out.sources["tipoFrete"] = src or "?"

    moeda_ocr = ocr_data.get("moeda") if isinstance(ocr_data, dict) else None
    if bedrock_first:
        moeda_order = [
            (rb.get("moeda"), "pedido"),
            (ai.get("moeda"), "bedrock"),
            (moeda_ocr, "ocr"),
        ]
    else:
        moeda_order = [
            (rb.get("moeda"), "pedido"),
            (moeda_ocr, "ocr"),
            (ai.get("moeda"), "bedrock"),
        ]
    moeda, src = _first(moeda_order)
    if moeda is not None:
        out.moeda_informada = moeda
        out.sources["moeda"] = src or "?"

    taxa, src = _first([(rb.get("taxaCambio"), "pedido"), (ai.get("taxaCambio"), "bedrock")])
    if taxa is not None:
        out.taxa_informada = taxa
        out.sources["taxaCambio"] = src or "?"

    em_px = px.get("emitente") if isinstance(px.get("emitente"), dict) else {}
    ocr_cnpj_legacy = _ocr_emitente_cnpj_legacy(
        ocr_data, validate_checksum=bedrock_first
    )
    ocr_validate = bedrock_first
    cnpj, src = _first(
        _ordered(
            bedrock_first,
            _normalize_cnpj(ai.get("cnpjEmitente"), validate_checksum=False),
            _normalize_cnpj(emit.get("cnpj"), validate_checksum=False),
            [
                (_normalize_cnpj(hints.get("cnpjEmitente"), validate_checksum=ocr_validate), "ocr"),
                (_normalize_cnpj(em_px.get("cnpj"), validate_checksum=ocr_validate), "ocr"),
                (_normalize_cnpj(ocr_cnpj_legacy, validate_checksum=False), "ocr"),
            ],
            _normalize_cnpj(rb.get("cnpjEmitente"), validate_checksum=ocr_validate),
        ),
    )
    if cnpj:
        out.cnpj_emitente = cnpj
        out.sources["cnpjEmitente"] = src or "?"

    if not out.cnpj_emitente:
        cpf, src = _first(
            _ordered(
                bedrock_first,
                _normalize_cpf(ai.get("cpfEmitente")),
                _normalize_cpf(emit.get("cpf")),
                [],
            ),
        )
        if cpf:
            out.cpf_emitente = cpf
            out.sources["cpfEmitente"] = src or "?"

    ie, src = _first(
        _ordered(
            bedrock_first,
            _non_empty_str(ai.get("ieEmitente")),
            _non_empty_str(emit.get("ie")),
            [],
        ),
    )
    if ie:
        out.ie_emitente = ie
        out.sources["ieEmitente"] = src or "?"

    return out
