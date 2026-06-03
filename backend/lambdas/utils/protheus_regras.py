"""
Extração de REGRA_ID do cause Protheus e classificação Operacional/OCR para métricas.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

_CATALOG_PATH = Path(__file__).resolve().parent / "protheus_regras_catalog.json"
_API_CATALOG_PATH = Path(__file__).resolve().parent / "api_regras_catalog.json"
RE_INVALID_FIELD = re.compile(r"(\w+)\s+:=\s*[^<\r\n]+<\s*--\s*Invalido", re.I)


def is_exec_auto_error(error_code: str) -> bool:
    return str(error_code or "").upper().startswith("EXEC_AUTO")


def load_regras_catalog(path: Path | None = None) -> dict:
    p = path or _CATALOG_PATH
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    return {"metrics": {"skip_tipos": ["Operacional"], "count_tipos": ["OCR"]}, "regras": {}}


def get_regra_meta(regra_id: str, catalog: dict | None = None) -> dict:
    catalog = catalog or load_regras_catalog()
    return (catalog.get("regras") or {}).get(regra_id, {})


def get_regra_tipo(regra_id: str, catalog: dict | None = None) -> str | None:
    return get_regra_meta(regra_id, catalog).get("tipo")


def catalog_regra_ids(catalog: dict | None = None) -> set[str]:
    catalog = catalog or load_regras_catalog()
    return set((catalog.get("regras") or {}).keys())


def metrics_regras_operacional_ids(catalog: dict | None = None) -> set[str]:
    """Regras tratadas como operacionais nas métricas (fora de failed_count / taxa OCR)."""
    catalog = catalog or load_regras_catalog()
    return set(catalog.get("metrics", {}).get("regras_operacional") or [])


def effective_metrics_tipo(
    regra_id: str,
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> str | None:
    catalog = catalog or load_regras_catalog()
    if regra_id in metrics_regras_operacional_ids(catalog):
        return "Operacional"
    skip_tipos = set(catalog.get("metrics", {}).get("skip_tipos", ["Operacional"]))
    if regra_id in (catalog.get("regras") or {}):
        t = get_regra_tipo(regra_id, catalog)
    elif api_catalog and regra_id in api_catalog:
        t = api_catalog[regra_id].get("tipo")
    else:
        t = None
    if t in skip_tipos:
        return "Operacional"
    return t


def filter_regras_catalog_only(
    regras: list[dict[str, str]], catalog: dict | None = None
) -> list[dict[str, str]]:
    """Mantém só REGRA_ID definidos no catálogo oficial (sem secundários SD1/SX3)."""
    allowed = catalog_regra_ids(catalog)
    return [r for r in regras if r.get("regra_id") in allowed]


def primary_catalog_regra(
    regras: list[dict[str, str]], catalog: dict | None = None
) -> dict[str, str] | None:
    """Primeira regra do cause que está no catálogo (ordem do texto Protheus)."""
    filtered = filter_regras_catalog_only(regras, catalog)
    return filtered[0] if filtered else None


def normalize_cause(cause_raw: Any) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {
        "documentoEntrada": [],
        "preNota": [],
        "outros": [],
    }
    if cause_raw is None:
        return out
    if isinstance(cause_raw, dict):
        for key in ("documentoEntrada", "preNota"):
            block = cause_raw.get(key)
            if isinstance(block, list):
                out[key] = [str(x) for x in block if x]
            elif block:
                out[key] = [str(block)]
        for k, v in cause_raw.items():
            if k in ("documentoEntrada", "preNota"):
                continue
            if isinstance(v, list):
                out["outros"].extend(str(x) for x in v if x)
            elif v:
                out["outros"].append(str(v))
        return out
    if isinstance(cause_raw, list):
        for item in cause_raw:
            if isinstance(item, dict):
                nested = normalize_cause(item)
                for k in out:
                    out[k].extend(nested.get(k, []))
            elif item:
                out["outros"].append(str(item))
        return out
    out["outros"].append(str(cause_raw))
    return out


def _cause_blob(cause_blocks: dict[str, list[str]]) -> str:
    return "\n".join(
        cause_blocks.get("documentoEntrada", [])
        + cause_blocks.get("preNota", [])
        + cause_blocks.get("outros", [])
    )


def _slug_ajuda(text: str) -> str:
    nfd = unicodedata.normalize("NFD", text)
    ascii_text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"[^A-Z0-9]+", "_", ascii_text.upper()).strip("_")[:80]


def _normalize_ajuda_token(raw: str) -> str | None:
    token = raw.strip()
    if not token:
        return None
    upper = token.upper()
    if "VALIDA" in upper and "NOTA FISCAL" in upper and "XML" in upper:
        return None
    if re.fullmatch(r"[A-Z0-9]{3,24}", token, re.I):
        return token.upper()
    return _slug_ajuda(token)


def _text_to_regra_id(text: str) -> str:
    t = text.strip().rstrip(":").strip()
    if not t:
        return "SEM_REGRA"
    low = t.lower()
    if low.startswith("divergência de") or low.startswith("divergencia de"):
        return _slug_ajuda(t)
    if "arquivo xml não encontrado" in low or "arquivo xml nao encontrado" in low:
        return "ARQUIVO_XML_NAO_ENCONTRADO_VERIFIQUE_IMPORTACAO_OU_FILIAL"
    if "file not found" in low:
        return "FILE_NOT_FOUND_CAMINHO_XML_NFE"
    if t == "Inconsistencia na Linha de Itens":
        return "INCONSISTENCIA_NA_LINHA_DE_ITENS"
    if t == "Inconsistencia nos Itens":
        return "INCONSISTENCIA_NOS_ITENS"
    if re.match(r"Erro no Item\s+\d+", t, re.I):
        return "ERRO_NO_ITEM_SD1"
    return _slug_ajuda(t)


def extract_regras_from_cause(cause_blocks: dict[str, list[str]]) -> list[dict[str, str]]:
    blob = _cause_blob(cause_blocks)
    regras: list[dict[str, str]] = []
    seen: set[str] = set()

    def _add(regra_id: str, mensagem: str) -> None:
        rid = regra_id.strip()
        if not rid or rid in seen:
            return
        seen.add(rid)
        regras.append({"regra_id": rid, "mensagem": mensagem.strip()})

    lines = [ln.strip() for ln in blob.splitlines() if ln.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]
        m_ajuda = re.match(r"AJUDA:([^\r\n]+)", line, re.I)
        if m_ajuda:
            token = m_ajuda.group(1).strip()
            upper = token.upper()
            if "VALIDA" in upper and "NOTA FISCAL" in upper and "XML" in upper:
                i += 1
                while i < len(lines):
                    sub = lines[i]
                    if sub.startswith("---") or sub.startswith("Tabela "):
                        break
                    if sub.lower().startswith("divergência de") or sub.lower().startswith(
                        "divergencia de"
                    ):
                        _add(_text_to_regra_id(sub), sub)
                    elif "arquivo xml não encontrado" in sub.lower() or "arquivo xml nao encontrado" in sub.lower():
                        _add(_text_to_regra_id(sub), sub)
                    elif "file not found" in sub.lower():
                        _add(_text_to_regra_id(sub), sub)
                    i += 1
                continue
            code = _normalize_ajuda_token(token)
            msg = lines[i + 1] if i + 1 < len(lines) and not lines[i + 1].startswith("Tabela") else token
            rid = code or _text_to_regra_id(token)
            _add(rid, msg)
            i += 1
            continue
        if line == "Inconsistencia na Linha de Itens":
            _add("INCONSISTENCIA_NA_LINHA_DE_ITENS", line)
        elif line == "Inconsistencia nos Itens":
            _add("INCONSISTENCIA_NOS_ITENS", line)
        elif re.match(r"Erro no Item\s+\d+", line, re.I):
            _add("ERRO_NO_ITEM_SD1", line)
        else:
            inv = RE_INVALID_FIELD.search(line)
            if inv:
                campo = inv.group(1)
                _add(f"CAMPO_INVALIDO_{campo}", f"{campo} inválido no SD1/SF1")
        i += 1

    for m in re.finditer(r"Diverg[eê]ncia de [^\r\n]+", blob, re.I):
        txt = m.group(0).strip()
        _add(_text_to_regra_id(txt), txt)

    return regras


def extract_api_error_from_body(body: dict) -> list[dict[str, str]]:
    """
    Falha API Protheus fora EXEC_AUTO_*: REGRA_ID = response.body.errorCode (ex. CALC_QTDVAL_002).
    Não usa cause — só errorCode + message.
    """
    if not isinstance(body, dict):
        return []
    code = str(body.get("errorCode") or "").strip()
    if not code or is_exec_auto_error(code):
        return []
    msg = str(body.get("message") or "").strip()
    return [{"regra_id": code, "mensagem": msg}]


def extract_regras_from_protheus_body(body: dict) -> list[dict[str, str]]:
    """EXEC_AUTO_* → cause/AJUDA; demais → errorCode direto."""
    if not isinstance(body, dict):
        return []
    code = str(body.get("errorCode") or "").strip()
    if is_exec_auto_error(code):
        return extract_regras_from_cause(normalize_cause(body.get("cause")))
    return extract_api_error_from_body(body)


def load_api_catalog_by_codigo(api_rules: list[dict]) -> dict[str, dict]:
    """Índice codigo (errorCode) → meta do Excel."""
    return {r["regra_id"]: r for r in api_rules if r.get("regra_id")}


def load_api_regras_catalog(path: Path | None = None) -> dict[str, dict]:
    """Catálogo API (errorCode → tipo/mensagem), gerado pelo sync a partir do Excel."""
    p = path or _API_CATALOG_PATH
    if not p.is_file():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "regras" in data:
        return data["regras"]
    return data if isinstance(data, dict) else {}


def parse_protheus_request_failure(info: dict) -> dict | None:
    """Lê protheus_request_info do METADATA; HTTP >= 400."""
    if not isinstance(info, dict):
        return None
    body = info.get("response_body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return None
    if not isinstance(body, dict):
        return None
    status = info.get("response_status_code")
    if status is not None and int(status) < 400:
        return None
    error_code = str(body.get("errorCode") or "").strip()
    if not error_code:
        return None
    return {
        "error_code": error_code,
        "message": body.get("message"),
        "body": body,
        "regras": extract_regras_from_protheus_body(body),
    }


def classify_regras(
    regras: list[dict[str, str]],
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> dict[str, list[dict[str, str]]]:
    catalog = catalog or load_regras_catalog()
    if api_catalog is None:
        regras = filter_regras_catalog_only(regras, catalog)
    else:
        allowed = catalog_regra_ids(catalog) | set(api_catalog.keys())
        regras = [r for r in regras if r.get("regra_id") in allowed]
    skip_tipos = set(catalog.get("metrics", {}).get("skip_tipos", ["Operacional"]))
    ocr: list[dict[str, str]] = []
    operacional: list[dict[str, str]] = []
    outros: list[dict[str, str]] = []
    for r in regras:
        rid = r["regra_id"]
        tipo = effective_metrics_tipo(rid, catalog, api_catalog)
        entry = {**r, "tipo": tipo}
        if tipo in skip_tipos:
            operacional.append(entry)
        elif tipo == "OCR":
            ocr.append(entry)
        else:
            outros.append(entry)
    return {"ocr": ocr, "operacional": operacional, "outros": outros}


def split_rules_for_metrics(
    validation_failed_rules: list[str],
    protheus_regras: list[dict[str, str]],
    ocr_pipeline_rules: list[str] | None = None,
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> tuple[list[str], list[str]]:
    """Separa regras OCR (entram em failed_count) vs operacional (falha de processo)."""
    catalog = catalog or load_regras_catalog()
    op_extra = metrics_regras_operacional_ids(catalog)
    val_ocr: list[str] = []
    val_op: list[str] = []
    for name in validation_failed_rules or []:
        if name in op_extra:
            val_op.append(name)
        else:
            val_ocr.append(name)

    classified = classify_regras(protheus_regras, catalog, api_catalog)
    ocr_rules = list(val_ocr)
    for rid in ocr_pipeline_rules or []:
        if rid not in ocr_rules:
            ocr_rules.append(rid)
    for r in classified["ocr"]:
        rid = r["regra_id"]
        if rid and rid not in ocr_rules:
            ocr_rules.append(rid)

    op_rules = list(val_op)
    for r in classified["operacional"]:
        rid = r["regra_id"]
        if rid and rid not in op_rules:
            op_rules.append(rid)
    return ocr_rules, op_rules


def protheus_regras_for_metrics(
    regras: list[dict[str, str]], catalog: dict | None = None
) -> list[str]:
    """REGRA_IDs Protheus que entram em failed_rules (somente OCR catalogado)."""
    classified = classify_regras(regras, catalog)
    return [r["regra_id"] for r in classified["ocr"]]


def parse_error_info(raw: Any) -> dict | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def ocr_pipeline_rule_id(error_info: dict) -> str:
    """REGRA_ID para falha OCR antes do Protheus (lambda, timeout, parse, etc.)."""
    lam = str(error_info.get("lambda") or "").strip()
    msg = str(
        error_info.get("message")
        or error_info.get("Error")
        or error_info.get("Cause")
        or ""
    )
    if "Timedout" in msg or "timeout" in msg.lower():
        return "OCR_LAMBDA_TIMEOUT"
    if lam:
        return f"OCR_LAMBDA_{lam}"
    if str(error_info.get("type") or "").upper() == "LAMBDA_ERROR":
        return "OCR_LAMBDA_ERROR"
    return "OCR_PROCESSING_ERROR"


def extract_ocr_pipeline_rule_ids(metadata: dict) -> list[str]:
    """Falhas técnicas no pipeline (sem protheus_request_info)."""
    if metadata.get("protheus_request_info"):
        return []
    err = parse_error_info(metadata.get("error_info"))
    if not err:
        return []
    rid = ocr_pipeline_rule_id(err)
    return [rid] if rid else []


def parse_validation_results(raw: Any) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


def validation_failed_rule_names(validation_results: list[dict]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in validation_results:
        if row.get("status") != "FAILED":
            continue
        name = str(row.get("rule") or "").strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def fetch_latest_validation_failed_rules(table: Any, pk: str) -> list[str]:
    """Regras validar_* com status FAILED no VALIDATION# mais recente."""
    try:
        resp = table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
            ExpressionAttributeValues={":pk": pk, ":sk": "VALIDATION#"},
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items") or []
        if not items:
            return []
        return validation_failed_rule_names(
            parse_validation_results(items[0].get("VALIDATION_RESULTS"))
        )
    except Exception:
        return []


def collect_ocr_failed_rule_ids(table: Any, metadata: dict) -> list[str]:
    """
    Falhas OCR fora do Protheus: validação (validar_*) e pipeline (error_info).
    Não inclui quando só há falha Protheus/API (protheus_request_info).
    """
    pk = metadata.get("PK") or ""
    if not pk:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def _add(rid: str) -> None:
        if rid and rid not in seen:
            seen.add(rid)
            out.append(rid)

    if not metadata.get("protheus_request_info"):
        for rid in fetch_latest_validation_failed_rules(table, pk):
            _add(rid)
        for rid in extract_ocr_pipeline_rule_ids(metadata):
            _add(rid)
    return out


def augment_failed_rules_from_metadata(
    failed_rules: list[str], metadata: dict | None, table: Any = None
) -> list[str]:
    """Inclui validar_* e OCR_LAMBDA_* no failed_rules para métricas."""
    if not metadata or metadata.get("STATUS") != "FAILED":
        return list(failed_rules or [])
    out = list(failed_rules or [])
    seen = set(out)
    if table is not None:
        for rid in collect_ocr_failed_rule_ids(table, metadata):
            if rid not in seen:
                seen.add(rid)
                out.append(rid)
    else:
        for rid in extract_ocr_pipeline_rule_ids(metadata):
            if rid not in seen:
                seen.add(rid)
                out.append(rid)
    return out


def should_skip_metrics_update(
    status: str,
    validation_failed_rules: list[str],
    protheus_regras: list[dict[str, str]],
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> tuple[bool, str]:
    """
    Operacional puro → não incrementa falha nem sucesso (ignora processo nas métricas).
    Retorna (skip, motivo).
    """
    catalog = catalog or load_regras_catalog()
    if api_catalog:
        allowed = catalog_regra_ids(catalog) | set(api_catalog.keys())
        protheus_regras = [r for r in protheus_regras if r.get("regra_id") in allowed]
    else:
        protheus_regras = filter_regras_catalog_only(protheus_regras, catalog)
    if status != "FAILED":
        return False, ""

    ocr_rules, op_rules = split_rules_for_metrics(
        validation_failed_rules,
        protheus_regras,
        ocr_pipeline_rules=None,
        catalog=catalog,
        api_catalog=api_catalog,
    )
    if ocr_rules:
        return False, ""
    if op_rules:
        return True, "somente_operacional"

    return False, ""


def merge_failed_rules_for_metrics(
    validation_failed_rules: list[str],
    protheus_regras: list[dict[str, str]],
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
    ocr_pipeline_rules: list[str] | None = None,
) -> list[str]:
    """União validação OCR + pipeline + REGRA_ID tipo OCR (Protheus/API)."""
    classified = classify_regras(protheus_regras, catalog, api_catalog)
    out = list(validation_failed_rules or [])
    for rid in ocr_pipeline_rules or []:
        if rid not in out:
            out.append(rid)
    for r in classified["ocr"]:
        rid = r["regra_id"]
        if rid not in out:
            out.append(rid)
    return out


def coerce_json_field(value: Any) -> dict | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def extract_protheus_regras_from_metadata(metadata: dict) -> list[dict[str, str]]:
    info = coerce_json_field(metadata.get("protheus_request_info"))
    if not info:
        return []
    parsed = parse_protheus_request_failure(info)
    if not parsed:
        return []
    return parsed.get("regras") or extract_regras_from_protheus_body(parsed["body"])


def build_failed_rules_for_metrics(
    table: Any,
    metadata: dict,
    validation_failed_rules: list[str] | None = None,
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> tuple[list[str], list[str], bool, str]:
    """
    Retorna (regras_ocr, regras_operacional, ignorar_processo_nas_metricas, motivo_skip).
    """
    catalog = catalog or load_regras_catalog()
    api_catalog = api_catalog if api_catalog is not None else load_api_regras_catalog()
    status = str(metadata.get("STATUS") or "")
    validation_only = list(validation_failed_rules or [])
    if status != "FAILED":
        return validation_only, [], False, ""

    pipeline_rules = [
        r
        for r in augment_failed_rules_from_metadata(validation_only, metadata, table)
        if r not in validation_only
    ]
    protheus_regras = extract_protheus_regras_from_metadata(metadata)
    skip, motivo = should_skip_metrics_update(
        status, validation_only, protheus_regras, catalog, api_catalog
    )
    ocr_rules, op_rules = split_rules_for_metrics(
        validation_only,
        protheus_regras,
        pipeline_rules,
        catalog,
        api_catalog,
    )
    if skip:
        return [], op_rules, True, motivo
    return ocr_rules, op_rules, False, ""


def protheus_regras_for_metrics_with_api(
    protheus_regras: list[dict[str, str]],
    catalog: dict | None = None,
    api_catalog: dict[str, dict] | None = None,
) -> list[str]:
    classified = classify_regras(protheus_regras, catalog, api_catalog)
    return [r["regra_id"] for r in classified["ocr"]]
