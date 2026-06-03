#!/usr/bin/env python3
"""
Atualiza tabela única de regras (Protheus + API OCR) com contagens do DynamoDB.

Saídas (--output-dir):
  - REGRAS_CONSOLIDADAS.md   tabela principal (atualizada a cada execução)
  - regras_consolidadas.csv
  - regras_contagens_snapshot.json

Uso:
  cd backend/scripts
  set -a && source ../.env.prod && set +a
  python3 sync_regras_metricas.py --output-dir ./out_regras

  # Reprocessar JSONL exportado antes:
  python3 sync_regras_metricas.py --input-jsonl ./out_regras/all_protheus_failures.jsonl --write-jsonl ''

Protheus: só as 17 REGRA_ID do catálogo (sem secundários SD1/SX3).
API: errorCode + message do response_body (ex. CALC_QTDVAL_002); catálogo Excel.
Use --all-failures (ou --error-code '') para contar API além de EXEC_AUTO_002.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

_SCRIPT_DIR = Path(__file__).resolve().parent
_LAMBDAS_DIR = _SCRIPT_DIR.parent / "lambdas"
sys.path.insert(0, str(_LAMBDAS_DIR))

from utils.protheus_regras import (  # noqa: E402
    collect_ocr_failed_rule_ids,
    extract_regras_from_protheus_body,
    filter_regras_catalog_only,
    get_regra_meta,
    is_exec_auto_error,
    load_api_catalog_by_codigo,
    load_regras_catalog,
    parse_protheus_request_failure,
    primary_catalog_regra,
)

def _parse_after_ts(after: str) -> int:
    from datetime import datetime

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return int(datetime.strptime(after.strip(), fmt).timestamp())
        except ValueError:
            continue
    raise SystemExit(f"Não foi possível interpretar --after={after!r}")


def _load_api_rules_from_xlsx(path: Path) -> list[dict]:
    try:
        import openpyxl
    except ImportError:
        print("openpyxl não instalado — pip install openpyxl", file=sys.stderr)
        return []
    if not path.is_file():
        print(f"Excel não encontrado: {path}", file=sys.stderr)
        return []
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []
    header = [str(h or "").strip().lower() for h in rows[0]]
    out = []
    for row in rows[1:]:
        if not row or not row[0]:
            continue
        d = dict(zip(header, row))
        codigo = str(d.get("codigo") or "").strip()
        if not codigo:
            continue
        tipo = str(d.get("tipo") or "").strip()
        out.append(
            {
                "fonte": "API",
                "regra_id": codigo,
                "categoria": "API / Schema",
                "mensagem_resumo": str(d.get("mensagem") or "").replace("\xa0", " ")[:200],
                "tipo": tipo,
                "http": d.get("http"),
            }
        )
    return out


def _failure_body_from_item(item: dict) -> dict | None:
    info = item.get("protheus_request_info")
    if isinstance(info, str):
        try:
            info = json.loads(info)
        except json.JSONDecodeError:
            return None
    parsed = parse_protheus_request_failure(info if isinstance(info, dict) else {})
    return parsed["body"] if parsed else None


def _scan_protheus_records(
    table_name: str,
    after: str,
    error_code: str,
    limit: int,
    only_exec_auto: bool,
) -> list[dict]:
    after_ts = _parse_after_ts(after) if after else None
    table = boto3.resource("dynamodb").Table(table_name)
    filter_expr = Attr("SK").eq("METADATA") & Attr("protheus_request_info").exists()
    if after_ts is not None:
        filter_expr = filter_expr & Attr("TIMESTAMP").gte(after_ts)

    records = []
    scan_kwargs: dict = {"FilterExpression": filter_expr}
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            if limit and len(records) >= limit:
                break
            body = _failure_body_from_item(item)
            if not body:
                continue
            ec = str(body.get("errorCode") or "")
            if only_exec_auto and not is_exec_auto_error(ec):
                continue
            if error_code and ec != error_code:
                continue
            records.append(
                {
                    "process_id": (item.get("PK") or "").replace("PROCESS#", ""),
                    "error_code": ec,
                    "body": body,
                    "regras": extract_regras_from_protheus_body(body),
                }
            )
        if limit and len(records) >= limit:
            break
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek
    return records


def _scan_failed_metadata(
    table_name: str,
    after: str,
    limit: int,
) -> list[dict]:
    """Todos os processos FAILED (validação OCR + pipeline + Protheus)."""
    after_ts = _parse_after_ts(after) if after else None
    table = boto3.resource("dynamodb").Table(table_name)
    filter_expr = Attr("SK").eq("METADATA") & Attr("STATUS").eq("FAILED")
    if after_ts is not None:
        filter_expr = filter_expr & Attr("TIMESTAMP").gte(after_ts)

    records = []
    scan_kwargs: dict = {"FilterExpression": filter_expr}
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            if limit and len(records) >= limit:
                break
            body = _failure_body_from_item(item)
            ec = str((body or {}).get("errorCode") or "")
            records.append(
                {
                    "process_id": (item.get("PK") or "").replace("PROCESS#", ""),
                    "error_code": ec,
                    "body": body,
                    "regras": extract_regras_from_protheus_body(body) if body else [],
                    "ocr_rules": collect_ocr_failed_rule_ids(table, item),
                    "has_protheus": bool(body),
                }
            )
        if limit and len(records) >= limit:
            break
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek
    return records


def _rec_body_from_jsonl(rec: dict) -> dict:
    body = rec.get("response_body")
    if isinstance(body, dict):
        return body
    ec = str(rec.get("error_code") or "")
    return {
        "errorCode": ec,
        "message": rec.get("message"),
        "cause": rec.get("cause") or rec.get("cause_blocks"),
    }


def _load_jsonl(path: Path, error_code: str, only_exec_auto: bool) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ec = str(rec.get("error_code") or "")
            if only_exec_auto and not is_exec_auto_error(ec):
                continue
            if error_code and ec != error_code:
                continue
            body = _rec_body_from_jsonl(rec)
            records.append(
                {
                    "process_id": rec.get("process_id", ""),
                    "error_code": ec or str(body.get("errorCode") or ""),
                    "body": body,
                    "regras": extract_regras_from_protheus_body(body),
                }
            )
    return records


def _split_regras_for_counting(
    rec: dict,
    catalog: dict,
    api_catalog: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    """Protheus (17 catálogo) vs API (errorCode do Excel)."""
    ec = rec.get("error_code", "")
    regras = rec.get("regras") or extract_regras_from_protheus_body(rec.get("body") or {})
    if is_exec_auto_error(ec):
        return filter_regras_catalog_only(regras, catalog), []
    api_ids = set(api_catalog.keys())
    api_regras = [r for r in regras if r.get("regra_id") in api_ids]
    return [], api_regras


def _write_api_catalog_json(api_rules: list[dict], out_path: Path) -> None:
    regras = {
        r["regra_id"]: {
            "tipo": r.get("tipo"),
            "mensagem_resumo": r.get("mensagem_resumo", ""),
            "categoria": r.get("categoria", "API / Schema"),
        }
        for r in api_rules
        if r.get("regra_id")
    }
    out_path.write_text(
        json.dumps({"regras": regras}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _build_ocr_pipeline_rows(
    ocr_processo: Counter,
    ocr_ocorrencias: Counter,
) -> list[dict]:
    rows = []
    for rid in sorted(ocr_ocorrencias.keys(), key=lambda x: (-ocr_ocorrencias[x], x)):
        rows.append(
            {
                "fonte": "OCR Pipeline",
                "categoria": "Pipeline / Lambda",
                "regra_id": rid,
                "qtd_processos": ocr_processo.get(rid, 0),
                "qtd_ocorrencias": ocr_ocorrencias.get(rid, 0),
                "mensagem_resumo": "Falha técnica antes do Protheus (timeout, lambda, etc.)",
                "tipo": "OCR",
                "conta_metricas": "Sim",
            }
        )
    return rows


def _build_ocr_validacao_rows(
    ocr_processo: Counter,
    ocr_ocorrencias: Counter,
) -> list[dict]:
    rows = []
    for rid in sorted(ocr_ocorrencias.keys(), key=lambda x: (-ocr_ocorrencias[x], x)):
        rows.append(
            {
                "fonte": "OCR Validação",
                "categoria": "Regras validar_*",
                "regra_id": rid,
                "qtd_processos": ocr_processo.get(rid, 0),
                "qtd_ocorrencias": ocr_ocorrencias.get(rid, 0),
                "mensagem_resumo": "Falha na validação OCR antes do envio ao Protheus",
                "tipo": "OCR",
                "conta_metricas": "Sim",
            }
        )
    return rows


def _build_table_rows(
    catalog: dict,
    processo_por_regra: Counter,
    ocorrencias_por_regra: Counter,
    api_processo: Counter,
    api_ocorrencias: Counter,
    api_rules: list[dict],
    ocr_val_processo: Counter,
    ocr_val_ocorrencias: Counter,
    ocr_pipe_processo: Counter,
    ocr_pipe_ocorrencias: Counter,
) -> list[dict]:
    regras_cat = catalog.get("regras") or {}
    ordem = catalog.get("ordem_exibicao") or list(regras_cat.keys())
    rows = []
    for regra_id in ordem:
        meta = regras_cat.get(regra_id)
        if not meta:
            continue
        rows.append(
            {
                "fonte": "Protheus",
                "categoria": meta.get("categoria", ""),
                "regra_id": regra_id,
                "qtd_processos": processo_por_regra.get(regra_id, 0),
                "qtd_ocorrencias": ocorrencias_por_regra.get(regra_id, 0),
                "mensagem_resumo": meta.get("mensagem_resumo", ""),
                "tipo": meta.get("tipo", ""),
                "conta_metricas": "Sim" if meta.get("tipo") == "OCR" else "Não (Operacional)",
            }
        )
    for ar in api_rules:
        rid = ar["regra_id"]
        rows.append(
            {
                "fonte": "API",
                "categoria": ar.get("categoria", "API / Schema"),
                "regra_id": rid,
                "qtd_processos": api_processo.get(rid, 0),
                "qtd_ocorrencias": api_ocorrencias.get(rid, 0),
                "mensagem_resumo": ar.get("mensagem_resumo", ""),
                "tipo": ar.get("tipo", ""),
                "conta_metricas": "Sim" if ar.get("tipo") == "OCR" else "Não (Operacional)",
            }
        )
    rows.extend(_build_ocr_validacao_rows(ocr_val_processo, ocr_val_ocorrencias))
    rows.extend(_build_ocr_pipeline_rows(ocr_pipe_processo, ocr_pipe_ocorrencias))
    return rows


def _write_jsonl(records: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            out = {
                "process_id": rec.get("process_id"),
                "error_code": rec.get("error_code"),
                "message": (rec.get("body") or {}).get("message"),
                "body": rec.get("body"),
            }
            f.write(json.dumps(out, ensure_ascii=False) + "\n")


def _write_md(rows: list[dict], out_path: Path, meta: dict) -> None:
    lines = [
        "# Regras consolidadas — AgroAmazonia",
        "",
        f"- **Atualizado em:** {meta['generated_at']}",
        f"- **Fonte scan:** {meta['source']}",
        f"- **Processos analisados:** {meta['processos']}",
        f"- **EXEC_AUTO_002 (catálogo 17):** {meta.get('processos_exec_auto', 0)}",
        f"- **API errorCode (Excel):** {meta.get('processos_api', 0)}",
        f"- **OCR validação (`validar_*`):** {meta.get('processos_ocr_validacao', 0)} processos",
        f"- **OCR pipeline (lambda/timeout):** {meta.get('processos_ocr_pipeline', 0)} processos",
        f"- **Soma ocorrências Protheus (catálogo):** {meta.get('soma_ocorrencias_catalogo', 0)}",
        f"- **Soma ocorrências API (catálogo Excel):** {meta.get('soma_ocorrencias_api', 0)}",
        f"- **Soma ocorrências OCR validação:** {meta.get('soma_ocorrencias_ocr_validacao', 0)}",
        f"- **Soma ocorrências OCR pipeline:** {meta.get('soma_ocorrencias_ocr_pipeline', 0)}",
        "",
        f"- **Regras no catálogo:** {meta.get('total_regras_catalogo', 0)} "
        f"({meta.get('regras_protheus', 0)} Protheus + {meta.get('regras_api', 0)} API)",
        f"- **Regras com ocorrência > 0:** {meta.get('regras_com_contagem', 0)}",
        f"- **Processos sem regra mapeada (errorCode fora do catálogo):** {meta.get('processos_sem_mapeamento', 0)}",
        "",
        "**Protheus:** REGRA_ID do `cause` (17 oficiais). **API:** `response_req.body.errorCode` + `message` (sem `cause`).",
        "",
        "| Fonte | Categoria | REGRA_ID | Processos | Ocorrências | Mensagem (resumo) | Tipo | Conta métricas |",
        "|-------|-----------|----------|----------:|------------:|-------------------|------|----------------|",
    ]
    for r in rows:
        lines.append(
            f"| {r['fonte']} | {r['categoria']} | `{r['regra_id']}` | "
            f"{r['qtd_processos']} | {r['qtd_ocorrencias']} | {r['mensagem_resumo']} | "
            f"{r['tipo']} | {r['conta_metricas']} |"
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Formato da regra (e-mail / JSON)")
    lines.append("")
    lines.append("```text")
    lines.append("EXEC_AUTO: {REGRA_ID}: {mensagem do cause}")
    lines.append("API: {errorCode}: {message}")
    lines.append("```")
    lines.append("")
    lines.append("Exemplo NF 10199:")
    lines.append("")
    lines.append("```text")
    lines.append(
        "DIVERGENCIA_DE_VALOR_TOTAL_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_TOTAL_NO_XML: "
        "Divergência de valor total da nota fiscal digitada com valor total no XML"
    )
    lines.append(
        "DIVERGENCIA_DE_VALOR_ICMS_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_ICMS_NO_XML: "
        "Divergência de valor ICMS da nota fiscal digitada com valor ICMS no XML"
    )
    lines.append("```")
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincroniza tabela única de regras com contagens PRD")
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME"))
    parser.add_argument("--input-jsonl", default="")
    parser.add_argument("--output-dir", default="out_regras")
    parser.add_argument("--after", default="")
    parser.add_argument(
        "--error-code",
        default="",
        help="Filtrar um errorCode (ex. EXEC_AUTO_002); vazio = todos",
    )
    parser.add_argument(
        "--exec-auto-only",
        action="store_true",
        help="Só EXEC_AUTO_002 / cause (não contar errorCode API do Excel)",
    )
    parser.add_argument(
        "--write-jsonl",
        default="",
        help="Exportar falhas brutas (process_id, error_code, body) para reprocessar offline",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--catalog", default="", help="protheus_regras_catalog.json")
    parser.add_argument(
        "--xlsx",
        default="/home/user/Downloads/Erros OCR.xlsx",
        help="Erros OCR.xlsx — regras API (use '' para omitir)",
    )
    args = parser.parse_args()

    catalog_path = Path(args.catalog) if args.catalog else _LAMBDAS_DIR / "utils" / "protheus_regras_catalog.json"
    catalog = load_regras_catalog(catalog_path)
    all_failures = not args.exec_auto_only
    only_exec_auto = not all_failures
    error_filter = (args.error_code or "").strip()
    if args.exec_auto_only and not error_filter:
        error_filter = "EXEC_AUTO_002"

    xlsx_path = Path(args.xlsx) if args.xlsx else None
    api_rules = _load_api_rules_from_xlsx(xlsx_path) if xlsx_path else []
    api_catalog = load_api_catalog_by_codigo(api_rules)
    if api_rules:
        _write_api_catalog_json(api_rules, _LAMBDAS_DIR / "utils" / "api_regras_catalog.json")

    if args.input_jsonl:
        records = _load_jsonl(Path(args.input_jsonl), error_filter, only_exec_auto)
        source = f"jsonl:{args.input_jsonl}"
    else:
        if not args.table_name:
            print("Defina TABLE_NAME ou --input-jsonl", file=sys.stderr)
            sys.exit(1)
        if only_exec_auto:
            records = _scan_protheus_records(
                args.table_name,
                args.after,
                error_filter,
                args.limit,
                only_exec_auto,
            )
            for rec in records:
                rec["ocr_rules"] = []
                rec["has_protheus"] = True
        else:
            records = _scan_failed_metadata(args.table_name, args.after, args.limit)
        source = f"dynamodb:{args.table_name}"

    processo_por_regra: Counter = Counter()
    ocorrencias_por_regra: Counter = Counter()
    api_processo: Counter = Counter()
    api_ocorrencias: Counter = Counter()
    por_tipo_processos: Counter = Counter()
    processos_com_catalogo = 0
    processos_exec_auto = 0
    processos_api = 0
    processos_sem_mapeamento = 0
    processos_ocr_validacao = 0
    processos_ocr_pipeline = 0
    ocr_val_processo: Counter = Counter()
    ocr_val_ocorrencias: Counter = Counter()
    ocr_pipe_processo: Counter = Counter()
    ocr_pipe_ocorrencias: Counter = Counter()
    error_codes_fora_catalogo: Counter = Counter()

    for rec in records:
        protheus_regras: list[dict] = []
        api_regras: list[dict] = []
        if rec.get("has_protheus") and rec.get("body"):
            ec = rec.get("error_code") or ""
            count_protheus = True
            if only_exec_auto and not is_exec_auto_error(ec):
                count_protheus = False
            if error_filter and ec != error_filter:
                count_protheus = False
            if count_protheus:
                protheus_regras, api_regras = _split_regras_for_counting(
                    rec, catalog, api_catalog
                )
        if protheus_regras:
            processos_com_catalogo += 1
            processos_exec_auto += 1
        if api_regras:
            processos_api += 1
        if not protheus_regras and not api_regras and rec.get("has_protheus"):
            ec = rec.get("error_code") or ""
            if ec:
                processos_sem_mapeamento += 1
                error_codes_fora_catalogo[ec] += 1

        ocr_rules = rec.get("ocr_rules") or []
        if ocr_rules:
            seen_val = set()
            seen_pipe = set()
            has_val = False
            has_pipe = False
            for rid in ocr_rules:
                if rid.startswith("validar_"):
                    has_val = True
                    ocr_val_ocorrencias[rid] += 1
                    if rid not in seen_val:
                        ocr_val_processo[rid] += 1
                        seen_val.add(rid)
                elif rid.startswith("OCR_"):
                    has_pipe = True
                    ocr_pipe_ocorrencias[rid] += 1
                    if rid not in seen_pipe:
                        ocr_pipe_processo[rid] += 1
                        seen_pipe.add(rid)
            if has_val:
                processos_ocr_validacao += 1
            if has_pipe:
                processos_ocr_pipeline += 1
        seen_pt = set()
        for r in protheus_regras:
            rid = r["regra_id"]
            ocorrencias_por_regra[rid] += 1
            if rid not in seen_pt:
                processo_por_regra[rid] += 1
                seen_pt.add(rid)
        seen_api = set()
        for r in api_regras:
            rid = r["regra_id"]
            api_ocorrencias[rid] += 1
            if rid not in seen_api:
                api_processo[rid] += 1
                seen_api.add(rid)
        todas = rec.get("regras") or []
        primary = primary_catalog_regra(
            protheus_regras if protheus_regras else todas, catalog
        )
        if primary:
            tipo = get_regra_meta(primary["regra_id"], catalog).get("tipo") or "—"
            por_tipo_processos[tipo] += 1
        elif api_regras:
            rid = api_regras[0]["regra_id"]
            tipo = api_catalog.get(rid, {}).get("tipo") or "—"
            por_tipo_processos[f"API:{tipo}"] += 1

    rows = _build_table_rows(
        catalog,
        processo_por_regra,
        ocorrencias_por_regra,
        api_processo,
        api_ocorrencias,
        api_rules,
        ocr_val_processo,
        ocr_val_ocorrencias,
        ocr_pipe_processo,
        ocr_pipe_ocorrencias,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    regras_com_contagem = sum(1 for r in rows if r["qtd_ocorrencias"] > 0)
    total_catalogo = len(catalog.get("regras") or {}) + len(api_rules)

    if args.write_jsonl:
        jl = Path(args.write_jsonl)
        if not jl.is_absolute():
            jl = out_dir / jl
        _write_jsonl(records, jl)

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "all_failures": all_failures,
        "error_code_filter": error_filter or None,
        "total_regras_catalogo": total_catalogo,
        "regras_com_contagem": regras_com_contagem,
        "processos_sem_mapeamento": processos_sem_mapeamento,
        "error_codes_fora_catalogo": dict(error_codes_fora_catalogo.most_common(20)),
        "processos_failed_total": len(records),
        "processos": len(records),
        "processos_exec_auto": processos_exec_auto,
        "processos_api": processos_api,
        "processos_ocr_validacao": processos_ocr_validacao,
        "processos_ocr_pipeline": processos_ocr_pipeline,
        "processos_com_regra_catalogo": processos_com_catalogo,
        "soma_ocorrencias_catalogo": sum(ocorrencias_por_regra.values()),
        "soma_ocorrencias_api": sum(api_ocorrencias.values()),
        "soma_ocorrencias_ocr_validacao": sum(ocr_val_ocorrencias.values()),
        "soma_ocorrencias_ocr_pipeline": sum(ocr_pipe_ocorrencias.values()),
        "ocorrencias_ocr_validacao": dict(ocr_val_ocorrencias),
        "ocorrencias_ocr_pipeline": dict(ocr_pipe_ocorrencias),
        "regras_protheus": len(catalog.get("regras") or {}),
        "regras_api": len(api_rules),
        "por_tipo_primario": dict(por_tipo_processos),
        "ocorrencias_api": dict(api_ocorrencias),
    }

    snapshot = {
        **meta,
        "rows": rows,
        "ocorrencias_protheus": {
            k: ocorrencias_por_regra[k]
            for k in catalog.get("ordem_exibicao", [])
            if ocorrencias_por_regra.get(k)
        },
    }
    (out_dir / "regras_contagens_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    csv_path = out_dir / "regras_consolidadas.csv"
    fields = [
        "fonte",
        "categoria",
        "regra_id",
        "qtd_processos",
        "qtd_ocorrencias",
        "mensagem_resumo",
        "tipo",
        "conta_metricas",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    md_path = out_dir / "REGRAS_CONSOLIDADAS.md"
    _write_md(rows, md_path, meta)

    print(json.dumps(meta, indent=2, ensure_ascii=False))
    print(f"\nArquivos em {out_dir.resolve()}:")
    print(f"  - {md_path.name}")
    print(f"  - {csv_path.name}")
    print(f"  - regras_contagens_snapshot.json")
    if args.write_jsonl:
        print(f"  - {Path(args.write_jsonl).name}")


if __name__ == "__main__":
    main()
