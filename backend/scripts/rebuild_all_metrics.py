#!/usr/bin/env python3
"""
Recalcula regras + métricas do dashboard para TODOS os processos no DynamoDB.

Gera em --output-dir (padrão: ./out_regras):
  - regras_consolidadas.*     (tabela de regras, igual sync_regras_metricas)
  - metrics_preview.json      (métricas diárias/mensais recalculadas)
  - METRICAS_PREVIEW.md       (resumo legível + comparação com METRICS# atual)

Uso:
  cd backend/scripts
  export TABLE_NAME=tabela-document-processor-prd
  export AWS_REGION=sa-east-1

  # Só preview (não grava METRICS# no DynamoDB)
  python3 rebuild_all_metrics.py --xlsx "/caminho/Erros OCR.xlsx"

  # Gravar métricas no DynamoDB (substitui METRICS# por dia/mês)
  python3 rebuild_all_metrics.py --apply --xlsx "/caminho/Erros OCR.xlsx"
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

_SCRIPT_DIR = Path(__file__).resolve().parent
_LAMBDAS_DIR = _SCRIPT_DIR.parent / "lambdas"
sys.path.insert(0, str(_LAMBDAS_DIR))

from utils.protheus_regras import (  # noqa: E402
    build_failed_rules_for_metrics,
    fetch_latest_validation_failed_rules,
    load_api_regras_catalog,
    load_regras_catalog,
)
from update_metrics.handler import (  # noqa: E402
    _build_failure_keys,
    _error_tag_from_failure_key,
    _extract_failure_identity,
    protheus_response_indicates_prenota,
)
from utils.metrics_process import effective_metrics_status_from_metadata  # noqa: E402
from utils.metrics_rates import success_rate_pct  # noqa: E402


def _json_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Reuso da agregação de regras
_SYNC = _SCRIPT_DIR / "sync_regras_metricas.py"


def _parse_after_ts(after: str) -> int:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return int(datetime.strptime(after.strip(), fmt).timestamp())
        except ValueError:
            continue
    raise SystemExit(f"Não foi possível interpretar --after={after!r}")


def _determine_metric_status(metadata: dict) -> str | None:
    """Último STATUS do processo; METRICS_STATUS só se ainda em PROCESSING/CREATED."""
    return effective_metrics_status_from_metadata(metadata)


def _date_key_from_metadata(metadata: dict) -> tuple[str, int]:
    metrics_date = metadata.get("METRICS_DATE")
    timestamp = metadata.get("METRICS_UPDATED_AT") or metadata.get("TIMESTAMP", 0)
    if metrics_date and len(str(metrics_date)) >= 10:
        date_key = str(metrics_date)[:10]
    elif timestamp:
        try:
            dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            date_key = dt.strftime("%Y-%m-%d")
        except (TypeError, ValueError, OSError):
            date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        hour = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).hour if timestamp else 0
    except (TypeError, ValueError, OSError):
        hour = 0
    return date_key, hour


def _processing_time(metadata: dict) -> float:
    saved = metadata.get("METRICS_PROCESSING_TIME")
    if saved is not None:
        try:
            return round(float(saved), 2)
        except (TypeError, ValueError):
            pass
    start = metadata.get("START_TIME")
    end = metadata.get("updated_at") or metadata.get("UPDATED_AT")
    if not start:
        return 30.0
    try:
        if isinstance(start, str) and "T" in start:
            s = start[:-1] + "+00:00" if start.endswith("Z") else start
            if "+" not in s and s.count("-") < 3:
                s += "+00:00"
            t0 = datetime.fromisoformat(s)
        else:
            t0 = datetime.fromtimestamp(float(start), tz=timezone.utc)
        if end and isinstance(end, str) and "T" in end:
            e = end[:-1] + "+00:00" if end.endswith("Z") else end
            if "+" not in e and e.count("-") < 3:
                e += "+00:00"
            t1 = datetime.fromisoformat(e)
        else:
            t1 = datetime.now(timezone.utc)
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=timezone.utc)
        if t1.tzinfo is None:
            t1 = t1.replace(tzinfo=timezone.utc)
        sec = (t1 - t0).total_seconds()
        if sec < 0 or sec > 86400:
            return 30.0
        return round(sec, 2)
    except Exception:
        return 30.0


def _scan_all_metadata(table, after: str = "", limit: int = 0) -> list[dict]:
    after_ts = _parse_after_ts(after) if after else None
    filter_expr = Attr("SK").eq("METADATA")
    if after_ts is not None:
        filter_expr = filter_expr & Attr("TIMESTAMP").gte(after_ts)
    items: list[dict] = []
    kwargs: dict = {"FilterExpression": filter_expr}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if limit and len(items) >= limit:
            return items[:limit]
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def _load_existing_metrics(table) -> dict[str, dict]:
    out: dict[str, dict] = {}
    kwargs = {
        "FilterExpression": Attr("PK").begins_with("METRICS#") & Attr("SK").eq("SUMMARY")
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            date = (item.get("PK") or "").replace("METRICS#", "")
            if date and len(date) == 10:
                out[date] = item
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return out


def _aggregate_metrics(table, items: list[dict], catalog: dict, api_catalog: dict) -> dict:
    daily: dict = defaultdict(
        lambda: {
            "total_count": 0,
            "success_count": 0,
            "success_prenota_count": 0,
            "failed_count": 0,
            "skipped_operacional": 0,
            "total_time": 0.0,
            "processes_by_hour": Counter(),
            "processes_by_type": Counter(),
            "failed_rules": Counter(),
            "failed_rules_operacional": Counter(),
            "failure_reasons": Counter(),
            "failure_dedup_registry": {},
        }
    )
    monthly: dict = defaultdict(
        lambda: {
            "total_count": 0,
            "success_count": 0,
            "success_prenota_count": 0,
            "failed_count": 0,
            "skipped_operacional": 0,
            "total_time": 0.0,
            "processes_by_type": Counter(),
        }
    )
    global_rules = Counter()
    stats = Counter()
    process_dedup_meta: dict[str, dict] = {}

    for metadata in items:
        metric_status = _determine_metric_status(metadata)
        if not metric_status:
            stats["ignorados_status"] += 1
            continue

        date_key, hour = _date_key_from_metadata(metadata)
        month_key = date_key[:7]
        process_type = metadata.get("PROCESS_TYPE") or "UNKNOWN"
        proc_time = _processing_time(metadata)
        pk = metadata.get("PK") or ""

        d = daily[date_key]
        m = monthly[month_key]
        d["total_count"] += 1
        m["total_count"] += 1
        d["total_time"] += proc_time
        m["total_time"] += proc_time
        d["processes_by_hour"][str(hour)] += 1
        d["processes_by_type"][process_type] += 1
        m["processes_by_type"][process_type] += 1

        if metric_status == "SUCCESS":
            d["success_count"] += 1
            m["success_count"] += 1
            stats["success"] += 1
            if protheus_response_indicates_prenota({}, metadata):
                d["success_prenota_count"] += 1
                m["success_prenota_count"] += 1
                stats["prenota"] += 1
            continue

        validation_rules = (
            fetch_latest_validation_failed_rules(table, pk) if pk else []
        )
        ocr_rules, op_rules, skip, skip_reason = build_failed_rules_for_metrics(
            table,
            metadata,
            validation_failed_rules=validation_rules,
            catalog=catalog,
            api_catalog=api_catalog,
        )

        if skip:
            d["skipped_operacional"] += 1
            m["skipped_operacional"] += 1
            stats["skipped_operacional"] += 1
            stats[f"skip:{skip_reason}"] += 1
            for rid in op_rules:
                d["failed_rules_operacional"][rid] += 1
            continue

        stats["failed"] += 1

        if metadata.get("error_info"):
            d["failure_reasons"]["LAMBDA_ERROR"] += 1
        elif validation_rules:
            d["failure_reasons"]["VALIDATION_FAILED"] += 1
        elif metadata.get("protheus_request_info"):
            d["failure_reasons"]["PROTHEUS_FAILED"] += 1
        else:
            d["failure_reasons"]["PROCESSING_ERROR"] += 1

        process_id = (pk or "").replace("PROCESS#", "")
        nf, cnpj, pedido = _extract_failure_identity(process_id, metadata)
        failure_keys = _build_failure_keys(nf, cnpj, pedido, ocr_rules, "FAILED")
        registry = d["failure_dedup_registry"]
        new_keys = 0
        primary_process_id = None
        for key in failure_keys:
            if key in registry:
                if registry[key] != (process_id or "unknown_process"):
                    primary_process_id = primary_process_id or registry[key]
                continue
            registry[key] = process_id or "unknown_process"
            new_keys += 1
            error_tag = _error_tag_from_failure_key(key)
            d["failed_rules"][error_tag] += 1
            global_rules[error_tag] += 1
        if new_keys:
            d["failed_count"] += new_keys
            m["failed_count"] += new_keys
        if failure_keys and process_id:
            process_dedup_meta[process_id] = {
                "role": "primary" if new_keys else "duplicate",
                "primary": primary_process_id,
            }

        for rid in op_rules:
            d["failed_rules_operacional"][rid] += 1

    def _serialize_daily() -> dict:
        out = {}
        for dk in sorted(daily.keys()):
            x = daily[dk]
            total = x["total_count"]
            success = x["success_count"]
            failed = x["failed_count"]
            prenota = x["success_prenota_count"]
            out[dk] = {
                "total_count": total,
                "success_count": success,
                "success_prenota_count": prenota,
                "success_classified_count": max(0, success - prenota),
                "failed_count": failed,
                "skipped_operacional": x["skipped_operacional"],
                "success_rate_pct": success_rate_pct(success, failed),
                "total_time_sec": round(x["total_time"], 2),
                "avg_time_sec": round(x["total_time"] / total, 2) if total else 0,
                "processes_by_hour": dict(x["processes_by_hour"]),
                "processes_by_type": dict(x["processes_by_type"]),
                "failed_rules": dict(
                    sorted(x["failed_rules"].items(), key=lambda i: (-i[1], i[0]))
                ),
                "failed_rules_operacional": dict(
                    sorted(x["failed_rules_operacional"].items(), key=lambda i: (-i[1], i[0]))
                ),
                "failure_reasons": dict(x["failure_reasons"]),
                "failure_dedup_registry": dict(x["failure_dedup_registry"]),
            }
        return out

    def _serialize_monthly() -> dict:
        out = {}
        for mk in sorted(monthly.keys()):
            x = monthly[mk]
            total = x["total_count"]
            success = x["success_count"]
            failed = x["failed_count"]
            prenota = x["success_prenota_count"]
            out[mk] = {
                "total_count": total,
                "success_count": success,
                "success_prenota_count": prenota,
                "failed_count": failed,
                "skipped_operacional": x["skipped_operacional"],
                "success_rate_pct": success_rate_pct(success, failed),
                "total_time_sec": round(x["total_time"], 2),
                "processes_by_type": dict(x["processes_by_type"]),
            }
        return out

    grand_total = sum(d["total_count"] for d in daily.values())
    grand_success = sum(d["success_count"] for d in daily.values())
    grand_failed = sum(d["failed_count"] for d in daily.values())
    grand_skipped = sum(d["skipped_operacional"] for d in daily.values())
    grand_prenota = sum(d["success_prenota_count"] for d in daily.values())
    sum_rules = sum(global_rules.values())

    return {
        "stats": dict(stats),
        "totals": {
            "processos_contabilizados": grand_total,
            "sucesso": grand_success,
            "prenotas": grand_prenota,
            "classificados": max(0, grand_success - grand_prenota),
            "falha": grand_failed,
            "ignorados_operacional": grand_skipped,
            "soma_failed_rules": sum_rules,
            "taxa_sucesso_pct": success_rate_pct(grand_success, grand_failed),
        },
        "failed_rules_global": dict(
            sorted(global_rules.items(), key=lambda i: (-i[1], i[0]))
        ),
        "daily": _serialize_daily(),
        "monthly": _serialize_monthly(),
        "process_dedup_meta": process_dedup_meta,
    }


def _write_metrics_md(
    path: Path,
    preview: dict,
    existing: dict[str, dict],
    table_name: str,
) -> None:
    lines = [
        "# Preview das métricas (recalculadas)",
        "",
        f"- **Tabela:** `{table_name}`",
        f"- **Gerado em:** {preview.get('generated_at', '')}",
        "",
        "## Totais (como no dashboard)",
        "",
    ]
    t = preview["totals"]
    lines.extend(
        [
            f"| Métrica | Valor |",
            f"|---------|------:|",
            f"| Processos contabilizados | {t['processos_contabilizados']} |",
            f"| Sucesso | {t['sucesso']} |",
            f"| Pré-notas (`success_prenota_count`) | {t.get('prenotas', 0)} |",
            f"| Classificados (sucesso − pré-nota) | {t.get('classificados', 0)} |",
            f"| Falha (conta no `failed_count`) | {t['falha']} |",
            f"| Ignorados (só Operacional Protheus) | {t['ignorados_operacional']} |",
            f"| Taxa de sucesso | {t['taxa_sucesso_pct']}% |",
            f"| Soma `failed_rules` (pode ser > falhas) | {t['soma_failed_rules']} |",
            "",
            "> **Nota:** `failed_count` = processos FAILED que entram nas métricas. "
            "`failed_rules` soma cada regra (várias por processo).",
            "",
            "## Top regras (`failed_rules`) — global",
            "",
            "| REGRA_ID | Ocorrências |",
            "|----------|------------:|",
        ]
    )
    for rid, cnt in list(preview.get("failed_rules_global", {}).items())[:40]:
        lines.append(f"| `{rid}` | {cnt} |")

    lines.extend(["", "## Por dia (recalculado)", ""])
    for dk, d in preview.get("daily", {}).items():
        lines.append(f"### {dk}")
        lines.append(
            f"- Total **{d['total_count']}** · Sucesso **{d['success_count']}** · "
            f"Pré-notas **{d.get('success_prenota_count', 0)}** · "
            f"Falha **{d['failed_count']}** · Ignorados operacional **{d['skipped_operacional']}** · "
            f"Taxa **{d['success_rate_pct']}%**"
        )
        if d.get("failed_rules"):
            top = list(d["failed_rules"].items())[:8]
            lines.append(f"- Regras OCR: {', '.join(f'`{k}` ({v})' for k, v in top)}")
        if d.get("failed_rules_operacional"):
            top_op = list(d["failed_rules_operacional"].items())[:8]
            lines.append(
                f"- Regras processo: {', '.join(f'`{k}` ({v})' for k, v in top_op)}"
            )
        lines.append("")

    if existing:
        lines.extend(["## Comparação com METRICS# atual no DynamoDB", ""])
        lines.append("| Data | failed_count (atual) | failed_count (novo) | Δ |")
        lines.append("|------|---------------------:|--------------------:|--:|")
        all_dates = sorted(set(existing.keys()) | set(preview.get("daily", {}).keys()))
        for dk in all_dates:
            old_f = int((existing.get(dk) or {}).get("failed_count", 0) or 0)
            new_f = int(preview.get("daily", {}).get(dk, {}).get("failed_count", 0) or 0)
            delta = new_f - old_f
            sign = f"+{delta}" if delta > 0 else str(delta)
            lines.append(f"| {dk} | {old_f} | {new_f} | {sign} |")

    path.write_text("\n".join(lines), encoding="utf-8")


def _failure_error_type(metadata: dict, validation_rules: list[str]) -> str | None:
    if metadata.get("error_info") or metadata.get("ERROR_INFO"):
        return "LAMBDA_ERROR"
    if validation_rules:
        return "VALIDATION_FAILED"
    if metadata.get("protheus_request_info"):
        return "PROTHEUS_FAILED"
    return "PROCESSING_ERROR"


def _backfill_metadata_metrics(
    table,
    items: list[dict],
    catalog: dict,
    api_catalog: dict,
    process_dedup_meta: dict | None = None,
) -> int:
    """Atualiza METRICS_FAILURE_KEYS / operacional no METADATA (alinhado ao update_metrics)."""
    updated = 0
    for metadata in items:
        metric_status = _determine_metric_status(metadata)
        if not metric_status:
            continue
        pk = metadata.get("PK") or ""
        sk = metadata.get("SK") or "METADATA"
        if not pk.startswith("PROCESS#"):
            continue
        process_id = pk.replace("PROCESS#", "")
        date_key, _ = _date_key_from_metadata(metadata)
        proc_time = _processing_time(metadata)
        is_prenota = metric_status == "SUCCESS" and protheus_response_indicates_prenota({}, metadata)

        if metric_status == "SUCCESS":
            expr = (
                "SET METRICS_STATUS = :st, METRICS_DATE = :dt, METRICS_FAILED_RULES = :rules, "
                "METRICS_FAILURE_KEYS = :fkeys, METRICS_PROCESSING_TIME = :pt, "
                "METRICS_IS_PRENOTA = :pn, METRICS_OPERACIONAL_RULES = :op "
                "REMOVE METRICS_FAILURE_ERROR_TYPE, METRICS_FAILURE_DEDUP_ROLE, "
                "METRICS_FAILURE_DEDUP_PRIMARY"
            )
            values = {
                ":st": "SUCCESS",
                ":dt": date_key,
                ":rules": json.dumps([]),
                ":fkeys": json.dumps([]),
                ":pt": Decimal(str(proc_time)),
                ":pn": is_prenota,
                ":op": json.dumps([]),
            }
        else:
            validation_rules = (
                fetch_latest_validation_failed_rules(table, pk) if pk else []
            )
            ocr_rules, op_rules, skip, _ = build_failed_rules_for_metrics(
                table,
                metadata,
                validation_failed_rules=validation_rules,
                catalog=catalog,
                api_catalog=api_catalog,
            )
            process_type = metadata.get("PROCESS_TYPE") or "UNKNOWN"
            nf, cnpj, pedido = _extract_failure_identity(process_id, metadata)
            failure_keys = (
                []
                if skip
                else _build_failure_keys(nf, cnpj, pedido, ocr_rules, "FAILED")
            )
            err_type = "OPERACIONAL_SKIP" if skip else _failure_error_type(metadata, validation_rules)
            dedup = (process_dedup_meta or {}).get(process_id) or {}
            expr = (
                "SET METRICS_STATUS = :st, METRICS_DATE = :dt, METRICS_FAILED_RULES = :rules, "
                "METRICS_FAILURE_KEYS = :fkeys, METRICS_PROCESSING_TIME = :pt, "
                "METRICS_IS_PRENOTA = :pn, METRICS_OPERACIONAL_RULES = :op, "
                "METRICS_FAILURE_ERROR_TYPE = :et"
            )
            values = {
                ":st": "FAILED",
                ":dt": date_key,
                ":rules": json.dumps(ocr_rules if not skip else []),
                ":fkeys": json.dumps(failure_keys),
                ":pt": Decimal(str(proc_time)),
                ":pn": False,
                ":op": json.dumps(op_rules),
                ":et": err_type,
            }
            if not skip and dedup.get("role"):
                expr += ", METRICS_FAILURE_DEDUP_ROLE = :dedup_role"
                values[":dedup_role"] = dedup["role"]
                if dedup.get("primary"):
                    expr += ", METRICS_FAILURE_DEDUP_PRIMARY = :dedup_primary"
                    values[":dedup_primary"] = dedup["primary"]
            elif skip:
                expr += " REMOVE METRICS_FAILURE_DEDUP_ROLE, METRICS_FAILURE_DEDUP_PRIMARY"
        try:
            table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression=expr,
                ExpressionAttributeValues=values,
            )
            updated += 1
        except Exception as e:
            print(f"  [backfill] Erro {pk}: {e}")
    return updated


def _apply_metrics(
    table,
    preview: dict,
    existing: dict[str, dict],
    dry_run: bool,
    *,
    only_date: str | None = None,
) -> None:
    if dry_run:
        return
    daily = preview.get("daily", {})
    only_months = {only_date[:7]} if only_date else None

    for date_key, d in daily.items():
        if only_date and date_key != only_date:
            continue
        item = {
            "PK": f"METRICS#{date_key}",
            "SK": "SUMMARY",
            "total_count": d["total_count"],
            "success_count": d["success_count"],
            "success_prenota_count": d.get("success_prenota_count", 0),
            "failed_count": d["failed_count"],
            "total_time": Decimal(str(d["total_time_sec"])),
            "processes_by_hour": d.get("processes_by_hour", {}),
            "processes_by_type": d.get("processes_by_type", {}),
            "failed_rules": d.get("failed_rules", {}),
            "failed_rules_operacional": d.get("failed_rules_operacional", {}),
            "failure_reasons": d.get("failure_reasons", {}),
            "skipped_operacional": d.get("skipped_operacional", 0),
            "failure_dedup_registry": d.get("failure_dedup_registry", {}),
        }
        table.put_item(Item=item)

    if not only_date:
        for date in set(existing.keys()) - set(daily.keys()):
            table.delete_item(Key={"PK": f"METRICS#{date}", "SK": "SUMMARY"})

    monthly = preview.get("monthly", {})
    for month_key, m in monthly.items():
        if only_months and month_key not in only_months:
            continue
        table.put_item(
            Item={
                "PK": f"METRICS#{month_key}",
                "SK": "MONTHLY_SUMMARY",
                "total_count": m["total_count"],
                "success_count": m["success_count"],
                "success_prenota_count": m.get("success_prenota_count", 0),
                "failed_count": m["failed_count"],
                "skipped_operacional": m.get("skipped_operacional", 0),
                "total_time": Decimal(str(m["total_time_sec"])),
                "processes_by_type": m.get("processes_by_type", {}),
            }
        )


def _run_sync_regras(output_dir: Path, table_name: str, xlsx: str) -> None:
    cmd = [
        sys.executable,
        str(_SYNC),
        "--table-name",
        table_name,
        "--output-dir",
        str(output_dir),
    ]
    if xlsx:
        cmd.extend(["--xlsx", xlsx])
    subprocess.run(cmd, check=True, cwd=str(_SCRIPT_DIR))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recalcula regras + métricas para todos os processos"
    )
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "sa-east-1"))
    parser.add_argument("--output-dir", default="out_regras")
    parser.add_argument("--after", default="", help="Filtrar processos após data")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--xlsx",
        default="/home/user/Downloads/Erros OCR.xlsx",
        help="Excel Erros OCR (use '' para pular sync de regras)",
    )
    parser.add_argument(
        "--skip-regras",
        action="store_true",
        help="Não rodar sync_regras_metricas (só métricas)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Gravar METRICS# no DynamoDB (sem isso = só preview)",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Recalcula e (com --apply) grava só este dia (YYYY-MM-DD). Padrão vazio = todos os dias.",
    )
    parser.add_argument(
        "--timezone",
        default="America/Sao_Paulo",
        help="Usado com --date=today para definir o dia local",
    )
    args = parser.parse_args()

    only_date: str | None = None
    if args.date:
        if args.date.strip().lower() == "today":
            try:
                from zoneinfo import ZoneInfo

                tz = ZoneInfo(args.timezone)
            except Exception:
                tz = timezone.utc
            only_date = datetime.now(tz).strftime("%Y-%m-%d")
        else:
            only_date = args.date.strip()[:10]

    if not args.table_name:
        print("Defina TABLE_NAME ou --table-name", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table_name)
    catalog = load_regras_catalog()
    api_catalog = load_api_regras_catalog()

    print(f"\n{'='*72}")
    print(f"  REBUILD ALL METRICS — {args.table_name}")
    print(f"  Modo: {'APLICAR no DynamoDB' if args.apply else 'PREVIEW (sem gravar)'}")
    if only_date:
        print(f"  Escopo: apenas o dia {only_date} (+ mês {only_date[:7]} no MONTHLY_SUMMARY)")
        print(
            "  AVISO: gráficos de período / últimos 7 dias só mudam após rebuild SEM --date."
        )
    print(f"{'='*72}\n")

    if not args.skip_regras:
        print("1/3 — Tabela de regras (sync_regras_metricas)...")
        _run_sync_regras(out_dir, args.table_name, args.xlsx)
    else:
        print("1/3 — Sync de regras ignorado (--skip-regras)")

    print("\n2/3 — Lendo todos os processos (METADATA)...")
    items = _scan_all_metadata(table, args.after, args.limit)
    print(f"     {len(items)} registros METADATA")

    print("\n3/3 — Recalculando métricas (mesma lógica do update_metrics)...")
    preview_body = _aggregate_metrics(table, items, catalog, api_catalog)
    preview_body["generated_at"] = datetime.now(timezone.utc).isoformat()
    preview_body["table_name"] = args.table_name

    existing = _load_existing_metrics(table)
    cmp_dates = sorted(set(existing.keys()) | set(preview_body.get("daily", {}).keys()))
    if only_date:
        cmp_dates = [only_date] if only_date in cmp_dates or only_date in preview_body.get("daily", {}) else [only_date]
    preview_body["comparison"] = {
        dk: {
            "failed_count_atual": int((existing.get(dk) or {}).get("failed_count", 0) or 0),
            "failed_count_novo": preview_body["daily"].get(dk, {}).get("failed_count", 0),
            "success_count_atual": int((existing.get(dk) or {}).get("success_count", 0) or 0),
            "success_count_novo": preview_body["daily"].get(dk, {}).get("success_count", 0),
            "failure_reasons_atual": (existing.get(dk) or {}).get("failure_reasons", {}),
            "failure_reasons_novo": preview_body["daily"].get(dk, {}).get("failure_reasons", {}),
        }
        for dk in cmp_dates
    }
    preview_body["only_date"] = only_date

    json_path = out_dir / "metrics_preview.json"
    json_path.write_text(
        json.dumps(preview_body, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8"
    )
    md_path = out_dir / "METRICAS_PREVIEW.md"
    _write_metrics_md(md_path, preview_body, existing, args.table_name)

    t = preview_body["totals"]
    print(f"\n{'─'*72}")
    print("  RESUMO MÉTRICAS (preview)" + (f" — dia {only_date}" if only_date else " — global"))
    print(f"{'─'*72}")
    if only_date and only_date in preview_body.get("daily", {}):
        d = preview_body["daily"][only_date]
        ex = existing.get(only_date) or {}
        print(f"  Dia {only_date}:")
        print(f"    total_count   novo={d['total_count']}  atual={ex.get('total_count', '?')}")
        print(f"    success_count novo={d['success_count']}  atual={ex.get('success_count', '?')}")
        print(f"    failed_count  novo={d['failed_count']}  atual={ex.get('failed_count', '?')}")
        print(f"    prenota       novo={d.get('success_prenota_count', 0)}")
        print(f"    taxa          novo={d['success_rate_pct']}%")
        print(f"    failure_reasons novo={d.get('failure_reasons', {})}")
        print(f"    failure_reasons atual={ex.get('failure_reasons', {})}")
        print(f"    skipped_operacional novo={d.get('skipped_operacional', 0)}  atual={ex.get('skipped_operacional', '?')}")
        top = list(d.get("failed_rules", {}).items())[:6]
        if top:
            print(f"    top failed_rules (OCR): {top}")
        top_op = list(d.get("failed_rules_operacional", {}).items())[:6]
        if top_op:
            print(f"    top failed_rules_operacional: {top_op}")
    print(f"  Processos contabilizados : {t['processos_contabilizados']}")
    print(f"  Sucesso                  : {t['sucesso']}")
    print(f"  Pré-notas                : {t.get('prenotas', 0)}")
    print(f"  Classificados            : {t.get('classificados', 0)}")
    print(f"  Falha (failed_count)     : {t['falha']}")
    print(f"  Ignorados operacional    : {t['ignorados_operacional']}")
    print(f"  Taxa sucesso             : {t['taxa_sucesso_pct']}%")
    print(f"  Soma failed_rules        : {t['soma_failed_rules']}")
    print(f"\n  Top 10 regras:")
    for rid, cnt in list(preview_body.get("failed_rules_global", {}).items())[:10]:
        print(f"    {cnt:4d}  {rid}")

    print(f"\n  Arquivos:")
    print(f"    {json_path.resolve()}")
    print(f"    {md_path.resolve()}")
    if not args.skip_regras:
        print(f"    {(out_dir / 'REGRAS_CONSOLIDADAS.md').resolve()}")

    if args.apply:
        print("\n  Aplicando METRICS# no DynamoDB...")
        _apply_metrics(table, preview_body, existing, dry_run=False, only_date=only_date)
        print("\n  Atualizando METADATA (METRICS_FAILURE_KEYS, operacional)...")
        n = _backfill_metadata_metrics(
            table,
            items,
            catalog,
            api_catalog,
            preview_body.get("process_dedup_meta"),
        )
        print(f"  METADATA atualizado: {n} processos")
        print("  Concluído.")
    else:
        print("\n  Preview apenas. Use --apply para gravar METRICS# no DynamoDB.")

    print(json.dumps({"totals": t, "stats": preview_body.get("stats")}, indent=2, default=_json_default))


if __name__ == "__main__":
    main()
