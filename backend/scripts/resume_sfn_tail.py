#!/usr/bin/env python3
"""
Retoma o fim do Step Functions (update_metrics + feedback) sem reprocessar o fluxo inteiro.

Ordem igual ao CDK (sucesso):
  SendToProtheus → UpdateMetrics → NotifySuccess

Uso típico após TIMED_OUT ou falha na última etapa:
  cd backend/scripts
  set -a && source ../.env.prod && set +a
  export UPDATE_METRICS_FUNCTION_NAME=lambda-update-metrics-prd
  export NOTIFY_SUCCESS_FUNCTION_NAME=lambda-notify-success-prd
  export SEND_FEEDBACK_FUNCTION_NAME=lambda-send-feedback-prd

  # Por process_id (lê METADATA no Dynamo)
  python3 resume_sfn_tail.py --process-id <uuid>

  # Descobrir último timeout e retomar
  python3 resume_sfn_tail.py --last-timed-out

  # Só feedback (métricas já gravadas)
  python3 resume_sfn_tail.py --process-id <uuid> --skip-metrics

  # Só métricas
  python3 resume_sfn_tail.py --process-id <uuid> --skip-feedback
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from replay_feedback_only import build_failure_payload  # noqa: E402


def _protheus_succeeded(meta: dict) -> bool:
    """True se METADATA indica documento criado no Protheus (ramo de sucesso do SFN)."""
    for key in ("protheus_response", "PROTHEUS_RESPONSE"):
        pr = _coerce_dict(meta.get(key))
        msg = (pr.get("message") or "").lower()
        if "criado com sucesso" in msg or "documento de entrada criado" in msg:
            return True
    pri = meta.get("protheus_request_info")
    if isinstance(pri, str):
        try:
            pri = json.loads(pri)
        except json.JSONDecodeError:
            pri = {}
    if isinstance(pri, dict):
        body = pri.get("response_body") or pri.get("responseBody") or {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                body = {}
        if isinstance(body, dict):
            msg = (body.get("message") or "").lower()
            if "criado com sucesso" in msg:
                return True
    return False


def _coerce_dict(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _infer_failure_error_type(meta: dict) -> str:
    et = meta.get("METRICS_FAILURE_ERROR_TYPE")
    if et:
        return str(et)
    ei = meta.get("error_info") or meta.get("ERROR_INFO")
    if isinstance(ei, str):
        try:
            ei = json.loads(ei)
        except json.JSONDecodeError:
            ei = {}
    if isinstance(ei, dict):
        return str(ei.get("type") or ei.get("TYPE") or "LAMBDA_ERROR")
    return "LAMBDA_ERROR"


def patch_metrics_failure_reason(
    table,
    date_key: str,
    error_type: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Remove contagem fantasma em failure_reasons (gráfico Tipos de Erro)."""
    if not date_key:
        return False
    key = {"PK": f"METRICS#{date_key[:10]}", "SK": "SUMMARY"}
    resp = table.get_item(Key=key)
    item = resp.get("Item")
    if not item:
        print(f"  METRICS#{date_key[:10]} não existe — nada a ajustar")
        return False
    reasons = item.get("failure_reasons") or {}
    cur = int(reasons.get(error_type, 0) or 0)
    if cur <= 0:
        print(f"  failure_reasons.{error_type} já está em 0")
        return False
    print(f"\n3) Ajuste dashboard METRICS#{date_key[:10]}: failure_reasons.{error_type} {cur} → {cur - 1}")
    if dry_run:
        return True
    err_key = "#e0"
    table.update_item(
        Key=key,
        UpdateExpression=f"SET failure_reasons.{err_key} = failure_reasons.{err_key} - :one",
        ExpressionAttributeNames={err_key: error_type},
        ExpressionAttributeValues={":one": 1},
    )
    print("   ✓ failure_reasons corrigido")
    return True


def _invoke_lambda(lam, fn: str, payload: dict, dry_run: bool) -> dict:
    body = json.dumps(payload, default=_json_default)
    print(f"  → {fn}")
    print(f"    payload: {body[:800]}{'...' if len(body) > 800 else ''}")
    if dry_run:
        return {"dry_run": True}
    resp = lam.invoke(
        FunctionName=fn,
        InvocationType="RequestResponse",
        Payload=body.encode("utf-8"),
    )
    raw = resp.get("Payload")
    out_s = raw.read().decode("utf-8") if raw else ""
    if resp.get("FunctionError"):
        raise RuntimeError(f"{fn} FunctionError={resp.get('FunctionError')}: {out_s}")
    try:
        return json.loads(out_s) if out_s else {}
    except json.JSONDecodeError:
        return {"raw": out_s}


def _has_validation_failure(table, process_id: str) -> bool:
    from boto3.dynamodb.conditions import Key

    items: List[dict] = []
    qkwargs = {
        "KeyConditionExpression": Key("PK").eq(f"PROCESS#{process_id}")
        & Key("SK").begins_with("VALIDATION#"),
    }
    while True:
        r = table.query(**qkwargs)
        items.extend(r.get("Items", []))
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
        qkwargs["ExclusiveStartKey"] = lek
    if not items:
        return False
    latest = max(items, key=lambda x: int(x.get("TIMESTAMP") or 0))
    raw = latest.get("VALIDATION_RESULTS", "[]")
    try:
        vr = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        vr = []
    return isinstance(vr, list) and any(
        isinstance(x, dict) and x.get("status") == "FAILED" for x in vr
    )


def _build_metrics_payload(process_id: str, status: str, meta: dict, table) -> dict:
    st = status.upper()
    if st == "SUCCESS" or st == "COMPLETED":
        pr = _coerce_dict(meta.get("protheus_response"))
        return {
            "process_id": process_id,
            "status": "SUCCESS",
            "protheus_response": pr,
            "failure_result": {},
        }
    if st == "FAILED":
        err = meta.get("error_info") or meta.get("ERROR_INFO")
        if isinstance(err, str):
            try:
                err = json.loads(err)
            except json.JSONDecodeError:
                err = {"message": err}
        if not isinstance(err, dict):
            err = {}
        has_validation = _has_validation_failure(table, process_id)
        if has_validation:
            return {
                "process_id": process_id,
                "status": "FAILED",
                "protheus_response": {},
                "failure_result": {"status": "FAILED"},
            }
        return {
            "process_id": process_id,
            "status": "FAILED",
            "protheus_response": {},
            "failure_result": {},
            "error": {
                "Error": str((err or {}).get("message", "LAMBDA_ERROR")),
                "Cause": json.dumps(err, default=str) if err else "",
            },
        }
    raise ValueError(f"STATUS não suportado para métricas: {status}")


def _find_last_timed_out(sfn, state_machine_arn: str) -> Optional[Tuple[str, str]]:
    resp = sfn.list_executions(
        stateMachineArn=state_machine_arn,
        statusFilter="TIMED_OUT",
        maxResults=10,
    )
    for ex in resp.get("executions", []):
        arn = ex["executionArn"]
        detail = sfn.describe_execution(executionArn=arn)
        inp = detail.get("input") or "{}"
        try:
            data = json.loads(inp)
        except json.JSONDecodeError:
            continue
        pid = data.get("process_id")
        if pid:
            return pid, arn
    return None


def _execution_last_state(sfn, execution_arn: str) -> Optional[str]:
    """Último nome de estado (ex.: NotifySuccessTask, UpdateMetricsSuccess)."""
    token = None
    last_name = None
    while True:
        kw: Dict[str, Any] = {"executionArn": execution_arn, "maxResults": 200, "reverseOrder": True}
        if token:
            kw["nextToken"] = token
        hist = sfn.get_execution_history(**kw)
        for ev in hist.get("events", []):
            if ev.get("type") == "TaskStateEntered":
                last_name = (ev.get("stateEnteredEventDetails") or {}).get("name")
                if last_name:
                    return last_name
            if ev.get("type") == "TaskFailed" and not last_name:
                last_name = (ev.get("taskFailedEventDetails") or {}).get("resource")
        token = hist.get("nextToken")
        if not token:
            break
    return last_name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--process-id", help="UUID do processo")
    parser.add_argument("--execution-arn", help="ARN da execução SFN (extrai process_id do input)")
    parser.add_argument("--last-timed-out", action="store_true", help="Usa o TIMED_OUT mais recente")
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME"))
    parser.add_argument("--state-machine-arn", default=os.environ.get("STATE_MACHINE_ARN"))
    parser.add_argument(
        "--update-metrics-function-name",
        default=os.environ.get("UPDATE_METRICS_FUNCTION_NAME", "lambda-update-metrics-prd"),
    )
    parser.add_argument(
        "--notify-success-function-name",
        default=os.environ.get("NOTIFY_SUCCESS_FUNCTION_NAME", "lambda-notify-success-prd"),
    )
    parser.add_argument(
        "--send-feedback-function-name",
        default=os.environ.get("SEND_FEEDBACK_FUNCTION_NAME", "lambda-send-feedback-prd"),
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-metrics", action="store_true", help="Não invoca update_metrics")
    parser.add_argument("--skip-feedback", action="store_true", help="Não invoca notify/send_feedback")
    parser.add_argument(
        "--force-metrics",
        action="store_true",
        help="Invoca update_metrics mesmo se METRICS_STATUS já existir (dedup na lambda)",
    )
    parser.add_argument(
        "--force-success",
        action="store_true",
        help="Trata como sucesso (notify-success + métricas SUCCESS) mesmo com STATUS=FAILED no Dynamo",
    )
    parser.add_argument(
        "--patch-dashboard-only",
        action="store_true",
        help="Só corrige failure_reasons no METRICS# (gráfico Tipos de Erro); não invoca lambdas",
    )
    args = parser.parse_args()

    if not args.table_name:
        print("ERRO: TABLE_NAME ou --table-name", file=sys.stderr)
        return 2

    import boto3

    kw: Dict[str, Any] = {}
    if args.region:
        kw["region_name"] = args.region
    dynamodb = boto3.resource("dynamodb", **kw)
    lam = boto3.client("lambda", **kw)
    sfn = boto3.client("stepfunctions", **kw)
    table = dynamodb.Table(args.table_name)

    process_id = args.process_id
    execution_arn = args.execution_arn

    if args.last_timed_out:
        if not args.state_machine_arn:
            print("ERRO: STATE_MACHINE_ARN para --last-timed-out", file=sys.stderr)
            return 2
        found = _find_last_timed_out(sfn, args.state_machine_arn)
        if not found:
            print("Nenhuma execução TIMED_OUT encontrada.")
            return 1
        process_id, execution_arn = found
        print(f"TIMED_OUT: process_id={process_id}")
        print(f"  executionArn={execution_arn}")

    if execution_arn and not process_id:
        detail = sfn.describe_execution(executionArn=execution_arn)
        inp = json.loads(detail.get("input") or "{}")
        process_id = inp.get("process_id")

    if not process_id:
        print("ERRO: informe --process-id, --execution-arn ou --last-timed-out", file=sys.stderr)
        return 2

    if execution_arn:
        last_state = _execution_last_state(sfn, execution_arn)
        if last_state:
            print(f"Último estado SFN (histórico): {last_state}")
            if "UpdateMetrics" in last_state and not args.skip_feedback:
                print("  (métricas provavelmente já rodaram → só feedback, use --skip-metrics se repetir)")
            if "NotifySuccess" in last_state or "SendFeedback" in last_state:
                print("  (feedback pode ter iniciado; reinvocar é idempotente no ServiceNow?)")

    meta_resp = table.get_item(Key={"PK": f"PROCESS#{process_id}", "SK": "METADATA"})
    meta = meta_resp.get("Item")
    if not meta:
        print(f"ERRO: METADATA não encontrado para {process_id}", file=sys.stderr)
        return 1

    status = (meta.get("STATUS") or "").upper()
    print(f"Processo {process_id} STATUS={status}")
    metrics_status = meta.get("METRICS_STATUS")
    if metrics_status:
        print(f"  METRICS_STATUS={metrics_status} METRICS_DATE={meta.get('METRICS_DATE')}")

    protheus_ok = _protheus_succeeded(meta)
    if args.force_success or protheus_ok:
        is_success = True
        is_failure = False
        if status == "FAILED" or metrics_status == "FAILED":
            print(
                "CORREÇÃO: Protheus OK mas SFN caiu no ramo de erro (ex.: update_metrics quebrado). "
                "Reenviando métricas/feedback de SUCESSO."
            )
    else:
        is_success = status in ("COMPLETED", "SUCCESS")
        is_failure = status == "FAILED"
        if not is_success and not is_failure:
            print(
                f"AVISO: STATUS={status!r} — assumindo sucesso se houver protheus_response, senão falha."
            )
            if meta.get("protheus_response"):
                is_success = True
            else:
                is_failure = True

    if args.patch_dashboard_only:
        args.skip_metrics = True
        args.skip_feedback = True

    prev_metrics_was_failed = (meta.get("METRICS_STATUS") or "").upper() == "FAILED"
    metrics_date = meta.get("METRICS_DATE") or ""

    run_metrics = not args.skip_metrics and (
        args.force_metrics or not metrics_status or (is_success and metrics_status == "FAILED")
    )
    run_feedback = not args.skip_feedback

    if run_metrics:
        metrics_payload = _build_metrics_payload(
            process_id, "SUCCESS" if is_success else "FAILED", meta, table
        )
        print("\n1) update_metrics")
        out = _invoke_lambda(lam, args.update_metrics_function_name, metrics_payload, args.dry_run)
        print(f"   resposta: {json.dumps(out, default=str)[:500]}")
    else:
        print("\n1) update_metrics — pulado (METRICS_STATUS já definido ou --skip-metrics)")

    if run_feedback:
        print("\n2) feedback")
        if is_success:
            fb_payload = {"process_id": process_id, "protheus_result": {}}
            fn = args.notify_success_function_name
        else:
            fb_payload = build_failure_payload(table, process_id, meta)
            fn = args.send_feedback_function_name
        out = _invoke_lambda(lam, fn, fb_payload, args.dry_run)
        print(f"   resposta: {json.dumps(out, default=str)[:500]}")
    else:
        print("\n2) feedback — pulado (--skip-feedback)")

    if metrics_date and (
        args.patch_dashboard_only
        or meta.get("error_info")
        or meta.get("METRICS_FAILURE_ERROR_TYPE")
        or prev_metrics_was_failed
    ):
        err_type = _infer_failure_error_type(meta)
        patch_metrics_failure_reason(
            table,
            str(metrics_date),
            err_type,
            dry_run=args.dry_run,
        )

    if not args.dry_run and is_success and status != "COMPLETED" and not args.patch_dashboard_only:
        table.update_item(
            Key={"PK": f"PROCESS#{process_id}", "SK": "METADATA"},
            UpdateExpression="SET #st = :c",
            ExpressionAttributeNames={"#st": "STATUS"},
            ExpressionAttributeValues={":c": "COMPLETED"},
        )
        print("\nSTATUS atualizado para COMPLETED no METADATA.")

    print("\nConcluído.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
