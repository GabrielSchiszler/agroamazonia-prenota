#!/usr/bin/env python3
"""
Reenvia apenas o feedback (ServiceNow + SNS), sem reexecutar o Step Functions.

São duas Lambdas diferentes no produto
---------------------------------------
  * **Sucesso** (`STATUS == COMPLETED`): Lambda **notify-success** — relê o
    Dynamo e envia o mesmo feedback de fim de fluxo com sucesso.
  * **Falha** (`STATUS == FAILED`): Lambda **send-feedback** — evento
    `success: false`; validação vs erro genérico é inferido (VALIDATION# /
    `error_info`).

Modo --mode (padrão: **both**)
------------------------------
  both
      Para cada processo no recorte temporal: COMPLETED → notify-success,
      FAILED → send-feedback. Outros status (CREATED, PROCESSING, …) são
      ignorados.

  success
      Só COMPLETED → notify-success.

  failure
      Só FAILED → send-feedback.

Critério temporal (igual ao reprocess_processes_after)
-----------------------------------------------------
  METADATA.TIMESTAMP > corte (ou >= com --inclusive), em --timezone.

Variáveis de ambiente
---------------------
  TABLE_NAME (ou --table-name)
  NOTIFY_SUCCESS_FUNCTION_NAME — ex.: ...lambda-notify-success
  SEND_FEEDBACK_FUNCTION_NAME — ex.: ...lambda-send-feedback

  Em --mode both (padrão) as duas são obrigatórias. Opcional: AWS_REGION.

Exemplos
--------
  export TABLE_NAME=...
  export NOTIFY_SUCCESS_FUNCTION_NAME=...notify-success
  export SEND_FEEDBACK_FUNCTION_NAME=...send-feedback

  python3 replay_feedback_only.py --after "2026-05-19 08:52:59" --dry-run
  python3 replay_feedback_only.py --after "2026-05-19 08:52:59" --sleep 0.5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore


def _int_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_after_local(after: str, tz_name: str) -> int:
    if ZoneInfo is None:
        raise SystemExit("Python 3.9+ é necessário (zoneinfo).")
    if tz_name.upper() == "UTC":
        tz = ZoneInfo("UTC")
    else:
        tz = ZoneInfo(tz_name)
    normalized = after.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(normalized, fmt)
            break
        except ValueError:
            dt = None  # type: ignore
    else:
        raise SystemExit(f"Não foi possível interpretar --after={after!r}")
    return int(dt.replace(tzinfo=tz).timestamp())


def _get_all_process_index_rows(table) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {
        "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk)",
        "ExpressionAttributeValues": {":pk": "PROCESS", ":sk": "PROCESS#"},
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def build_failure_payload(table, process_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    err = metadata.get("error_info") or metadata.get("ERROR_INFO")
    if isinstance(err, str):
        try:
            err = json.loads(err)
        except json.JSONDecodeError:
            err = {"message": err}
    if not isinstance(err, dict):
        err = {"message": str(err)}

    use_validation = False
    try:
        items: List[Dict[str, Any]] = []
        qkwargs: Dict[str, Any] = {
            "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk)",
            "ExpressionAttributeValues": {":pk": f"PROCESS#{process_id}", ":sk": "VALIDATION#"},
        }
        while True:
            r = table.query(**qkwargs)
            items.extend(r.get("Items", []))
            lek = r.get("LastEvaluatedKey")
            if not lek:
                break
            qkwargs["ExclusiveStartKey"] = lek
        if items:
            def _ts_key(it: Dict[str, Any]) -> int:
                t = it.get("TIMESTAMP", 0)
                if isinstance(t, Decimal):
                    return int(t)
                try:
                    return int(t or 0)
                except (TypeError, ValueError):
                    return 0

            latest = max(items, key=_ts_key)
            raw = latest.get("VALIDATION_RESULTS", "[]")
            try:
                vr = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                vr = []
            if isinstance(vr, list) and any(isinstance(x, dict) and x.get("status") == "FAILED" for x in vr):
                use_validation = True
    except Exception:
        use_validation = False

    err_type = (err.get("type") or err.get("TYPE") or "").upper()
    if err_type == "VALIDATION_ERROR" or err_type == "VALIDATION_FAILURE":
        use_validation = True

    if use_validation:
        return {
            "process_id": process_id,
            "success": False,
            "details": {
                "status": "VALIDATION_FAILURE",
                "validation_status": "FAILED",
                "timestamp": iso,
            },
        }

    try:
        cause_str = json.dumps(err, default=str)
    except TypeError:
        cause_str = str(err)

    return {
        "process_id": process_id,
        "success": False,
        "details": {
            "status": "LAMBDA_ERROR",
            "timestamp": iso,
            "error_details": {
                "Error": str(err.get("message", "FAILED")),
                "Cause": cause_str,
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME"))
    parser.add_argument(
        "--notify-success-function-name",
        default=os.environ.get("NOTIFY_SUCCESS_FUNCTION_NAME"),
        help="Nome da Lambda notify-success",
    )
    parser.add_argument(
        "--send-feedback-function-name",
        default=os.environ.get("SEND_FEEDBACK_FUNCTION_NAME"),
        help="Nome da Lambda send-feedback",
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--after", required=True, help='Corte local, ex.: "2026-05-19 08:52:59"')
    parser.add_argument("--timezone", default="America/Sao_Paulo")
    parser.add_argument("--inclusive", action="store_true")
    parser.add_argument(
        "--mode",
        choices=("success", "failure", "both"),
        default="both",
        help="both = sucesso (notify-success) + falha (send-feedback); default both",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--async-invoke",
        action="store_true",
        help="InvocationType Event (não espera resposta; mais rápido, erros só no CloudWatch)",
    )
    args = parser.parse_args()

    def say(msg: str) -> None:
        """Sempre no stdout com flush (evita 'silêncio' se o terminal não mostrar stderr)."""
        print(msg, flush=True)

    say(
        f"[replay_feedback_only] dry_run={args.dry_run} mode={args.mode} "
        f"table={args.table_name or '(não definida)'} "
        f"notify_success_fn={'sim' if args.notify_success_function_name else 'não'} "
        f"send_feedback_fn={'sim' if args.send_feedback_function_name else 'não'}"
    )

    if not args.table_name:
        say("ERRO: defina TABLE_NAME ou passe --table-name (o script parou antes de falar com a AWS).")
        return 2

    if args.mode in ("success", "both") and not args.notify_success_function_name:
        say("ERRO: defina NOTIFY_SUCCESS_FUNCTION_NAME ou --notify-success-function-name.")
        return 2
    if args.mode in ("failure", "both") and not args.send_feedback_function_name:
        say("ERRO: defina SEND_FEEDBACK_FUNCTION_NAME ou --send-feedback-function-name.")
        return 2

    cutoff = _parse_after_local(args.after, args.timezone)
    op = ">=" if args.inclusive else ">"
    say(f"Corte: {args.after} ({args.timezone}) → unix {cutoff}  (TIMESTAMP {op} corte)  mode={args.mode}")

    import boto3

    kw: Dict[str, Any] = {}
    if args.region:
        kw["region_name"] = args.region
    dynamodb = boto3.resource("dynamodb", **kw)
    lam = boto3.client("lambda", **kw)
    table = dynamodb.Table(args.table_name)

    index_rows = _get_all_process_index_rows(table)
    say(f"Processos no índice (PK=PROCESS): {len(index_rows)}")
    if not index_rows:
        say(
            "Nenhuma linha no índice de processos. Confirme TABLE_NAME, região AWS e credenciais. "
            "Itens esperados: PK='PROCESS', SK começando por 'PROCESS#'."
        )

    invoked = 0
    invoked_success_lambda = 0
    invoked_failure_lambda = 0
    skipped_time = 0
    skipped_status = 0
    errors: List[str] = []

    for row in index_rows:
        if args.limit and invoked >= args.limit:
            say(f"Limite --limit={args.limit} atingido.")
            break
        pid = row.get("PROCESS_ID") or (row.get("SK") or "").replace("PROCESS#", "", 1)
        if not pid:
            continue
        meta = table.get_item(Key={"PK": f"PROCESS#{pid}", "SK": "METADATA"}).get("Item")
        if not meta:
            continue
        ts = _int_ts(meta.get("TIMESTAMP"))
        if ts is None:
            continue
        if args.inclusive:
            if ts < cutoff:
                skipped_time += 1
                continue
        else:
            if ts <= cutoff:
                skipped_time += 1
                continue

        status = (meta.get("STATUS") or "").upper()

        which: str  # "notify_success" | "send_feedback"

        if args.mode == "success":
            if status != "COMPLETED":
                skipped_status += 1
                continue
            payload = {"process_id": pid, "protheus_result": {}}
            fn = args.notify_success_function_name
            which = "notify_success"
        elif args.mode == "failure":
            if status != "FAILED":
                skipped_status += 1
                continue
            payload = build_failure_payload(table, pid, meta)
            fn = args.send_feedback_function_name
            which = "send_feedback"
        else:
            if status == "COMPLETED":
                payload = {"process_id": pid, "protheus_result": {}}
                fn = args.notify_success_function_name
                which = "notify_success"
            elif status == "FAILED":
                payload = build_failure_payload(table, pid, meta)
                fn = args.send_feedback_function_name
                which = "send_feedback"
            else:
                skipped_status += 1
                continue

        say(
            f"  {'[dry-run] ' if args.dry_run else ''}{which}  {fn}  "
            f"process_id={pid}  status={status}"
        )

        if args.dry_run:
            invoked += 1
            if which == "notify_success":
                invoked_success_lambda += 1
            else:
                invoked_failure_lambda += 1
            continue

        try:
            inv_kw: Dict[str, Any] = {
                "FunctionName": fn,
                "InvocationType": "Event" if args.async_invoke else "RequestResponse",
                "Payload": json.dumps(payload).encode("utf-8"),
            }
            resp = lam.invoke(**inv_kw)
            invoke_ok = True
            if resp.get("FunctionError"):
                err_payload = resp.get("Payload")
                raw_e = err_payload.read().decode("utf-8") if err_payload else ""
                errors.append(f"{pid}: Lambda FunctionError {resp.get('FunctionError')}: {raw_e}")
                say(f"    ERRO Lambda: {resp.get('FunctionError')} {raw_e[:500]}")
                invoke_ok = False
            elif not args.async_invoke:
                body = resp.get("Payload")
                raw = body.read().decode("utf-8") if body else ""
                try:
                    out = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    out = {"raw": raw}
                if isinstance(out, dict) and out.get("statusCode", 200) >= 400:
                    errors.append(f"{pid}: {out}")
                    say(f"    ERRO resposta: {out}")
                    invoke_ok = False
                elif isinstance(out, dict) and out.get("notification_sent") is False:
                    errors.append(f"{pid}: notify failed {out}")
                    say(f"    ERRO notificação: {out}")
                    invoke_ok = False
                elif isinstance(out, dict) and out.get("feedback_sent") is False and not out.get("skipped"):
                    errors.append(f"{pid}: send_feedback {out}")
                    say(f"    AVISO feedback: {out}")
                    invoke_ok = False
            if invoke_ok:
                invoked += 1
                if which == "notify_success":
                    invoked_success_lambda += 1
                else:
                    invoked_failure_lambda += 1
        except Exception as e:
            errors.append(f"{pid}: {e}")
            say(f"    ERRO invoke: {e}")

        if args.sleep > 0:
            time.sleep(args.sleep)

    say("\n--- Resumo ---")
    say(f"Invocações {'simuladas' if args.dry_run else 'concluídas com sucesso'}: {invoked}")
    if args.mode in ("success", "both"):
        say(f"  → notify-success (COMPLETED): {invoked_success_lambda}")
    if args.mode in ("failure", "both"):
        say(f"  → send-feedback (FAILED): {invoked_failure_lambda}")
    say(f"Pulados (fora do tempo): {skipped_time}")
    say(f"Pulados (status não elegível para o modo): {skipped_status}")
    if invoked == 0 and not errors:
        say(
            "\nDica: se esperavas vários processos, verifica o fuso (--timezone), "
            "se o corte deve ser --inclusive, e se STATUS no Dynamo é COMPLETED/FAILED "
            "(modo both ignora CREATED, PROCESSING, etc.)."
        )
    if errors:
        say(f"Com problema: {len(errors)}")
        for e in errors[:25]:
            say(f"  {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
