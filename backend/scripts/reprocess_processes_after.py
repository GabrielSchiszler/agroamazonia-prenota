#!/usr/bin/env python3
"""
Reinicia o Step Functions para processos com METADATA.TIMESTAMP estritamente
posterior ao horário informado (útil após incidentes ou correções em Lambda/CDK).

Critério padrão
---------------
  int(METADATA.TIMESTAMP) > cutoff_unix

TIMESTAMP no METADATA é atualizado em vários passos (ex.: notify_receipt).
Não corresponde necessariamente ao instante exato do “retorno ServiceNow”; se
precisar incluir processos limítrofes, use --inclusive ou ajuste o horário de
corte (ex.: um minuto antes).

Fuso horário
------------
  O valor de --after é interpretado no fuso dado por --timezone (padrão:
  America/Sao_Paulo). Para UTC explícito: --timezone UTC

Pré-requisitos (iguais ao start_process na API)
-----------------------------------------------
  DANFE (FILE# + .xml) e PEDIDO_COMPRA_METADATA devem existir; caso contrário o
  processo é listado em skipped.

Variáveis de ambiente (ou flags equivalentes)
---------------------------------------------
  TABLE_NAME, STATE_MACHINE_ARN, AWS_REGION (opcional)

Exemplos
--------
  export TABLE_NAME=...
  export STATE_MACHINE_ARN=arn:aws:states:...

  # Listar o que seria reiniciado
  python3 reprocess_processes_after.py --after "2026-05-19 08:52:59" --dry-run

  # Executar com pausa entre arranques (evita throttling)
  python3 reprocess_processes_after.py --after "2026-05-19 08:52:59" --sleep 0.4

Para reenviar só feedback (sucesso: notify-success; falha: send-feedback), sem
Step Functions, ver replay_feedback_only.py no mesmo diretório.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
    """Devolve epoch UTC (segundos) para comparar com METADATA.TIMESTAMP."""
    if ZoneInfo is None:
        raise SystemExit("Python 3.9+ é necessário (zoneinfo).")
    if tz_name.upper() == "UTC":
        tz = ZoneInfo("UTC")
    else:
        tz = ZoneInfo(tz_name)
    # Formatos aceites: "2026-05-19 08:52:59" ou "2026-05-19T08:52:59"
    normalized = after.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(normalized, fmt)
            break
        except ValueError:
            dt = None  # type: ignore
    else:
        raise SystemExit(f"Não foi possível interpretar --after={after!r} (use YYYY-MM-DD HH:MM:SS)")
    aware = dt.replace(tzinfo=tz)
    return int(aware.timestamp())


def _get_all_process_index_rows(table) -> List[Dict[str, Any]]:
    """PK=PROCESS, SK begins_with PROCESS#"""
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


def _resolve_process_type(pedido_item: Optional[Dict[str, Any]]) -> str:
    if not pedido_item:
        return "AGROQUIMICOS"
    raw = pedido_item.get("METADADOS", "{}")
    try:
        pedido = json.loads(raw) if isinstance(raw, str) else raw
        request_body = pedido.get("requestBody", {}) if isinstance(pedido, dict) else {}
        is_commodities = request_body.get("isCommodities", False)
        if is_commodities is True or str(is_commodities).lower() == "true":
            return "BARTER"
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return "AGROQUIMICOS"


def _validate_ready_for_start(items: List[Dict[str, Any]]) -> Tuple[bool, str]:
    by_sk = {i.get("SK"): i for i in items}
    if "METADATA" not in by_sk:
        return False, "sem METADATA"
    pedido = by_sk.get("PEDIDO_COMPRA_METADATA")
    if not pedido:
        return False, "sem PEDIDO_COMPRA_METADATA"
    has_danfe = False
    for it in items:
        sk = it.get("SK", "")
        if sk.startswith("FILE#") and it.get("DOC_TYPE") == "DANFE":
            fn = (it.get("FILE_NAME") or "").lower()
            if fn.endswith(".xml"):
                has_danfe = True
                break
    if not has_danfe:
        return False, "sem DANFE .xml"
    return True, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME"), help="DynamoDB table (ou env TABLE_NAME)")
    parser.add_argument(
        "--state-machine-arn",
        default=os.environ.get("STATE_MACHINE_ARN"),
        help="ARN da state machine (ou env STATE_MACHINE_ARN)",
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--after", required=True, help='Corte local, ex.: "2026-05-19 08:52:59"')
    parser.add_argument(
        "--timezone",
        default="America/Sao_Paulo",
        help="Fuso para interpretar --after (padrão: America/Sao_Paulo; use UTC se o horário for UTC)",
    )
    parser.add_argument(
        "--inclusive",
        action="store_true",
        help="Usar METADATA.TIMESTAMP >= cutoff em vez de >",
    )
    parser.add_argument("--dry-run", action="store_true", help="Não chama StartExecution")
    parser.add_argument("--sleep", type=float, default=0.35, help="Segundos entre cada StartExecution (default: 0.35)")
    parser.add_argument("--limit", type=int, default=0, help="Máximo de execuções a arrancar (0 = sem limite)")
    args = parser.parse_args()

    def say(msg: str) -> None:
        print(msg, flush=True)

    say(
        f"[reprocess_processes_after] dry_run={args.dry_run} "
        f"table={args.table_name or '(não definida)'} "
        f"sfn_arn={'sim' if args.state_machine_arn else 'não'}"
    )

    if not args.table_name:
        say("ERRO: defina TABLE_NAME ou --table-name.")
        return 2
    if not args.state_machine_arn:
        say("ERRO: defina STATE_MACHINE_ARN ou --state-machine-arn.")
        return 2

    cutoff = _parse_after_local(args.after, args.timezone)
    op = ">=" if args.inclusive else ">"
    say(f"Corte: {args.after} ({args.timezone}) → unix UTC {cutoff}  (filtro TIMESTAMP {op} corte)")

    import boto3

    session_kw: Dict[str, Any] = {}
    if args.region:
        session_kw["region_name"] = args.region
    dynamodb = boto3.resource("dynamodb", **session_kw)
    sfn = boto3.client("stepfunctions", **session_kw)
    table = dynamodb.Table(args.table_name)

    index_rows = _get_all_process_index_rows(table)
    say(f"Índice PROCESS: {len(index_rows)} linhas PROCESS#...")
    if not index_rows:
        say(
            "Nenhuma linha no índice. Confirme TABLE_NAME, região e credenciais AWS "
            "(PK=PROCESS, SK=PROCESS#...)."
        )

    to_start: List[Tuple[str, int, str]] = []  # process_id, ts, reason_if_skip not used
    skipped_meta: List[Tuple[str, str]] = []
    skipped_time: List[str] = []

    for row in index_rows:
        pid = row.get("PROCESS_ID") or (row.get("SK") or "").replace("PROCESS#", "", 1)
        if not pid:
            continue
        meta = table.get_item(Key={"PK": f"PROCESS#{pid}", "SK": "METADATA"}).get("Item")
        if not meta:
            skipped_meta.append((pid, "METADATA inexistente"))
            continue
        ts = _int_ts(meta.get("TIMESTAMP"))
        if ts is None:
            skipped_meta.append((pid, "TIMESTAMP ausente no METADATA"))
            continue
        if args.inclusive:
            if ts < cutoff:
                skipped_time.append(pid)
                continue
        else:
            if ts <= cutoff:
                skipped_time.append(pid)
                continue
        to_start.append((pid, ts, meta.get("STATUS", "?")))

    to_start.sort(key=lambda x: x[1])
    say(f"Após filtro temporal: {len(to_start)} processo(s)")

    started = 0
    skipped_ready: List[Tuple[str, str]] = []
    errors: List[Tuple[str, str]] = []

    for pid, ts, status in to_start:
        if args.limit and started >= args.limit:
            say(f"Limite --limit={args.limit} atingido; parando.")
            break

        q = table.query(KeyConditionExpression="PK = :pk", ExpressionAttributeValues={":pk": f"PROCESS#{pid}"})
        items = q.get("Items", [])
        while "LastEvaluatedKey" in q:
            q = table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": f"PROCESS#{pid}"},
                ExclusiveStartKey=q["LastEvaluatedKey"],
            )
            items.extend(q.get("Items", []))

        ok, reason = _validate_ready_for_start(items)
        if not ok:
            skipped_ready.append((pid, reason))
            say(f"  skip {pid}  ts={ts}  status={status!r}  ({reason})")
            continue

        pedido_item = next((i for i in items if i.get("SK") == "PEDIDO_COMPRA_METADATA"), None)
        process_type = _resolve_process_type(pedido_item)
        payload = {"process_id": pid, "process_type": process_type, "files": []}

        say(f"  start {pid}  ts={ts}  status={status!r}  type={process_type}  dry_run={args.dry_run}")
        if args.dry_run:
            started += 1
            continue
        try:
            sfn.start_execution(
                stateMachineArn=args.state_machine_arn,
                input=json.dumps(payload),
            )
            table.update_item(
                Key={"PK": f"PROCESS#{pid}", "SK": "METADATA"},
                UpdateExpression="SET #st = :processing",
                ExpressionAttributeNames={"#st": "STATUS"},
                ExpressionAttributeValues={":processing": "PROCESSING"},
            )
            started += 1
        except Exception as e:
            errors.append((pid, str(e)))
            say(f"  ERRO {pid}: {e}")

        if args.sleep > 0:
            time.sleep(args.sleep)

    say("\n--- Resumo ---")
    say(f"Arranques {'simulados' if args.dry_run else 'enviados'}: {started}")
    say(f"Pulados (fora do recorte temporal): {len(skipped_time)}")
    say(f"Pulados (METADATA inválido): {len(skipped_meta)}")
    if skipped_meta:
        for p, r in skipped_meta[:20]:
            say(f"  {p}: {r}")
        if len(skipped_meta) > 20:
            say(f"  ... +{len(skipped_meta) - 20} mais")
    say(f"Pulados (não elegível p/ start): {len(skipped_ready)}")
    for p, r in skipped_ready[:30]:
        say(f"  {p}: {r}")
    if len(skipped_ready) > 30:
        say(f"  ... +{len(skipped_ready) - 30} mais")
    if errors:
        say(f"Falhas StartExecution: {len(errors)}")
        for p, err in errors:
            say(f"  {p}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
