#!/usr/bin/env python3
"""
Backfill de success_prenota_count em METRICS# (SUMMARY) a partir de processos já concluídos.

Usa METADATA (STATUS=COMPLETED) + campo protheus_response para detectar a mesma regra do
update_metrics (mensagem contendo "Documento de entrada criado como pré-nota").

- Não altera success_count (já reflete o total de sucessos).
- Marca METADATA.METRICS_PRENOTA_BACKFILL_AT para não contar duas vezes se rodar de novo.

Uso:
  export TABLE_NAME=...
  export AWS_DEFAULT_REGION=us-east-1
  python3 scripts/backfill_prenota_dashboard_metrics.py --dry-run
  python3 scripts/backfill_prenota_dashboard_metrics.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# Reutiliza a mesma função da Lambda update_metrics
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(_ROOT, "lambdas"))
from update_metrics.handler import protheus_response_indicates_prenota  # noqa: E402


def _date_for_metrics(item: dict) -> str | None:
    """YYYY-MM-DD para agregar em METRICS# (usa METRICS_DATE se já existir, senão updated_at)."""
    md = item.get("METRICS_DATE")
    if isinstance(md, str) and len(md) >= 10:
        return md[:10]
    upd = item.get("updated_at") or item.get("UPDATED_AT")
    if isinstance(upd, str) and len(upd) >= 10:
        return upd[:10]
    return None


def main():
    parser = argparse.ArgumentParser(description="Backfill success_prenota_count no DynamoDB")
    parser.add_argument("--dry-run", action="store_true", help="Só imprime, não grava")
    parser.add_argument("--limit", type=int, default=0, help="Máximo de itens (0 = sem limite)")
    args = parser.parse_args()

    table_name = os.environ.get("TABLE_NAME")
    if not table_name:
        print("Defina TABLE_NAME", file=sys.stderr)
        sys.exit(1)

    table = boto3.resource("dynamodb").Table(table_name)
    scan_kwargs = {
        "FilterExpression": "SK = :sk AND #st = :completed AND attribute_exists(protheus_response)",
        "ExpressionAttributeValues": {":sk": "METADATA", ":completed": "COMPLETED"},
        "ExpressionAttributeNames": {"#st": "STATUS"},
    }

    processed = 0
    bumped = 0
    last_key = None

    while True:
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            if args.limit and processed >= args.limit:
                last_key = None
                break
            processed += 1

            if item.get("METRICS_PRENOTA_BACKFILL_AT"):
                continue

            fake_event = {}
            if protheus_response_indicates_prenota(fake_event, item):
                date_key = _date_for_metrics(item)
                if not date_key:
                    print(f"skip (sem data): PK={item.get('PK')}")
                    continue
                bumped += 1
                iso = datetime.now(timezone.utc).isoformat()
                print(f"{'[dry-run] ' if args.dry_run else ''}prenota PK={item.get('PK')} date={date_key}")
                if not args.dry_run:
                    try:
                        table.update_item(
                            Key={"PK": f"METRICS#{date_key}", "SK": "SUMMARY"},
                            UpdateExpression="ADD success_prenota_count :one",
                            ExpressionAttributeValues={":one": Decimal(1)},
                        )
                    except Exception as e:
                        print(f"  ERRO ao atualizar METRICS#{date_key}: {e}", file=sys.stderr)
                        continue
                    table.update_item(
                        Key={"PK": item["PK"], "SK": item["SK"]},
                        UpdateExpression="SET METRICS_PRENOTA_BACKFILL_AT = :ts",
                        ExpressionAttributeValues={":ts": iso},
                    )
        last_key = resp.get("LastEvaluatedKey")
        if not last_key or (args.limit and processed >= args.limit):
            break

    print(f"Scanned metadata items (approx): {processed}, pré-nota incrementados: {bumped}")


if __name__ == "__main__":
    main()
