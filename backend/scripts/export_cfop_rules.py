#!/usr/bin/env python3
"""
Script para exportar regras de CFOP x Chave do DynamoDB para arquivo JSON local.

As regras ficam na PK:
- PK: CFOP_OPERATION
- SK: MAPPING#... e CFOP#...

Uso:
    python export_cfop_rules.py --source-table SOURCE_TABLE --output-file cfop_export.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


def check_table_exists(table_name, dynamodb):
    try:
        table = dynamodb.Table(table_name)
        table.load()
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def to_json_compatible(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, dict):
        return {k: to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_compatible(v) for v in value]
    return value


def get_all_cfop_items(table, pk="CFOP_OPERATION"):
    items = []
    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    return items


def export_cfop_rules(source_table_name, output_file, region_name):
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    source_table = dynamodb.Table(source_table_name)

    print(
        f"Verificando tabela de origem '{source_table_name}' "
        f"(região: {region_name})..."
    )
    if not check_table_exists(source_table_name, dynamodb):
        print(f"❌ Erro: Tabela de origem '{source_table_name}' não encontrada!")
        sys.exit(1)

    print("Buscando regras de CFOP (PK=CFOP_OPERATION)...")
    items = get_all_cfop_items(source_table, "CFOP_OPERATION")

    if not items:
        print("❌ Nenhum item CFOP encontrado para exportação.")
        sys.exit(1)

    mapping_items = [item for item in items if item.get("SK", "").startswith("MAPPING#")]
    cfop_items = [item for item in items if item.get("SK", "").startswith("CFOP#")]

    payload = {
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_table": source_table_name,
            "source_region": region_name,
            "pk": "CFOP_OPERATION",
            "total_items": len(items),
            "mapping_items": len(mapping_items),
            "cfop_items": len(cfop_items),
            "format_version": 1,
        },
        "items": [to_json_compatible(item) for item in items],
    }

    with open(output_file, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("Exportação CFOP concluída com sucesso!")
    print(f"  Arquivo: {output_file}")
    print(f"  MAPPING#: {len(mapping_items)}")
    print(f"  CFOP#: {len(cfop_items)}")
    print(f"  Total: {len(items)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Exporta regras de CFOP x Chave para arquivo JSON local"
    )
    parser.add_argument(
        "--source-table",
        required=True,
        help="Nome da tabela DynamoDB de origem",
    )
    parser.add_argument(
        "--output-file",
        required=True,
        help="Caminho do arquivo JSON de saída",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Região AWS (padrão: us-east-1)",
    )
    args = parser.parse_args()

    export_cfop_rules(
        source_table_name=args.source_table,
        output_file=args.output_file,
        region_name=args.region,
    )


if __name__ == "__main__":
    main()



