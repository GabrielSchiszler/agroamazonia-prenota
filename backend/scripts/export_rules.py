#!/usr/bin/env python3
"""
Script para exportar regras de validação do DynamoDB para um arquivo JSON local.

As regras são armazenadas com:
- PK: RULES#{process_type} (ex: RULES#AGROQUIMICOS)
- SK: RULE#{rule_name} (ex: RULE#validar_cnpj_fornecedor)

Uso:
    python export_rules.py --source-table SOURCE_TABLE --output-file rules_export.json
    python export_rules.py --source-table SOURCE_TABLE --output-file rules_export.json --process-type AGROQUIMICOS
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal


def check_table_exists(table_name, dynamodb):
    """Verifica se a tabela existe."""
    try:
        table = dynamodb.Table(table_name)
        table.load()
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def to_json_compatible(value):
    """Converte Decimal em tipos JSON compatíveis."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, dict):
        return {k: to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_compatible(v) for v in value]
    return value


def export_rules(source_table_name, output_file, region_name, process_type=None):
    """Exporta regras para arquivo JSON local."""
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    source_table = dynamodb.Table(source_table_name)

    print(
        f"Verificando tabela de origem '{source_table_name}' "
        f"(região: {region_name})..."
    )
    if not check_table_exists(source_table_name, dynamodb):
        print(f"❌ Erro: Tabela de origem '{source_table_name}' não encontrada!")
        sys.exit(1)

    process_types = [process_type] if process_type else ["AGROQUIMICOS", "BARTER"]
    exported_items = []

    for pt in process_types:
        pk = f"RULES#{pt}"
        print(f"Buscando regras do tipo '{pt}' (PK: {pk})...")

        try:
            response = source_table.query(
                KeyConditionExpression=Key("PK").eq(pk) & Key("SK").begins_with("RULE#")
            )
            items = response["Items"]

            while "LastEvaluatedKey" in response:
                response = source_table.query(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    KeyConditionExpression=Key("PK").eq(pk)
                    & Key("SK").begins_with("RULE#"),
                )
                items.extend(response["Items"])

            print(f"  Encontradas {len(items)} regras do tipo '{pt}'")
            exported_items.extend(items)

        except ClientError as error:
            print(f"  ⚠️ Erro ao buscar regras do tipo '{pt}': {error}")
            continue

    if not exported_items:
        print("\n❌ Nenhuma regra encontrada para exportação.")
        sys.exit(1)

    payload = {
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_table": source_table_name,
            "source_region": region_name,
            "process_types": process_types,
            "total_rules": len(exported_items),
            "format_version": 1,
        },
        "rules": [to_json_compatible(item) for item in exported_items],
    }

    with open(output_file, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("Exportação concluída com sucesso!")
    print(f"  Arquivo: {output_file}")
    print(f"  Total de regras exportadas: {len(exported_items)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Exporta regras de validação do DynamoDB para arquivo JSON local",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Exportar todas as regras (AGROQUIMICOS + BARTER)
  python export_rules.py --source-table tabela-document-processor-dev --output-file rules_export.json

  # Exportar somente regras AGROQUIMICOS
  python export_rules.py --source-table tabela-document-processor-dev --output-file agroquimicos.json --process-type AGROQUIMICOS
        """,
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
    parser.add_argument(
        "--process-type",
        choices=["AGROQUIMICOS", "BARTER"],
        help="Exportar apenas regras de um tipo específico",
    )

    args = parser.parse_args()

    try:
        export_rules(
            source_table_name=args.source_table,
            output_file=args.output_file,
            region_name=args.region,
            process_type=args.process_type,
        )
    except KeyboardInterrupt:
        print("\n\nOperação cancelada pelo usuário.")
        sys.exit(1)
    except Exception as error:  # pragma: no cover
        print(f"\n❌ Erro inesperado: {error}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()



