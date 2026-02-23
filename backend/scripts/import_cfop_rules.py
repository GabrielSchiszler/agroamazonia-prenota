#!/usr/bin/env python3
"""
Script para importar regras de CFOP x Chave de arquivo JSON local para DynamoDB.

Formato esperado do arquivo (gerado por export_cfop_rules.py):
{
  "metadata": {...},
  "items": [
    {"PK": "CFOP_OPERATION", "SK": "MAPPING#..."},
    {"PK": "CFOP_OPERATION", "SK": "CFOP#..."}
  ]
}
"""

import argparse
import json
import os
import sys

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


def load_cfop_file(input_file):
    if not os.path.exists(input_file):
        print(f"❌ Erro: arquivo '{input_file}' não encontrado.")
        sys.exit(1)

    with open(input_file, "r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)

    if not isinstance(payload, dict) or "items" not in payload:
        print("❌ Erro: formato inválido. Campo 'items' não encontrado.")
        sys.exit(1)

    items = payload.get("items", [])
    if not isinstance(items, list):
        print("❌ Erro: campo 'items' deve ser uma lista.")
        sys.exit(1)

    for index, item in enumerate(items, 1):
        if not isinstance(item, dict):
            print(f"❌ Erro: item #{index} em 'items' não é objeto.")
            sys.exit(1)
        if "PK" not in item or "SK" not in item:
            print(f"❌ Erro: item #{index} sem PK/SK.")
            sys.exit(1)

    return payload


def import_cfop_rules(target_table_name, input_file, region_name, overwrite=False, dry_run=False):
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    target_table = dynamodb.Table(target_table_name)

    print(
        f"Verificando tabela de destino '{target_table_name}' "
        f"(região: {region_name})..."
    )
    if not check_table_exists(target_table_name, dynamodb):
        print(f"❌ Erro: Tabela de destino '{target_table_name}' não encontrada!")
        sys.exit(1)

    payload = load_cfop_file(input_file)
    metadata = payload.get("metadata", {})
    items = payload.get("items", [])

    if not items:
        print("❌ Nenhum item encontrado para importação.")
        sys.exit(1)

    # Garante que só importa CFOP_OPERATION
    items = [item for item in items if item.get("PK") == "CFOP_OPERATION"]
    if not items:
        print("❌ Nenhum item com PK=CFOP_OPERATION encontrado após filtro.")
        sys.exit(1)

    mapping_items = [item for item in items if item.get("SK", "").startswith("MAPPING#")]
    cfop_items = [item for item in items if item.get("SK", "").startswith("CFOP#")]

    print("\nResumo da importação CFOP:")
    print(f"  Arquivo: {input_file}")
    print(f"  Origem do arquivo: {metadata.get('source_table', 'N/A')} ({metadata.get('source_region', 'N/A')})")
    print(f"  MAPPING#: {len(mapping_items)}")
    print(f"  CFOP#: {len(cfop_items)}")
    print(f"  Total: {len(items)}")
    print(f"  Overwrite habilitado: {overwrite}")
    print(f"  Modo dry-run: {dry_run}")

    if dry_run:
        print("\n--- MODO DRY-RUN ---")
        for item in items:
            print(f"  PK={item.get('PK')} SK={item.get('SK')}")
        print("--------------------")
        return

    confirmation = input("\nDeseja continuar? (sim/não): ").strip().lower()
    if confirmation not in ["sim", "s", "yes", "y"]:
        print("Operação cancelada pelo usuário.")
        return

    created_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for item in items:
        pk = item["PK"]
        sk = item["SK"]
        try:
            existing = target_table.get_item(Key={"PK": pk, "SK": sk})
            already_exists = "Item" in existing

            if already_exists and not overwrite:
                skipped_count += 1
                print(f"  ⚠️ Já existe, pulando: {pk} / {sk}")
                continue

            # Remove campos internos, se vierem no export
            item_to_save = {k: v for k, v in item.items() if not k.startswith("_")}
            target_table.put_item(Item=item_to_save)

            if already_exists and overwrite:
                updated_count += 1
                print(f"  ↺ Atualizado: {pk} / {sk}")
            else:
                created_count += 1
                print(f"  ✓ Criado: {pk} / {sk}")
        except ClientError as error:
            error_count += 1
            print(f"  ❌ Erro ao importar {pk}/{sk}: {error}")

    print("\n" + "=" * 60)
    print("Importação CFOP concluída!")
    print(f"  ✓ Criados: {created_count}")
    print(f"  ↺ Atualizados: {updated_count}")
    print(f"  ⚠️ Pulados: {skipped_count}")
    print(f"  ❌ Erros: {error_count}")
    print(f"  Total processado: {len(items)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Importa regras de CFOP x Chave de arquivo JSON local para DynamoDB"
    )
    parser.add_argument(
        "--target-table",
        required=True,
        help="Nome da tabela DynamoDB de destino",
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Arquivo JSON de entrada (export_cfop_rules.py)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Região AWS (padrão: us-east-1)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescreve itens existentes na tabela de destino",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria importado",
    )
    args = parser.parse_args()

    import_cfop_rules(
        target_table_name=args.target_table,
        input_file=args.input_file,
        region_name=args.region,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()



