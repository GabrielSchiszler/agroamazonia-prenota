#!/usr/bin/env python3
"""
Script para importar regras de validação de um arquivo JSON local para DynamoDB.

Formato esperado do arquivo (gerado por export_rules.py):
{
  "metadata": {...},
  "rules": [
    {"PK": "RULES#AGROQUIMICOS", "SK": "RULE#validar_x", ...}
  ]
}

Uso:
    python import_rules.py --target-table TARGET_TABLE --input-file rules_export.json
    python import_rules.py --target-table TARGET_TABLE --input-file rules_export.json --overwrite
"""

import argparse
import json
import os
import sys

import boto3
from botocore.exceptions import ClientError


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


def load_rules_file(input_file):
    """Carrega e valida arquivo JSON de regras."""
    if not os.path.exists(input_file):
        print(f"❌ Erro: arquivo '{input_file}' não encontrado.")
        sys.exit(1)

    with open(input_file, "r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)

    if not isinstance(payload, dict) or "rules" not in payload:
        print("❌ Erro: formato inválido. Campo 'rules' não encontrado.")
        sys.exit(1)

    rules = payload.get("rules", [])
    if not isinstance(rules, list):
        print("❌ Erro: campo 'rules' deve ser uma lista.")
        sys.exit(1)

    for index, item in enumerate(rules, 1):
        if not isinstance(item, dict):
            print(f"❌ Erro: item #{index} em 'rules' não é objeto.")
            sys.exit(1)
        if "PK" not in item or "SK" not in item:
            print(f"❌ Erro: item #{index} sem PK/SK.")
            sys.exit(1)

    return payload


def import_rules(target_table_name, input_file, region_name, overwrite, process_type=None, dry_run=False):
    """Importa regras do arquivo JSON para a tabela de destino."""
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    target_table = dynamodb.Table(target_table_name)

    print(
        f"Verificando tabela de destino '{target_table_name}' "
        f"(região: {region_name})..."
    )
    if not check_table_exists(target_table_name, dynamodb):
        print(f"❌ Erro: Tabela de destino '{target_table_name}' não encontrada!")
        sys.exit(1)

    payload = load_rules_file(input_file)
    metadata = payload.get("metadata", {})
    rules = payload.get("rules", [])

    if process_type:
        pk_filter = f"RULES#{process_type}"
        rules = [item for item in rules if item.get("PK") == pk_filter]
        print(f"Filtro process_type={process_type} aplicado. Itens filtrados: {len(rules)}")

    if not rules:
        print("❌ Nenhuma regra encontrada para importação após filtros.")
        sys.exit(1)

    print("\nResumo da importação:")
    print(f"  Arquivo: {input_file}")
    print(f"  Total de regras no arquivo: {len(payload.get('rules', []))}")
    print(f"  Total após filtros: {len(rules)}")
    print(f"  Origem do arquivo: {metadata.get('source_table', 'N/A')} ({metadata.get('source_region', 'N/A')})")
    print(f"  Overwrite habilitado: {overwrite}")
    print(f"  Modo dry-run: {dry_run}")

    if dry_run:
        print("\n--- MODO DRY-RUN ---")
        for item in rules:
            print(
                f"  PK={item.get('PK')} SK={item.get('SK')} "
                f"RULE_NAME={item.get('RULE_NAME', 'N/A')} ORDER={item.get('ORDER', 'N/A')}"
            )
        print("--------------------")
        return

    confirmation = input("\nDeseja continuar? (sim/não): ").strip().lower()
    if confirmation not in ["sim", "s", "yes", "y"]:
        print("Operação cancelada pelo usuário.")
        return

    copied_count = 0
    skipped_count = 0
    updated_count = 0
    error_count = 0

    for item in rules:
        pk = item["PK"]
        sk = item["SK"]
        rule_name = item.get("RULE_NAME", "N/A")

        try:
            existing = target_table.get_item(Key={"PK": pk, "SK": sk})
            already_exists = "Item" in existing

            if already_exists and not overwrite:
                skipped_count += 1
                print(f"  ⚠️ Já existe, pulando: PK={pk}, SK={sk} (RULE_NAME: {rule_name})")
                continue

            target_table.put_item(Item=item)
            if already_exists and overwrite:
                updated_count += 1
                print(f"  ↺ Atualizado: PK={pk}, SK={sk} (RULE_NAME: {rule_name})")
            else:
                copied_count += 1
                print(f"  ✓ Criado: PK={pk}, SK={sk} (RULE_NAME: {rule_name})")
        except ClientError as error:
            error_count += 1
            print(f"  ❌ Erro ao importar PK={pk}, SK={sk}: {error}")

    print("\n" + "=" * 60)
    print("Importação concluída!")
    print(f"  ✓ Criados: {copied_count}")
    print(f"  ↺ Atualizados: {updated_count}")
    print(f"  ⚠️ Pulados: {skipped_count}")
    print(f"  ❌ Erros: {error_count}")
    print(f"  Total processado: {len(rules)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Importa regras de validação de arquivo JSON local para DynamoDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Importar regras (sem sobrescrever itens existentes)
  python import_rules.py --target-table tabela-document-processor-prd --input-file rules_export.json

  # Importar sobrescrevendo regras existentes
  python import_rules.py --target-table tabela-document-processor-prd --input-file rules_export.json --overwrite

  # Dry-run para validar antes
  python import_rules.py --target-table tabela-document-processor-prd --input-file rules_export.json --dry-run
        """,
    )

    parser.add_argument(
        "--target-table",
        required=True,
        help="Nome da tabela DynamoDB de destino",
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Arquivo JSON com regras exportadas",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Região AWS (padrão: us-east-1)",
    )
    parser.add_argument(
        "--process-type",
        choices=["AGROQUIMICOS", "BARTER"],
        help="Importar apenas regras de um tipo específico",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescreve regras que já existirem na tabela de destino",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria importado",
    )

    args = parser.parse_args()

    try:
        import_rules(
            target_table_name=args.target_table,
            input_file=args.input_file,
            region_name=args.region,
            overwrite=args.overwrite,
            process_type=args.process_type,
            dry_run=args.dry_run,
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



