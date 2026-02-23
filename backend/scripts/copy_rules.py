#!/usr/bin/env python3
"""
Script para copiar regras de validação de uma tabela DynamoDB para outra.

As regras são armazenadas com:
- PK: RULES#{process_type} (ex: RULES#AGROQUIMICOS)
- SK: RULE#{rule_name} (ex: RULE#validar_cnpj_fornecedor)
- Campos: RULE_NAME, ORDER, ENABLED

Uso:
    python copy_rules.py --source-table SOURCE_TABLE --target-table TARGET_TABLE [--region us-east-1] [--dry-run]
    python3 copy_rules.py --source-table tabela-document-processor-test --target-table tabela-document-processor-dev --region us-east-1
"""

import boto3
import argparse
import sys
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

def check_table_exists(table_name, dynamodb):
    """Verifica se a tabela existe"""
    try:
        table = dynamodb.Table(table_name)
        table.load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return False
        raise

def copy_rules(
    source_table_name,
    target_table_name,
    source_region_name,
    target_region_name,
    dry_run,
    process_type=None,
    source_profile=None,
    target_profile=None
):
    """
    Copia regras de validação de uma tabela DynamoDB para outra.
    
    Args:
        source_table_name: Nome da tabela de origem
        target_table_name: Nome da tabela de destino
        source_region_name: Região AWS da tabela de origem
        target_region_name: Região AWS da tabela de destino
        dry_run: Se True, apenas mostra o que seria copiado sem fazer a cópia
        process_type: Se fornecido, copia apenas regras deste tipo (ex: AGROQUIMICOS)
        source_profile: Profile AWS para acessar a tabela de origem
        target_profile: Profile AWS para acessar a tabela de destino
    """
    source_session = boto3.Session(profile_name=source_profile) if source_profile else boto3.Session()
    target_session = boto3.Session(profile_name=target_profile) if target_profile else boto3.Session()

    source_dynamodb = source_session.resource('dynamodb', region_name=source_region_name)
    target_dynamodb = target_session.resource('dynamodb', region_name=target_region_name)

    source_table = source_dynamodb.Table(source_table_name)
    target_table = target_dynamodb.Table(target_table_name)
    
    # Verificar se as tabelas existem
    print(f"Verificando tabela de origem '{source_table_name}' (região: {source_region_name}, profile: {source_profile or 'default'})...")
    if not check_table_exists(source_table_name, source_dynamodb):
        print(f"❌ Erro: Tabela de origem '{source_table_name}' não encontrada!")
        sys.exit(1)
    
    print(f"Verificando tabela de destino '{target_table_name}' (região: {target_region_name}, profile: {target_profile or 'default'})...")
    if not check_table_exists(target_table_name, target_dynamodb):
        print(f"❌ Erro: Tabela de destino '{target_table_name}' não encontrada!")
        sys.exit(1)
    
    print("✓ Ambas as tabelas existem.\n")
    
    # Tipos de processo possíveis (apenas AGROQUIMICOS e BARTER)
    process_types = [process_type] if process_type else ["AGROQUIMICOS", "BARTER"]
    
    all_items = []
    
    # Buscar regras de cada tipo de processo
    for pt in process_types:
        pk = f"RULES#{pt}"
        print(f"Buscando regras do tipo '{pt}' (PK: {pk})...")
        
        try:
            response = source_table.query(
                KeyConditionExpression=Key('PK').eq(pk) & Key('SK').begins_with('RULE#')
            )
            items = response['Items']
            
            # Paginar se necessário
            while 'LastEvaluatedKey' in response:
                response = source_table.query(
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    KeyConditionExpression=Key('PK').eq(pk) & Key('SK').begins_with('RULE#')
                )
                items.extend(response['Items'])
            
            print(f"  Encontradas {len(items)} regras do tipo '{pt}'")
            all_items.extend(items)
            
        except ClientError as e:
            print(f"  ⚠️  Erro ao buscar regras do tipo '{pt}': {e}")
            continue
    
    if not all_items:
        print("\n❌ Nenhuma regra encontrada na tabela de origem.")
        sys.exit(1)
    
    print(f"\nTotal de regras encontradas: {len(all_items)}")
    
    if dry_run:
        print("\n--- MODO DRY-RUN ---")
        print("Os seguintes itens seriam copiados:")
        for item in all_items:
            pk = item['PK']
            sk = item['SK']
            rule_name = item.get('RULE_NAME', 'N/A')
            order = item.get('ORDER', 'N/A')
            enabled = item.get('ENABLED', 'N/A')
            print(f"  PK: {pk}, SK: {sk}")
            print(f"    → RULE_NAME: {rule_name}, ORDER: {order}, ENABLED: {enabled}")
        print("--------------------")
        return
    
    # Confirmação do usuário
    print(f"\n⚠️  ATENÇÃO: Você está prestes a copiar {len(all_items)} regra(s) de")
    print(f"   '{source_table_name}' para '{target_table_name}'")
    print(f"   Origem  → região: {source_region_name}, profile: {source_profile or 'default'}")
    print(f"   Destino → região: {target_region_name}, profile: {target_profile or 'default'}")
    response = input("\nDeseja continuar? (sim/não): ").strip().lower()
    
    if response not in ['sim', 's', 'yes', 'y']:
        print("Operação cancelada pelo usuário.")
        return
    
    print(f"\nIniciando cópia de {len(all_items)} regra(s) para '{target_table_name}'...")
    copied_count = 0
    skipped_count = 0
    error_count = 0
    
    for item in all_items:
        pk = item['PK']
        sk = item['SK']
        rule_name = item.get('RULE_NAME', 'N/A')
        
        try:
            # Verificar se já existe na tabela de destino
            existing = target_table.get_item(Key={'PK': pk, 'SK': sk})
            if 'Item' in existing:
                print(f"  ⚠️  Já existe: PK={pk}, SK={sk} (RULE_NAME: {rule_name}) - Pulando")
                skipped_count += 1
                continue
            
            # Copiar item
            target_table.put_item(Item=item)
            copied_count += 1
            print(f"  ✓ Copiado: PK={pk}, SK={sk} (RULE_NAME: {rule_name})")
            
        except ClientError as e:
            error_count += 1
            print(f"  ❌ Erro ao copiar item PK={pk}, SK={sk}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Cópia concluída!")
    print(f"  ✓ Copiados: {copied_count}")
    print(f"  ⚠️  Pulados (já existiam): {skipped_count}")
    print(f"  ❌ Erros: {error_count}")
    print(f"  Total processado: {len(all_items)}")
    print(f"{'='*60}")

def main():
    parser = argparse.ArgumentParser(
        description='Copia regras de validação entre tabelas DynamoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Modo dry-run (apenas visualizar)
  python copy_rules.py --source-table source-table --target-table target-table --dry-run
  
  # Copiar todas as regras
  python copy_rules.py --source-table source-table --target-table target-table --source-region us-east-1 --target-region us-east-1
  
  # Copiar apenas regras de um tipo específico
  python copy_rules.py --source-table source-table --target-table target-table --process-type AGROQUIMICOS

  # Copiar entre contas diferentes usando profiles AWS distintos
  python copy_rules.py --source-table source-table --target-table target-table --source-profile conta-origem --target-profile conta-destino
        """
    )
    
    parser.add_argument(
        '--source-table',
        required=True,
        help='Nome da tabela DynamoDB de origem'
    )
    
    parser.add_argument(
        '--target-table',
        required=True,
        help='Nome da tabela DynamoDB de destino'
    )
    
    parser.add_argument(
        '--source-region',
        default='us-east-1',
        help='Região AWS da tabela de origem (padrão: us-east-1)'
    )

    parser.add_argument(
        '--target-region',
        default='us-east-1',
        help='Região AWS da tabela de destino (padrão: us-east-1)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modo dry-run: apenas mostra o que seria copiado sem fazer a cópia'
    )
    
    parser.add_argument(
        '--process-type',
        choices=['AGROQUIMICOS', 'BARTER'],
        help='Copiar apenas regras de um tipo específico (opcional)'
    )

    parser.add_argument(
        '--source-profile',
        help='AWS profile para acessar a conta/tabela de origem (opcional)'
    )

    parser.add_argument(
        '--target-profile',
        help='AWS profile para acessar a conta/tabela de destino (opcional)'
    )
    
    args = parser.parse_args()
    
    try:
        copy_rules(
            args.source_table,
            args.target_table,
            args.source_region,
            args.target_region,
            args.dry_run,
            args.process_type,
            args.source_profile,
            args.target_profile
        )
    except KeyboardInterrupt:
        print("\n\nOperação cancelada pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

