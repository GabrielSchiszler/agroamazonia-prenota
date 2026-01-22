#!/usr/bin/env python3
"""
Script para copiar regras de CFOP x Chave de uma tabela DynamoDB para outra.

Uso:
    python copy_cfop_rules.py --source-table TABELA_ORIGEM --target-table TABELA_DESTINO [--region us-east-1] [--dry-run]

Exemplo:
    python copy_cfop_rules.py --source-table tabela-document-processor-dev --target-table tabela-document-processor-prod
"""

import boto3
import argparse
import sys
from typing import List, Dict, Any
from botocore.exceptions import ClientError

def get_all_cfop_items(table, pk: str) -> List[Dict[str, Any]]:
    """Busca todos os itens relacionados a CFOP_OPERATION"""
    items = []
    
    try:
        # Buscar todos os registros com PK = CFOP_OPERATION
        # Usar query com begins_with para pegar MAPPING# e CFOP#
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': pk}
        )
        
        items.extend(response.get('Items', []))
        
        # Paginar se necessário
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={':pk': pk},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        return items
    except ClientError as e:
        print(f"❌ Erro ao buscar itens da tabela origem: {e}")
        sys.exit(1)

def copy_items(source_table, target_table, items: List[Dict[str, Any]], dry_run: bool = False):
    """Copia itens da tabela origem para a tabela destino"""
    
    if not items:
        print("⚠️  Nenhum item encontrado para copiar")
        return
    
    print(f"\n📋 Resumo dos itens encontrados:")
    mapping_items = [item for item in items if item['SK'].startswith('MAPPING#')]
    cfop_items = [item for item in items if item['SK'].startswith('CFOP#')]
    
    print(f"   • Registros MAPPING#: {len(mapping_items)}")
    print(f"   • Registros CFOP#: {len(cfop_items)}")
    print(f"   • Total: {len(items)}")
    
    if dry_run:
        print(f"\n🔍 DRY RUN - Nenhum item será copiado")
        print(f"\n📝 Itens que seriam copiados:")
        for item in items:
            print(f"   • {item['PK']} / {item['SK']}")
        return
    
    print(f"\n🔄 Iniciando cópia...")
    
    # Primeiro, copiar todos os registros MAPPING# (principais)
    print(f"\n1️⃣  Copiando registros MAPPING# ({len(mapping_items)})...")
    copied_mappings = 0
    for item in mapping_items:
        try:
            # Remover campos internos do DynamoDB se existirem
            item_to_copy = {k: v for k, v in item.items() if not k.startswith('_')}
            
            target_table.put_item(Item=item_to_copy)
            copied_mappings += 1
            print(f"   ✓ {item['SK']} - {item.get('CHAVE', 'N/A')}")
        except ClientError as e:
            print(f"   ❌ Erro ao copiar {item['SK']}: {e}")
    
    # Depois, copiar todos os registros CFOP# (individuais)
    print(f"\n2️⃣  Copiando registros CFOP# ({len(cfop_items)})...")
    copied_cfops = 0
    for item in cfop_items:
        try:
            # Remover campos internos do DynamoDB se existirem
            item_to_copy = {k: v for k, v in item.items() if not k.startswith('_')}
            
            target_table.put_item(Item=item_to_copy)
            copied_cfops += 1
            cfop_code = item['SK'].replace('CFOP#', '')
            print(f"   ✓ {item['SK']} (CFOP: {cfop_code})")
        except ClientError as e:
            print(f"   ❌ Erro ao copiar {item['SK']}: {e}")
    
    print(f"\n✅ Cópia concluída!")
    print(f"   • Registros MAPPING# copiados: {copied_mappings}/{len(mapping_items)}")
    print(f"   • Registros CFOP# copiados: {copied_cfops}/{len(cfop_items)}")
    print(f"   • Total copiado: {copied_mappings + copied_cfops}/{len(items)}")

def verify_tables(source_table, target_table):
    """Verifica se as tabelas existem e são acessíveis"""
    try:
        source_table.load()
        print(f"✓ Tabela origem encontrada: {source_table.table_name}")
    except ClientError as e:
        print(f"❌ Erro ao acessar tabela origem: {e}")
        sys.exit(1)
    
    try:
        target_table.load()
        print(f"✓ Tabela destino encontrada: {target_table.table_name}")
    except ClientError as e:
        print(f"❌ Erro ao acessar tabela destino: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Copia regras de CFOP x Chave entre tabelas DynamoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Dry run (apenas visualizar)
  python copy_cfop_rules.py --source-table tabela-dev --target-table tabela-prod --dry-run
  
  # Copiar de fato
  python copy_cfop_rules.py --source-table tabela-dev --target-table tabela-prod
  
  # Especificar região
  python copy_cfop_rules.py --source-table tabela-dev --target-table tabela-prod --region us-west-2
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
        '--region',
        default='us-east-1',
        help='Região AWS (padrão: us-east-1)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Apenas visualizar o que seria copiado, sem fazer a cópia'
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("🔄 Script de Cópia de Regras CFOP x Chave")
    print("="*80)
    print(f"\n📊 Configuração:")
    print(f"   • Tabela origem: {args.source_table}")
    print(f"   • Tabela destino: {args.target_table}")
    print(f"   • Região: {args.region}")
    print(f"   • Modo: {'DRY RUN (visualização apenas)' if args.dry_run else 'CÓPIA REAL'}")
    
    # Conectar ao DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    source_table = dynamodb.Table(args.source_table)
    target_table = dynamodb.Table(args.target_table)
    
    # Verificar se as tabelas existem
    print(f"\n🔍 Verificando tabelas...")
    verify_tables(source_table, target_table)
    
    # Buscar todos os itens relacionados a CFOP_OPERATION
    print(f"\n📥 Buscando regras na tabela origem...")
    pk = "CFOP_OPERATION"
    items = get_all_cfop_items(source_table, pk)
    
    if not items:
        print("⚠️  Nenhuma regra encontrada na tabela origem")
        sys.exit(0)
    
    # Confirmar antes de copiar (se não for dry-run)
    if not args.dry_run:
        print(f"\n⚠️  ATENÇÃO: Você está prestes a copiar {len(items)} itens para a tabela destino.")
        print(f"   Isso pode sobrescrever dados existentes!")
        resposta = input("   Deseja continuar? (sim/não): ").strip().lower()
        if resposta not in ['sim', 's', 'yes', 'y']:
            print("❌ Operação cancelada pelo usuário")
            sys.exit(0)
    
    # Copiar itens
    copy_items(source_table, target_table, items, dry_run=args.dry_run)
    
    print("\n" + "="*80)
    print("✅ Processo finalizado!")
    print("="*80)

if __name__ == '__main__':
    main()

