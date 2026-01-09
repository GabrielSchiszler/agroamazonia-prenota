#!/usr/bin/env python3
"""
Script para adicionar a regra validar_cfop_chave no DynamoDB
"""
import os
import sys
import boto3
from decimal import Decimal

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configurar DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = os.environ.get('TABLE_NAME', 'agroamazonia-table')
table = dynamodb.Table(table_name)

# Tipos de processo que devem ter a regra
PROCESS_TYPES = ['AGROQUIMICOS', 'SEMENTES', 'FERTILIZANTES']

def add_rule_to_process_type(process_type, rule_name, order, enabled=True):
    """Adiciona uma regra a um tipo de processo"""
    pk = f"RULES#{process_type}"
    sk = f"RULE#{rule_name}"
    
    try:
        # Verificar se a regra já existe
        response = table.get_item(Key={'PK': pk, 'SK': sk})
        
        if 'Item' in response:
            print(f"⚠️  Regra {rule_name} já existe para {process_type}, atualizando...")
            # Atualizar regra existente (usar campos em maiúsculas como no RulesService)
            table.update_item(
                Key={'PK': pk, 'SK': sk},
                UpdateExpression='SET #order = :order, #enabled = :enabled',
                ExpressionAttributeNames={
                    '#order': 'ORDER',
                    '#enabled': 'ENABLED'
                },
                ExpressionAttributeValues={
                    ':order': Decimal(str(order)),
                    ':enabled': enabled
                }
            )
            print(f"✅ Regra {rule_name} atualizada para {process_type} (order: {order}, enabled: {enabled})")
        else:
            # Criar nova regra (usar campos em maiúsculas como no RulesService)
            table.put_item(Item={
                'PK': pk,
                'SK': sk,
                'RULE_NAME': rule_name,
                'ORDER': Decimal(str(order)),
                'ENABLED': enabled
            })
            print(f"✅ Regra {rule_name} criada para {process_type} (order: {order}, enabled: {enabled})")
            
    except Exception as e:
        print(f"❌ Erro ao adicionar regra {rule_name} para {process_type}: {e}")
        raise

def main():
    """Adiciona a regra validar_cfop_chave para todos os tipos de processo"""
    print("🌱 Adicionando regra validar_cfop_chave...")
    print(f"📋 Tipos de processo: {', '.join(PROCESS_TYPES)}\n")
    
    rule_name = 'validar_cfop_chave'
    order = 6  # Ordem de execução (pode ser ajustada)
    enabled = True
    
    success_count = 0
    error_count = 0
    
    for process_type in PROCESS_TYPES:
        try:
            add_rule_to_process_type(process_type, rule_name, order, enabled)
            success_count += 1
        except Exception as e:
            print(f"❌ Falha ao adicionar regra para {process_type}: {e}")
            error_count += 1
    
    print(f"\n📊 Resumo:")
    print(f"   ✅ Sucessos: {success_count}")
    print(f"   ❌ Erros: {error_count}")
    print("\n✨ Concluído!")
    
    if error_count == 0:
        print("\n💡 A regra validar_cfop_chave foi adicionada com sucesso!")
        print("   Ela será executada durante a validação para buscar a chave correspondente ao CFOP do DANFE.")

if __name__ == '__main__':
    main()

