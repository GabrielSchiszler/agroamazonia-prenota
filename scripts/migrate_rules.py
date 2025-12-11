#!/usr/bin/env python3
import boto3
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

# Regras padrão por tipo de processo
DEFAULT_RULES = {
    'AGROQUIMICOS': [
        {'rule_name': 'validar_numero_nota', 'order': 1},
        {'rule_name': 'validar_serie', 'order': 2},
        {'rule_name': 'validar_data_emissao', 'order': 3},
        {'rule_name': 'validar_cnpj_fornecedor', 'order': 4},
        {'rule_name': 'validar_produtos', 'order': 5},
        {'rule_name': 'validar_rastreabilidade', 'order': 6},
        {'rule_name': 'validar_icms', 'order': 7}
    ],
    'SEMENTES': [
        {'rule_name': 'validar_numero_nota', 'order': 1},
        {'rule_name': 'validar_serie', 'order': 2},
        {'rule_name': 'validar_data_emissao', 'order': 3},
        {'rule_name': 'validar_cnpj_fornecedor', 'order': 4},
        {'rule_name': 'validar_produtos', 'order': 5},
        {'rule_name': 'validar_rastreabilidade', 'order': 6}
    ],
    'FERTILIZANTES': [
        {'rule_name': 'validar_numero_nota', 'order': 1},
        {'rule_name': 'validar_serie', 'order': 2},
        {'rule_name': 'validar_data_emissao', 'order': 3},
        {'rule_name': 'validar_cnpj_fornecedor', 'order': 4},
        {'rule_name': 'validar_produtos', 'order': 5}
    ]
}

def delete_old_rules():
    """Apaga regras antigas (RULES=)"""
    print("Buscando regras antigas...")
    
    for process_type in ['AGROQUIMICOS', 'SEMENTES', 'FERTILIZANTES']:
        old_pk = f'RULES={process_type}'
        
        try:
            response = table.query(
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={':pk': old_pk}
            )
            
            items = response.get('Items', [])
            print(f"Encontradas {len(items)} regras antigas para {process_type}")
            
            for item in items:
                table.delete_item(Key={'PK': item['PK'], 'SK': item['SK']})
                print(f"  Deletada: {item['SK']}")
        
        except Exception as e:
            print(f"Erro ao deletar regras antigas de {process_type}: {e}")

def create_new_rules():
    """Cria regras novas (RULES#)"""
    print("\nCriando regras novas...")
    
    for process_type, rules in DEFAULT_RULES.items():
        pk = f'RULES#{process_type}'
        print(f"\nCriando regras para {process_type}:")
        
        for rule in rules:
            sk = f"RULE#{rule['rule_name']}"
            
            table.put_item(Item={
                'PK': pk,
                'SK': sk,
                'RULE_NAME': rule['rule_name'],
                'ORDER': rule['order'],
                'ENABLED': True
            })
            
            print(f"  ✓ {rule['rule_name']} (ordem {rule['order']})")

if __name__ == '__main__':
    print("=== Migração de Regras ===\n")
    delete_old_rules()
    create_new_rules()
    print("\n✓ Migração concluída!")
