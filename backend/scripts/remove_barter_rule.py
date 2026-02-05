#!/usr/bin/env python3
"""
Script para remover a regra validar_barter do DynamoDB.

Uso:
    python remove_barter_rule.py --table TABLE_NAME [--region us-east-1]
    
Exemplo:
    python remove_barter_rule.py --table tabela-document-processor-dev
"""

import boto3
import argparse
import sys
from botocore.exceptions import ClientError


def remove_barter_rule(table_name, region_name):
    """
    Remove a regra validar_barter do DynamoDB.
    
    Args:
        table_name: Nome da tabela DynamoDB
        region_name: Região AWS
    """
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    table = dynamodb.Table(table_name)
    
    # Verificar se a tabela existe
    print(f"Verificando tabela '{table_name}'...")
    try:
        table.load()
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"❌ Erro: Tabela '{table_name}' não encontrada!")
            sys.exit(1)
        raise
    
    print("✓ Tabela encontrada.\n")
    
    # Chave da regra a ser removida
    pk = 'RULES#BARTER'
    sk = 'RULE#validar_barter'
    
    print(f"Removendo regra validar_barter...")
    print(f"  PK: {pk}")
    print(f"  SK: {sk}")
    
    try:
        # Verificar se a regra existe
        response = table.get_item(Key={'PK': pk, 'SK': sk})
        
        if 'Item' in response:
            # Remover a regra
            table.delete_item(Key={'PK': pk, 'SK': sk})
            print(f"✓ Regra validar_barter removida com sucesso!")
        else:
            print(f"ℹ️  Regra validar_barter não encontrada no DynamoDB (já foi removida ou nunca existiu)")
            
    except ClientError as e:
        print(f"❌ Erro ao remover regra: {e}")
        sys.exit(1)
    
    # Listar regras restantes para BARTER
    print(f"\nRegras restantes para BARTER:")
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('PK').eq('RULES#BARTER')
    )
    
    remaining_rules = response.get('Items', [])
    if remaining_rules:
        for item in remaining_rules:
            rule_name = item.get('RULE_NAME') or item.get('rule_name')
            print(f"  - {rule_name}")
    else:
        print(f"  (Nenhuma regra configurada para BARTER)")


def main():
    parser = argparse.ArgumentParser(
        description='Remove a regra validar_barter do DynamoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Remover regra validar_barter
  python remove_barter_rule.py --table tabela-document-processor-dev
  
  # Especificar região
  python remove_barter_rule.py --table tabela-document-processor-dev --region us-east-1
        """
    )
    
    parser.add_argument(
        '--table',
        required=True,
        help='Nome da tabela DynamoDB'
    )
    
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='Região AWS (padrão: us-east-1)'
    )
    
    args = parser.parse_args()
    
    try:
        remove_barter_rule(args.table, args.region)
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

