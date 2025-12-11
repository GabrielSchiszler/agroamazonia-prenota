#!/usr/bin/env python3
import boto3
import sys

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

process_type = 'AGROQUIMICOS'
rule_name = 'validar_numero_nota'

pk = f'RULES#{process_type}'
sk = f'RULE#{rule_name}'

try:
    table.delete_item(Key={'PK': pk, 'SK': sk})
    print(f"✓ Regra '{rule_name}' desativada para {process_type}")
except Exception as e:
    print(f"✗ Erro: {e}")
    sys.exit(1)
