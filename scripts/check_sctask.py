#!/usr/bin/env python3
import boto3
import sys

if len(sys.argv) < 2:
    print("Uso: python3 check_sctask.py <process_id>")
    sys.exit(1)

process_id = sys.argv[1]

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

try:
    response = table.get_item(Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'})
    
    if 'Item' in response:
        item = response['Item']
        print(f"Status: {item.get('STATUS', 'N/A')}")
        print(f"SCTASK ID: {item.get('sctask_id', 'NÃO ENCONTRADO')}")
        print(f"\nTodos os campos:")
        for key, value in item.items():
            print(f"  {key}: {value}")
    else:
        print(f"Processo {process_id} não encontrado")
except Exception as e:
    print(f"Erro: {e}")
