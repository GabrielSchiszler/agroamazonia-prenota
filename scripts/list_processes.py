#!/usr/bin/env python3
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

try:
    response = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={
            ':pk': 'PROCESS',
            ':sk': 'PROCESS#'
        }
    )
    
    print("Processos encontrados:\n")
    for item in response['Items']:
        process_id = item.get('PROCESS_ID')
        print(f"ID: {process_id}")
    
    if response['Items']:
        print(f"\nUse: python3 check_sctask.py {response['Items'][0]['PROCESS_ID']}")
    
except Exception as e:
    print(f"Erro: {e}")
