import boto3
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('DocumentProcessorTable')

process_types = ['SEMENTES', 'AGROQUIMICOS', 'FERTILIZANTES']

for process_type in process_types:
    pk = f'RULES#{process_type}'
    sk = 'RULE#validar_numero_pedido'
    
    try:
        table.update_item(
            Key={'PK': pk, 'SK': sk},
            UpdateExpression='SET enabled = :val',
            ExpressionAttributeValues={':val': False}
        )
        print(f"✓ Desativada regra validar_numero_pedido para {process_type}")
    except Exception as e:
        print(f"✗ Erro ao desativar regra para {process_type}: {e}")

print("\nConcluído!")
