import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

process_types = ['AGROQUIMICOS', 'SEMENTES', 'FERTILIZANTES']

for process_type in process_types:
    pk = f'RULES#{process_type}'
    sk = 'RULE#validar_numero_pedido'
    
    table.put_item(Item={
        'PK': pk,
        'SK': sk,
        'RULE_NAME': 'validar_numero_pedido',
        'RULE_MODULE': 'validar_numero_pedido',
        'RULE_ORDER': 3,
        'ENABLED': True
    })
    
    print(f'Added validar_numero_pedido to {process_type}')

print('Done!')
