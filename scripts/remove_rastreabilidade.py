import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('DocumentProcessorTable')

process_types = ['AGROQUIMICOS', 'SEMENTES', 'FERTILIZANTES']

for process_type in process_types:
    pk = f'RULES#{process_type}'
    sk = 'RULE#validar_rastreabilidade'
    
    try:
        table.delete_item(Key={'PK': pk, 'SK': sk})
        print(f'Removed validar_rastreabilidade from {process_type}')
    except Exception as e:
        print(f'Error removing from {process_type}: {e}')

print('Done!')
