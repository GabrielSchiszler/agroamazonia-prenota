import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def handler(event, context):
    """Atualiza status do arquivo quando upload é concluído no S3"""
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info(f"File uploaded: {key}")
        
        # Extrair process_id e file_name do key: processes/{process_id}/{file_name}
        parts = key.split('/')
        if len(parts) >= 3 and parts[0] == 'processes':
            process_id = parts[1]
            file_name = parts[2]
            
            pk = f"PROCESS={process_id}"
            
            # Buscar arquivo com status PENDING
            response = table.query(
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={':pk': pk}
            )
            
            for item in response.get('Items', []):
                if 'FILE=' in item['SK'] and file_name in item['SK']:
                    # Atualizar status para UPLOADED
                    table.update_item(
                        Key={'PK': pk, 'SK': item['SK']},
                        UpdateExpression='SET #status = :status',
                        ExpressionAttributeNames={'#status': 'STATUS'},
                        ExpressionAttributeValues={':status': 'UPLOADED'}
                    )
                    
                    logger.info(f"Updated file status: {file_name} -> UPLOADED")
                    break
    
    return {'statusCode': 200}
