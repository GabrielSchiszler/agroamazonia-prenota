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
            
            pk = f"PROCESS#{process_id}"
            
            response = table.query(
                KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
                ExpressionAttributeValues={':pk': pk, ':sk': 'FILE#'}
            )
            
            for item in response.get('Items', []):
                if item.get('FILE_KEY') == key:
                    table.update_item(
                        Key={'PK': pk, 'SK': item['SK']},
                        UpdateExpression='SET #status = :status',
                        ExpressionAttributeNames={'#status': 'STATUS'},
                        ExpressionAttributeValues={':status': 'UPLOADED'}
                    )
                    logger.info(f"Updated file status: {key} -> UPLOADED")
                    break
    
    return {'statusCode': 200}
