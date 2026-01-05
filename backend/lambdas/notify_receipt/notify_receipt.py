import os
import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def handler(event, context):
    """Notifica recebimento e inicia processamento"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    process_type = event['process_type']
    files = event.get('files', [])
    
    logger.info(f"Processing {len(files)} files for process {process_id}")
    
    # Salvar timestamp de início no DynamoDB para uso posterior nas métricas
    start_time = datetime.utcnow().isoformat() + 'Z'
    timestamp = int(datetime.utcnow().timestamp())
    
    pk = f"PROCESS#{process_id}"
    
    # Salvar start_time no DynamoDB
    try:
        table.update_item(
            Key={'PK': pk, 'SK': 'METADATA'},
            UpdateExpression='SET START_TIME = :start_time, TIMESTAMP = :timestamp',
            ExpressionAttributeValues={
                ':start_time': start_time,
                ':timestamp': timestamp
            }
        )
        logger.info(f"Saved start_time {start_time} to DynamoDB for process {process_id}")
    except Exception as e:
        logger.error(f"Failed to save start_time to DynamoDB: {e}")
        # Continuar mesmo se falhar, mas logar o erro
    
    # Retornar apenas dados necessários para próximos steps
    result = {
        'process_id': process_id,
        'process_type': process_type,
        'files': files,
        'status': 'NOTIFIED'
    }
    
    logger.info(f"Returning: {json.dumps(result)}")
    return result
