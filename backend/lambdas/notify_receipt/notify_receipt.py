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
    
    # Retornar dados para próximo step
    result = {
        'process_id': process_id,
        'process_type': process_type,
        'files': files,
        'status': 'NOTIFIED'
    }
    
    logger.info(f"Returning: {json.dumps(result)}")
    return result
