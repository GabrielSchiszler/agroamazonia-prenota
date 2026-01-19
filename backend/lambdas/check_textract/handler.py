import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def handler(event, context):
    """Verifica se Textract já foi processado"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    files = event.get('files', [])
    
    pk = f"PROCESS#{process_id}"
    
    # Buscar dados do Textract existentes
    existing = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={':pk': pk, ':sk': 'TEXTRACT='}
    )['Items']
    
    existing_files = {item['FILE_NAME'] for item in existing}
    
    # Filtrar apenas arquivos que ainda não foram processados
    files_to_process = []
    for file in files:
        if file['FILE_NAME'] not in existing_files:
            files_to_process.append(file)
    
    logger.info(f"Total files: {len(files)}, Already processed: {len(existing_files)}, To process: {len(files_to_process)}")
    
    return {
        'process_id': process_id,
        'process_type': event.get('process_type'),
        'files': files_to_process,
        'already_processed': len(existing_files),
        'needs_textract': len(files_to_process) > 0
    }
