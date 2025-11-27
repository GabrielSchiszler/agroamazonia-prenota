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
    """Processa e persiste resultados do Textract"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    process_type = event['process_type']
    textract_results = event.get('textract_results', [])
    
    logger.info(f"Processing {len(textract_results)} Textract results")
    
    timestamp = int(datetime.now().timestamp())
    pk = f"PROCESS={process_id}"
    
    # Persiste resultados do Textract
    for result in textract_results:
        sk = f"TEXTRACT={result['file_name']}"
        
        table.put_item(Item={
            'PK': pk,
            'SK': sk,
            'FILE_NAME': result['file_name'],
            'FILE_KEY': result['file_key'],
            'JOB_ID': result['job_id'],
            'TABLE_COUNT': len(result.get('tables', [])),
            'TABLES_DATA': json.dumps(result.get('tables', [])),
            'TIMESTAMP': timestamp
        })
        
        logger.info(f"Saved Textract result for {result['file_name']}")
    
    # Atualiza status do processo para COMPLETED
    items = table.query(
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={':pk': pk}
    )['Items']
    
    for item in items:
        if 'METADATA=' in item['SK']:
            table.update_item(
                Key={'PK': pk, 'SK': item['SK']},
                UpdateExpression='SET #status = :status',
                ExpressionAttributeNames={'#status': 'STATUS'},
                ExpressionAttributeValues={':status': 'COMPLETED'}
            )
            logger.info(f"Updated process status to COMPLETED")
            break
    
    result = {
        'process_id': process_id,
        'status': 'COMPLETED',
        'results_count': len(textract_results),
        'timestamp': timestamp
    }
    
    logger.info(f"Returning: {json.dumps(result)}")
    return result
