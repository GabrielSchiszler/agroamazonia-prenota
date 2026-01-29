import os
import json
import boto3
import logging
from datetime import datetime, timezone

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
    # Usar timezone UTC explicitamente para garantir consistência
    # Formato ISO 8601 com 'Z' no final para facilitar parse posterior
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc.isoformat().replace('+00:00', 'Z')
    timestamp = int(now_utc.timestamp())
    
    pk = f"PROCESS#{process_id}"
    
    # Salvar start_time no DynamoDB
    # Usar put_item com condição para garantir que o item seja criado/atualizado
    # Isso garante que START_TIME seja sempre salvo, mesmo se METADATA não existir ainda
    try:
        # Primeiro, tentar buscar o item existente para preservar outros campos
        try:
            response = table.get_item(
                Key={'PK': pk, 'SK': 'METADATA'}
            )
            existing_item = response.get('Item', {})
            
            # Se o item existe, fazer update preservando campos existentes
            if existing_item:
                table.update_item(
                    Key={'PK': pk, 'SK': 'METADATA'},
                    UpdateExpression='SET START_TIME = :start_time, TIMESTAMP = :timestamp',
                    ExpressionAttributeValues={
                        ':start_time': start_time,
                        ':timestamp': timestamp
                    }
                )
                logger.info(f"Updated start_time {start_time} in existing METADATA for process {process_id}")
            else:
                # Se não existe, criar o item com START_TIME e outros campos básicos
                table.put_item(
                    Item={
                        'PK': pk,
                        'SK': 'METADATA',
                        'START_TIME': start_time,
                        'TIMESTAMP': timestamp,
                        'STATUS': 'NOTIFIED',
                        'PROCESS_TYPE': process_type
                    }
                )
                logger.info(f"Created METADATA with start_time {start_time} for process {process_id}")
        except Exception as get_error:
            # Se falhar ao buscar, tentar criar diretamente
            logger.warning(f"Failed to get existing METADATA, creating new: {get_error}")
            table.put_item(
                Item={
                    'PK': pk,
                    'SK': 'METADATA',
                    'START_TIME': start_time,
                    'TIMESTAMP': timestamp,
                    'STATUS': 'NOTIFIED',
                    'PROCESS_TYPE': process_type
                }
            )
            logger.info(f"Created METADATA with start_time {start_time} for process {process_id} (fallback)")
    except Exception as e:
        logger.error(f"Failed to save start_time to DynamoDB: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Continuar mesmo se falhar, mas logar o erro
    
    # Retornar apenas process_id para próximo step
    return {
        'process_id': process_id
    }
