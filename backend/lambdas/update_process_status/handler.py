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
    """
    Atualiza o status do processo para FAILED e salva informações do erro.
    Usado quando há erros em qualquer etapa do processamento.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Extrair informações do evento
    # Quando vem do Step Functions Catch, o process_id está no nível raiz do evento
    process_id = event.get('process_id') or event.get('processId')
    error_message = event.get('error_message') or event.get('errorMessage') or 'Erro desconhecido'
    error_type = event.get('error_type') or event.get('errorType') or 'UNKNOWN_ERROR'
    lambda_name = event.get('lambda_name') or event.get('lambdaName') or 'unknown'
    
    # Se o evento vem do Step Functions Catch, extrair informações do erro
    protheus_cause = None
    if 'error' in event:
        error_info = event.get('error', {})
        if isinstance(error_info, dict):
            # Step Functions Catch coloca Error e Cause no objeto error
            error_message = error_info.get('Error', error_info.get('Cause', error_message))
            # Tentar extrair do JSON string se Error ou Cause for uma string JSON
            if isinstance(error_message, str):
                try:
                    error_json = json.loads(error_message)
                    if isinstance(error_json, dict):
                        error_message = error_json.get('errorMessage', error_json.get('error', error_message))
                        # Extrair campo "cause" do Protheus se existir no error_details
                        if 'error_details' in error_json:
                            error_details = error_json.get('error_details', {})
                            if isinstance(error_details, dict) and 'cause' in error_details:
                                protheus_cause = error_details.get('cause')
                except:
                    pass
            # O process_id deve estar no nível raiz do evento (Step Functions mantém variáveis do contexto)
            # Se não encontrou, tentar extrair do error_info (pode estar em alguns casos)
            if not process_id:
                process_id = error_info.get('process_id')
    
    if not process_id:
        logger.error("process_id não encontrado no evento")
        return {
            'statusCode': 400,
            'message': 'process_id é obrigatório'
        }
    
    pk = f"PROCESS#{process_id}"
    sk = "METADATA"
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    try:
        # Verificar se o item existe
        response = table.get_item(Key={'PK': pk, 'SK': sk})
        
        error_info = {
            'message': str(error_message),
            'type': error_type,
            'timestamp': timestamp,
            'lambda': lambda_name
        }
        
        # Incluir campo "cause" do Protheus se disponível
        if protheus_cause is not None:
            error_info['protheus_cause'] = protheus_cause
        
        if 'Item' in response:
            # Atualizar item existente
            table.update_item(
                Key={'PK': pk, 'SK': sk},
                UpdateExpression='SET #status = :status, error_info = :error, updated_at = :timestamp',
                ExpressionAttributeNames={
                    '#status': 'STATUS'
                },
                ExpressionAttributeValues={
                    ':status': 'FAILED',
                    ':error': error_info,
                    ':timestamp': timestamp
                }
            )
            logger.info(f"Status atualizado para FAILED para processo {process_id}")
        else:
            # Criar item se não existir
            table.put_item(Item={
                'PK': pk,
                'SK': sk,
                'STATUS': 'FAILED',
                'PROCESS_ID': process_id,
                'error_info': error_info,
                'updated_at': timestamp
            })
            logger.info(f"Item METADATA criado com status FAILED para processo {process_id}")
        
        result = {
            'statusCode': 200,
            'process_id': process_id,
            'status': 'FAILED',
            'message': 'Status atualizado com sucesso',
            'error_info': error_info
        }
        
        # Incluir protheus_cause no resultado se disponível
        if protheus_cause is not None:
            result['protheus_cause'] = protheus_cause
        
        return result
        
    except Exception as e:
        logger.error(f"Erro ao atualizar status: {str(e)}")
        logger.exception("Full traceback:")
        return {
            'statusCode': 500,
            'process_id': process_id,
            'message': f'Erro ao atualizar status: {str(e)}'
        }

