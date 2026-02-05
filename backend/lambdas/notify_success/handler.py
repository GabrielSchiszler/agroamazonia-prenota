import json
import os
import boto3
import logging
import requests
import base64
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
sns_client = boto3.client('sns')

def get_oauth2_token():
    """Obtém token OAuth2 para API do ServiceNow"""
    auth_url = os.environ.get('OCR_FAILURE_AUTH_URL')
    client_id = os.environ.get('OCR_FAILURE_CLIENT_ID')
    client_secret = os.environ.get('OCR_FAILURE_CLIENT_SECRET')
    username = os.environ.get('OCR_FAILURE_USERNAME')
    password = os.environ.get('OCR_FAILURE_PASSWORD')
    
    if not all([auth_url, client_id, client_secret, username, password]):
        return None
    
    try:
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'grant_type': 'password',
            'username': username,
            'password': password
        }
        response = requests.post(auth_url, data=data, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json().get('access_token')
    except Exception as e:
        logger.warning(f"Failed to obtain OAuth2 token: {str(e)}")
        return None

def send_feedback_to_api(process_id, success, details):
    """Envia feedback para API do ServiceNow"""
    feedback_url = os.environ.get('SERVICENOW_FEEDBACK_API_URL')
    if not feedback_url:
        return False
    
    try:
        access_token = get_oauth2_token()
        if not access_token:
            return False
        
        full_url = f"{feedback_url.rstrip('/')}/{process_id}"
        payload = {"success": success, "details": details}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        response = requests.post(full_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"Feedback enviado para API com sucesso. Status: {response.status_code}")
        return True
    except Exception as e:
        logger.warning(f"Erro ao enviar feedback para API: {str(e)}")
        return False


def lambda_handler(event, context):
    """
    Envia notificação SNS quando um processo é concluído com sucesso.
    Inclui o payload enviado para o Protheus e detalhes do processo.
    """
    logger.info(f"NotifySuccess - Event recebido: {json.dumps(event, default=str)}")
    
    process_id = event.get('process_id')
    protheus_result = event.get('protheus_result', {})
    
    # Extrair Payload do protheus_result se existir (vem do Step Functions)
    if protheus_result and 'Payload' in protheus_result:
        protheus_result = protheus_result['Payload']
    
    if not process_id:
        logger.error("process_id não encontrado no evento")
        return {
            'statusCode': 400,
            'message': 'process_id é obrigatório'
        }
    
    # Buscar dados completos do processo no DynamoDB
    pk = f"PROCESS#{process_id}"
    
    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': pk}
        )
        
        items = {item['SK']: item for item in response.get('Items', [])}
        metadata = items.get('METADATA', {})
        
        # Extrair informações relevantes
        process_type = metadata.get('PROCESS_TYPE', 'UNKNOWN')
        start_time = metadata.get('START_TIME', '')
        protheus_response_str = metadata.get('protheus_response', '{}')
        id_unico = metadata.get('id_unico', '')
        
        # Tentar parsear resposta do Protheus
        try:
            protheus_response = json.loads(protheus_response_str) if isinstance(protheus_response_str, str) else protheus_response_str
        except:
            protheus_response = protheus_response_str
        
        # Buscar payload enviado ao Protheus (do pedido de compra ou input_json)
        payload_enviado = None
        
        # Tentar buscar do PEDIDO_COMPRA_METADATA
        pedido_compra_item = items.get('PEDIDO_COMPRA_METADATA')
        if pedido_compra_item:
            metadados = pedido_compra_item.get('METADADOS')
            if metadados:
                try:
                    payload_enviado = json.loads(metadados) if isinstance(metadados, str) else metadados
                except:
                    pass
        
        # Se não encontrou, tentar buscar dos arquivos
        if not payload_enviado:
            for sk, item in items.items():
                if sk.startswith('FILE#'):
                    file_metadata = item.get('METADADOS')
                    if file_metadata:
                        try:
                            parsed = json.loads(file_metadata) if isinstance(file_metadata, str) else file_metadata
                            if isinstance(parsed, dict) and ('header' in parsed or 'requestBody' in parsed):
                                payload_enviado = parsed
                                break
                        except:
                            pass
        
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Extrair código de status do protheus_result
        codigo_status = None
        if protheus_result:
            codigo_status = protheus_result.get('codigoStatus') or protheus_result.get('protheus_response', {}).get('codigoStatus')
        
        # Buscar informações completas da requisição Protheus (headers, payload, response)
        protheus_request_info = None
        try:
            protheus_request_info_str = metadata.get('protheus_request_info')
            if protheus_request_info_str:
                protheus_request_info = json.loads(protheus_request_info_str) if isinstance(protheus_request_info_str, str) else protheus_request_info_str
        except Exception as parse_err:
            logger.warning(f"Erro ao parsear protheus_request_info: {str(parse_err)}")
        
        # Se não encontrou protheus_request_info, tentar buscar do protheus_request_payload
        if not protheus_request_info:
            protheus_request_payload_str = metadata.get('protheus_request_payload')
            if protheus_request_payload_str:
                try:
                    protheus_request_payload = json.loads(protheus_request_payload_str) if isinstance(protheus_request_payload_str, str) else protheus_request_payload_str
                    protheus_request_info = {
                        'request_payload': protheus_request_payload
                    }
                except:
                    pass
        
        # Reorganizar details em estrutura mais clara: payload_req, header_req, response_req
        organized_details = {
            'process_id': process_id,
            'process_type': process_type,
            'status': 'SUCCESS',
            'start_time': start_time,
            'end_time': timestamp,
            'id_unico': id_unico,
            'codigo_status': codigo_status
        }
        
        # Extrair informações da requisição Protheus
        if protheus_request_info:
            # Payload da requisição
            payload_req = protheus_request_info.get('request_payload') or payload_enviado
            if payload_req:
                organized_details['payload_req'] = payload_req
            
            # Headers da requisição
            header_req = protheus_request_info.get('request_headers')
            if header_req:
                organized_details['header_req'] = header_req
            
            # Resposta da requisição
            response_req = {
                'status_code': protheus_request_info.get('response_status_code'),
                'headers': protheus_request_info.get('response_headers'),
                'body': protheus_request_info.get('response_body') or protheus_response
            }
            organized_details['response_req'] = response_req
            
            # URL da requisição
            if protheus_request_info.get('request_url'):
                organized_details['request_url'] = protheus_request_info.get('request_url')
        else:
            # Fallback: usar informações disponíveis mesmo sem protheus_request_info
            if payload_enviado:
                organized_details['payload_req'] = payload_enviado
            if protheus_response:
                organized_details['response_req'] = {
                    'body': protheus_response
                }
        
        # Construir payload no formato da API (mesmo formato que será enviado para SNS)
        api_payload = {
            "success": True,
            "details": organized_details
        }
        
        # Enviar feedback para API
        logger.info("Enviando feedback de sucesso para API do ServiceNow...")
        send_feedback_to_api(process_id, True, details)
        
        # Enviar o mesmo payload para SNS
        topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if topic_arn:
            try:
                sns_response = sns_client.publish(
                    TopicArn=topic_arn,
                    Subject=f'[SUCESSO] Processo {process_id} concluído - AgroAmazonia',
                    Message=json.dumps(api_payload, indent=2, default=str, ensure_ascii=False),
                    MessageAttributes={
                        'notification_type': {'DataType': 'String', 'StringValue': 'SUCCESS'},
                        'process_id': {'DataType': 'String', 'StringValue': process_id},
                        'process_type': {'DataType': 'String', 'StringValue': process_type}
                    }
                )
                logger.info(f"Notificação SNS enviada com o mesmo payload da API. MessageId: {sns_response.get('MessageId')}")
            except Exception as sns_err:
                logger.warning(f"Erro ao enviar SNS: {str(sns_err)}")
        
        return {
            'statusCode': 200,
            'process_id': process_id,
            'notification_sent': True
        }
        
    except Exception as e:
        logger.error(f"Erro ao enviar notificação de sucesso: {str(e)}")
        logger.exception("Full traceback:")
        # Não falhar o processo por causa de notificação
        return {
            'statusCode': 500,
            'process_id': process_id,
            'notification_sent': False,
            'message': f'Erro ao enviar notificação: {str(e)}'
        }

