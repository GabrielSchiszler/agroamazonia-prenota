import json
import os
import boto3
import requests
import base64
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
sns_client = boto3.client('sns')

def get_oauth2_token():
    """
    Obtém token de acesso OAuth2 para API do ServiceNow.
    Retorna o access_token ou None em caso de erro.
    """
    auth_url = os.environ.get('OCR_FAILURE_AUTH_URL')
    client_id = os.environ.get('OCR_FAILURE_CLIENT_ID')
    client_secret = os.environ.get('OCR_FAILURE_CLIENT_SECRET')
    username = os.environ.get('OCR_FAILURE_USERNAME')
    password = os.environ.get('OCR_FAILURE_PASSWORD')
    
    if not all([auth_url, client_id, client_secret, username, password]):
        print("WARNING: OAuth2 credentials not fully configured")
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
        
        token_response = response.json()
        access_token = token_response.get('access_token')
        
        return access_token
    except Exception as e:
        print(f"WARNING: Failed to obtain OAuth2 token: {str(e)}")
        return None

def lambda_handler(event, context):
    """
    Envia feedback para o ServiceNow (sucesso ou falha).
    
    Event structure:
    {
        "process_id": "...",
        "success": true/false,
        "details": {
            "status": "SUCCESS" or "FAILURE",
            "protheus_response": {...},  // se sucesso
            "payload_sent": {...},       // payload enviado para Protheus
            "error_details": {...},     // se falha
            "protheus_api_url": "...",
            "timestamp": "..."
        }
    }
    """
    print("="*80)
    print("SEND FEEDBACK - INICIO")
    print("="*80)
    print(f"Event recebido: {json.dumps(event)}")
    
    try:
        process_id = event.get('process_id')
        success = event.get('success', False)
        details = event.get('details', {})
        
        if not process_id:
            print("ERROR: process_id não encontrado no event")
            return {
                'statusCode': 400,
                'message': 'process_id is required'
            }
        
        # Se for falha, enriquecer detalhes com informações do DynamoDB e parsear Cause
        if not success:
            # Tentar parsear Cause se for string JSON (pode estar em error_details.Cause ou diretamente em details)
            cause = None
            if 'error_details' in details and isinstance(details['error_details'], dict) and 'Cause' in details['error_details']:
                cause = details['error_details']['Cause']
            elif 'Cause' in details:
                cause = details['Cause']
            
            if cause and isinstance(cause, str):
                try:
                    cause_parsed = json.loads(cause)
                    if isinstance(cause_parsed, dict):
                        # Mesclar campos do Cause diretamente em details
                        for key, value in cause_parsed.items():
                            if key == 'error_details' and isinstance(value, dict):
                                # Se tem error_details dentro do Cause, mesclar seus campos
                                for error_key, error_value in value.items():
                                    details[error_key] = error_value
                            else:
                                details[key] = value
                        print(f"[FEEDBACK] Cause parseado e mesclado com details")
                except json.JSONDecodeError:
                    print(f"[FEEDBACK] Cause não é JSON válido, mantendo como string")
            
            # Tentar buscar informações completas da requisição Protheus do DynamoDB
            try:
                pk = f"PROCESS#{process_id}"
                response = table.get_item(Key={'PK': pk, 'SK': 'METADATA'})
                if 'Item' in response:
                    metadata = response['Item']
                    
                    # Prioridade 1: Buscar protheus_request_info (contém headers, payload, response)
                    protheus_request_info_str = metadata.get('protheus_request_info')
                    if protheus_request_info_str:
                        try:
                            protheus_request_info = json.loads(protheus_request_info_str) if isinstance(protheus_request_info_str, str) else protheus_request_info_str
                            # Adicionar informações organizadas em details
                            if isinstance(protheus_request_info, dict):
                                # Adicionar informações da requisição Protheus
                                if 'request_payload' in protheus_request_info:
                                    details['request_payload'] = protheus_request_info['request_payload']
                                if 'request_headers' in protheus_request_info:
                                    details['request_headers'] = protheus_request_info['request_headers']
                                if 'request_url' in protheus_request_info:
                                    details['request_url'] = protheus_request_info['request_url']
                                if 'response_status_code' in protheus_request_info:
                                    details['response_status_code'] = protheus_request_info['response_status_code']
                                if 'response_headers' in protheus_request_info:
                                    details['response_headers'] = protheus_request_info['response_headers']
                                if 'response_body' in protheus_request_info:
                                    details['response_body'] = protheus_request_info['response_body']
                                print(f"[FEEDBACK] Informações completas da requisição Protheus recuperadas do DynamoDB")
                        except Exception as parse_err:
                            print(f"[FEEDBACK] WARNING: Erro ao parsear protheus_request_info: {str(parse_err)}")
                    
                    # Fallback: Buscar apenas payload se protheus_request_info não existir
                    if 'request_payload' not in details:
                        payload_saved = metadata.get('protheus_request_payload')
                        if payload_saved:
                            try:
                                payload_parsed = json.loads(payload_saved) if isinstance(payload_saved, str) else payload_saved
                                details['request_payload'] = payload_parsed
                                details['payload_sent'] = payload_parsed
                                print(f"[FEEDBACK] Payload recuperado do DynamoDB (fallback)")
                            except Exception as parse_err:
                                print(f"[FEEDBACK] WARNING: Erro ao parsear payload do DynamoDB: {str(parse_err)}")
            except Exception as db_err:
                print(f"[FEEDBACK] WARNING: Erro ao buscar informações do DynamoDB: {str(db_err)}")
        
        feedback_url = os.environ.get('SERVICENOW_FEEDBACK_API_URL')
        if not feedback_url:
            print("WARNING: SERVICENOW_FEEDBACK_API_URL not configured, skipping feedback")
            return {
                'statusCode': 200,
                'message': 'Feedback URL not configured',
                'skipped': True
            }
        
        # Construir URL completa com process_id
        full_url = f"{feedback_url.rstrip('/')}/{process_id}"
        
        # Obter token OAuth2
        access_token = get_oauth2_token()
        if not access_token:
            print("WARNING: Failed to obtain OAuth2 token, skipping feedback")
            return {
                'statusCode': 200,
                'message': 'Failed to obtain OAuth2 token',
                'skipped': True
            }
        
        # Reorganizar details em estrutura mais clara: payload_req, header_req, response_req
        organized_details = {}
        
        # Extrair informações da requisição Protheus
        payload_req = None
        header_req = None
        response_req = {}
        
        # Lista de headers HTTP conhecidos (para identificar headers de resposta que podem estar no nível raiz)
        http_response_headers = ['Date', 'Content-Type', 'Content-Length', 'Connection', 'Server', 
                                 'Nel', 'Content-Language', 'Vary', 'X-Kong-Upstream-Latency', 
                                 'X-Kong-Proxy-Latency', 'Via', 'Strict-Transport-Security', 
                                 'X-Xss-Protection', 'X-Frame-Options', 'X-Content-Type-Options', 
                                 'Referrer-Policy', 'cf-cache-status', 'Report-To', 'CF-RAY', 'alt-svc']
        
        # Buscar informações do protheus_request_info se disponível
        if 'request_payload' in details:
            payload_req = details.get('request_payload')
        elif 'payload_sent' in details:
            payload_req = details.get('payload_sent')
        
        if 'request_headers' in details:
            header_req = details.get('request_headers')
        
        # Construir response_req com informações da resposta
        response_status_code = details.get('response_status_code')
        response_headers_dict = details.get('response_headers')
        response_body = details.get('response_body')
        
        # Se response_headers não estiver disponível, mas houver headers de resposta no nível raiz, coletá-los
        if not response_headers_dict:
            response_headers_dict = {}
            for key, value in details.items():
                if key in http_response_headers or key.startswith('X-') or key.startswith('CF-'):
                    response_headers_dict[key] = value
        
        if response_status_code or response_headers_dict or response_body:
            response_req = {
                'status_code': response_status_code,
                'headers': response_headers_dict if response_headers_dict else None,
                'body': response_body
            }
        elif 'protheus_response' in details:
            response_req = {
                'body': details.get('protheus_response')
            }
        
        # Adicionar informações organizadas
        if payload_req:
            organized_details['payload_req'] = payload_req
        if header_req:
            organized_details['header_req'] = header_req
        if response_req:
            organized_details['response_req'] = response_req
        
        # Adicionar outros campos importantes (process_id, status, etc) diretamente em details
        # Excluir campos que são headers de resposta ou campos já organizados
        excluded_fields = ['request_payload', 'payload_sent', 'request_headers', 'response_status_code', 
                          'response_headers', 'response_body', 'protheus_response'] + http_response_headers
        
        important_fields = ['process_id', 'process_type', 'status', 'start_time', 'end_time', 
                           'timestamp', 'errorMessage', 'errorType', 'errorCode', 'message', 
                           'cause', 'stackTrace', 'requestId', 'request_url', 'Error', 'state_name']
        
        for field in important_fields:
            if field in details:
                organized_details[field] = details[field]
        
        # Se houver outros campos que não foram organizados e não são headers de resposta, adicionar também
        for key, value in details.items():
            if key not in excluded_fields and key not in important_fields:
                # Verificar se não é um header de resposta
                if not (key in http_response_headers or key.startswith('X-') or key.startswith('CF-')):
                    if key not in organized_details:
                        organized_details[key] = value
        
        payload = {
            "success": success,
            "details": organized_details
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token[:20]}...'  # Log apenas início do token por segurança
        }
        
        print(f"\n[FEEDBACK] Enviando feedback para ServiceNow:")
        print(f"  - URL: {full_url}")
        print(f"  - Success: {success}")
        print(f"  - Details keys: {list(details.keys())}")
        print(f"\n[FEEDBACK] PAYLOAD COMPLETO QUE SERÁ ENVIADO:")
        print(f"{'='*80}")
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        print(f"{'='*80}")
        print(f"\n[FEEDBACK] HEADERS:")
        print(f"{'='*80}")
        for key, value in headers.items():
            if key == 'Authorization':
                print(f"  {key}: {value}")
            else:
                print(f"  {key}: {value}")
        print(f"{'='*80}")
        
        response = requests.post(
            full_url,
            json=payload,
            headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'},
            timeout=30
        )
        
        response.raise_for_status()
        print(f"[FEEDBACK] Feedback enviado para API com sucesso. Status: {response.status_code}")
        
        # Enviar o mesmo payload para SNS
        topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if topic_arn:
            try:
                subject = f'[{"SUCESSO" if success else "FALHA"}] Processo {process_id} - AgroAmazonia'
                sns_response = sns_client.publish(
                    TopicArn=topic_arn,
                    Subject=subject,
                    Message=json.dumps(payload, indent=2, default=str, ensure_ascii=False),
                    MessageAttributes={
                        'notification_type': {'DataType': 'String', 'StringValue': 'SUCCESS' if success else 'FAILURE'},
                        'process_id': {'DataType': 'String', 'StringValue': process_id}
                    }
                )
                print(f"[FEEDBACK] Notificação SNS enviada com o mesmo payload da API. MessageId: {sns_response.get('MessageId')}")
            except Exception as sns_err:
                print(f"[FEEDBACK] WARNING: Erro ao enviar SNS: {str(sns_err)}")
        
        return {
            'statusCode': 200,
            'process_id': process_id,
            'success': success,
            'feedback_sent': True
        }
        
    except requests.exceptions.HTTPError as http_err:
        print(f"\n[FEEDBACK] HTTP Error ao enviar feedback: {http_err}")
        print(f"{'='*80}")
        if http_err.response:
            print(f"  - Status Code: {http_err.response.status_code}")
            print(f"  - Response Headers:")
            for key, value in http_err.response.headers.items():
                print(f"    {key}: {value}")
            print(f"  - Response Body:")
            try:
                response_json = http_err.response.json()
                print(json.dumps(response_json, indent=2, ensure_ascii=False, default=str))
            except:
                response_text = http_err.response.text
                print(f"    {response_text[:1000]}")  # Primeiros 1000 caracteres
                if len(response_text) > 1000:
                    print(f"    ... (truncado, total: {len(response_text)} caracteres)")
        print(f"{'='*80}")
        print(f"\n[FEEDBACK] PAYLOAD QUE FOI TENTADO ENVIAR (para debug):")
        print(f"{'='*80}")
        try:
            if 'payload' in locals():
                print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
            else:
                print("Payload não disponível no escopo")
        except Exception as log_err:
            print(f"Erro ao logar payload: {str(log_err)}")
        print(f"{'='*80}")
        
        # Mesmo com erro na API, enviar o mesmo payload para SNS
        topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if topic_arn and 'payload' in locals():
            try:
                subject = f'[FALHA] Processo {process_id if "process_id" in locals() else "N/A"} - AgroAmazonia'
                sns_response = sns_client.publish(
                    TopicArn=topic_arn,
                    Subject=subject,
                    Message=json.dumps(payload, indent=2, default=str, ensure_ascii=False),
                    MessageAttributes={
                        'notification_type': {'DataType': 'String', 'StringValue': 'FAILURE'},
                        'process_id': {'DataType': 'String', 'StringValue': process_id if 'process_id' in locals() else 'N/A'}
                    }
                )
                print(f"[FEEDBACK] Notificação SNS enviada com o mesmo payload da API (mesmo com erro). MessageId: {sns_response.get('MessageId')}")
            except Exception as sns_err:
                print(f"[FEEDBACK] WARNING: Erro ao enviar SNS: {str(sns_err)}")
        
        # Não re-raise para não bloquear o fluxo
        return {
            'statusCode': 200,
            'process_id': process_id if 'process_id' in locals() else None,
            'success': success if 'success' in locals() else False,
            'feedback_sent': False,
            'error': f'HTTP {http_err.response.status_code if http_err.response else "N/A"}'
        }
    except requests.exceptions.RequestException as req_err:
        print(f"[FEEDBACK] Request Error ao enviar feedback: {req_err}")
        return {
            'statusCode': 200,
            'process_id': process_id if 'process_id' in locals() else None,
            'success': success if 'success' in locals() else False,
            'feedback_sent': False,
            'error': str(req_err)
        }
    except Exception as e:
        print(f"[FEEDBACK] Unexpected error ao enviar feedback: {str(e)}")
        import traceback
        traceback.print_exc()
        # Não re-raise para não bloquear o fluxo
        return {
            'statusCode': 200,
            'process_id': process_id if 'process_id' in locals() else None,
            'success': success if 'success' in locals() else False,
            'feedback_sent': False,
            'error': str(e)
        }
    finally:
        print("="*80)
        print("SEND FEEDBACK - FIM")
        print("="*80)

