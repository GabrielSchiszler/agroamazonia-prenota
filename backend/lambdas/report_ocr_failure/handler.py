import json
import os
import boto3
import requests
import random
import base64
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def get_oauth2_token():
    """
    Obtém token de acesso OAuth2 usando password credentials grant.
    Retorna o access_token ou None em caso de erro.
    """
    auth_url = os.environ.get('OCR_FAILURE_AUTH_URL')
    client_id = os.environ.get('OCR_FAILURE_CLIENT_ID')
    client_secret = os.environ.get('OCR_FAILURE_CLIENT_SECRET')
    username = os.environ.get('OCR_FAILURE_USERNAME')
    password = os.environ.get('OCR_FAILURE_PASSWORD')
    
    if not all([auth_url, client_id, client_secret, username, password]):
        print("ERROR: Missing OAuth2 credentials in environment variables")
        return None
    
    try:
        # ServiceNow OAuth2 pode aceitar client credentials de duas formas:
        # 1. Basic Auth no header (Authorization: Basic base64(client_id:client_secret))
        # 2. client_id e client_secret no body
        # Vamos tentar ambas as abordagens
        
        # Abordagem 1: Basic Auth no header (padrão OAuth2)
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers_basic = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Abordagem 2: client_id e client_secret no body
        headers_body = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Tentar diferentes combinações
        # grant_type pode ser "password" (padrão OAuth2) ou "password_credentials" (ServiceNow)
        approaches = [
            {
                'name': 'Basic Auth + password',
                'headers': headers_basic,
                'data': {
                    'grant_type': 'password',
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Basic Auth + password_credentials',
                'headers': headers_basic,
                'data': {
                    'grant_type': 'password_credentials',
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Body Auth + password',
                'headers': headers_body,
                'data': {
                    'grant_type': 'password',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Body Auth + password_credentials',
                'headers': headers_body,
                'data': {
                    'grant_type': 'password_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'username': username,
                    'password': password
                }
            }
        ]
        
        response = None
        last_error = None
        
        for approach in approaches:
            try:
                print(f"Trying approach: {approach['name']}")
                print(f"URL: {auth_url}")
                
                response = requests.post(
                    auth_url, 
                    data=approach['data'], 
                    headers=approach['headers'], 
                    timeout=30
                )
                
                print(f"Response status: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"✓ Success with approach: {approach['name']}")
                    break
                else:
                    print(f"✗ Failed with approach: {approach['name']}")
                    print(f"Response body: {response.text[:300]}")
                    last_error = response
                    
            except Exception as e:
                print(f"✗ Exception with approach {approach['name']}: {str(e)}")
                last_error = e
                continue
        
        if not response or response.status_code != 200:
            if last_error:
                if hasattr(last_error, 'raise_for_status'):
                    last_error.raise_for_status()
                else:
                    raise Exception(f"All OAuth2 approaches failed. Last error: {str(last_error)}")
            else:
                raise Exception("All OAuth2 approaches failed")
        
        response.raise_for_status()
        
        # ServiceNow geralmente retorna JSON
        try:
            token_response = response.json()
            print(f"OAuth2 token response keys: {list(token_response.keys())}")
        except ValueError:
            # Se não for JSON, tentar parsear como form-urlencoded
            from urllib.parse import parse_qs
            token_response = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                            for k, v in parse_qs(response.text).items()}
            print(f"OAuth2 token response (form-urlencoded) keys: {list(token_response.keys())}")
        
        # Tentar diferentes campos possíveis para o access_token
        # ServiceNow pode retornar: access_token, accessToken, token, etc.
        access_token = (
            token_response.get('access_token') or 
            token_response.get('accessToken') or
            token_response.get('token')
        )
        
        if access_token:
            print("OAuth2 token obtained successfully")
            # Não logar o token completo por segurança, apenas os primeiros caracteres
            print(f"Token preview: {access_token[:20]}...")
            return access_token
        else:
            print(f"ERROR: No access_token in response.")
            print(f"Response keys: {list(token_response.keys()) if isinstance(token_response, dict) else 'N/A'}")
            print(f"Response preview: {str(token_response)[:500]}")
            return None
            
    except Exception as e:
        print(f"ERROR: Failed to obtain OAuth2 token: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")
    
    # Extrair process_id do evento (pode estar em diferentes níveis)
    process_id = event.get('process_id')
    if not process_id:
        # Tentar extrair do Payload se vier do Step Functions
        if 'Payload' in event:
            process_id = event['Payload'].get('process_id')
        if not process_id:
            print("ERROR: process_id not found in event")
            return {'statusCode': 400, 'error': 'process_id not found'}
    
    # Extrair validation_results do evento
    # Pode estar em: validation_result.Payload.validation_results ou validation_results diretamente
    validation_results = []
    if 'validation_result' in event:
        # Estrutura do Step Functions: validation_result.Payload.validation_results
        validation_result = event.get('validation_result', {})
        if isinstance(validation_result, dict):
            payload = validation_result.get('Payload', validation_result)
            validation_results = payload.get('validation_results', [])
    elif 'validation_results' in event:
        # Estrutura direta
        validation_results = event.get('validation_results', [])
    
    error_info = event.get('error', {})
    print(f"Processing failure for process_id: {process_id}")
    print(f"Found {len(validation_results)} validation results")
    
    # Gerar ID único numérico de 6 dígitos
    id_unico = random.randint(100000, 999999)
    
    # Extrair detalhes das validações que falharam
    detalhes = []
    failed_rules = [r for r in validation_results if r.get('status') == 'FAILED']
    print(f"Found {len(failed_rules)} failed rules")
    
    if failed_rules:
        for rule in failed_rules:
            rule_name = rule.get('rule', 'Desconhecido')
            message = rule.get('message', 'Sem mensagem')
            danfe_value = rule.get('danfe_value', 'N/A')
            comparisons = rule.get('comparisons', [])
            
            # Se não há comparisons, criar um detalhe baseado na regra
            if not comparisons:
                detalhes.append({
                    "pagina": 1,
                    "campo": rule_name,
                    "mensagemErro": f"{message}. Valor DANFE: {danfe_value}"
                })
                continue
            
            # Para cada documento que falhou
            for comp in comparisons:
                doc_file = comp.get('doc_file', 'Documento desconhecido')
                doc_value = comp.get('doc_value', 'N/A')
                comp_status = comp.get('status', 'MISMATCH')
                
                # Se for validação de produtos, detalhar cada campo que falhou
                if 'items' in comp:
                    items = comp.get('items', [])
                    if items:
                        for item in items:
                            item_status = item.get('status', 'MISMATCH')
                            if item_status == 'MISMATCH':
                                fields = item.get('fields', {})
                                failed_fields = [f for f, v in fields.items() if v.get('status') == 'MISMATCH']
                                if failed_fields:
                                    detalhes.append({
                                        "pagina": 1,
                                        "campo": f"{rule_name} - Item {item.get('item', 'N/A')} - {', '.join(failed_fields)}",
                                        "mensagemErro": f"Documento: {doc_file}. Divergências: " + 
                                                       ", ".join([f"{f}: DANFE={fields[f].get('danfe', 'N/A')} vs DOC={fields[f].get('doc', 'N/A')}" 
                                                                  for f in failed_fields])
                                    })
                                else:
                                    # Item com status MISMATCH mas sem campos específicos
                                    detalhes.append({
                                        "pagina": 1,
                                        "campo": f"{rule_name} - Item {item.get('item', 'N/A')}",
                                        "mensagemErro": f"Documento: {doc_file}. Item não corresponde ao esperado no DANFE"
                                    })
                    else:
                        # Comparação de produtos mas sem items (ex: 0 produtos encontrados)
                        detalhes.append({
                            "pagina": 1,
                            "campo": rule_name,
                            "mensagemErro": f"Documento: {doc_file}. {message}. {doc_value}"
                        })
                else:
                    # Validação simples (não produtos)
                    detalhes.append({
                        "pagina": 1,
                        "campo": rule_name,
                        "mensagemErro": f"Documento: {doc_file}. {message}. DANFE: {danfe_value}, DOC: {doc_value}"
                    })
    else:
        # Falha técnica (não de validação)
        detalhes.append({
            "pagina": 1,
            "campo": error_info.get('Cause', 'Erro técnico'),
            "mensagemErro": error_info.get('Error', 'Erro no processamento OCR')
        })
    
    # Preparar payload para API externa
    if failed_rules:
        rule_names = [r.get('rule', 'Desconhecida') for r in failed_rules]
        descricao_falha = f"Validação falhou: {len(failed_rules)} regra(s) com divergência ({', '.join(rule_names)})"
    else:
        descricao_falha = "Erro no processamento OCR"
    
    payload = {
        "idUnico": id_unico,
        "descricaoFalha": descricao_falha,
        "traceAWS": process_id,
        "detalhes": detalhes
    }
    
    # Obter token OAuth2
    access_token = get_oauth2_token()
    if not access_token:
        print("WARNING: Could not obtain OAuth2 token, proceeding without authentication")
    
    # Enviar para API externa e obter sctask_id
    sctask_id = None
    try:
        api_url = os.environ.get('OCR_FAILURE_API_URL')
        if not api_url:
            raise ValueError("OCR_FAILURE_API_URL environment variable is required")
        
        print(f"Sending to API: {api_url}")
        print(f"Payload: {json.dumps(payload, ensure_ascii=False)}")
        
        # Preparar headers com token OAuth2
        headers = {
            'Content-Type': 'application/json'
        }
        if access_token:
            headers['Authorization'] = f'Bearer {access_token}'
        
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        api_response = response.json()
        print(f"API response: {api_response}")
        # Extrair sctask_id do campo 'tarefa' da resposta
        sctask_id = api_response.get('tarefa')
        print(f"SCTASK ID from API: {sctask_id}")
    except Exception as e:
        print(f"Erro ao reportar falha para API externa: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        api_response = {'error': str(e)}
    
    # Atualizar status no DynamoDB com sctask_id da API
    try:
        print(f"Updating DynamoDB: PK=PROCESS#{process_id}, SK=METADATA")
        update_expr = 'SET #status = :status, error_info = :error, updated_at = :timestamp'
        expr_values = {
            ':status': 'FAILED',
            ':error': json.dumps(error_info),
            ':timestamp': datetime.utcnow().isoformat()
        }
        if sctask_id:
            update_expr += ', sctask_id = :sctask'
            expr_values[':sctask'] = sctask_id
        
        table.update_item(
            Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={'#status': 'STATUS'},
            ExpressionAttributeValues=expr_values
        )
        print("DynamoDB updated successfully")
    except Exception as e:
        print(f"Erro ao atualizar DynamoDB: {str(e)}")
        import traceback
        traceback.print_exc()
    
    result = {
        'statusCode': 200,
        'process_id': process_id,
        'status': 'FAILED',
        'api_response': api_response
    }
    print(f"Returning: {json.dumps(result)}")
    return result
