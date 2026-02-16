"""
Lambda Router - Redireciona requisições do API Gateway para Lambdas específicos por ambiente

Este Lambda recebe requisições do API Gateway e redireciona para:
- /hml/* → lambda-api-stg
- /prd/* → lambda-api-prd

Repassa todo o evento recebido para o Lambda de destino.
"""

import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')

def get_lambda_name_from_path(path: str) -> str:
    """
    Extrai o nome do Lambda baseado no path.
    
    Args:
        path: Path da requisição (ex: /hml/process/123 ou /prd/dashboard/metrics)
    
    Returns:
        Nome do Lambda (ex: lambda-api-stg ou lambda-api-prd)
    """
    # Normalizar path (remover barra inicial se houver)
    path = path.lstrip('/')
    
    # Dividir path em partes
    parts = path.split('/')
    
    if not parts or not parts[0]:
        logger.warning(f"Path vazio ou inválido: {path}")
        return None
    
    # Primeira parte do path é o ambiente
    environment = parts[0].lower()
    
    # Mapear ambiente para nome do Lambda
    if environment == 'hml':
        return 'lambda-api-stg'
    elif environment == 'prd':
        return 'lambda-api-prd'
    else:
        logger.warning(f"Ambiente não reconhecido no path: {environment}")
        return None


def lambda_handler(event, context):
    """
    Handler principal do Lambda Router.
    
    Recebe o evento do API Gateway, identifica o ambiente pelo path,
    e invoca o Lambda correto repassando todo o evento.
    """
    # Extrair path do evento
    # requestContext['path'] já contém o path completo com stage (ex: /prd/fast/health)
    request_context = event.get('requestContext', {})
    path = request_context.get('path')
    
    # Fallback para API Gateway v2 (HTTP API)
    if not path:
        path = event.get('rawPath') or event.get('path')
    
    # Fallback: construir path a partir de stage + proxy
    if not path:
        stage = request_context.get('stage', '')
        path_params = event.get('pathParameters', {})
        if 'proxy' in path_params and stage:
            proxy_value = path_params.get('proxy', '')
            path = f'/{stage}/{proxy_value}'
    
    if not path:
        logger.error(f"[ROUTER] Não foi possível extrair path do evento")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': 'Não foi possível determinar o path da requisição'
            })
        }
    
    # Log da entrada
    logger.info(f"[ROUTER] Entrada: {path}")
    
    # Identificar Lambda de destino baseado no path
    target_lambda_name = get_lambda_name_from_path(path)
    
    if not target_lambda_name:
        logger.error(f"[ROUTER] Path inválido: {path} (deve começar com /hml/ ou /prd/)")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': f'Path inválido. Deve começar com /hml/ ou /prd/. Path recebido: {path}'
            })
        }
    
    # Log do redirecionamento
    logger.info(f"[ROUTER] Redirecionando para: {target_lambda_name}")
    
    # Invocar Lambda de destino
    try:
        response = lambda_client.invoke(
            FunctionName=target_lambda_name,
            InvocationType='RequestResponse',  # Síncrono para receber resposta
            Payload=json.dumps(event, default=str)
        )
        
        # Ler resposta do Lambda
        response_payload = response['Payload'].read().decode('utf-8')
        function_error = response.get('FunctionError')
        
        # Se o Lambda retornou erro
        if function_error:
            logger.error(f"[ROUTER] Erro no Lambda {target_lambda_name}: {function_error}")
            try:
                error_data = json.loads(response_payload)
                error_message = error_data.get('errorMessage', 'Erro desconhecido no Lambda de destino')
            except:
                error_message = response_payload[:500]
            
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Internal Server Error',
                    'message': f'Erro no Lambda de destino: {error_message}',
                    'lambda': target_lambda_name
                })
            }
        
        # Processar resposta do Lambda
        try:
            lambda_response = json.loads(response_payload)
            
            # Se a resposta já está no formato API Gateway (com statusCode e body)
            if isinstance(lambda_response, dict) and 'statusCode' in lambda_response:
                return lambda_response
            
            # Se a resposta é um objeto simples, converter para formato API Gateway
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps(lambda_response, default=str)
            }
            
        except json.JSONDecodeError:
            # Resposta não é JSON, retornar como texto
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': response_payload
            }
    
    except Exception as e:
        logger.error(f"[ROUTER] Erro ao invocar Lambda {target_lambda_name}: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': f'Erro ao invocar Lambda de destino: {str(e)}',
                'lambda': target_lambda_name
            })
        }


