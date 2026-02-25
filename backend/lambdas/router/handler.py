"""
Lambda Router - Redireciona requisições do API Gateway para Lambdas específicos por ambiente

Este Lambda recebe requisições do API Gateway e redireciona para:
- stage hml → lambda-api-stg
- stage prd → lambda-api-prd

IMPORTANTE: Este Router adiciona headers CORS em TODAS as respostas.
"""

import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')


def get_lambda_name_from_environment(environment: str) -> str:
    """
    Retorna o nome do Lambda baseado no ambiente.
    """
    if not environment:
        return None

    environment = environment.lower()

    if environment == 'hml':
        return 'lambda-api-stg'
    elif environment == 'prd':
        return 'lambda-api-prd'
    else:
        logger.warning(f"[ROUTER] Ambiente não reconhecido: {environment}")
        return None


def get_origin_from_event(event: dict) -> str:
    """
    Extrai o origin do header da requisição.
    """
    headers = event.get('headers', {})
    # Headers podem vir em diferentes formatos (lowercase, camelCase, etc)
    origin = (
        headers.get('origin') or 
        headers.get('Origin') or 
        headers.get('ORIGIN') or 
        "*"
    )
    return origin


def add_cors_headers(response: dict, origin: str = "*") -> dict:
    """
    Adiciona headers CORS a uma resposta.
    """
    if not isinstance(response, dict):
        return response
    
    if 'headers' not in response:
        response['headers'] = {}
    
    # Sempre adicionar/sobrescrever headers CORS
    cors_origin = origin if origin != "*" else "*"
    response['headers']['Access-Control-Allow-Origin'] = cors_origin
    response['headers']['Access-Control-Allow-Credentials'] = 'true'
    response['headers']['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD'
    response['headers']['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With'
    
    return response


def create_cors_preflight_response(origin: str = "*") -> dict:
    """
    Cria resposta CORS para requisição OPTIONS (preflight).
    """
    cors_origin = origin if origin != "*" else "*"
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': cors_origin,
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With',
            'Access-Control-Max-Age': '3600',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({})
    }


def lambda_handler(event, context):
    """
    Handler principal do Lambda Router.
    """

    # =========================
    # LOG COMPLETO DA ENTRADA
    # =========================
    try:
        logger.info("[ROUTER] ===== EVENTO RECEBIDO =====")
        logger.info(json.dumps(event, indent=2, default=str))
        logger.info(f"[ROUTER] RequestId: {context.aws_request_id}")
    except Exception as log_error:
        logger.warning(f"[ROUTER] Falha ao serializar evento para log: {str(log_error)}")

    # =========================
    # PRIORIDADE 1: TRATAR OPTIONS (PREFLIGHT CORS)
    # =========================
    http_method = None
    request_context = event.get('requestContext', {})
    
    # Detectar método HTTP (API Gateway v1 ou v2)
    if 'httpMethod' in event:
        # API Gateway v1
        http_method = event.get('httpMethod')
    elif 'http' in request_context:
        # API Gateway v2
        http_method = request_context.get('http', {}).get('method')
    elif 'httpMethod' in request_context:
        # Outro formato possível
        http_method = request_context.get('httpMethod')
    
    # Se for OPTIONS, retornar resposta CORS imediatamente (SEM invocar Lambda destino)
    if http_method == "OPTIONS":
        origin = get_origin_from_event(event)
        logger.info(f"[ROUTER] ⚡ OPTIONS (preflight) request detected - returning CORS response for origin: {origin}")
        cors_response = create_cors_preflight_response(origin)
        logger.info(f"[ROUTER] ✅ CORS preflight response created")
        return cors_response

    # =========================
    # PRIORIDADE 2: PEGAR STAGE
    # =========================
    stage = request_context.get('stage')

    if stage:
        logger.info(f"[ROUTER] Ambiente identificado pelo stage: {stage}")
        target_lambda_name = get_lambda_name_from_environment(stage)
    else:
        # =========================
        # FALLBACK: EXTRAIR DO PATH
        # =========================
        path = request_context.get('path') or event.get('rawPath') or event.get('path')

        if not path:
            logger.error("[ROUTER] Não foi possível determinar o path")
            origin = get_origin_from_event(event)
            error_response = {
                'statusCode': 400,
                'headers': {},
                'body': json.dumps({
                    'error': 'Bad Request',
                    'message': 'Não foi possível determinar o ambiente'
                })
            }
            return add_cors_headers(error_response, origin)

        logger.info(f"[ROUTER] Path resolvido: {path}")

        path = path.lstrip('/')
        environment = path.split('/')[0]

        logger.info(f"[ROUTER] Ambiente identificado pelo path: {environment}")
        target_lambda_name = get_lambda_name_from_environment(environment)

    if not target_lambda_name:
        logger.error("[ROUTER] Ambiente inválido")
        origin = get_origin_from_event(event)
        error_response = {
            'statusCode': 400,
            'headers': {},
            'body': json.dumps({
                'error': 'Bad Request',
                'message': 'Ambiente inválido'
            })
        }
        return add_cors_headers(error_response, origin)

    logger.info(f"[ROUTER] Redirecionando para: {target_lambda_name}")

    # =========================
    # INVOCAR LAMBDA DESTINO
    # =========================
    origin = get_origin_from_event(event)
    
    try:
        response = lambda_client.invoke(
            FunctionName=target_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(event, default=str)
        )

        response_payload = response['Payload'].read().decode('utf-8')
        function_error = response.get('FunctionError')

        logger.info(f"[ROUTER] Resposta bruta do Lambda destino: {response_payload[:500]}...")  # Limitar log

        if function_error:
            logger.error(f"[ROUTER] Erro no Lambda {target_lambda_name}: {function_error}")
            error_response = {
                'statusCode': 500,
                'headers': {},
                'body': json.dumps({
                    'error': 'Internal Server Error',
                    'message': f'Erro no Lambda destino: {function_error}'
                })
            }
            return add_cors_headers(error_response, origin)

        try:
            lambda_response = json.loads(response_payload)

            # Se a resposta já tem estrutura de Lambda Proxy Integration
            if isinstance(lambda_response, dict) and 'statusCode' in lambda_response:
                # GARANTIR que headers CORS estão presentes (mesmo que Lambda destino tenha adicionado)
                logger.info(f"[ROUTER] Resposta do Lambda destino tem statusCode: {lambda_response.get('statusCode')}")
                logger.info(f"[ROUTER] Headers antes de adicionar CORS: {list(lambda_response.get('headers', {}).keys())}")
                
                # Adicionar/sobrescrever headers CORS
                lambda_response = add_cors_headers(lambda_response, origin)
                
                logger.info(f"[ROUTER] ✅ Headers CORS adicionados na resposta")
                logger.info(f"[ROUTER] Headers finais: {list(lambda_response.get('headers', {}).keys())}")
                
                return lambda_response

            # Se a resposta não tem estrutura de Lambda Proxy Integration, converter
            logger.info(f"[ROUTER] Resposta do Lambda destino não tem estrutura de proxy, convertendo...")
            proxy_response = {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(lambda_response, default=str)
            }
            return add_cors_headers(proxy_response, origin)

        except json.JSONDecodeError:
            logger.warning(f"[ROUTER] Resposta do Lambda destino não é JSON válido, retornando como texto")
            proxy_response = {
                'statusCode': 200,
                'headers': {'Content-Type': 'text/plain'},
                'body': response_payload
            }
            return add_cors_headers(proxy_response, origin)

    except Exception as e:
        logger.error(
            f"[ROUTER] ❌ Erro ao invocar Lambda {target_lambda_name}: {str(e)}",
            exc_info=True
        )
        error_response = {
            'statusCode': 500,
            'headers': {},
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': str(e)
            })
        }
        return add_cors_headers(error_response, origin)

