import os
import sys
import logging
import re
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente do .env
load_dotenv()
logger.info("Environment variables loaded")

from src.controllers.process_controller import router as process_router
from src.controllers.rules_controller import router as rules_router
from src.controllers.dashboard_controller import router as dashboard_router
from src.controllers.cfop_operation_controller import router as cfop_operation_router
from fastapi import Request, HTTPException

# Detectar se está rodando na AWS Lambda
is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None

description = """API Serverless para processamento de documentos e validação de regras.

## Estrutura de Rotas

- `/process/*` - Gerenciamento de processos
- `/rules/*` - Gerenciamento de regras
- `/dashboard/*` - Dashboard e métricas
- `/cfop-operation/*` - Operações CFOP

A autenticação é gerenciada pelo API Gateway com Cognito.
"""

app = FastAPI(
    title="AgroAmazonia API",
    version="1.0.0",
    description=description,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    root_path="/v1" if is_lambda else "",
    swagger_ui_parameters={
        "persistAuthorization": True
    },
    openapi_tags=[
        {
            "name": "Process (Fluxo Moderno)",
            "description": "Endpoints para upload e processamento de documentos."
        }
    ]
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handler para retornar status codes corretos
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

# Middleware para remover prefixo do API Gateway e logar requisições
@app.middleware("http")
async def remove_api_gateway_prefix_and_log(request: Request, call_next):
    """Remove prefixo do API Gateway (ex: /ocr-prd) e loga requisições"""
    original_path = request.url.path
    method = request.method
    
    # Se estiver no Lambda, tentar extrair o path real do evento
    # O Mangum passa o path completo, mas pode ter prefixo do API Gateway
    # Vamos detectar e remover prefixos comuns
    path = original_path
    
    # Lista de prefixos conhecidos que podem vir do API Gateway
    # Ex: /ocr-prd, /v1, /api, etc.
    known_prefixes = ['/ocr-prd', '/ocr-dev', '/ocr-stg', '/v1']
    
    # Remover prefixo se encontrado
    for prefix in known_prefixes:
        if path.startswith(prefix):
            path = path[len(prefix):]
            # Garantir que começa com /
            if not path.startswith('/'):
                path = '/' + path
            logger.info(f"[PATH_REWRITE] Removido prefixo '{prefix}': {original_path} -> {path}")
            break
    
    # Se o path ainda não começa com /, adicionar
    if not path.startswith('/'):
        path = '/' + path
    
    # Modificar o scope do request para usar o path correto
    if path != original_path:
        # Criar novo URL com o path corrigido
        from starlette.datastructures import URL
        new_url = request.url.replace(path=path)
        # Modificar o scope do request
        request.scope['path'] = path
        request.scope['raw_path'] = path.encode()
        # Atualizar o URL do request
        request._url = new_url
    
    query_params = str(request.query_params) if request.query_params else ""
    logger.info(f"[REQUEST] {method} {path}" + (f"?{query_params}" if query_params else ""))
    if original_path != path:
        logger.info(f"[REQUEST] Path original (API Gateway): {original_path}")
    
    try:
        response = await call_next(request)
        status_code = response.status_code
        logger.info(f"[RESPONSE] {method} {path} -> {status_code}")
        
        # Se for 404, logar rotas disponíveis
        if status_code == 404:
            available_routes = []
            for route in app.routes:
                if hasattr(route, 'methods') and hasattr(route, 'path'):
                    methods = ', '.join(sorted(route.methods))
                    path_route = route.path
                    available_routes.append(f"{methods} {path_route}")
            
            logger.warning(f"[404] Rota não encontrada: {method} {path} (original: {original_path})")
            logger.warning(f"[404] Rotas disponíveis ({len(available_routes)}):")
            for route in sorted(available_routes):
                logger.warning(f"  - {route}")
        
        return response
    except Exception as e:
        logger.error(f"[ERROR] {method} {path} -> Exception: {str(e)}", exc_info=True)
        raise

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handler para exceções HTTP"""
    if exc.status_code == 404:
        method = request.method
        path = request.url.path
        logger.warning(f"[404 HANDLER] Rota não encontrada: {method} {path}")
        
        # Listar rotas disponíveis
        available_routes = []
        for route in app.routes:
            if hasattr(route, 'methods') and hasattr(route, 'path'):
                methods = ', '.join(sorted(route.methods))
                path_route = route.path
                available_routes.append(f"{methods} {path_route}")
        
        logger.warning(f"[404 HANDLER] Total de rotas disponíveis: {len(available_routes)}")
        for route in sorted(available_routes):
            logger.warning(f"  - {route}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(StarletteHTTPException)
async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handler para exceções HTTP do Starlette"""
    if exc.status_code == 404:
        method = request.method
        path = request.url.path
        logger.warning(f"[404 STARLETTE] Rota não encontrada: {method} {path}")
        
        # Listar rotas disponíveis
        available_routes = []
        for route in app.routes:
            if hasattr(route, 'methods') and hasattr(route, 'path'):
                methods = ', '.join(sorted(route.methods))
                path_route = route.path
                available_routes.append(f"{methods} {path_route}")
        
        logger.warning(f"[404 STARLETTE] Total de rotas disponíveis: {len(available_routes)}")
        for route in sorted(available_routes):
            logger.warning(f"  - {route}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail if exc.detail else "Not found"}
    )

app.include_router(process_router)
app.include_router(rules_router)
app.include_router(dashboard_router)
app.include_router(cfop_operation_router)

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {
        "message": "AgroAmazonia API",
        "docs": "/docs",
        "health": "/health"
    }

# Listar todas as rotas disponíveis (apenas no Lambda, após todos os routers serem incluídos)
if is_lambda:
    def log_available_routes():
        """Loga todas as rotas disponíveis no app"""
        routes = []
        for route in app.routes:
            if hasattr(route, 'methods') and hasattr(route, 'path'):
                methods = ', '.join(sorted(route.methods))
                path = route.path
                routes.append(f"{methods:8} {path}")
        
        logger.info("=" * 80)
        logger.info("ROTAS DISPONÍVEIS NA API:")
        logger.info("=" * 80)
        for route in sorted(routes):
            logger.info(f"  {route}")
        logger.info("=" * 80)
    
    # Logar rotas na inicialização (após todos os routers serem incluídos)
    log_available_routes()

# Handler para Lambda com remoção de prefixo do API Gateway
try:
    from mangum import Mangum
    
    # Criar handler Mangum base
    mangum_handler = Mangum(app, lifespan="off")
    
    def handler(event, context):
        """
        Wrapper do handler Mangum que remove prefixos do API Gateway do path.
        
        O API Gateway pode enviar paths como /ocr-prd/health, mas a rota real é /health.
        Este wrapper detecta e remove o prefixo antes de passar para o FastAPI.
        """
        # Log completo do evento recebido do API Gateway
        logger.info("=" * 80)
        logger.info("[HANDLER] EVENTO RECEBIDO DO API GATEWAY:")
        logger.info("=" * 80)
        
        # Criar uma cópia do evento para log (sem dados sensíveis)
        event_for_log = {}
        for key, value in event.items():
            if key == 'headers':
                # Logar headers mas mascarar valores sensíveis
                headers_log = {}
                sensitive_headers = ['authorization', 'x-api-key', 'cookie', 'set-cookie']
                for hk, hv in value.items() if isinstance(value, dict) else {}:
                    if hk.lower() in sensitive_headers:
                        headers_log[hk] = '***MASKED***'
                    else:
                        headers_log[hk] = hv
                event_for_log[key] = headers_log
            elif key == 'body':
                # Logar apenas o tamanho do body se for muito grande
                if isinstance(value, str) and len(value) > 500:
                    event_for_log[key] = f"<body length: {len(value)} chars>"
                else:
                    event_for_log[key] = value
            else:
                event_for_log[key] = value
        
        logger.info(json.dumps(event_for_log, indent=2, default=str))
        logger.info("=" * 80)
        
        # Extrair o path do evento do API Gateway (suporta v1 e v2)
        path = None
        
        # Log detalhado dos campos de path
        logger.info("[HANDLER] Extraindo path do evento...")
        logger.info(f"  - event.get('rawPath'): {event.get('rawPath', 'N/A')}")
        logger.info(f"  - event.get('path'): {event.get('path', 'N/A')}")
        
        request_context = event.get('requestContext', {})
        logger.info(f"  - requestContext keys: {list(request_context.keys())}")
        if 'http' in request_context:
            logger.info(f"  - requestContext['http'].get('path'): {request_context['http'].get('path', 'N/A')}")
        if 'path' in request_context:
            logger.info(f"  - requestContext.get('path'): {request_context.get('path', 'N/A')}")
        if 'resourcePath' in request_context:
            logger.info(f"  - requestContext.get('resourcePath'): {request_context.get('resourcePath', 'N/A')}")
        
        # API Gateway v2 (HTTP API) - formato mais comum
        if 'rawPath' in event:
            path = event['rawPath']
            logger.info(f"[HANDLER] Path extraído de event['rawPath']: {path}")
        elif 'path' in event:
            path = event['path']
            logger.info(f"[HANDLER] Path extraído de event['path']: {path}")
        
        # API Gateway v1 (REST API) - fallback
        if not path:
            if 'http' in request_context:
                # API Gateway v2
                path = request_context['http'].get('path', '')
                if path:
                    logger.info(f"[HANDLER] Path extraído de requestContext['http']['path']: {path}")
            elif 'path' in request_context:
                # API Gateway v1
                path = request_context.get('path', '')
                if path:
                    logger.info(f"[HANDLER] Path extraído de requestContext['path']: {path}")
            elif 'resourcePath' in request_context:
                # Outro formato possível
                path = request_context['resourcePath']
                if path:
                    logger.info(f"[HANDLER] Path extraído de requestContext['resourcePath']: {path}")
        
        if not path:
            logger.warning(f"[HANDLER] Não foi possível extrair path do evento!")
            logger.warning(f"[HANDLER] Chaves disponíveis no evento: {list(event.keys())}")
            logger.warning(f"[HANDLER] Chaves disponíveis no requestContext: {list(request_context.keys())}")
            # Tentar usar o handler original mesmo assim
            return mangum_handler(event, context)
        
        original_path = path
        
        # Extrair prefixo dinamicamente do campo 'resource' ou 'resourcePath'
        # Formato esperado: '/ocr-prd/{proxy+}' ou '/ocr-prd/{proxy}'
        # O prefixo é tudo antes de '{proxy+}' ou '{proxy}'
        prefix = None
        
        # Tentar extrair do campo 'resource' (API Gateway v1)
        resource = event.get('resource', '')
        if resource:
            logger.info(f"[HANDLER] Campo 'resource' encontrado: {resource}")
            # Procurar por {proxy+} ou {proxy} no resource
            match = re.search(r'^(.+?)/\{proxy\+?\}', resource)
            if match:
                prefix = match.group(1)
                logger.info(f"[HANDLER] Prefixo extraído de 'resource': {prefix}")
        
        # Se não encontrou no 'resource', tentar no 'resourcePath' do requestContext
        if not prefix:
            resource_path = request_context.get('resourcePath', '')
            if resource_path:
                logger.info(f"[HANDLER] Campo 'resourcePath' encontrado: {resource_path}")
                match = re.search(r'^(.+?)/\{proxy\+?\}', resource_path)
                if match:
                    prefix = match.group(1)
                    logger.info(f"[HANDLER] Prefixo extraído de 'resourcePath': {prefix}")
        
        # Remover prefixo do path se encontrado
        if prefix:
            if path.startswith(prefix):
                path = path[len(prefix):]
                # Garantir que começa com /
                if not path.startswith('/'):
                    path = '/' + path
                logger.info(f"[HANDLER] Removido prefixo dinâmico '{prefix}': {original_path} -> {path}")
            else:
                logger.warning(f"[HANDLER] Prefixo '{prefix}' encontrado mas path '{path}' não começa com ele")
                # Tentar remover prefixo parcialmente (caso o path tenha mais camadas)
                # Ex: prefix=/api/fast, path=/api/fast/apiprocess -> /apiprocess
                if prefix in path:
                    path = path.replace(prefix, '', 1)
                    if not path.startswith('/'):
                        path = '/' + path
                    logger.info(f"[HANDLER] Removido prefixo parcialmente: {original_path} -> {path}")
        else:
            logger.info(f"[HANDLER] Nenhum prefixo detectado no resource/resourcePath. Path mantido: {path}")
            
        # Log adicional: verificar se o path final corresponde a alguma rota conhecida
        logger.info(f"[HANDLER] Path final antes de chamar Mangum: {path}")
        
        # Se o path ainda não começa com /, adicionar
        if not path.startswith('/'):
            path = '/' + path
        
        # Modificar o evento para usar o path correto
        if path != original_path:
            logger.info(f"[HANDLER] Modificando evento: path original '{original_path}' -> path corrigido '{path}'")
            
            # API Gateway v2
            if 'rawPath' in event:
                event['rawPath'] = path
                logger.info(f"[HANDLER] event['rawPath'] atualizado para: {path}")
            if 'path' in event:
                event['path'] = path
                logger.info(f"[HANDLER] event['path'] atualizado para: {path}")
            
            # Atualizar no requestContext se existir
            if 'requestContext' in event:
                request_context = event['requestContext']
                if 'http' in request_context:
                    # API Gateway v2
                    request_context['http']['path'] = path
                    logger.info(f"[HANDLER] requestContext['http']['path'] atualizado para: {path}")
                elif 'path' in request_context:
                    # API Gateway v1
                    request_context['path'] = path
                    logger.info(f"[HANDLER] requestContext['path'] atualizado para: {path}")
        else:
            logger.info(f"[HANDLER] Path não modificado (sem prefixo detectado): {path}")
        
        logger.info(f"[HANDLER] Chamando handler Mangum com path final: {path}")
        logger.info("=" * 80)
        
        # Chamar o handler Mangum original
        return mangum_handler(event, context)
    
except ImportError:
    handler = None
    logger.warning("Mangum não encontrado. Handler Lambda não disponível.")
