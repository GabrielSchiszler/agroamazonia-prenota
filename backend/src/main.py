import os
import sys
import logging
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

from src.controllers.document_controller import router as document_router
from src.controllers.process_controller import router as process_router
from src.controllers.rules_controller import router as rules_router
from src.security import get_api_key_scheme
from fastapi import Request, HTTPException
import json
import boto3

# Detectar se está rodando na AWS Lambda
is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None

# Descrição muda baseado no ambiente
if is_lambda:
    description = """API Serverless para processamento de documentos com AWS Textract e validação de regras.
    
## Autenticação

Todas as rotas sob `/api/*` requerem API Key no header:

```
x-api-key: agroamazonia_key_<seu_codigo>
```

**Exemplo**:
```bash
curl -H "x-api-key: agroamazonia_key_abc123def456" \\
     https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1/api/process/presigned-url/xml
```

## Estrutura de Rotas

**Rotas Públicas** (sem autenticação):
- `GET /` - Informações da API
- `GET /health` - Status da API
- `GET /docs` - Documentação Swagger
- `GET /openapi.json` - Especificação OpenAPI

**Rotas Protegidas** (requerem x-api-key):
- `/api/process/*` - Gerenciamento de processos
- `/api/rules/*` - Gerenciamento de regras

Para obter sua API Key, entre em contato com o administrador do sistema.
    """
else:
    description = """API Serverless para processamento de documentos com AWS Textract e validação de regras.
    
⚠️ **MODO DESENVOLVIMENTO**: Autenticação desabilitada localmente.
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
            "description": "Endpoints para upload e processamento de documentos." + (" **Requer API Key**." if is_lambda else " (Dev: sem autenticação)")
        }
    ]
)

# Add security scheme to OpenAPI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    from fastapi.openapi.utils import get_openapi
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Add servers for correct path resolution
    if is_lambda:
        openapi_schema["servers"] = [
            {"url": "https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1", "description": "Production"}
        ]
    
    openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": get_api_key_scheme()
    }
    
    # Apply security to all endpoints only in Lambda
    if is_lambda:
        for path in openapi_schema["paths"]:
            for method in openapi_schema["paths"][path]:
                if method in ["get", "post", "put", "delete", "patch"]:
                    openapi_schema["paths"][path][method]["security"] = [{"ApiKeyAuth": []}]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# CORS para desenvolvimento
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handler para retornar status codes corretos
from fastapi.responses import JSONResponse

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

# Middleware de autenticação
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # Rotas públicas
    public_paths = ["/", "/health", "/docs", "/redoc", "/openapi.json"]
    # Também permitir assets do Swagger
    if request.url.path in public_paths or request.url.path.startswith("/static"):
        return await call_next(request)
    
    # Rotas protegidas requerem API key apenas em Lambda
    if is_lambda and request.url.path.startswith("/api/"):
        api_key = request.headers.get("x-api-key")
        if not api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "API Key required"}
            )
        
        # Validar API key
        try:
            secrets_client = boto3.client('secretsmanager')
            secret_arn = os.environ.get('API_KEYS_SECRET_ARN')
            response = secrets_client.get_secret_value(SecretId=secret_arn)
            api_keys = json.loads(response['SecretString'])
            
            if api_key not in api_keys or api_keys[api_key].get('status') != 'active':
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Invalid API Key"}
                )
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return JSONResponse(
                status_code=403,
                content={"detail": "Authentication failed"}
            )
    
    return await call_next(request)

app.include_router(process_router)
app.include_router(rules_router)
app.include_router(document_router)

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

# Handler para Lambda
try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except ImportError:
    pass
