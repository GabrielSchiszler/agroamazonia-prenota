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

from src.controllers.process_controller import router as process_router
from src.controllers.rules_controller import router as rules_router
from src.controllers.dashboard_controller import router as dashboard_router
from src.controllers.cfop_operation_controller import router as cfop_operation_router
from fastapi import Request, HTTPException

# Detectar se está rodando na AWS Lambda
is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None

description = """API Serverless para processamento de documentos e validação de regras.

## Estrutura de Rotas

- `/api/process/*` - Gerenciamento de processos
- `/api/rules/*` - Gerenciamento de regras
- `/api/dashboard/*` - Dashboard e métricas
- `/api/cfop-operations/*` - Operações CFOP

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

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
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

# Handler para Lambda
try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except ImportError:
    pass
