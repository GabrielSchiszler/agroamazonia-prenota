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

app = FastAPI(
    title="AgroAmazonia Document API",
    version="1.0.0",
    description="API Serverless para processamento de documentos financeiros"
)

# CORS para desenvolvimento
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(document_router)
app.include_router(process_router)

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
