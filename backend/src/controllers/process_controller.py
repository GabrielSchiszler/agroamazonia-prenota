import logging
import traceback
from fastapi import APIRouter, HTTPException
from src.models.api import (
    ProcessCreateRequest, ProcessCreateResponse,
    PresignedUrlRequest, PresignedUrlResponse,
    ProcessStartRequest, ProcessStartResponse,
    ProcessResponse
)
from src.services.process_service import ProcessService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/process", tags=["process"])
service = ProcessService()

@router.post("/create", response_model=ProcessCreateResponse)
async def create_process(request: ProcessCreateRequest):
    """Cria novo processo"""
    try:
        logger.info(f"Creating process with type: {request.process_type}")
        result = service.create_process(request.process_type)
        logger.info(f"Process created: {result}")
        return ProcessCreateResponse(**result)
    except Exception as e:
        logger.error(f"Error creating process: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/presigned-url")
async def get_presigned_url(request: PresignedUrlRequest):
    """Gera URL assinada para upload"""
    try:
        result = service.generate_presigned_url(
            request.process_id,
            request.file_name,
            request.file_type
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/start", response_model=ProcessStartResponse)
async def start_process(request: ProcessStartRequest):
    """Inicia processamento"""
    try:
        logger.info(f"Starting process: {request.process_id}")
        result = service.start_process(request.process_id)
        logger.info(f"Process started successfully: {result}")
        return ProcessStartResponse(**result)
    except Exception as e:
        logger.error(f"Error starting process: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}", response_model=ProcessResponse)
async def get_process(process_id: str):
    """Busca detalhes do processo"""
    try:
        result = service.get_process(process_id)
        return ProcessResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/")
async def list_processes():
    """Lista todos os processos"""
    try:
        processes = service.list_processes()
        return {"processes": processes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}/results")
async def get_process_results(process_id: str):
    """Busca resultados do Textract"""
    try:
        results = service.get_textract_results(process_id)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
