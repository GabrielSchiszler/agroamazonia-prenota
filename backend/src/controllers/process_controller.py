import logging
from fastapi import APIRouter, HTTPException
from src.models.api import (
    XmlPresignedUrlRequest, DocsPresignedUrlRequest, DocsPresignedUrlResponse,
    ProcessStartRequest, ProcessStartResponse,
    ProcessResponse, UpdateFileMetadataRequest, UpdateFileMetadataResponse,
    PedidoCompraMetadataRequest, PedidoCompraMetadataResponse
)
from src.services.process_service import ProcessService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/process", tags=["Process"])
service = ProcessService()

@router.post("/presigned-url/xml", summary="Upload DANFE XML")
async def get_xml_presigned_url(request: XmlPresignedUrlRequest):
    """Gera URL para upload do XML da DANFE"""
    try:
        return service.generate_presigned_url(
            request.process_id, request.file_name, request.file_type, 'DANFE', request.metadados
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/presigned-url/docs", response_model=DocsPresignedUrlResponse, summary="Upload Documento Adicional")
async def get_docs_presigned_url(request: DocsPresignedUrlRequest):
    """Gera URL para upload de documento adicional"""
    try:
        return service.generate_presigned_url(
            request.process_id, request.file_name, request.file_type, "ADDITIONAL", request.metadados
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/metadados/pedido", response_model=PedidoCompraMetadataResponse, summary="Vincular Metadados do Pedido de Compra")
async def link_pedido_compra_metadata(request: PedidoCompraMetadataRequest):
    """
    Vincula metadados do pedido de compra ao processo (sem arquivo físico).
    
    Os metadados são salvos no DynamoDB e serão lidos pelo Lambda send_to_protheus
    durante o processamento, mantendo compatibilidade com o código existente.
    """
    try:
        logger.info(f"Linking pedido compra metadata for process: {request.process_id}")
        logger.info(f"Metadados type: {type(request.metadados)}, keys: {list(request.metadados.keys()) if isinstance(request.metadados, dict) else 'N/A'}")
        result = service.link_pedido_compra_metadata(request.process_id, request.metadados)
        logger.info(f"Successfully linked metadata for process: {request.process_id}")
        return result
    except ValueError as e:
        logger.error(f"Validation error linking pedido compra metadata: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error linking pedido compra metadata: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=f"Erro interno ao vincular metadados: {str(e)}")

@router.post("/start", response_model=ProcessStartResponse, summary="Iniciar Processamento")
async def start_process(request: ProcessStartRequest):
    """Inicia processamento via Step Functions"""
    try:
        logger.info(f"Starting process: {request.process_id}")
        result = service.start_process(request.process_id)
        return ProcessStartResponse(**result)
    except ValueError as e:
        status = 404 if "não encontrado" in str(e) or "Metadados" in str(e) else 400
        raise HTTPException(status_code=status, detail=str(e))
    except Exception as e:
        logger.error(f"Error starting process: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}", response_model=ProcessResponse, summary="Buscar Processo", include_in_schema=False)
async def get_process(process_id: str):
    """Busca detalhes do processo"""
    try:
        return ProcessResponse(**service.get_process(process_id))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", summary="Listar Processos", include_in_schema=False)
async def list_processes():
    """Lista todos os processos"""
    try:
        return {"processes": service.list_processes()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/download", summary="Download de Arquivo", include_in_schema=False)
async def download_file(request: dict):
    """Gera URL de download para arquivo"""
    try:
        if not request.get('file_key'):
            raise HTTPException(status_code=400, detail="file_key é obrigatório")
        return {"download_url": service.generate_download_url(request['file_key'])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}/validations", summary="Buscar Validações", include_in_schema=False)
async def get_validations(process_id: str):
    """Retorna resultados das validações"""
    try:
        return {"validations": service.get_validation_results(process_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/file/metadata", response_model=UpdateFileMetadataResponse, summary="Atualizar Metadados")
async def update_file_metadata(request: UpdateFileMetadataRequest):
    """Atualiza metadados JSON de um arquivo"""
    try:
        return service.update_file_metadata(request.process_id, request.file_name, request.metadados)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
