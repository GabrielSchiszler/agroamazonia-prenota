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
router = APIRouter(prefix="/process", tags=["Process"])
service = ProcessService()

@router.post("/presigned-url/xml", summary="Upload DANFE XML")
async def get_xml_presigned_url(request: XmlPresignedUrlRequest):
    """Gera URL para upload do XML da DANFE"""
    logger.info("=" * 80)
    logger.info("[get_xml_presigned_url] Requisição recebida para gerar URL pré-assinada XML")
    logger.info(f"[get_xml_presigned_url] process_id: {request.process_id} (tipo: {type(request.process_id)})")
    logger.info(f"[get_xml_presigned_url] file_name: {request.file_name} (tipo: {type(request.file_name)})")
    logger.info(f"[get_xml_presigned_url] file_type: {request.file_type} (tipo: {type(request.file_type)})")
    logger.info(f"[get_xml_presigned_url] metadados: {request.metadados} (tipo: {type(request.metadados)})")
    logger.info(f"[get_xml_presigned_url] Request completo: {request.model_dump()}")
    
    try:
        logger.info("[get_xml_presigned_url] Chamando service.generate_presigned_url...")
        result = service.generate_presigned_url(
            request.process_id, request.file_name, request.file_type, 'DANFE', request.metadados
        )
        logger.info(f"[get_xml_presigned_url] URL gerada com sucesso!")
        logger.info(f"[get_xml_presigned_url] file_key: {result.get('file_key')}")
        logger.info(f"[get_xml_presigned_url] file_name: {result.get('file_name')}")
        logger.info(f"[get_xml_presigned_url] upload_url (primeiros 100 chars): {result.get('upload_url', '')[:100]}...")
        logger.info(f"[get_xml_presigned_url] Resposta completa: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[get_xml_presigned_url] Erro de validação: {str(e)}")
        logger.error(f"[get_xml_presigned_url] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_xml_presigned_url] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[get_xml_presigned_url] Erro inesperado: {str(e)}")
        logger.error(f"[get_xml_presigned_url] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_xml_presigned_url] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/presigned-url/docs", response_model=DocsPresignedUrlResponse, summary="Upload Documento Adicional")
async def get_docs_presigned_url(request: DocsPresignedUrlRequest):
    """Gera URL para upload de documento adicional"""
    logger.info("=" * 80)
    logger.info("[get_docs_presigned_url] Requisição recebida para gerar URL pré-assinada de documento")
    logger.info(f"[get_docs_presigned_url] process_id: {request.process_id} (tipo: {type(request.process_id)})")
    logger.info(f"[get_docs_presigned_url] file_name: {request.file_name} (tipo: {type(request.file_name)})")
    logger.info(f"[get_docs_presigned_url] file_type: {request.file_type} (tipo: {type(request.file_type)})")
    logger.info(f"[get_docs_presigned_url] metadados: {request.metadados} (tipo: {type(request.metadados)})")
    logger.info(f"[get_docs_presigned_url] Request completo: {request.model_dump()}")
    
    try:
        logger.info("[get_docs_presigned_url] Chamando service.generate_presigned_url...")
        result = service.generate_presigned_url(
            request.process_id, request.file_name, request.file_type, "ADDITIONAL", request.metadados
        )
        logger.info(f"[get_docs_presigned_url] URL gerada com sucesso!")
        logger.info(f"[get_docs_presigned_url] file_key: {result.get('file_key')}")
        logger.info(f"[get_docs_presigned_url] Resposta completa: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[get_docs_presigned_url] Erro de validação: {str(e)}")
        logger.error(f"[get_docs_presigned_url] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_docs_presigned_url] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[get_docs_presigned_url] Erro inesperado: {str(e)}")
        logger.error(f"[get_docs_presigned_url] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_docs_presigned_url] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/metadados/pedido", response_model=PedidoCompraMetadataResponse, summary="Vincular Metadados do Pedido de Compra")
async def link_pedido_compra_metadata(request: PedidoCompraMetadataRequest):
    """
    Vincula metadados do pedido de compra ao processo (sem arquivo físico).
    
    Os metadados são salvos no DynamoDB e serão lidos pelo Lambda send_to_protheus
    durante o processamento, mantendo compatibilidade com o código existente.
    """
    logger.info("=" * 80)
    logger.info("[link_pedido_compra_metadata] Requisição recebida para vincular metadados do pedido de compra")
    logger.info(f"[link_pedido_compra_metadata] process_id: {request.process_id} (tipo: {type(request.process_id)})")
    logger.info(f"[link_pedido_compra_metadata] metadados: {request.metadados} (tipo: {type(request.metadados)})")
    if isinstance(request.metadados, dict):
        logger.info(f"[link_pedido_compra_metadata] Chaves dos metadados: {list(request.metadados.keys())}")
    logger.info(f"[link_pedido_compra_metadata] Request completo: {request.model_dump()}")
    
    try:
        logger.info("[link_pedido_compra_metadata] Chamando service.link_pedido_compra_metadata...")
        result = service.link_pedido_compra_metadata(request.process_id, request.metadados)
        logger.info(f"[link_pedido_compra_metadata] Metadados vinculados com sucesso para process_id: {request.process_id}")
        logger.info(f"[link_pedido_compra_metadata] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[link_pedido_compra_metadata] Erro de validação: {str(e)}")
        logger.error(f"[link_pedido_compra_metadata] Tipo do erro: {type(e).__name__}")
        logger.exception("[link_pedido_compra_metadata] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[link_pedido_compra_metadata] Erro inesperado: {str(e)}")
        logger.error(f"[link_pedido_compra_metadata] Tipo do erro: {type(e).__name__}")
        logger.exception("[link_pedido_compra_metadata] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=f"Erro interno ao vincular metadados: {str(e)}")

@router.post("/start", response_model=ProcessStartResponse, summary="Iniciar Processamento")
async def start_process(request: ProcessStartRequest):
    """Inicia processamento via Step Functions"""
    logger.info("=" * 80)
    logger.info("[start_process] Requisição recebida para iniciar processamento")
    logger.info(f"[start_process] process_id: {request.process_id} (tipo: {type(request.process_id)})")
    logger.info(f"[start_process] Request completo: {request.model_dump()}")
    
    try:
        logger.info("[start_process] Chamando service.start_process...")
        result = service.start_process(request.process_id)
        logger.info(f"[start_process] Processo iniciado com sucesso!")
        logger.info(f"[start_process] Resposta: {result}")
        logger.info("=" * 80)
        return ProcessStartResponse(**result)
    except ValueError as e:
        status = 404 if "não encontrado" in str(e) or "Metadados" in str(e) else 400
        logger.error(f"[start_process] Erro de validação: {str(e)}")
        logger.error(f"[start_process] Tipo do erro: {type(e).__name__}")
        logger.error(f"[start_process] Status HTTP: {status}")
        logger.exception("[start_process] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=status, detail=str(e))
    except Exception as e:
        logger.error(f"[start_process] Erro inesperado: {str(e)}")
        logger.error(f"[start_process] Tipo do erro: {type(e).__name__}")
        logger.exception("[start_process] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}", response_model=ProcessResponse, summary="Buscar Processo", include_in_schema=False)
async def get_process(process_id: str):
    """Busca detalhes do processo"""
    logger.info("=" * 80)
    logger.info("[get_process] Requisição recebida para buscar processo")
    logger.info(f"[get_process] process_id: {process_id} (tipo: {type(process_id)})")
    
    try:
        logger.info("[get_process] Chamando service.get_process...")
        result = service.get_process(process_id)
        logger.info(f"[get_process] Processo encontrado com sucesso!")
        logger.info(f"[get_process] Resposta: {result}")
        logger.info("=" * 80)
        return ProcessResponse(**result)
    except ValueError as e:
        logger.error(f"[get_process] Erro de validação: {str(e)}")
        logger.error(f"[get_process] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_process] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[get_process] Erro inesperado: {str(e)}")
        logger.error(f"[get_process] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_process] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", summary="Listar Processos", include_in_schema=False)
async def list_processes():
    """Lista todos os processos"""
    logger.info("=" * 80)
    logger.info("[list_processes] Requisição recebida para listar processos")
    
    try:
        logger.info("[list_processes] Chamando service.list_processes...")
        processes = service.list_processes()
        logger.info(f"[list_processes] Processos listados com sucesso! Total: {len(processes) if processes else 0}")
        logger.info("=" * 80)
        return {"processes": processes}
    except Exception as e:
        logger.error(f"[list_processes] Erro inesperado: {str(e)}")
        logger.error(f"[list_processes] Tipo do erro: {type(e).__name__}")
        logger.exception("[list_processes] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/download", summary="Download de Arquivo", include_in_schema=False)
async def download_file(request: dict):
    """Gera URL de download para arquivo"""
    logger.info("=" * 80)
    logger.info("[download_file] Requisição recebida para gerar URL de download")
    logger.info(f"[download_file] Request completo: {request}")
    logger.info(f"[download_file] file_key: {request.get('file_key')} (tipo: {type(request.get('file_key'))})")
    
    try:
        if not request.get('file_key'):
            logger.error("[download_file] Erro: file_key é obrigatório")
            logger.info("=" * 80)
            raise HTTPException(status_code=400, detail="file_key é obrigatório")
        
        logger.info("[download_file] Chamando service.generate_download_url...")
        download_url = service.generate_download_url(request['file_key'])
        logger.info(f"[download_file] URL de download gerada com sucesso!")
        logger.info(f"[download_file] download_url (primeiros 100 chars): {download_url[:100] if download_url else 'None'}...")
        logger.info("=" * 80)
        return {"download_url": download_url}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[download_file] Erro inesperado: {str(e)}")
        logger.error(f"[download_file] Tipo do erro: {type(e).__name__}")
        logger.exception("[download_file] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}/validations", summary="Buscar Validações", include_in_schema=False)
async def get_validations(process_id: str):
    """Retorna resultados das validações"""
    logger.info("=" * 80)
    logger.info("[get_validations] Requisição recebida para buscar validações")
    logger.info(f"[get_validations] process_id: {process_id} (tipo: {type(process_id)})")
    
    try:
        logger.info("[get_validations] Chamando service.get_validation_results...")
        validations = service.get_validation_results(process_id)
        logger.info(f"[get_validations] Validações encontradas com sucesso! Total: {len(validations) if validations else 0}")
        logger.info(f"[get_validations] Resposta: {validations}")
        logger.info("=" * 80)
        return {"validations": validations}
    except Exception as e:
        logger.error(f"[get_validations] Erro inesperado: {str(e)}")
        logger.error(f"[get_validations] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_validations] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/file/metadata", response_model=UpdateFileMetadataResponse, summary="Atualizar Metadados")
async def update_file_metadata(request: UpdateFileMetadataRequest):
    """Atualiza metadados JSON de um arquivo"""
    logger.info("=" * 80)
    logger.info("[update_file_metadata] Requisição recebida para atualizar metadados de arquivo")
    logger.info(f"[update_file_metadata] process_id: {request.process_id} (tipo: {type(request.process_id)})")
    logger.info(f"[update_file_metadata] file_name: {request.file_name} (tipo: {type(request.file_name)})")
    logger.info(f"[update_file_metadata] metadados: {request.metadados} (tipo: {type(request.metadados)})")
    logger.info(f"[update_file_metadata] Request completo: {request.model_dump()}")
    
    try:
        logger.info("[update_file_metadata] Chamando service.update_file_metadata...")
        result = service.update_file_metadata(request.process_id, request.file_name, request.metadados)
        logger.info(f"[update_file_metadata] Metadados atualizados com sucesso!")
        logger.info(f"[update_file_metadata] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[update_file_metadata] Erro de validação: {str(e)}")
        logger.error(f"[update_file_metadata] Tipo do erro: {type(e).__name__}")
        logger.exception("[update_file_metadata] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[update_file_metadata] Erro inesperado: {str(e)}")
        logger.error(f"[update_file_metadata] Tipo do erro: {type(e).__name__}")
        logger.exception("[update_file_metadata] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))
