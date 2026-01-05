import logging
import traceback
from fastapi import APIRouter, HTTPException
from src.models.api import (
    XmlPresignedUrlRequest, DocsPresignedUrlRequest, DocsPresignedUrlResponse,
    ProcessStartRequest, ProcessStartResponse,
    ProcessResponse, UpdateFileMetadataRequest, UpdateFileMetadataResponse
)
from src.services.process_service import ProcessService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/process", 
    tags=["Process (Fluxo Moderno)"],
    responses={
        401: {
            "description": "Não autorizado - API Key inválida ou ausente",
            "content": {
                "application/json": {
                    "example": {"message": "Unauthorized"}
                }
            }
        }
    }
)
service = ProcessService()

@router.post("/presigned-url/xml", summary="1. Upload DANFE XML", 
    description="""Primeiro passo: gera URL para upload do XML da nota fiscal.
    
    **Autenticação**: Requer header `x-api-key: agroamazonia_key_<codigo>`
    """,
    responses={
        200: {
            "description": "URL de upload gerada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "upload_url": "https://agroamazonia-raw-documents-481665100875.s3.amazonaws.com/processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/danfe/nota_fiscal.xml?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAXAJL2FRFUBK54PNR%2F20251201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20251201T160000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=content-type%3Bhost&X-Amz-Signature=abc123def456",
                        "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/danfe/nota_fiscal.xml",
                        "file_name": "nota_fiscal.xml",
                        "content_type": "application/xml",
                        "doc_type": "DANFE"
                    }
                }
            }
        },
        404: {
            "description": "Processo não encontrado",
            "content": {"application/json": {"example": {"detail": "Processo não existe"}}}
        },
        500: {
            "description": "Erro ao gerar URL",
            "content": {"application/json": {"example": {"detail": "Falha ao gerar presigned URL do S3"}}}
        }
    })
async def get_xml_presigned_url(request: XmlPresignedUrlRequest):
    """Gera URL para upload do XML da DANFE.
    
    - **process_id**: ID do processo
    - **file_name**: Nome do arquivo XML
    - **file_type**: application/xml (padrão)
    
    **Após receber a URL**, faça um PUT request:
    ```bash
    curl -X PUT "<upload_url>" \
      -H "Content-Type: application/xml" \
      --data-binary @nota_fiscal.xml
    ```
    """
    try:
        result = service.generate_presigned_url(
            request.process_id,
            request.file_name,
            request.file_type,
            'DANFE',
            request.metadados
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/presigned-url/docs", response_model=DocsPresignedUrlResponse, summary="2. Upload Documento Adicional", 
    description="""Segundo passo: gera URL para upload de um documento extra (PDF).
    
    **Autenticação**: Requer header `x-api-key: agroamazonia_key_<codigo>`
    """,
    responses={
        200: {
            "description": "URL de upload gerada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "upload_url": "https://agroamazonia-raw-documents-481665100875.s3.amazonaws.com/processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/pedido.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAXAJL2FRFUBK54PNR%2F20251201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20251201T160000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=content-type%3Bhost&X-Amz-Signature=abc123def456",
                        "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/pedido.pdf",
                        "file_name": "pedido.pdf",
                        "content_type": "application/pdf",
                        "doc_type": "ADDITIONAL"
                    }
                }
            }
        },
        404: {
            "description": "Processo não encontrado",
            "content": {"application/json": {"example": {"detail": "Processo não existe"}}}
        },
        500: {
            "description": "Erro ao gerar URL",
            "content": {"application/json": {"example": {"detail": "Falha ao gerar presigned URL do S3"}}}
        }
    })
async def get_docs_presigned_url(request: DocsPresignedUrlRequest):
    """Gera URL para upload de um documento adicional.
    
    - **process_id**: ID do processo
    - **file_name**: Nome do arquivo
    - **file_type**: Content-Type do arquivo (padrão: application/pdf)
    - **metadados**: Metadados adicionais do arquivo (opcional)
    
    **Após receber a URL**, faça um PUT request:
    ```bash
    curl -X PUT "<upload_url>" \
      -H "Content-Type: application/pdf" \
      --data-binary @arquivo.pdf
    ```
    """
    try:
        result = service.generate_presigned_url(
            request.process_id,
            request.file_name,
            "application/pdf",
            "ADDITIONAL",
            request.metadados
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/start", response_model=ProcessStartResponse, summary="3. Iniciar Processamento", 
    description="""Terceiro passo: iniciar processamento após uploads.
    
    **Autenticação**: Requer header `x-api-key: agroamazonia_key_<codigo>`
    
    **Nota**: O tipo de processo é automaticamente definido como AGROQUIMICOS.
    """,
    responses={
        200: {"description": "Processamento iniciado com sucesso"},
        400: {
            "description": "Requisição inválida",
            "content": {"application/json": {"examples": {
                "no_files": {"value": {"detail": "Nenhum arquivo enviado"}},
                "no_danfe": {"value": {"detail": "DANFE obrigatório não encontrado"}},
                "no_additional": {"value": {"detail": "Pelo menos um documento adicional é necessário"}}
            }}}
        },
        404: {
            "description": "Processo não encontrado",
            "content": {"application/json": {"examples": {
                "not_found": {"value": {"detail": "Processo não encontrado"}},
                "no_metadata": {"value": {"detail": "Metadados do processo não encontrados"}}
            }}}
        },
        500: {
            "description": "Erro ao iniciar processamento",
            "content": {"application/json": {"example": {"detail": "Falha ao iniciar Step Functions"}}}
        }
    })
async def start_process(request: ProcessStartRequest):
    """**PASSO FINAL**: Inicia o processamento via Step Functions.
    
    - **process_id**: ID do processo com arquivos já enviados
    
    **Pré-requisitos**:
    - Pelo menos 1 arquivo DANFE (XML ou PDF) enviado
    - Pelo menos 1 documento ADDITIONAL enviado
    
    **O processamento inclui**:
    1. Extração de dados com Textract (PDFs)
    2. Parse de XML (DANFE)
    3. Conversão OCR para JSON estruturado com Bedrock
    4. Validação de regras de negócio (AGROQUIMICOS)
    
    **Exemplo**:
    ```json
    {
      "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318"
    }
    ```
    """
    try:
        logger.info(f"Starting process: {request.process_id}")
        result = service.start_process(request.process_id)
        logger.info(f"Process started successfully: {result}")
        return ProcessStartResponse(**result)
    except ValueError as e:
        error_msg = str(e)
        if "não encontrado" in error_msg or "Metadados" in error_msg:
            raise HTTPException(status_code=404, detail=error_msg)
        else:
            raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        logger.error(f"Error starting process: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}", response_model=ProcessResponse, summary="Buscar Processo", description="Retorna detalhes e arquivos de um processo", include_in_schema=False)
async def get_process(process_id: str):
    """Busca detalhes completos do processo.
    
    - **process_id**: ID do processo
    """
    try:
        result = service.get_process(process_id)
        return ProcessResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", summary="Listar Processos", description="Lista todos os processos criados", include_in_schema=False,
    responses={
        200: {
            "description": "Lista de processos",
            "content": {
                "application/json": {
                    "example": {
                        "processes": [
                            {
                                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                                "process_type": "SEMENTES",
                                "status": "COMPLETED",
                                "created_at": "1733068800"
                            },
                            {
                                "process_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                                "process_type": "FERTILIZANTES",
                                "status": "PROCESSING",
                                "created_at": "1733155200"
                            }
                        ]
                    }
                }
            }
        }
    })
async def list_processes():
    """Lista todos os processos do sistema."""
    try:
        processes = service.list_processes()
        return {"processes": processes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/download", summary="Download de Arquivo", description="Gera URL de download para arquivo do processo")
async def download_file(request: dict):
    """Gera URL de download para arquivo."""
    try:
        file_key = request.get('file_key')
        if not file_key:
            raise HTTPException(status_code=400, detail="file_key é obrigatório")
        
        download_url = service.generate_download_url(file_key)
        return {"download_url": download_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_id}/validations", summary="Buscar Validações", description="Retorna resultados das validações de regras", include_in_schema=False,
    responses={
        200: {
            "description": "Validações executadas com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "validations": [
                            {
                                "type": "validar_numero_nota",
                                "danfe_value": "2655",
                                "status": "PASSED",
                                "message": "Número da nota validado em todos os documentos",
                                "docs": [
                                    {
                                        "file_name": "NF_000002655.PDF",
                                        "value": "000002655",
                                        "status": "MATCH"
                                    },
                                    {
                                        "file_name": "FIL_013_NF_000002655.PDF",
                                        "value": "000002655",
                                        "status": "MATCH"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
    })
async def get_validations(process_id: str):
    """Busca resultados das validações executadas.
    
    - **process_id**: ID do processo
    
    Retorna lista de validações com status MATCH/MISMATCH para cada documento.
    """
    try:
        results = service.get_validation_results(process_id)
        return {"validations": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/file/metadata", response_model=UpdateFileMetadataResponse, summary="Atualizar Metadados de Arquivo",
    description="""Atualiza os metadados JSON de um arquivo específico.
    
    **Autenticação**: Requer header `x-api-key: agroamazonia_key_<codigo>`
    
    Útil para corrigir ou atualizar metadados após o upload do arquivo.
    """,
    responses={
        200: {
            "description": "Metadados atualizados com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "message": "Metadados atualizados com sucesso",
                        "file_name": "pedido_compra.pdf",
                        "metadados": {
                            "moeda": "BRL",
                            "pedidoFornecedor": "369763"
                        }
                    }
                }
            }
        },
        404: {
            "description": "Arquivo não encontrado",
            "content": {"application/json": {"example": {"detail": "Arquivo não encontrado"}}}
        },
        500: {
            "description": "Erro ao atualizar metadados",
            "content": {"application/json": {"example": {"detail": "Erro ao atualizar metadados"}}}
        }
    })
async def update_file_metadata(request: UpdateFileMetadataRequest):
    """Atualiza metadados JSON de um arquivo específico"""
    try:
        result = service.update_file_metadata(
            request.process_id,
            request.file_name,
            request.metadados
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating file metadata: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
