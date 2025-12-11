from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from src.services.document_service import DocumentService
from src.services.document_list_service import DocumentListService

class DocumentSubmitRequest(BaseModel):
    document_id: str = Field(..., description="ID único do documento")
    document_type: str = Field(..., description="Tipo do documento", example="PRE_NOTE")
    process_type: str = Field(..., description="Tipo de processo", example="SEMENTES")
    s3_path: str = Field(..., description="Caminho S3 do documento", example="s3://bucket/pre-notas/doc123/file.pdf")
    
    class Config:
        schema_extra = {
            "example": {
                "document_id": "0101600000051601",
                "document_type": "PRE_NOTE",
                "process_type": "SEMENTES",
                "s3_path": "s3://agroamazonia-raw-documents/pre-notas/0101600000051601/doc.pdf"
            }
        }

router = APIRouter(prefix="/api/v1", tags=["Documents (Legacy)"], include_in_schema=False)
service = DocumentService()
list_service = DocumentListService()

@router.get("/documents", summary="Listar Documentos", description="Lista todos os documentos processados (Legacy)",
    responses={
        200: {
            "description": "Lista de documentos",
            "content": {
                "application/json": {
                    "example": {
                        "documents": [
                            {
                                "document_id": "0101600000051601",
                                "document_type": "PRE_NOTE",
                                "process_type": "SEMENTES",
                                "status": "PROCESSED",
                                "created_at": "1733068800"
                            }
                        ]
                    }
                }
            }
        }
    })
async def list_documents():
    """Lista todos os documentos do sistema."""
    try:
        documents = list_service.list_all_documents()
        return {"documents": documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/document/submit", summary="Submeter Documento (Legacy)", description="Inicia processamento de documento via caminho S3")
async def submit_document(request: DocumentSubmitRequest):
    """Submete documento para processamento (Método Legacy).
    
    **Nota**: Este endpoint é legacy. Use o fluxo moderno:
    1. POST /api/v1/process/create
    2. POST /api/v1/process/presigned-url (para cada arquivo)
    3. PUT na URL retornada (upload direto ao S3)
    4. POST /api/v1/process/start
    
    **Exemplos de uso:**
    
    **Exemplo 1 - DANFE XML:**
    ```json
    {
      "document_id": "0101600000051601",
      "document_type": "DANFE_XML",
      "process_type": "SEMENTES",
      "s3_path": "s3://bucket/danfe/nota_fiscal.xml"
    }
    ```
    
    **Exemplo 2 - Documento Adicional PDF:**
    ```json
    {
      "document_id": "0101600000051601",
      "document_type": "PEDIDO_COMPRA",
      "process_type": "FERTILIZANTES",
      "s3_path": "s3://bucket/docs/pedido_48277.pdf"
    }
    ```
    
    **Exemplo 3 - Nota Fiscal PDF:**
    ```json
    {
      "document_id": "0101600000051601",
      "document_type": "PRE_NOTE",
      "process_type": "AGROQUIMICOS",
      "s3_path": "s3://bucket/pre-notas/nf_000002655.pdf"
    }
    ```
    """
    try:
        result = service.submit_document(
            request.document_id,
            request.document_type,
            request.process_type,
            request.s3_path
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}", summary="Buscar Documento", description="Consulta todos os dados de um documento",
    responses={
        200: {
            "description": "Dados do documento",
            "content": {
                "application/json": {
                    "example": {
                        "document_id": "0101600000051601",
                        "document_type": "PRE_NOTE",
                        "process_type": "SEMENTES",
                        "status": "PROCESSED",
                        "s3_path": "s3://bucket/pre-notas/0101600000051601/doc.pdf",
                        "created_at": "1733068800"
                    }
                }
            }
        }
    })
async def get_document(document_id: str):
    """Busca todos os dados de um documento.
    
    - **document_id**: ID do documento
    """
    try:
        result = service.get_document(document_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}/pre-note", summary="Buscar Pré-Nota", description="Consulta apenas dados da pré-nota",
    responses={
        200: {
            "description": "Dados da pré-nota",
            "content": {
                "application/json": {
                    "example": {
                        "document_id": "0101600000051601",
                        "pre_note_data": {
                            "numero_nota": "2655",
                            "serie": "1",
                            "valor_total": "34200.00"
                        }
                    }
                }
            }
        }
    })
async def get_pre_note(document_id: str):
    """Busca apenas dados da pré-nota.
    
    - **document_id**: ID do documento
    """
    try:
        result = service.get_pre_note(document_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/rules/{process_type}", summary="Buscar Regras", description="Retorna regras configuradas para um tipo de processo",
    responses={
        200: {
            "description": "Regras do tipo de processo",
            "content": {
                "application/json": {
                    "example": {
                        "process_type": "SEMENTES",
                        "rules": [
                            {
                                "name": "Validação de Imposto",
                                "description": "Verifica se o imposto total está dentro do limite"
                            }
                        ]
                    }
                }
            }
        }
    })
async def get_process_rules(process_type: str):
    """Retorna as regras configuradas.
    
    - **process_type**: SEMENTES, AGROQUIMICOS ou FERTILIZANTES
    """
    try:
        from src.services.rules_service import RulesService
        rules_info = RulesService.get_rules_info(process_type)
        return {"process_type": process_type, "rules": rules_info}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
