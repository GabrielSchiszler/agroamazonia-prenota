from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from src.services.document_service import DocumentService

class DocumentSubmitRequest(BaseModel):
    document_id: str = Field(..., description="ID único do documento")
    document_type: str = Field(..., description="Tipo do documento")
    process_type: str = Field(..., description="Tipo de processo")
    s3_path: str = Field(..., description="Caminho S3 do documento")

router = APIRouter(prefix="/api/v1", tags=["Documents (Legacy)"], include_in_schema=False)
service = DocumentService()

@router.get("/documents", summary="Listar Documentos")
async def list_documents():
    """Lista todos os documentos (Legacy)"""
    try:
        return {"documents": service.list_all_documents()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/document/submit", summary="Submeter Documento (Legacy)")
async def submit_document(request: DocumentSubmitRequest):
    """Submete documento para processamento (Método Legacy)"""
    try:
        return service.submit_document(
            request.document_id, request.document_type, request.process_type, request.s3_path
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}", summary="Buscar Documento")
async def get_document(document_id: str):
    """Busca dados de um documento"""
    try:
        return service.get_document(document_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}/pre-note", summary="Buscar Pré-Nota")
async def get_pre_note(document_id: str):
    """Busca dados da pré-nota"""
    try:
        return service.get_pre_note(document_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/rules/{process_type}", summary="Buscar Regras")
async def get_process_rules(process_type: str):
    """Retorna regras configuradas para um tipo de processo"""
    try:
        from src.services.rules_service import RulesService
        return {"process_type": process_type, "rules": RulesService.get_rules_info(process_type)}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
