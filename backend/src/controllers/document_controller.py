from fastapi import APIRouter, HTTPException
from src.services.document_service import DocumentService
from src.services.document_list_service import DocumentListService

router = APIRouter(prefix="/api/v1", tags=["documents"])
service = DocumentService()
list_service = DocumentListService()

@router.get("/documents")
async def list_documents():
    """Lista todos os documentos"""
    try:
        documents = list_service.list_all_documents()
        return {"documents": documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/document/submit")
async def submit_document(request: dict):
    """Inicia processamento de documento"""
    try:
        result = service.submit_document(
            request['document_id'],
            request['document_type'],
            request['process_type'],
            request['s3_path']
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}")
async def get_document(document_id: str):
    """Consulta todos os dados de um documento"""
    try:
        result = service.get_document(document_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/{document_id}/pre-note")
async def get_pre_note(document_id: str):
    """Consulta apenas dados da pré-nota"""
    try:
        result = service.get_pre_note(document_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/document/rules/{process_type}")
async def get_process_rules(process_type: str):
    """Retorna as regras configuradas para um tipo de processo"""
    try:
        from src.services.rules_service import RulesService
        rules_info = RulesService.get_rules_info(process_type)
        return {"process_type": process_type, "rules": rules_info}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
