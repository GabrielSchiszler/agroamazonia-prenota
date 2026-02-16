import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from src.services.cfop_operation_service import CfopOperationService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/cfop-operation", tags=["Chave x CFOP"])
service = CfopOperationService()

class CfopOperationCreate(BaseModel):
    chave: str = Field(..., description="Chave da operação (ex: 1B, 3I)")
    descricao: str = Field(..., description="Descrição da operação")
    cfop: str = Field(..., description="CFOP(s) separados por espaço (ex: 5101 6101 5102)")
    operacao: str = Field(..., description="Código da operação para Protheus (mesmo da chave)")
    regra: Optional[str] = Field(None, description="Texto descritivo de quando usar a regra")
    observacao: Optional[str] = Field(None, description="Observações adicionais")
    pedido_compra: Optional[bool] = Field(False, description="Se requer pedido de compra")
    ativo: Optional[bool] = Field(True, description="Se a regra está ativa")

class CfopOperationUpdate(BaseModel):
    chave: Optional[str] = Field(None, description="Chave da operação")
    descricao: Optional[str] = Field(None, description="Descrição")
    cfop: Optional[str] = Field(None, description="CFOP(s)")
    operacao: Optional[str] = Field(None, description="Código da operação")
    regra: Optional[str] = Field(None, description="Texto descritivo de quando usar")
    observacao: Optional[str] = Field(None, description="Observações")
    pedido_compra: Optional[bool] = Field(None, description="Se requer pedido de compra")
    ativo: Optional[bool] = Field(None, description="Se a regra está ativa")

@router.get("/", summary="Listar todas as regras")
async def list_all():
    """Lista todas as regras Chave x CFOP"""
    try:
        return {"rules": service.list_all()}
    except Exception as e:
        logger.error(f"Error listing CFOP operations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{cfop}", summary="Buscar por CFOP")
async def get_by_cfop(cfop: str):
    """Busca operação por CFOP"""
    try:
        result = service.get_by_cfop(cfop)
        if not result:
            raise HTTPException(status_code=404, detail=f"CFOP {cfop} não encontrado")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting CFOP operation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="Criar nova regra")
async def create(rule: CfopOperationCreate):
    """Cria nova regra Chave x CFOP"""
    try:
        return service.create(
            rule.chave,
            rule.descricao,
            rule.cfop,
            rule.operacao,
            rule.regra or '',
            rule.observacao or '',
            rule.pedido_compra or False,
            rule.ativo if rule.ativo is not None else True
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating CFOP operation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{mapping_id}", summary="Atualizar regra")
async def update(mapping_id: str, rule: CfopOperationUpdate):
    """Atualiza regra existente"""
    try:
        return service.update(
            mapping_id,
            rule.chave,
            rule.descricao,
            rule.cfop,
            rule.operacao,
            rule.regra,
            rule.observacao,
            rule.pedido_compra,
            rule.ativo
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating CFOP operation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{mapping_id}", summary="Remover regra")
async def delete(mapping_id: str):
    """Remove regra"""
    try:
        return service.delete(mapping_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting CFOP operation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

