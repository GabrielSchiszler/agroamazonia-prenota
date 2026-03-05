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
    logger.info("=" * 80)
    logger.info("[list_all] Requisição recebida para listar todas as regras CFOP")
    
    try:
        logger.info("[list_all] Chamando service.list_all...")
        rules = service.list_all()
        logger.info(f"[list_all] Regras listadas com sucesso! Total: {len(rules) if rules else 0}")
        logger.info("=" * 80)
        return {"rules": rules}
    except Exception as e:
        logger.error(f"[list_all] Erro inesperado: {str(e)}")
        logger.error(f"[list_all] Tipo do erro: {type(e).__name__}")
        logger.exception("[list_all] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{cfop}", summary="Buscar por CFOP")
async def get_by_cfop(cfop: str):
    """Busca operação por CFOP"""
    logger.info("=" * 80)
    logger.info("[get_by_cfop] Requisição recebida para buscar operação por CFOP")
    logger.info(f"[get_by_cfop] cfop: {cfop} (tipo: {type(cfop)})")
    
    try:
        logger.info("[get_by_cfop] Chamando service.get_by_cfop...")
        result = service.get_by_cfop(cfop)
        if not result:
            logger.warning(f"[get_by_cfop] CFOP {cfop} não encontrado")
            logger.info("=" * 80)
            raise HTTPException(status_code=404, detail=f"CFOP {cfop} não encontrado")
        logger.info(f"[get_by_cfop] Operação encontrada com sucesso!")
        logger.info(f"[get_by_cfop] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[get_by_cfop] Erro inesperado: {str(e)}")
        logger.error(f"[get_by_cfop] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_by_cfop] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="Criar nova regra")
async def create(rule: CfopOperationCreate):
    """Cria nova regra Chave x CFOP"""
    logger.info("=" * 80)
    logger.info("[create] Requisição recebida para criar regra CFOP")
    logger.info(f"[create] chave: {rule.chave} (tipo: {type(rule.chave)})")
    logger.info(f"[create] descricao: {rule.descricao} (tipo: {type(rule.descricao)})")
    logger.info(f"[create] cfop: {rule.cfop} (tipo: {type(rule.cfop)})")
    logger.info(f"[create] operacao: {rule.operacao} (tipo: {type(rule.operacao)})")
    logger.info(f"[create] Request completo: {rule.model_dump()}")
    
    try:
        logger.info("[create] Chamando service.create...")
        result = service.create(
            rule.chave,
            rule.descricao,
            rule.cfop,
            rule.operacao,
            rule.regra or '',
            rule.observacao or '',
            rule.pedido_compra or False,
            rule.ativo if rule.ativo is not None else True
        )
        logger.info(f"[create] Regra criada com sucesso!")
        logger.info(f"[create] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[create] Erro de validação: {str(e)}")
        logger.error(f"[create] Tipo do erro: {type(e).__name__}")
        logger.exception("[create] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[create] Erro inesperado: {str(e)}")
        logger.error(f"[create] Tipo do erro: {type(e).__name__}")
        logger.exception("[create] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{mapping_id}", summary="Atualizar regra")
async def update(mapping_id: str, rule: CfopOperationUpdate):
    """Atualiza regra existente"""
    logger.info("=" * 80)
    logger.info("[update] Requisição recebida para atualizar regra CFOP")
    logger.info(f"[update] mapping_id: {mapping_id} (tipo: {type(mapping_id)})")
    logger.info(f"[update] Request completo: {rule.model_dump()}")
    
    try:
        logger.info("[update] Chamando service.update...")
        result = service.update(
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
        logger.info(f"[update] Regra atualizada com sucesso!")
        logger.info(f"[update] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[update] Erro de validação: {str(e)}")
        logger.error(f"[update] Tipo do erro: {type(e).__name__}")
        logger.exception("[update] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[update] Erro inesperado: {str(e)}")
        logger.error(f"[update] Tipo do erro: {type(e).__name__}")
        logger.exception("[update] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{mapping_id}", summary="Remover regra")
async def delete(mapping_id: str):
    """Remove regra"""
    logger.info("=" * 80)
    logger.info("[delete] Requisição recebida para remover regra CFOP")
    logger.info(f"[delete] mapping_id: {mapping_id} (tipo: {type(mapping_id)})")
    
    try:
        logger.info("[delete] Chamando service.delete...")
        result = service.delete(mapping_id)
        logger.info(f"[delete] Regra removida com sucesso!")
        logger.info(f"[delete] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except ValueError as e:
        logger.error(f"[delete] Erro de validação: {str(e)}")
        logger.error(f"[delete] Tipo do erro: {type(e).__name__}")
        logger.exception("[delete] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[delete] Erro inesperado: {str(e)}")
        logger.error(f"[delete] Tipo do erro: {type(e).__name__}")
        logger.exception("[delete] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

