import logging
from fastapi import APIRouter, HTTPException
from src.services.rules_service import RulesService
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/rules", tags=["Rules"])
service = RulesService()

class RuleCreate(BaseModel):
    process_type: str
    rule_name: str
    order: int
    enabled: bool = True

class RuleUpdate(BaseModel):
    order: int = None
    enabled: bool = None

@router.get("/available", summary="Listar Regras Disponíveis")
async def list_available_rules():
    """Lista todas as regras disponíveis com descrições"""
    logger.info("=" * 80)
    logger.info("[list_available_rules] Requisição recebida para listar regras disponíveis")
    
    try:
        result = {
        "rules": [
            {
                "name": "validar_numero_nota",
                "description": "Valida número da nota fiscal"
            },
            {
                "name": "validar_serie",
                "description": "Valida série da nota fiscal"
            },
            {
                "name": "validar_data_emissao",
                "description": "Valida data de emissão"
            },
            {
                "name": "validar_cnpj_fornecedor",
                "description": "Valida CNPJ do fornecedor (primeiros 8 dígitos)"
            },
            {
                "name": "validar_produtos",
                "description": "Valida produtos (nome)"
            },
            {
                "name": "validar_numero_pedido",
                "description": "Valida número do pedido"
            },
            {
                "name": "validar_cfop_chave",
                "description": "Valida CFOP e busca chave correspondente"
            },
            {
                "name": "validar_icms",
                "description": "Valida ICMS (interno zerado, interestadual)"
            },
            {
                "name": "validar_cnpj_destinatario",
                "description": "Valida CNPJ do destinatário (primeiros 8 dígitos)"
            }
        ]
        }
        logger.info(f"[list_available_rules] Regras listadas com sucesso! Total: {len(result.get('rules', []))}")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[list_available_rules] Erro inesperado: {str(e)}")
        logger.error(f"[list_available_rules] Tipo do erro: {type(e).__name__}")
        logger.exception("[list_available_rules] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{process_type}", summary="Listar Regras")
async def list_rules(process_type: str):
    """Lista regras de um tipo de processo"""
    logger.info("=" * 80)
    logger.info("[list_rules] Requisição recebida para listar regras")
    logger.info(f"[list_rules] process_type: {process_type} (tipo: {type(process_type)})")
    
    try:
        logger.info("[list_rules] Chamando service.list_rules...")
        rules = service.list_rules(process_type)
        logger.info(f"[list_rules] Regras listadas com sucesso! Total: {len(rules) if rules else 0}")
        logger.info("=" * 80)
        return {"rules": rules}
    except Exception as e:
        logger.error(f"[list_rules] Erro inesperado: {str(e)}")
        logger.error(f"[list_rules] Tipo do erro: {type(e).__name__}")
        logger.exception("[list_rules] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="Criar Regra")
async def create_rule(rule: RuleCreate):
    """Cria nova regra de validação"""
    logger.info("=" * 80)
    logger.info("[create_rule] Requisição recebida para criar regra")
    logger.info(f"[create_rule] process_type: {rule.process_type} (tipo: {type(rule.process_type)})")
    logger.info(f"[create_rule] rule_name: {rule.rule_name} (tipo: {type(rule.rule_name)})")
    logger.info(f"[create_rule] order: {rule.order} (tipo: {type(rule.order)})")
    logger.info(f"[create_rule] enabled: {rule.enabled} (tipo: {type(rule.enabled)})")
    logger.info(f"[create_rule] Request completo: {rule.model_dump()}")
    
    try:
        logger.info("[create_rule] Chamando service.create_rule...")
        result = service.create_rule(rule.process_type, rule.rule_name, rule.order, rule.enabled)
        logger.info(f"[create_rule] Regra criada com sucesso!")
        logger.info(f"[create_rule] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[create_rule] Erro inesperado: {str(e)}")
        logger.error(f"[create_rule] Tipo do erro: {type(e).__name__}")
        logger.exception("[create_rule] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{process_type}/{rule_name}", summary="Atualizar Regra")
async def update_rule(process_type: str, rule_name: str, rule: RuleUpdate):
    """Atualiza regra existente"""
    logger.info("=" * 80)
    logger.info("[update_rule] Requisição recebida para atualizar regra")
    logger.info(f"[update_rule] process_type: {process_type} (tipo: {type(process_type)})")
    logger.info(f"[update_rule] rule_name: {rule_name} (tipo: {type(rule_name)})")
    logger.info(f"[update_rule] Request completo: {rule.model_dump()}")
    
    try:
        logger.info("[update_rule] Chamando service.update_rule...")
        result = service.update_rule(process_type, rule_name, rule.model_dump(exclude_none=True))
        logger.info(f"[update_rule] Regra atualizada com sucesso!")
        logger.info(f"[update_rule] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[update_rule] Erro inesperado: {str(e)}")
        logger.error(f"[update_rule] Tipo do erro: {type(e).__name__}")
        logger.exception("[update_rule] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{process_type}/{rule_name}", summary="Remover Regra")
async def delete_rule(process_type: str, rule_name: str):
    """Remove regra"""
    logger.info("=" * 80)
    logger.info("[delete_rule] Requisição recebida para remover regra")
    logger.info(f"[delete_rule] process_type: {process_type} (tipo: {type(process_type)})")
    logger.info(f"[delete_rule] rule_name: {rule_name} (tipo: {type(rule_name)})")
    
    try:
        logger.info("[delete_rule] Chamando service.delete_rule...")
        result = service.delete_rule(process_type, rule_name)
        logger.info(f"[delete_rule] Regra removida com sucesso!")
        logger.info(f"[delete_rule] Resposta: {result}")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[delete_rule] Erro inesperado: {str(e)}")
        logger.error(f"[delete_rule] Tipo do erro: {type(e).__name__}")
        logger.exception("[delete_rule] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/process-types/all", summary="Listar Todos os Tipos")
async def list_all_process_types():
    """Lista todos os tipos de processo e suas regras"""
    logger.info("=" * 80)
    logger.info("[list_all_process_types] Requisição recebida para listar todos os tipos de processo")
    
    try:
        logger.info("[list_all_process_types] Chamando service.list_rules para cada tipo...")
        result = {pt: service.list_rules(pt) for pt in ["SEMENTES", "AGROQUIMICOS", "FERTILIZANTES"]}
        logger.info(f"[list_all_process_types] Tipos listados com sucesso! Total de tipos: {len(result)}")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[list_all_process_types] Erro inesperado: {str(e)}")
        logger.error(f"[list_all_process_types] Tipo do erro: {type(e).__name__}")
        logger.exception("[list_all_process_types] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))
