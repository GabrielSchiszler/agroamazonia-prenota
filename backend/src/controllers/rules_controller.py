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
    return {
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

@router.get("/{process_type}", summary="Listar Regras")
async def list_rules(process_type: str):
    """Lista regras de um tipo de processo"""
    try:
        return {"rules": service.list_rules(process_type)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="Criar Regra")
async def create_rule(rule: RuleCreate):
    """Cria nova regra de validação"""
    try:
        return service.create_rule(rule.process_type, rule.rule_name, rule.order, rule.enabled)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{process_type}/{rule_name}", summary="Atualizar Regra")
async def update_rule(process_type: str, rule_name: str, rule: RuleUpdate):
    """Atualiza regra existente"""
    try:
        return service.update_rule(process_type, rule_name, rule.dict(exclude_none=True))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{process_type}/{rule_name}", summary="Remover Regra")
async def delete_rule(process_type: str, rule_name: str):
    """Remove regra"""
    try:
        return service.delete_rule(process_type, rule_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/process-types/all", summary="Listar Todos os Tipos")
async def list_all_process_types():
    """Lista todos os tipos de processo e suas regras"""
    try:
        return {pt: service.list_rules(pt) for pt in ["SEMENTES", "AGROQUIMICOS", "FERTILIZANTES"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
