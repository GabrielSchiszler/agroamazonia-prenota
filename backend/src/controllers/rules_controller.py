import logging
from fastapi import APIRouter, HTTPException
from src.services.rules_service import RulesService
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/rules", tags=["Rules (Gerenciamento de Regras)"], include_in_schema=False)
service = RulesService()

class RuleCreate(BaseModel):
    process_type: str
    rule_name: str
    order: int
    enabled: bool = True
    
    class Config:
        schema_extra = {
            "example": {
                "process_type": "SEMENTES",
                "rule_name": "validar_numero_nota",
                "order": 1,
                "enabled": True
            }
        }

class RuleUpdate(BaseModel):
    order: int = None
    enabled: bool = None

@router.get("/{process_type}", summary="Listar Regras", description="Lista regras configuradas para um tipo de processo",
    responses={
        200: {
            "description": "Lista de regras configuradas",
            "content": {
                "application/json": {
                    "example": {
                        "rules": [
                            {
                                "RULE_NAME": "validar_numero_nota",
                                "ORDER": 1,
                                "ENABLED": True
                            },
                            {
                                "RULE_NAME": "validar_serie",
                                "ORDER": 2,
                                "ENABLED": True
                            }
                        ]
                    }
                }
            }
        }
    })
async def list_rules(process_type: str):
    """Lista regras de um tipo de processo.
    
    - **process_type**: SEMENTES, AGROQUIMICOS ou FERTILIZANTES
    """
    try:
        rules = service.list_rules(process_type)
        return {"rules": rules}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="Criar Regra", description="Adiciona nova regra a um tipo de processo",
    responses={
        200: {
            "description": "Regra criada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Regra criada com sucesso",
                        "process_type": "SEMENTES",
                        "rule_name": "validar_numero_nota"
                    }
                }
            }
        }
    })
async def create_rule(rule: RuleCreate):
    """Cria nova regra de validação.
    
    - **process_type**: Tipo de processo
    - **rule_name**: Nome da regra (ex: validar_numero_nota)
    - **order**: Ordem de execução
    - **enabled**: Se a regra está ativa
    """
    try:
        result = service.create_rule(
            rule.process_type,
            rule.rule_name,
            rule.order,
            rule.enabled
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{process_type}/{rule_name}", summary="Atualizar Regra", description="Atualiza ordem ou status de uma regra",
    responses={
        200: {
            "description": "Regra atualizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Regra atualizada com sucesso",
                        "process_type": "SEMENTES",
                        "rule_name": "validar_numero_nota"
                    }
                }
            }
        }
    })
async def update_rule(process_type: str, rule_name: str, rule: RuleUpdate):
    """Atualiza regra existente.
    
    - **process_type**: Tipo de processo
    - **rule_name**: Nome da regra
    - **order**: Nova ordem (opcional)
    - **enabled**: Novo status (opcional)
    """
    try:
        result = service.update_rule(process_type, rule_name, rule.dict(exclude_none=True))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{process_type}/{rule_name}", summary="Remover Regra", description="Remove uma regra de um tipo de processo",
    responses={
        200: {
            "description": "Regra removida com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Regra removida com sucesso",
                        "process_type": "SEMENTES",
                        "rule_name": "validar_numero_nota"
                    }
                }
            }
        }
    })
async def delete_rule(process_type: str, rule_name: str):
    """Remove regra.
    
    - **process_type**: Tipo de processo
    - **rule_name**: Nome da regra
    """
    try:
        result = service.delete_rule(process_type, rule_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/available", summary="Listar Regras Disponíveis", description="Lista todas as regras que podem ser configuradas",
    responses={
        200: {
            "description": "Lista de regras disponíveis",
            "content": {
                "application/json": {
                    "example": {
                        "rules": [
                            "validar_numero_nota",
                            "validar_serie",
                            "validar_data_emissao",
                            "validar_cnpj_fornecedor"
                        ]
                    }
                }
            }
        }
    })
async def list_available_rules():
    """Lista todas as regras disponíveis no sistema."""
    return {
        "rules": [
            "validar_numero_nota",
            "validar_serie",
            "validar_data_emissao",
            "validar_cnpj_fornecedor"
        ]
    }

@router.get("/process-types/all", summary="Listar Todos os Tipos", description="Lista todos os tipos de processo e suas regras",
    responses={
        200: {
            "description": "Tipos de processo e suas regras",
            "content": {
                "application/json": {
                    "example": {
                        "SEMENTES": [
                            {"RULE_NAME": "validar_numero_nota", "ORDER": 1, "ENABLED": True}
                        ],
                        "AGROQUIMICOS": [
                            {"RULE_NAME": "validar_numero_nota", "ORDER": 1, "ENABLED": True}
                        ],
                        "FERTILIZANTES": [
                            {"RULE_NAME": "validar_numero_nota", "ORDER": 1, "ENABLED": True}
                        ]
                    }
                }
            }
        }
    })
async def list_all_process_types():
    """Lista todos os tipos de processo e suas regras configuradas."""
    try:
        process_types = ["SEMENTES", "AGROQUIMICOS", "FERTILIZANTES"]
        result = {}
        for pt in process_types:
            result[pt] = service.list_rules(pt)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
