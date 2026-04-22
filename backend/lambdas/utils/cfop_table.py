"""
CFOP × Chave (DynamoDB) — leitura da tabela e desambiguação compartilhada entre
validate_rules e send_to_protheus.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

import boto3

logger = logging.getLogger(__name__)


def normalize_cfop(cfop):
    """Normaliza o CFOP removendo espaços e caracteres especiais."""
    if not cfop:
        return ""
    return str(cfop).strip()


def get_all_cfop_mappings_direct(table, cfop):
    """Busca todos os mapeamentos ativos do CFOP na tabela (PK CFOP_OPERATION)."""
    try:
        pk = "CFOP_OPERATION"
        sk = f"CFOP#{cfop}"

        logger.info(
            "[cfop_table] Buscando CFOP no DynamoDB - PK: %s, SK: %s, Tabela: %s",
            pk,
            sk,
            getattr(table, "name", "?"),
        )

        try:
            response = table.get_item(Key={"PK": pk, "SK": sk})
        except Exception as get_item_err:
            logger.error("[cfop_table] Erro ao fazer get_item: %s", get_item_err)
            raise

        if "Item" not in response:
            logger.info("[cfop_table] CFOP %s não encontrado (PK: %s, SK: %s)", cfop, pk, sk)
            try:
                query_response = table.query(
                    KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
                    ExpressionAttributeValues={":pk": pk, ":sk_prefix": f"CFOP#{cfop}"},
                )
                logger.info(
                    "[cfop_table] Query alternativa: %s item(ns)",
                    len(query_response.get("Items", [])),
                )
            except Exception as query_err:
                logger.warning("[cfop_table] Erro na query alternativa: %s", query_err)
            return []

        cfop_item = response["Item"]

        mapping_ids = []
        if cfop_item.get("MAPPING_ID"):
            mapping_ids.append(cfop_item.get("MAPPING_ID"))
        if cfop_item.get("MAPPING_IDS"):
            mapping_ids.extend(cfop_item.get("MAPPING_IDS", []))

        mapping_ids = list(set(mapping_ids))

        if not mapping_ids:
            logger.info("[cfop_table] CFOP %s sem mapping_ids", cfop)
            return []

        mappings = []
        mappings_inativos = []
        for mapping_id in mapping_ids:
            mapping_sk = f"MAPPING#{mapping_id}"
            mapping_response = table.get_item(Key={"PK": pk, "SK": mapping_sk})

            if "Item" not in mapping_response:
                logger.warning("[cfop_table] Mapeamento %s não encontrado", mapping_id)
                continue

            mapping_item = mapping_response["Item"]
            is_ativo = mapping_item.get("ATIVO", True)

            mapping_data = {
                "id": mapping_id,
                "chave": mapping_item.get("CHAVE", ""),
                "descricao": mapping_item.get("DESCRICAO", ""),
                "cfop": mapping_item.get("CFOP", ""),
                "operacao": mapping_item.get("OPERACAO", ""),
                "regra": mapping_item.get("REGRA", ""),
                "observacao": mapping_item.get("OBSERVACAO", ""),
                "pedido_compra": mapping_item.get("PEDIDO_COMPRA", False),
                "ativo": is_ativo,
            }

            if is_ativo:
                mappings.append(mapping_data)
            else:
                mappings_inativos.append(mapping_data)

        if mappings_inativos:
            logger.info(
                "[cfop_table] CFOP %s: %s ativo(s), %s inativo(s)",
                cfop,
                len(mappings),
                len(mappings_inativos),
            )
        else:
            logger.info("[cfop_table] CFOP %s: %s mapeamento(s) ativo(s)", cfop, len(mappings))

        return mappings
    except Exception as e:
        logger.error("[cfop_table] Erro ao buscar CFOP: %s", e)
        return []


def disambiguate_cfop_mappings(mappings: list, context: Optional[Dict[str, Any]]) -> list:
    """
    Quando o mesmo CFOP tem vários MAPPING# ativos, reduz usando PEDIDO_COMPRA + contexto
    (uso e consumo / pedido nos metadados / natureza × regra), sem alterar lista quando não há sinal.
    """
    if not mappings or len(mappings) <= 1:
        return mappings
    ctx = context or {}
    uso = bool(ctx.get("uso_e_consumo")) or ctx.get("process_type") == "USOCONSUMO"
    has_ped = bool(ctx.get("has_pedido_de_compra"))
    natureza = (ctx.get("natureza") or "").strip().lower()

    if not uso and not has_ped:
        return mappings

    def pick(pred):
        nu = [m for m in mappings if pred(m)]
        return nu if nu else mappings

    candidates = mappings
    if uso:
        candidates = pick(lambda m: not m.get("pedido_compra"))
        logger.info(
            "[cfop_table] Disambiguação USOCONSUMO/uso: %d de %d (PEDIDO_COMPRA=false)",
            len(candidates),
            len(mappings),
        )
    elif has_ped:
        candidates = pick(lambda m: m.get("pedido_compra"))
        logger.info(
            "[cfop_table] Disambiguação pedido compra: %d de %d (PEDIDO_COMPRA=true)",
            len(candidates),
            len(mappings),
        )

    if len(candidates) > 1 and natureza and (uso or has_ped):
        tokens = [t for t in natureza.replace(",", " ").split() if len(t) > 3]
        if tokens:
            scored = []
            for m in candidates:
                r = (m.get("regra") or "").lower()
                score = sum(1 for t in tokens if t in r)
                scored.append((score, m))
            scored.sort(key=lambda x: -x[0])
            top = scored[0][0]
            if top > 0:
                best = [m for s, m in scored if s == top]
                if len(best) == 1:
                    candidates = best
                elif len(best) < len(candidates):
                    candidates = best

    return candidates


def resolve_codigo_operacao_from_cfop(
    cfop: str,
    context: Optional[Dict[str, Any]] = None,
    table=None,
) -> str:
    """
    Retorna CHAVE (ou OPERACAO) do mapeamento ativo para o CFOP, após desambiguação.
    Se não houver CFOP, mapeamento ou ainda ambíguo, retorna string vazia.
    """
    cfop_n = normalize_cfop(cfop)
    if not cfop_n:
        return ""
    try:
        if table is None:
            aws_region = (
                os.environ.get("AWS_REGION")
                or os.environ.get("AWS_DEFAULT_REGION")
                or "sa-east-1"
            )
            dynamodb = boto3.resource("dynamodb", region_name=aws_region)
            table = dynamodb.Table(os.environ.get("TABLE_NAME", "DocumentProcessorTable"))

        mappings = get_all_cfop_mappings_direct(table, cfop_n)
        if len(mappings) > 1:
            mappings = disambiguate_cfop_mappings(mappings, context)
        if len(mappings) != 1:
            logger.info(
                "[cfop_table] resolve_codigo_operacao CFOP=%s → %s mapeamentos (esperado 1)",
                cfop_n,
                len(mappings),
            )
            return ""
        m = mappings[0]
        ch = (m.get("chave") or "").strip()
        if ch:
            return ch
        op = (m.get("operacao") or "").strip()
        return op or ""
    except Exception as e:
        logger.warning("[cfop_table] resolve_codigo_operacao_from_cfop: %s", e)
        return ""
