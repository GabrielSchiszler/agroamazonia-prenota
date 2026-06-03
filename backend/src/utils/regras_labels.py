"""Labels legíveis para regras de falha (dashboard e relatórios)."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_CATALOG_PATH = Path(__file__).resolve().parent / "regras_labels_catalog.json"

# Validação OCR (validar_*) — alinhado às mensagens das lambdas
_VALIDACAO_REGRAS: dict[str, tuple[str, str]] = {
    "validar_produtos": (
        "Validação OCR",
        "Produtos da NF não conferem com pedido/XML",
    ),
    "validar_cnpj_fornecedor": (
        "Validação OCR",
        "CNPJ do fornecedor divergente (raiz/matriz)",
    ),
    "validar_cnpj_destinatario": (
        "Validação OCR",
        "CNPJ do destinatário divergente (raiz/matriz)",
    ),
    "validar_numero_pedido": (
        "Validação OCR",
        "Número do pedido divergente",
    ),
    "validar_numero_nota": (
        "Validação OCR",
        "Número da nota divergente",
    ),
    "validar_serie": (
        "Validação OCR",
        "Série divergente entre documentos",
    ),
    "validar_data_emissao": (
        "Validação OCR",
        "Data de emissão divergente",
    ),
    "validar_cfop_chave": (
        "Validação OCR",
        "CFOP / mapeamento de operação divergente",
    ),
    "validar_icms": (
        "Validação OCR",
        "ICMS divergente (NF × XML)",
    ),
}

_PIPELINE_OCR: dict[str, tuple[str, str]] = {
    "OCR_LAMBDA_TIMEOUT": (
        "Pipeline OCR",
        "Timeout no processamento (Step Functions)",
    ),
    "OCR_LAMBDA_UpdateStatusBeforeError": (
        "Pipeline OCR",
        "Erro técnico ao finalizar status do processo",
    ),
    "OCR_LAMBDA_ERROR": (
        "Pipeline OCR",
        "Erro genérico na lambda OCR",
    ),
}

_OUTROS = (
    "Agregado",
    "Falhas sem regra nomeada (lambda, integração ou legado)",
)


def _entry(
    *,
    categoria: str,
    mensagem_resumo: str,
    fonte: str | None = None,
    tipo: str | None = None,
) -> dict:
    return {
        "categoria": categoria,
        "mensagem_resumo": mensagem_resumo,
        "label": mensagem_resumo,
        "fonte": fonte,
        "tipo": tipo,
    }


def build_regras_labels_catalog() -> dict[str, dict]:
    """Monta catálogo unificado a partir dos JSONs de regras Protheus/API + OCR."""
    root = Path(__file__).resolve().parents[2]
    protheus_path = root / "lambdas" / "utils" / "protheus_regras_catalog.json"
    api_path = root / "lambdas" / "utils" / "api_regras_catalog.json"

    out: dict[str, dict] = {}

    if protheus_path.is_file():
        data = json.loads(protheus_path.read_text(encoding="utf-8"))
        for regra_id, meta in (data.get("regras") or {}).items():
            out[regra_id] = _entry(
                categoria=meta.get("categoria") or "Protheus",
                mensagem_resumo=meta.get("mensagem_resumo") or regra_id,
                fonte="Protheus",
                tipo=meta.get("tipo"),
            )

    if api_path.is_file():
        data = json.loads(api_path.read_text(encoding="utf-8"))
        for regra_id, meta in (data.get("regras") or {}).items():
            out[regra_id] = _entry(
                categoria=meta.get("categoria") or "API / Schema",
                mensagem_resumo=meta.get("mensagem_resumo") or regra_id,
                fonte="API",
                tipo=meta.get("tipo"),
            )

    for regra_id, (cat, msg) in _VALIDACAO_REGRAS.items():
        out[regra_id] = _entry(categoria=cat, mensagem_resumo=msg, fonte="Validação OCR", tipo="OCR")

    for regra_id, (cat, msg) in _PIPELINE_OCR.items():
        out[regra_id] = _entry(categoria=cat, mensagem_resumo=msg, fonte="Pipeline OCR", tipo="OCR")

    if protheus_path.is_file():
        metrics_op = (json.loads(protheus_path.read_text(encoding="utf-8")).get("metrics") or {}).get(
            "regras_operacional"
        ) or []
        for regra_id in metrics_op:
            if regra_id in out:
                out[regra_id]["tipo"] = "Operacional"

    out["Outros"] = _entry(categoria=_OUTROS[0], mensagem_resumo=_OUTROS[1], fonte="Agregado")

    return out


@lru_cache(maxsize=1)
def load_regras_labels_catalog() -> dict[str, dict]:
    if _CATALOG_PATH.is_file():
        return json.loads(_CATALOG_PATH.read_text(encoding="utf-8"))
    return build_regras_labels_catalog()


def regra_display_label(regra_id: str, catalog: dict | None = None) -> str:
    """Texto curto para gráficos: mensagem_resumo (ou id se desconhecido)."""
    catalog = catalog or load_regras_labels_catalog()
    if not regra_id:
        return ""
    meta = catalog.get(regra_id)
    if meta:
        return meta.get("label") or meta.get("mensagem_resumo") or regra_id
    return regra_id


def get_regras_labels_for_dashboard() -> dict[str, dict]:
    return load_regras_labels_catalog()
