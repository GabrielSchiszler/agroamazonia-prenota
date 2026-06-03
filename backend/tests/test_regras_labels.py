"""Labels de regras para dashboard."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from src.utils.regras_labels import (  # noqa: E402
    build_regras_labels_catalog,
    load_regras_labels_catalog,
    regra_display_label,
)


def test_catalog_has_protheus_api_and_validacao():
    cat = build_regras_labels_catalog()
    assert "SCHEMA_ITEM_011" in cat
    assert cat["SCHEMA_ITEM_011"]["mensagem_resumo"]
    assert "QTD_MAIOR_PEDIDO" in cat
    assert "validar_produtos" in cat
    assert cat["validar_produtos"]["categoria"] == "Validação OCR"
    assert "Outros" in cat


def test_bundled_catalog_file_loads():
    cat = load_regras_labels_catalog()
    assert len(cat) >= 60
    assert regra_display_label("validar_produtos", cat) == cat["validar_produtos"]["label"]


def test_unknown_regra_falls_back_to_id():
    assert regra_display_label("REGRA_INEXISTENTE_XYZ") == "REGRA_INEXISTENTE_XYZ"


def test_catalog_json_valid():
    path = _BACKEND / "src" / "utils" / "regras_labels_catalog.json"
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "DIVERGENCIA_DE_VALOR_TOTAL_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_TOTAL_NO_XML" in data
    assert data["DIVERGENCIA_DE_VALOR_TOTAL_DA_NOTA_FISCAL_DIGITADA_COM_VALOR_TOTAL_NO_XML"]["mensagem_resumo"]
