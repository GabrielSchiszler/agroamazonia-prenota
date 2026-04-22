"""Tests for CFOP mapping disambiguation (uso e consumo / pedido compra / natureza)."""

import sys
from pathlib import Path

_lb = Path(__file__).resolve().parents[1] / "lambdas"
if str(_lb) not in sys.path:
    sys.path.insert(0, str(_lb))

from utils.cfop_table import disambiguate_cfop_mappings  # noqa: E402


def test_disambiguate_uso_consumo_prefere_sem_pedido_compra():
    mappings = [
        {"chave": "A1", "pedido_compra": True, "regra": "Com pedido"},
        {"chave": "B1", "pedido_compra": False, "regra": "Uso interno"},
    ]
    out = disambiguate_cfop_mappings(mappings, {"uso_e_consumo": True})
    assert len(out) == 1
    assert out[0]["chave"] == "B1"


def test_disambiguate_com_pedido_nos_metadados_prefere_com_flag():
    mappings = [
        {"chave": "A1", "pedido_compra": True, "regra": ""},
        {"chave": "B1", "pedido_compra": False, "regra": ""},
    ]
    out = disambiguate_cfop_mappings(mappings, {"has_pedido_de_compra": True})
    assert len(out) == 1
    assert out[0]["chave"] == "A1"


def test_disambiguate_sem_uso_sem_pedido_nao_altera_lista_comportamento_legado():
    mappings = [
        {"chave": "A1", "pedido_compra": True, "regra": ""},
        {"chave": "B1", "pedido_compra": False, "regra": ""},
    ]
    out = disambiguate_cfop_mappings(mappings, {})
    assert out == mappings
    out2 = disambiguate_cfop_mappings(
        mappings,
        {"natureza": "Compra de Mercadorias"},
    )
    assert len(out2) == 2


def test_disambiguate_has_pedido_mas_so_mapeamentos_sem_flag_mantem_lista():
    mappings = [
        {"chave": "X", "pedido_compra": False, "regra": ""},
        {"chave": "Y", "pedido_compra": False, "regra": ""},
    ]
    out = disambiguate_cfop_mappings(mappings, {"has_pedido_de_compra": True})
    assert len(out) == 2
