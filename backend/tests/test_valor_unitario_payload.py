"""Regressão: valor total como unitário só em USO E CONSUMO."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas"))

from send_to_protheus.handler import _valor_unitario_payload  # noqa: E402


def test_agroquimicos_xml_uma_linha_usa_preco_unitario_nao_vnf():
    """Processo 5ac87176: 24 × 22 = 528; agroquímicos nunca usa vNF como unitário."""
    produto_xml = {
        "valor_unitario": "22.0000000",
        "valor_total": "528.00",
        "quantidade": "24.0000",
    }
    assert _valor_unitario_payload(produto_xml, 24, 528.0, 1, uso_consumo=False) == 22.0


def test_agroquimicos_nao_usa_vnf_inteiro_em_uma_linha():
    """Sem preço de linha: legado divide vNF pela quantidade, não usa total inteiro."""
    assert _valor_unitario_payload({}, 24, 528.0, 1, uso_consumo=False) == 22.0


def test_uso_consumo_sem_preco_linha_usa_total_documento():
    """Uso e consumo: uma linha sem XML usa total da nota como valor unitário."""
    assert _valor_unitario_payload({}, 1, 5746.2, 1, uso_consumo=True) == 5746.2


def test_uso_consumo_nao_aplica_em_multiplas_linhas():
    produto_xml = {"valor_total": "1000.00"}
    assert _valor_unitario_payload(produto_xml, 10, 2000.0, 2, uso_consumo=True) == 100.0
