"""Testes para _merge_uso_consumo_protheus_item (send_to_protheus)."""

import os
import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_merge_usa_codigoProduto_do_pedido_quando_sem_id():
    from send_to_protheus.handler import _merge_uso_consumo_protheus_item

    item = {"codigoProduto": "", "quantidade": 1, "codigoOperacao": "X"}
    rb = {
        "codigoProduto": "AHYMC0000000001",
        "codigoOperacao": "1B",
        "unidadeMedida": "TON",
        "produto": "USO E CONSUMO - PADRAO",
    }
    out = _merge_uso_consumo_protheus_item(dict(item), rb)
    assert out["codigoProduto"] == "AHYMC0000000001"
    assert out["codigoOperacao"] == "1B"
    assert out["unidadeMedida"] == "TON"
    assert "produto" not in out


def test_merge_repassa_unidadeMedida_do_pedido():
    from send_to_protheus.handler import _merge_uso_consumo_protheus_item

    item = {"codigoProduto": "X", "quantidade": 1}
    rb = {"codigoProduto": "AHYMC0000000001", "codigoOperacao": "1B", "unidadeMedida": "TON"}
    out = _merge_uso_consumo_protheus_item(dict(item), rb)
    assert out["unidadeMedida"] == "TON"


def test_merge_prioriza_id_sobre_codigoProduto():
    from send_to_protheus.handler import _merge_uso_consumo_protheus_item

    item = {"codigoProduto": "999", "quantidade": 1}
    rb = {"id": "26480", "codigoProduto": "AHYMC0000000001", "codigoOperacao": "1B"}
    out = _merge_uso_consumo_protheus_item(dict(item), rb)
    assert out["codigoProduto"] == "26480"
