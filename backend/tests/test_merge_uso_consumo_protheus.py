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


def test_merge_repassa_pedidoDeCompra_do_pedido():
    from send_to_protheus.handler import _merge_uso_consumo_protheus_item

    item = {"codigoProduto": "X", "quantidade": 1, "codigoOperacao": "1B"}
    rb = {
        "codigoProduto": "AHYMC0000000001",
        "codigoOperacao": "1B",
        "pedidoDeCompra": {"pedidoErp": "582992", "itemPedidoErp": "0001"},
    }
    out = _merge_uso_consumo_protheus_item(dict(item), rb)
    assert out["pedidoDeCompra"] == {"pedidoErp": "582992", "itemPedidoErp": "0001"}


def test_merge_pedidoDeCompra_sobrescreve_vazio():
    from send_to_protheus.handler import _merge_uso_consumo_protheus_item

    item = {"codigoProduto": "X", "quantidade": 1}
    rb = {
        "codigoProduto": "AHYMC0000000001",
        "pedidoDeCompra": {"pedidoErp": "AACBKE", "itemPedidoErp": "0002"},
    }
    out = _merge_uso_consumo_protheus_item(dict(item), rb)
    assert out["pedidoDeCompra"]["pedidoErp"] == "AACBKE"


def test_quantidade_uc_sem_chave_no_pedido_retorna_1():
    from send_to_protheus.handler import _quantidade_uso_consumo_pedido

    rb = {"codigoProduto": "CHU00047UN00010", "valorUnitario": 575.06}
    assert _quantidade_uso_consumo_pedido(rb, 0.0) == 1.0


def test_quantidade_uc_zero_no_metadado_retorna_1():
    from send_to_protheus.handler import _quantidade_uso_consumo_pedido

    rb = {"codigoProduto": "X", "quantidade": 0}
    assert _quantidade_uso_consumo_pedido(rb, 10.0) == 1.0


def test_quantidade_uc_positivo_no_metadado_preserva():
    from send_to_protheus.handler import _quantidade_uso_consumo_pedido

    rb = {"codigoProduto": "X", "quantidade": 3}
    assert _quantidade_uso_consumo_pedido(rb, 1.0) == 3.0


def test_quantidade_uc_sem_item_pedido_usa_xml_se_positivo():
    from send_to_protheus.handler import _quantidade_uso_consumo_pedido

    assert _quantidade_uso_consumo_pedido(None, 5.0) == 5.0
    assert _quantidade_uso_consumo_pedido(None, 0.0) == 1.0
