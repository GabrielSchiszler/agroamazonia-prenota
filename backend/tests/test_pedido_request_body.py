"""Tests for lambdas/utils/pedido_request_body.py."""

from utils.pedido_request_body import (
    any_pedido_de_compra_in_itens,
    natureza_from_pedido,
    uso_e_consumo_active,
)


def test_uso_e_consumo_active_request_body():
    pedido = {"requestBody": {"usoEConsumo": True, "itens": []}}
    assert uso_e_consumo_active(pedido) is True


def test_uso_e_consumo_active_header():
    pedido = {"header": {"usoEConsumo": "true"}, "requestBody": {"itens": []}}
    assert uso_e_consumo_active(pedido) is True


def test_any_pedido_de_compra():
    pedido = {
        "requestBody": {
            "itens": [{"pedidoDeCompra": {"pedidoErp": "P1", "itemPedidoErp": "1"}}]
        }
    }
    assert any_pedido_de_compra_in_itens(pedido) is True


def test_natureza_from_pedido():
    pedido = {"requestBody": {"natureza": "Compra de Mercadorias"}}
    assert natureza_from_pedido(pedido) == "Compra de Mercadorias"
