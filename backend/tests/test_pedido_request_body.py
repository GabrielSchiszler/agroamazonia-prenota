"""Tests for lambdas/utils/pedido_request_body.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.pedido_request_body import (  # noqa: E402
    any_pedido_de_compra_in_itens,
    natureza_from_pedido,
    rateio_centro_custo_from_request_body,
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


def test_rateio_centro_custo_from_request_body():
    rb = {
        "natureza": "3310201401",
        "rateioCentroCusto": [
            {"centroDeCusto": "10001105", "percentual": "100"},
        ],
    }
    assert rateio_centro_custo_from_request_body(rb) == [
        {"centroDeCusto": "10001105", "percentual": "100"},
    ]


def test_rateio_centro_custo_ignora_vazio():
    assert rateio_centro_custo_from_request_body({"rateioCentroCusto": []}) is None
    assert rateio_centro_custo_from_request_body({}) is None
    assert rateio_centro_custo_from_request_body(
        {"rateioCentroCusto": [{"centroDeCusto": ""}]}
    ) is None
