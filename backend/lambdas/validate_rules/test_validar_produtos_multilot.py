"""
Teste: N linhas XML (mesmo produto, lotes distintos em rastro) + 1 linha no pedido.

- Casos sintéticos (rápidos).
- Caso real: NF-e 23260307467822000126551010000878991216037201.xml (3 itens OPTERADUO,
  lotes 0011-26-7400 / 0028-26-9000 / 0029-26-8000) + pedido AACBKV item 0001.

Executar:
  cd backend/lambdas/validate_rules && python3 test_validar_produtos_multilot.py -v

QA deployado: process_id e39b7d074733be50019fa562036d43c0 (VALIDATION_RESULTS + Protheus).
"""
import json
import logging
import os
import sys
import unittest
from unittest.mock import patch

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_LAMBDAS = os.path.normpath(os.path.join(_THIS_DIR, ".."))
# validate_rules -> lambdas -> backend -> scripts
_SCRIPTS = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "scripts"))

sys.path.insert(0, _THIS_DIR)

# parse_xml/handler.py instancia DynamoDB no import — evita erro de env
os.environ.setdefault("TABLE_NAME", "test-table-import-only")
os.environ.setdefault("BUCKET_NAME", "test-bucket-import-only")
os.environ.setdefault("AWS_DEFAULT_REGION", "sa-east-1")
sys.path.insert(0, os.path.join(_LAMBDAS, "parse_xml"))
import handler as _parse_xml_handler  # noqa: E402

# Pedido de compra (metadados) — mesmo contrato do fluxo real
PEDIDO_COMPRA_EXEMPLO = {
    "header": {"tenantId": "00,010159"},
    "requestBody": {
        "moeda": "BRL",
        "itens": [
            {
                "codigoProduto": "I3000001GL00200",
                "produto": "OPTERADUO GL 20 LT",
                "valorUnitario": 2480,
                "codigoOperacao": "1B",
                "tipoDeProduto": {"chave": "ME", "descricao": "MERCADORIA"},
                "pedidoDeCompra": {"pedidoErp": "AACBKV", "itemPedidoErp": "0001"},
            }
        ],
        "cnpjEmitente": "07467822001289",
        "cnpjDestinatario": "13563680004603",
    },
}

XML_NFE_FILENAME = "23260307467822000126551010000878991216037201.xml"


def _xml_path():
    return os.path.join(_SCRIPTS, XML_NFE_FILENAME)


def _danfe_line(desc, codigo, qtd, lote):
    return {
        "descricao": desc,
        "codigo": codigo,
        "quantidade": str(qtd),
        "rastro": [{"lote": lote, "data_validade": "2028-01-01", "data_fabricacao": "2025-01-01"}],
    }


def _pedido_item(codigo, pedido_erp, item_erp):
    return {
        "codigoProduto": codigo,
        "produto": "PRODUTO TESTE MULTILOTE",
        "quantidade": 300,
        "pedidoDeCompra": {"pedidoErp": pedido_erp, "itemPedidoErp": item_erp},
    }


def _load_produtos_from_nfe_xml():
    path = _xml_path()
    if not os.path.isfile(path):
        raise FileNotFoundError(f"XML não encontrado: {path}")
    with open(path, "rb") as f:
        data = f.read()
    parsed = _parse_xml_handler.parse_nfe_xml(data)
    return parsed.get("produtos") or []


class TestMultiLoteUmPedido(unittest.TestCase):
    @patch("rules.utils.compare_with_bedrock")
    def test_tres_linhas_xml_um_pedido_tres_match(self, mock_bedrock):
        mock_bedrock.return_value = "MATCH"
        logging.basicConfig(level=logging.INFO)

        from rules.validar_produtos import validate_products_comparison

        danfe = [
            _danfe_line("PROD X", "SKU1", 100, "LOTE-A"),
            _danfe_line("PROD X", "SKU1", 100, "LOTE-B"),
            _danfe_line("PROD X", "SKU1", 100, "LOTE-C"),
        ]
        doc = [_pedido_item("SKU1", "PED1", "0001")]

        out = validate_products_comparison(danfe, doc, "pedido.json", "METADADOS JSON", None)
        self.assertTrue(out["has_match"])
        self.assertTrue(out["all_match"], msg=out)
        positions = sorted(out["matched_danfe_positions"])
        self.assertEqual(positions, [1, 2, 3])

        items = out["comparison"]["items"]
        danfe_items = [i for i in items if i.get("danfe_position") is not None]
        self.assertEqual(len(danfe_items), 3)
        for it in danfe_items:
            self.assertEqual(it["status"], "MATCH")
            self.assertEqual(it["doc_position"], 1)

    @patch("rules.utils.compare_with_bedrock")
    def test_lote_duplicado_nao_reusa(self, mock_bedrock):
        mock_bedrock.return_value = "MATCH"
        logging.basicConfig(level=logging.INFO)
        from rules.validar_produtos import validate_products_comparison

        danfe = [
            _danfe_line("PROD X", "SKU1", 100, "LOTE-A"),
            _danfe_line("PROD X", "SKU1", 100, "LOTE-A"),
        ]
        doc = [_pedido_item("SKU1", "PED1", "0001")]
        out = validate_products_comparison(danfe, doc, "pedido.json", "METADADOS JSON", None)
        self.assertTrue(out["has_match"])
        self.assertFalse(out["all_match"])
        self.assertEqual(len(out["matched_danfe_positions"]), 1)


class TestNfeOpteraduoXmlReal(unittest.TestCase):
    """3 <det> no XML (40055406 / OPTERADUO 1X20L) + 1 linha no pedido (I3000001GL00200)."""

    @patch("rules.utils.compare_with_bedrock")
    def test_tres_itens_xml_um_item_pedido_aacbkv(self, mock_bedrock):
        mock_bedrock.return_value = "MATCH"
        logging.basicConfig(level=logging.INFO)

        from rules.validar_produtos import (
            extract_lote_signature,
            validate_products_comparison,
        )

        danfe = _load_produtos_from_nfe_xml()
        self.assertEqual(
            len(danfe),
            3,
            msg="Esperado 3 itens no XML da NF-e de exemplo",
        )
        lotes = [extract_lote_signature(p) for p in danfe]
        self.assertEqual(
            lotes,
            [("0011-26-7400",), ("0028-26-9000",), ("0029-26-8000",)],
            msg=f"Lotes extraídos do XML: {lotes}",
        )

        doc = PEDIDO_COMPRA_EXEMPLO["requestBody"]["itens"]
        self.assertEqual(len(doc), 1)

        out = validate_products_comparison(
            danfe,
            doc,
            "pedido_opteraduo_aacbkv.json",
            "METADADOS JSON",
            None,
        )
        self.assertTrue(out["has_match"], msg=json.dumps(out, indent=2, default=str))
        self.assertTrue(out["all_match"], msg=json.dumps(out, indent=2, default=str))
        self.assertEqual(sorted(out["matched_danfe_positions"]), [1, 2, 3])

        for it in out["comparison"]["items"]:
            if it.get("danfe_position") is None:
                continue
            self.assertEqual(it["status"], "MATCH")
            self.assertEqual(it["doc_position"], 1)


if __name__ == "__main__":
    unittest.main()
