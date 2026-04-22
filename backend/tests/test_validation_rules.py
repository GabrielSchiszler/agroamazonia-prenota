"""
Testes unitários das regras de validação (validate_rules/rules/*.py).

Executa ANTES de deploy para garantir que as regras continuam funcionando.
Cada cenário é determinístico (mock do Bedrock) e roda em < 1s.

Rodar:
  cd backend && python3 -m pytest tests/test_validation_rules.py -v
"""

import json
import sys
import os
import pytest
from unittest.mock import patch, MagicMock

# Setup path so rule modules resolve
_rules_dir = os.path.join(os.path.dirname(__file__), "..", "lambdas", "validate_rules")
if _rules_dir not in sys.path:
    sys.path.insert(0, os.path.abspath(_rules_dir))


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _danfe(overrides=None):
    """Minimal valid DANFE parsed data."""
    d = {
        "numero_nota": "878991",
        "serie": "1",
        "data_emissao": "2026-03-15T10:00:00-03:00",
        "info_adicional": "Pedido AACBKV item 0001",
        "emitente": {
            "cnpj": "07467822000126",
            "xNome": "Fornecedor Teste",
            "IE": "123456789",
            "endereco": {"UF": "GO"},
        },
        "destinatario": {
            "cnpj": "13563680000101",
            "xNome": "AgroAmazonia",
            "IE": "987654321",
            "endereco": {"UF": "AM"},
        },
        "produtos": [
            {
                "item": "1",
                "codigo": "40055406",
                "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100",
                "valor_unitario": "2480.00",
                "valor_total": "248000.00",
                "unidade": "UN",
                "cfop": "6102",
                "rastro": [
                    {"lote": "0011-26-7400", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}
                ],
                "icms": {"cst": "00", "valor": "0.00", "base_calculo": "0.00"},
            }
        ],
        "totais": {
            "valor_icms": "0.00",
            "valor_nota": "248000.00",
            "valor_produtos": "248000.00",
        },
    }
    if overrides:
        _deep_merge(d, overrides)
    return d


def _pedido_doc(overrides=None):
    """Minimal purchase-order doc as prepared by validate_rules handler."""
    d = {
        "file_name": "Metadados do Pedido de Compra",
        "_has_metadata": True,
        "cnpjEmitente": "07467822001289",
        "cnpjDestinatario": "13563680004603",
        "requestBody": {
            "cnpjEmitente": "07467822001289",
            "cnpjDestinatario": "13563680004603",
            "itens": [
                {
                    "codigoProduto": "I3000001GL00200",
                    "produto": "OPTERADUO GL 20 LT",
                    "valorUnitario": 2480,
                    "codigoOperacao": "1B",
                    "pedidoDeCompra": {"pedidoErp": "AACBKV", "itemPedidoErp": "0001"},
                }
            ],
        },
        "itens": [
            {
                "codigoProduto": "I3000001GL00200",
                "produto": "OPTERADUO GL 20 LT",
                "valorUnitario": 2480,
                "codigoOperacao": "1B",
                "pedidoDeCompra": {"pedidoErp": "AACBKV", "itemPedidoErp": "0001"},
            }
        ],
    }
    if overrides:
        _deep_merge(d, overrides)
    return d


def _deep_merge(base, override):
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


# ─── Mock Bedrock (deterministic) ────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_bedrock():
    with patch("rules.utils.compare_with_bedrock") as m:
        m.return_value = {"status": "MATCH", "bedrock": {"explicacao": "mock"}}
        yield m


# =============================================================================
# validar_cnpj_fornecedor
# =============================================================================

class TestCnpjFornecedor:
    """CNPJ emitente: compara raiz (8 primeiros dígitos)."""

    def test_same_root_passes(self):
        from rules.validar_cnpj_fornecedor import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "PASSED"

    def test_different_root_fails(self):
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "99999999000100",
                           "requestBody": {"cnpjEmitente": "99999999000100"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_same_root_different_branch_passes(self):
        """Filial vs matriz = mesma raiz (8 dígitos) → PASS."""
        from rules.validar_cnpj_fornecedor import validate
        danfe = _danfe({"emitente": {"cnpj": "07467822000126"}})
        doc = _pedido_doc({"cnpjEmitente": "07467822001289",
                           "requestBody": {"cnpjEmitente": "07467822001289"}})
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"

    def test_no_cnpj_in_metadata_fails(self):
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc()
        doc["cnpjEmitente"] = None
        doc["requestBody"]["cnpjEmitente"] = None
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_cnpj_destinatario
# =============================================================================

class TestCnpjDestinatario:

    def test_same_root_passes(self):
        from rules.validar_cnpj_destinatario import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "PASSED"

    def test_different_root_fails(self):
        from rules.validar_cnpj_destinatario import validate
        doc = _pedido_doc({"cnpjDestinatario": "11111111000199",
                           "requestBody": {"cnpjDestinatario": "11111111000199"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_no_cnpj_in_metadata_fails(self):
        from rules.validar_cnpj_destinatario import validate
        doc = _pedido_doc()
        doc.pop("cnpjDestinatario", None)
        doc["requestBody"].pop("cnpjDestinatario", None)
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_serie
# =============================================================================

class TestSerie:

    def test_matching_serie_passes(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = "1"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_serie_with_leading_zeros_passes(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = "001"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_different_serie_fails(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = "5"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_missing_serie_in_metadata_fails(self):
        from rules.validar_serie import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_numero_nota
# =============================================================================

class TestNumeroNota:

    def test_exact_match_passes(self):
        from rules.validar_numero_nota import validate
        doc = _pedido_doc()
        doc["documento"] = "878991"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_leading_zeros_match_passes(self):
        from rules.validar_numero_nota import validate
        doc = _pedido_doc()
        doc["documento"] = "000878991"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_different_number_uses_bedrock(self):
        from rules.validar_numero_nota import validate
        with patch("rules.validar_numero_nota.compare_with_bedrock") as m, \
             patch("rules.validar_numero_nota.bedrock_compare_status", return_value="MISMATCH"):
            m.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diferentes"}}
            doc = _pedido_doc()
            doc["documento"] = "999999"
            r = validate(_danfe(), [doc])
            assert r["status"] == "FAILED"

    def test_missing_numero_in_metadata_fails(self):
        from rules.validar_numero_nota import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_data_emissao
# =============================================================================

class TestDataEmissao:

    def test_same_date_passes(self):
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2026-03-15"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_timestamp_vs_date_passes(self):
        """DANFE has timestamp, metadata has plain date → both normalize to same YYYY-MM-DD."""
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2026-03-15T00:00:00-03:00"
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_different_date_fails(self):
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2026-04-01"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_missing_date_fails(self):
        from rules.validar_data_emissao import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_numero_pedido
# =============================================================================

class TestNumeroPedido:

    def test_pedido_in_info_adicional_passes(self):
        from rules.validar_numero_pedido import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "PASSED"

    def test_pedido_not_in_info_adicional_fails(self):
        from rules.validar_numero_pedido import validate
        danfe = _danfe({"info_adicional": "Texto sem referencia ao pedido"})
        r = validate(danfe, [_pedido_doc()])
        assert r["status"] == "FAILED"

    def test_no_pedido_in_metadata_passes(self):
        """If metadata has no pedidoErp → automatic MATCH (no reference to check)."""
        from rules.validar_numero_pedido import validate
        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "X", "produto": "Y"}]
        r = validate(_danfe(), [doc])
        assert r["status"] == "PASSED"

    def test_empty_info_adicional_but_has_pedido_fails(self):
        from rules.validar_numero_pedido import validate
        danfe = _danfe({"info_adicional": ""})
        r = validate(danfe, [_pedido_doc()])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_icms
# =============================================================================

class TestIcms:

    def test_internal_operation_both_zero_passes(self):
        """Same UF (internal) — both ICMS must be 0."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"cnpj": "111", "uf": "SP"},
            "destinatario": {"cnpj": "222", "uf": "SP"},
            "totais": {"valor_icms": "0.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "0.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"

    def test_internal_operation_nonzero_fails(self):
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"cnpj": "111", "uf": "SP"},
            "destinatario": {"cnpj": "222", "uf": "SP"},
            "totais": {"valor_icms": "180.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "180.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_interstate_matching_values_passes(self):
        """Different UF — values must match within 0.01."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"cnpj": "111", "uf": "SP"},
            "destinatario": {"cnpj": "222", "uf": "AM"},
            "totais": {"valor_icms": "180.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "180.005"}
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"

    def test_interstate_divergent_values_fails(self):
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"cnpj": "111", "uf": "GO"},
            "destinatario": {"cnpj": "222", "uf": "AM"},
            "totais": {"valor_icms": "180.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "200.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_missing_icms_in_metadata_fails(self):
        from rules.validar_icms import validate
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_produtos — find_matching_product: cada camada de matching
# =============================================================================

class TestFindMatchingProductExact:
    """PRIORIDADE 1: Match exato por nome (case-insensitive)."""

    def test_exact_name_match(self):
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "OPTERADUO GL 1X20L"}
        docs = [{"produto": "OPTERADUO GL 1X20L"}]
        idx, prod, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_exact_name_case_insensitive(self):
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "opteraduo gl 1x20l"}
        docs = [{"produto": "OPTERADUO GL 1X20L"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_exact_name_skips_used_index(self):
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "PRODUTO A"}
        docs = [{"produto": "PRODUTO A"}, {"produto": "PRODUTO A"}]
        idx, _, _ = find_matching_product(danfe, docs, {0})
        assert idx == 1

    def test_exact_no_match_returns_none(self, mock_bedrock):
        """Nome completamente diferente + Bedrock MISMATCH → None."""
        from rules.validar_produtos import find_matching_product
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        danfe = {"descricao": "HERBICIDA GLIFOSATO"}
        docs = [{"produto": "FERTILIZANTE NPK"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx is None


class TestFindMatchingProductCodeSeparators:
    """PRIORIDADE 1.5: Normalização de separadores em códigos numéricos."""

    def test_dots_vs_dashes(self):
        """15.15.15 no DANFE vs 15-15-15 no pedido → match por código numérico equivalente."""
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "FERTILIZANTE NPK 15.15.15 50KG"}
        docs = [{"produto": "FERTILIZANTE NPK 15-15-15 50KG"}]
        idx, _, has_eq = find_matching_product(danfe, docs, set())
        assert idx == 0
        assert has_eq is True

    def test_dots_vs_spaces(self):
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "ADUBO 30.00.20 SACO"}
        docs = [{"produto": "ADUBO 30 00 20 SACO"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_different_codes_no_match_by_code_equivalence(self, mock_bedrock):
        """Códigos numéricos diferentes (15.15.15 vs 20.05.20) → NÃO faz match por código equivalente.
        Nota: ainda pode match por palavras-chave se houver >= 2 em comum."""
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        from rules.validar_produtos import find_matching_product
        # Usar nomes sem palavras em comum para isolar o teste de código
        danfe = {"descricao": "ADUBO 15.15.15 SACO"}
        docs = [{"produto": "CALCARIO 20.05.20 GRANEL"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx is None


class TestFindMatchingProductPartial:
    """PRIORIDADE 2: Match parcial por palavras-chave (>= 2 palavras em comum) e substring."""

    def test_two_common_keywords_match(self, mock_bedrock):
        """DANFE e pedido compartilham >= 2 palavras significativas (> 2 chars) → match parcial."""
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "GLIFOSATO NORTOX 20L CONCENTRADO"}
        docs = [{"produto": "GLIFOSATO NORTOX HERBICIDA 20 LITROS"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_one_common_keyword_not_enough(self, mock_bedrock):
        """Apenas 1 palavra em comum + sem substring + Bedrock MISMATCH → rejeita."""
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "GLIFOSATO PRODUTO UNICO"}
        docs = [{"produto": "GLIFOSATO TOTALMENTE DIFERENTE OUTRO"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        # "GLIFOSATO" é 1 palavra em comum, mas "PRODUTO" vs "TOTALMENTE" etc não batem
        # Porém pode cair no Bedrock. Com mock MISMATCH, deve ser None
        # Na verdade vamos checar: GLIFOSATO (>2), PRODUTO (>2), UNICO (>2) vs GLIFOSATO, TOTALMENTE, DIFERENTE, OUTRO
        # Common = {GLIFOSATO} → 1 → não atinge 2
        assert idx is None

    def test_substring_match(self, mock_bedrock):
        """Nome do pedido é substring do DANFE → match parcial."""
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "OPTERADUO GL 1X20L CAIXA FECHADA"}
        docs = [{"produto": "OPTERADUO GL 1X20L"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_short_names_skip_partial(self, mock_bedrock):
        """Nomes com <= 3 chars não entram no match parcial."""
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "AB"}
        docs = [{"produto": "AB"}]
        # Exato match still works even if short
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0  # exact match always works


class TestFindMatchingProductBedrock:
    """PRIORIDADE 3: Fallback para Bedrock quando nenhum heurístico funciona."""

    def test_bedrock_match_as_last_resort(self, mock_bedrock):
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "mesmo produto"}}
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "PRIMESTRA GOLD 20L"}
        docs = [{"produto": "PRIMESTRA GOLD VINTE LITROS"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx == 0

    def test_bedrock_mismatch_returns_none(self, mock_bedrock):
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "nope"}}
        from rules.validar_produtos import find_matching_product
        danfe = {"descricao": "HERBICIDA X"}
        docs = [{"produto": "INSETICIDA Y"}]
        idx, _, _ = find_matching_product(danfe, docs, set())
        assert idx is None


# =============================================================================
# validar_produtos — validate (end-to-end via validate()) — 1 produto
# =============================================================================

class TestProdutos1Item:
    """Single product scenarios."""

    def test_one_product_exact_name_passes(self, mock_bedrock):
        """Nome exato (case-insensitive) → PASSED sem depender do Bedrock."""
        from rules.validar_produtos import validate
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "X", "descricao": "PRODUTO TESTE ABC", "quantidade": "10", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "Y", "produto": "produto teste abc"}]
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert len(r["matched_danfe_positions"]) == 1

    def test_one_product_bedrock_match_passes(self, mock_bedrock):
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}
        r = validate(_danfe(), [_pedido_doc()])
        assert r["status"] == "PASSED"
        assert len(r["matched_danfe_positions"]) == 1

    def test_one_product_bedrock_mismatch_fails(self, mock_bedrock):
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "nope"}}
        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "X", "produto": "PRODUTO COMPLETAMENTE DIFERENTE"}]
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_no_products_in_metadata_fails(self):
        from rules.validar_produtos import validate
        doc = _pedido_doc()
        doc["itens"] = []
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


# =============================================================================
# validar_produtos — 2 produtos
# =============================================================================

class TestProdutos2Items:
    """Two products in DANFE + two in pedido."""

    def test_two_products_exact_name_match(self, mock_bedrock):
        """Ambos matcham por nome exato → PASSED, 2 posições."""
        from rules.validar_produtos import validate
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "Produto A 10L", "quantidade": "50", "unidade": "UN"},
            {"item": "2", "codigo": "B", "descricao": "Produto B 5KG", "quantidade": "30", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "PA", "produto": "Produto A 10L", "valorUnitario": 100},
            {"codigoProduto": "PB", "produto": "Produto B 5KG", "valorUnitario": 200},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert len(r["matched_danfe_positions"]) == 2

    def test_two_products_one_missing_in_pedido(self, mock_bedrock):
        """2 in DANFE, 1 in pedido → at least 1 match → PASSED (partial)."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "Produto A", "quantidade": "50", "unidade": "UN"},
            {"item": "2", "codigo": "B", "descricao": "Produto B", "quantidade": "30", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "PA", "produto": "Produto A", "valorUnitario": 100},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert len(r["matched_danfe_positions"]) == 1

    def test_two_products_swapped_order_still_matches(self, mock_bedrock):
        """Produtos em ordem invertida → matching não depende de ordem → PASSED."""
        from rules.validar_produtos import validate
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "HERBICIDA ATRAZINA 5L", "quantidade": "50", "unidade": "UN"},
            {"item": "2", "codigo": "B", "descricao": "INSETICIDA LAMBDA 1L", "quantidade": "30", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "PB", "produto": "INSETICIDA LAMBDA 1L", "valorUnitario": 200},
            {"codigoProduto": "PA", "produto": "HERBICIDA ATRAZINA 5L", "valorUnitario": 100},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert sorted(r["matched_danfe_positions"]) == [1, 2]

    def test_two_products_both_mismatch_fails(self, mock_bedrock):
        """Nenhum produto pareia → FAILED."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "HERBICIDA X", "quantidade": "50", "unidade": "UN"},
            {"item": "2", "codigo": "B", "descricao": "INSETICIDA Y", "quantidade": "30", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "PA", "produto": "FERTILIZANTE Z", "valorUnitario": 100},
            {"codigoProduto": "PB", "produto": "FUNGICIDA W", "valorUnitario": 200},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"
        assert len(r["matched_danfe_positions"]) == 0


# =============================================================================
# validar_produtos — multi-lote (N linhas XML, 1 linha pedido)
# =============================================================================

class TestProdutosMultiLote:
    """3 XML lines (same product, different lots) + 1 pedido line."""

    def test_three_lots_one_pedido_all_match(self, mock_bedrock):
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe({"produtos": [
            {
                "item": "1", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "0011-26-7400", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
            {
                "item": "2", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "0028-26-9000", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
            {
                "item": "3", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "0029-26-8000", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
        ]})

        doc = _pedido_doc()
        doc["itens"] = [{
            "codigoProduto": "I3000001GL00200",
            "produto": "OPTERADUO GL 20 LT",
            "valorUnitario": 2480,
            "pedidoDeCompra": {"pedidoErp": "AACBKV", "itemPedidoErp": "0001"},
        }]

        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert sorted(r["matched_danfe_positions"]) == [1, 2, 3]

    def test_duplicate_lot_does_not_reuse(self, mock_bedrock):
        """Same lot number on 2 lines → only 1 match (lot already consumed)."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe({"produtos": [
            {
                "item": "1", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "SAME-LOT", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
            {
                "item": "2", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "SAME-LOT", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
        ]})

        doc = _pedido_doc()
        doc["itens"] = [{
            "codigoProduto": "I3000001GL00200",
            "produto": "OPTERADUO GL 20 LT",
        }]

        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        assert len(r["matched_danfe_positions"]) == 1

    def test_multilot_different_products_no_reuse(self, mock_bedrock):
        """2 produtos DIFERENTES com lotes, 1 linha no pedido → multi-lote NÃO ativa (produtos != equivalentes)."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe({"produtos": [
            {
                "item": "1", "codigo": "AAA", "descricao": "PRODUTO A 20L",
                "quantidade": "100", "unidade": "UN",
                "rastro": [{"lote": "LOTE-1", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
            {
                "item": "2", "codigo": "BBB", "descricao": "PRODUTO B 5KG",
                "quantidade": "50", "unidade": "UN",
                "rastro": [{"lote": "LOTE-2", "data_fabricacao": "2025-01-01", "data_validade": "2028-01-01"}],
            },
        ]})

        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "AAA", "produto": "PRODUTO A 20L"}]

        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"
        # Only first product matches (exact name); second product is different → not multi-lote reuse
        assert 1 in r["matched_danfe_positions"]
        assert len(r["matched_danfe_positions"]) == 1

    def test_multilot_without_rastro_no_reuse(self, mock_bedrock):
        """Mesmo produto repetido mas SEM rastro/lote → multi-lote não ativa (precisa de lote)."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
             "quantidade": "100", "unidade": "UN"},
            {"item": "2", "codigo": "40055406", "descricao": "OPTERADUO GL 1X20L",
             "quantidade": "100", "unidade": "UN"},
        ]})

        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "X", "produto": "OPTERADUO GL 1X20L"}]

        r = validate(danfe, [doc])
        # First matches exact, second has no lote → try_resolve_multi_lot returns None
        assert len(r["matched_danfe_positions"]) == 1


# =============================================================================
# validar_produtos — validate_products_comparison (cenários realistas)
# =============================================================================

class TestValidateProductsComparison:
    """Testa validate_products_comparison diretamente com cenários de negócio."""

    def test_separator_normalization_in_full_flow(self, mock_bedrock):
        """Produto com NPK 15.15.15 no XML vs 15-15-15 no pedido → match."""
        from rules.validar_produtos import validate_products_comparison
        danfe_prods = [{"descricao": "FERTILIZANTE NPK 15.15.15 50KG", "codigo": "F1", "quantidade": "100"}]
        doc_prods = [{"produto": "FERTILIZANTE NPK 15-15-15 50KG", "codigoProduto": "F1"}]
        r = validate_products_comparison(danfe_prods, doc_prods, "pedido.json", "METADADOS JSON")
        assert r["has_match"] is True
        assert r["all_match"] is True

    def test_partial_keyword_match_in_full_flow(self, mock_bedrock):
        """Nomes com >= 2 palavras em comum → match parcial."""
        from rules.validar_produtos import validate_products_comparison
        danfe_prods = [{"descricao": "GLIFOSATO NORTOX 480 20L HERBICIDA", "codigo": "G1", "quantidade": "50"}]
        doc_prods = [{"produto": "GLIFOSATO NORTOX CONCENTRADO 20 LITROS", "codigoProduto": "G2"}]
        r = validate_products_comparison(danfe_prods, doc_prods, "pedido.json", "METADADOS JSON")
        assert r["has_match"] is True

    def test_unmatched_doc_products_tracked(self, mock_bedrock):
        """Produto no pedido que não existe no XML → registra como unmatched.
        Bedrock MATCH para o par correto; o segundo produto do pedido fica orphan."""
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}
        from rules.validar_produtos import validate_products_comparison
        danfe_prods = [{"descricao": "HERBICIDA GLIFOSATO 20L", "codigo": "A", "quantidade": "10"}]
        doc_prods = [
            {"produto": "HERBICIDA GLIFOSATO 20L", "codigoProduto": "A"},
            {"produto": "INSETICIDA LAMBDA CYHALOTHRIN 1L", "codigoProduto": "Z"},
        ]
        r = validate_products_comparison(danfe_prods, doc_prods, "pedido.json", "METADADOS JSON")
        assert r["has_match"] is True
        items = r["comparison"]["items"]
        unmatched_docs = [i for i in items if i.get("danfe_position") is None]
        assert len(unmatched_docs) == 1

    def test_unmatched_danfe_products_tracked(self, mock_bedrock):
        """Produto no XML que não existe no pedido → registra como unmatched."""
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
        from rules.validar_produtos import validate_products_comparison
        danfe_prods = [
            {"descricao": "PRODUTO A", "codigo": "A", "quantidade": "10"},
            {"descricao": "PRODUTO ORFAO", "codigo": "B", "quantidade": "20"},
        ]
        doc_prods = [{"produto": "PRODUTO A", "codigoProduto": "A"}]
        r = validate_products_comparison(danfe_prods, doc_prods, "pedido.json", "METADADOS JSON")
        items = r["comparison"]["items"]
        unmatched_danfe = [i for i in items if i.get("doc_position") is None]
        assert len(unmatched_danfe) == 1
        assert r["all_match"] is False

    def test_empty_danfe_empty_doc_no_match(self, mock_bedrock):
        """Nenhum produto em nenhum lado → has_match=False (nada para parear)."""
        from rules.validar_produtos import validate_products_comparison
        r = validate_products_comparison([], [], "pedido.json", "METADADOS JSON")
        assert r["has_match"] is False


# =============================================================================
# validar_produtos — utility functions
# =============================================================================

class TestProdutoUtils:
    """Pure-function tests (no Bedrock)."""

    def test_normalize_number_br_format(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number("3.200,50") == 3200.50

    def test_normalize_number_us_format(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number("3,200.50") == 3200.50

    def test_normalize_number_plain_int(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number("100") == 100.0

    def test_normalize_number_float_passthrough(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number(99.5) == 99.5

    def test_normalize_codigo_strips_leading_zeros(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo("000000010000013136") == "10000013136"

    def test_normalize_codigo_preserves_letters(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo("I3000001GL00200") == "I3000001GL00200"

    def test_normalize_codigo_strips_special_chars(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo("I3-000.001 GL/00200") == "I3000001GL00200"

    def test_normalize_code_separators_dots(self):
        from rules.validar_produtos import normalize_code_separators
        assert normalize_code_separators("15.15.15") == "15 15 15"

    def test_normalize_code_separators_dashes(self):
        from rules.validar_produtos import normalize_code_separators
        assert normalize_code_separators("15-15-15") == "15 15 15"

    def test_normalize_code_separators_mixed(self):
        from rules.validar_produtos import normalize_code_separators
        assert normalize_code_separators("15.15-15") == "15 15 15"

    def test_extract_lote_signature_sorted(self):
        from rules.validar_produtos import extract_lote_signature
        prod = {"rastro": [
            {"lote": "B", "data_fabricacao": "2025-01-01"},
            {"lote": "A", "data_fabricacao": "2025-01-01"},
        ]}
        assert extract_lote_signature(prod) == ("A", "B")

    def test_extract_lote_signature_no_rastro(self):
        from rules.validar_produtos import extract_lote_signature
        assert extract_lote_signature({"codigo": "X"}) is None

    def test_extract_lote_signature_single_lote(self):
        from rules.validar_produtos import extract_lote_signature
        prod = {"rastro": [{"lote": "LOTE-UNICO"}]}
        assert extract_lote_signature(prod) == ("LOTE-UNICO",)

    def test_danfe_products_equivalent_by_code(self):
        from rules.validar_produtos import danfe_products_equivalent
        p1 = {"codigo": "40055406", "descricao": "OPTERADUO GL 1X20L"}
        p2 = {"codigo": "40055406", "descricao": "OPTERADUO GL 1X20L OUTRA DESC"}
        assert danfe_products_equivalent(p1, p2) is True

    def test_danfe_products_equivalent_by_name(self):
        from rules.validar_produtos import danfe_products_equivalent
        p1 = {"codigo": "A", "descricao": "OPTERADUO GL 1X20L"}
        p2 = {"codigo": "B", "descricao": "OPTERADUO GL 1X20L"}
        assert danfe_products_equivalent(p1, p2) is True

    def test_danfe_products_not_equivalent(self):
        from rules.validar_produtos import danfe_products_equivalent
        p1 = {"codigo": "A", "descricao": "HERBICIDA X"}
        p2 = {"codigo": "B", "descricao": "INSETICIDA Y"}
        assert danfe_products_equivalent(p1, p2) is False

    def test_quantities_match_within_tolerance(self):
        from rules.validar_produtos import quantities_match
        assert quantities_match(100.0, "UN", 100.005, "UN") is True

    def test_quantities_mismatch_different_units(self):
        from rules.validar_produtos import quantities_match
        assert quantities_match(100.0, "KG", 100.0, "UN") is False

    def test_quantities_match_unit_synonyms(self):
        """UNID e UN são sinônimos → match."""
        from rules.validar_produtos import quantities_match
        assert quantities_match(50.0, "UNID", 50.0, "UN") is True

    def test_quantities_match_both_zero(self):
        from rules.validar_produtos import quantities_match
        assert quantities_match(0.0, "UN", 0.0, "UN") is True

    def test_codes_are_similar_ocr_error(self):
        from rules.validar_produtos import codes_are_similar
        assert codes_are_similar("I3000", "13000") is True  # I <-> 1

    def test_codes_are_similar_too_many_diffs(self):
        from rules.validar_produtos import codes_are_similar
        assert codes_are_similar("ABCD", "WXYZ") is False

    def test_codes_are_similar_different_length(self):
        from rules.validar_produtos import codes_are_similar
        assert codes_are_similar("ABC", "ABCD") is False

    def test_make_product_key_danfe(self):
        from rules.validar_produtos import make_product_key
        k = make_product_key({"codigo": "00123", "quantidade": "50.0"}, is_danfe=True)
        assert k == ("123", 50.0)

    def test_make_product_key_doc(self):
        from rules.validar_produtos import make_product_key
        k = make_product_key({"codigoProduto": "00123", "quantidade": 50}, is_danfe=False)
        assert k == ("123", 50.0)

    def test_make_product_key_doc_no_quantity(self):
        from rules.validar_produtos import make_product_key
        k = make_product_key({"codigoProduto": "ABC"}, is_danfe=False)
        assert k == ("ABC", 0.0)


# =============================================================================
# Cenário end-to-end: todas as regras passam
# =============================================================================

class TestAllRulesPass:
    """Full set of rules on a well-formed process → all PASSED."""

    def test_complete_valid_process(self, mock_bedrock):
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe()
        doc = _pedido_doc()
        doc["serie"] = "1"
        doc["documento"] = "878991"
        doc["dataEmissao"] = "2026-03-15"
        doc["totais"] = {"valor_icms": "0.00"}

        from rules.validar_cnpj_fornecedor import validate as v_cnpj_forn
        from rules.validar_cnpj_destinatario import validate as v_cnpj_dest
        from rules.validar_serie import validate as v_serie
        from rules.validar_numero_nota import validate as v_num_nota
        from rules.validar_data_emissao import validate as v_data
        from rules.validar_numero_pedido import validate as v_pedido
        from rules.validar_produtos import validate as v_prod

        results = {
            "cnpj_forn": v_cnpj_forn(danfe, [doc]),
            "cnpj_dest": v_cnpj_dest(danfe, [doc]),
            "serie": v_serie(danfe, [doc]),
            "numero_nota": v_num_nota(danfe, [doc]),
            "data_emissao": v_data(danfe, [doc]),
            "pedido": v_pedido(danfe, [doc]),
            "produtos": v_prod(danfe, [doc]),
        }

        for name, r in results.items():
            assert r["status"] == "PASSED", f"Rule {name} should PASSED but got {r['status']}: {r.get('message')}"


# =============================================================================
# Cenário: CNPJ errado, tudo mais ok → só CNPJ falha
# =============================================================================

class TestOnlyOneFails:

    def test_wrong_cnpj_only_cnpj_fails(self, mock_bedrock):
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}

        danfe = _danfe()
        doc = _pedido_doc()
        doc["serie"] = "1"
        doc["documento"] = "878991"
        doc["dataEmissao"] = "2026-03-15"
        doc["totais"] = {"valor_icms": "0.00"}
        doc["cnpjEmitente"] = "99999999000100"
        doc["requestBody"]["cnpjEmitente"] = "99999999000100"

        from rules.validar_cnpj_fornecedor import validate as v_cnpj_forn
        from rules.validar_serie import validate as v_serie
        from rules.validar_numero_nota import validate as v_num_nota

        assert v_cnpj_forn(danfe, [doc])["status"] == "FAILED"
        assert v_serie(danfe, [doc])["status"] == "PASSED"
        assert v_num_nota(danfe, [doc])["status"] == "PASSED"


# #############################################################################
#  TESTES NEGATIVOS — garantir que a regra REJEITA o que não deveria passar
# #############################################################################


class TestRejectCnpjFornecedorNegative:
    """Cenários que NÃO devem ser aceitos pelo validar_cnpj_fornecedor."""

    def test_rejects_cpf_vs_cnpj(self):
        """CPF (11 dígitos) no metadata vs CNPJ (14) no XML → raízes diferentes → FAIL."""
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "12345678901",
                           "requestBody": {"cnpjEmitente": "12345678901"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_cnpj_with_only_punctuation(self):
        """CNPJ que é só pontuação / vazio mascarado → FAIL."""
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "...---...",
                           "requestBody": {"cnpjEmitente": "...---..."}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_partial_cnpj_6_digits(self):
        """CNPJ incompleto (6 dígitos) → raiz com < 8 dígitos → FAIL."""
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "074678",
                           "requestBody": {"cnpjEmitente": "074678"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_swapped_emitente_destinatario(self):
        """CNPJ do destinatário colocado no campo do emitente → raízes diferentes → FAIL."""
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "13563680004603",
                           "requestBody": {"cnpjEmitente": "13563680004603"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_empty_string_cnpj(self):
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc({"cnpjEmitente": "",
                           "requestBody": {"cnpjEmitente": ""}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_has_metadata_false_even_if_cnpj_present(self):
        """Doc sem _has_metadata=True → regra não consulta campos → FAIL."""
        from rules.validar_cnpj_fornecedor import validate
        doc = _pedido_doc()
        doc["_has_metadata"] = False
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectCnpjDestinatarioNegative:

    def test_rejects_inverted_cnpjs(self):
        """CNPJ do emitente no campo de destinatário → FAIL."""
        from rules.validar_cnpj_destinatario import validate
        doc = _pedido_doc({"cnpjDestinatario": "07467822001289",
                           "requestBody": {"cnpjDestinatario": "07467822001289"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_zeros_cnpj(self):
        from rules.validar_cnpj_destinatario import validate
        doc = _pedido_doc({"cnpjDestinatario": "00000000000000",
                           "requestBody": {"cnpjDestinatario": "00000000000000"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_alphabetic_cnpj(self):
        from rules.validar_cnpj_destinatario import validate
        doc = _pedido_doc({"cnpjDestinatario": "ABCDEFGH000100",
                           "requestBody": {"cnpjDestinatario": "ABCDEFGH000100"}})
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectSerieNegative:

    def test_rejects_serie_0_vs_1(self):
        """Série 0 no metadata vs série 1 no XML → FAIL."""
        from rules.validar_serie import validate
        danfe = _danfe({"serie": "1"})
        doc = _pedido_doc()
        doc["serie"] = "0"
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_rejects_serie_alphanumeric_vs_numeric(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = "A1"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_none_serie_value(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = None
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_whitespace_only_serie(self):
        from rules.validar_serie import validate
        doc = _pedido_doc()
        doc["serie"] = "   "
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectNumeroNotaNegative:

    def test_rejects_reversed_digits(self):
        """878991 vs 199878 → not equal, Bedrock says MISMATCH → FAIL."""
        from rules.validar_numero_nota import validate
        with patch("rules.validar_numero_nota.compare_with_bedrock") as m, \
             patch("rules.validar_numero_nota.bedrock_compare_status", return_value="MISMATCH"):
            m.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "invertido"}}
            doc = _pedido_doc()
            doc["documento"] = "199878"
            r = validate(_danfe(), [doc])
            assert r["status"] == "FAILED"

    def test_rejects_numero_off_by_one(self):
        """878991 vs 878992 → 1 digit off, Bedrock MISMATCH → FAIL."""
        from rules.validar_numero_nota import validate
        with patch("rules.validar_numero_nota.compare_with_bedrock") as m, \
             patch("rules.validar_numero_nota.bedrock_compare_status", return_value="MISMATCH"):
            m.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "1 digito"}}
            doc = _pedido_doc()
            doc["documento"] = "878992"
            r = validate(_danfe(), [doc])
            assert r["status"] == "FAILED"

    def test_rejects_numero_with_letters(self):
        from rules.validar_numero_nota import validate
        with patch("rules.validar_numero_nota.compare_with_bedrock") as m, \
             patch("rules.validar_numero_nota.bedrock_compare_status", return_value="MISMATCH"):
            m.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "letras"}}
            doc = _pedido_doc()
            doc["documento"] = "ABC123"
            r = validate(_danfe(), [doc])
            assert r["status"] == "FAILED"

    def test_rejects_empty_string_numero(self):
        from rules.validar_numero_nota import validate
        doc = _pedido_doc()
        doc["documento"] = ""
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectDataEmissaoNegative:

    def test_rejects_one_day_off(self):
        """15 vs 16 março → FAIL."""
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2026-03-16"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_wrong_month(self):
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2026-04-15"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_wrong_year(self):
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "2025-03-15"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_ddmmyyyy_format(self):
        """15/03/2026 string raw não normaliza para YYYY-MM-DD → FAIL."""
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "15/03/2026"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_garbage_date(self):
        from rules.validar_data_emissao import validate
        doc = _pedido_doc()
        doc["dataEmissao"] = "data invalida"
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectNumeroPedidoNegative:

    def test_rejects_similar_but_different_pedido(self):
        """Pedido AACBKW (1 letra diferente) não está na info_adicional que tem AACBKV → FAIL."""
        from rules.validar_numero_pedido import validate
        doc = _pedido_doc()
        doc["itens"] = [{
            "codigoProduto": "X",
            "produto": "Y",
            "pedidoDeCompra": {"pedidoErp": "AACBKW", "itemPedidoErp": "0001"},
        }]
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_pedido_case_sensitive(self):
        """Pedido 'aacbkv' (lowercase) vs info_adicional 'AACBKV' → substring check is case-sensitive → FAIL."""
        from rules.validar_numero_pedido import validate
        doc = _pedido_doc()
        doc["itens"] = [{
            "codigoProduto": "X",
            "produto": "Y",
            "pedidoDeCompra": {"pedidoErp": "aacbkv", "itemPedidoErp": "0001"},
        }]
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_numeric_pedido_not_in_text(self):
        """Pedido '999999' not in info_adicional → FAIL."""
        from rules.validar_numero_pedido import validate
        doc = _pedido_doc()
        doc["itens"] = [{
            "codigoProduto": "X",
            "produto": "Y",
            "pedidoDeCompra": {"pedidoErp": "999999", "itemPedidoErp": "0001"},
        }]
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectIcmsNegative:

    def test_rejects_internal_with_danfe_nonzero_doc_zero(self):
        """Operação interna: DANFE ICMS = 100, doc = 0 → FAIL (ambos devem ser 0)."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"uf": "SP"},
            "destinatario": {"uf": "SP"},
            "totais": {"valor_icms": "100.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "0.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_rejects_internal_with_both_nonzero(self):
        """Operação interna: ambos com ICMS alto → FAIL."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"uf": "MG"},
            "destinatario": {"uf": "MG"},
            "totais": {"valor_icms": "500.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "500.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_rejects_interstate_off_by_one_real(self):
        """Interestadual: diferença de R$ 1,00 (> 0.01 tolerância) → FAIL."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"uf": "SP"},
            "destinatario": {"uf": "AM"},
            "totais": {"valor_icms": "180.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "181.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_rejects_interstate_negative_icms(self):
        """ICMS negativo vs positivo → abs diff grande → FAIL."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"uf": "GO"},
            "destinatario": {"uf": "SP"},
            "totais": {"valor_icms": "100.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "-100.00"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"

    def test_rejects_icms_text_garbage(self):
        """Valor de ICMS não numérico → FAIL."""
        from rules.validar_icms import validate
        danfe = _danfe({
            "emitente": {"uf": "SP"},
            "destinatario": {"uf": "AM"},
            "totais": {"valor_icms": "180.00"},
        })
        doc = _pedido_doc()
        doc["totais"] = {"valor_icms": "N/A"}
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"


class TestRejectProdutosNegative:
    """Cenários onde a regra de produtos DEVE rejeitar."""

    def test_rejects_all_products_completely_different(self, mock_bedrock):
        """Todos os nomes de produto são totalmente diferentes → Bedrock MISMATCH → FAIL."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "nope"}}
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "HERBICIDA GLIFOSATO 20L", "quantidade": "50", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "Z", "produto": "FERTILIZANTE NPK 10-10-10 50KG"},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "FAILED"
        assert len(r["matched_danfe_positions"]) == 0

    def test_rejects_extra_product_in_pedido_not_in_xml(self, mock_bedrock):
        """Pedido tem 2 produtos, XML tem 1 → 1 match + 1 unmatched doc → all_match = False."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MATCH", "bedrock": {"explicacao": "ok"}}
        danfe = _danfe({"produtos": [
            {"item": "1", "codigo": "A", "descricao": "Produto A", "quantidade": "50", "unidade": "UN"},
        ]})
        doc = _pedido_doc()
        doc["itens"] = [
            {"codigoProduto": "PA", "produto": "Produto A", "valorUnitario": 100},
            {"codigoProduto": "PB", "produto": "Produto B EXTRA", "valorUnitario": 200},
        ]
        r = validate(danfe, [doc])
        assert r["status"] == "PASSED"  # has_match=True (at least 1)
        comparisons = r.get("comparisons", [])
        items = comparisons[0].get("items", []) if comparisons else []
        unmatched = [i for i in items if i.get("danfe_position") is None]
        assert len(unmatched) >= 1, "Pedido extra should appear as unmatched"

    def test_rejects_zero_products_in_danfe(self, mock_bedrock):
        """XML sem produtos (lista vazia) → nada para parear → FAIL."""
        from rules.validar_produtos import validate
        danfe = _danfe({"produtos": []})
        r = validate(danfe, [_pedido_doc()])
        assert r["status"] == "FAILED"

    def test_rejects_no_metadata_flag(self, mock_bedrock):
        """Doc com _has_metadata=False → produtos não são consultados → FAIL."""
        from rules.validar_produtos import validate
        doc = _pedido_doc()
        doc["_has_metadata"] = False
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"

    def test_rejects_empty_doc_list(self, mock_bedrock):
        """Nenhum doc de pedido de compra → nenhum match → FAIL."""
        from rules.validar_produtos import validate
        r = validate(_danfe(), [])
        assert r["status"] == "FAILED"

    def test_rejects_itens_with_none_produto_name(self, mock_bedrock):
        """Produto no pedido sem nome → Bedrock MISMATCH → FAIL."""
        from rules.validar_produtos import validate
        mock_bedrock.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "sem nome"}}
        doc = _pedido_doc()
        doc["itens"] = [{"codigoProduto": "X", "produto": ""}]
        r = validate(_danfe(), [doc])
        assert r["status"] == "FAILED"


class TestRejectProdutoUtilsNegative:
    """Funções utilitárias: entradas inválidas / edge cases."""

    def test_normalize_number_garbage_returns_zero(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number("abc") == 0

    def test_normalize_number_empty_string_returns_zero(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number("") == 0

    def test_normalize_number_none_returns_zero(self):
        from rules.validar_produtos import normalize_number
        assert normalize_number(None) == 0

    def test_normalize_codigo_empty_returns_empty(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo("") == ""

    def test_normalize_codigo_none_returns_empty(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo(None) == ""

    def test_normalize_codigo_all_zeros_returns_zero(self):
        from rules.validar_produtos import normalize_codigo
        assert normalize_codigo("00000") == "0"

    def test_quantities_mismatch_large_difference(self):
        from rules.validar_produtos import quantities_match
        assert quantities_match(100.0, "UN", 200.0, "UN") is False

    def test_quantities_mismatch_kg_vs_litro(self):
        from rules.validar_produtos import quantities_match
        assert quantities_match(50.0, "KG", 50.0, "L") is False

    def test_extract_lote_signature_empty_rastro_list(self):
        from rules.validar_produtos import extract_lote_signature
        assert extract_lote_signature({"rastro": []}) is None

    def test_extract_lote_signature_rastro_without_lote_key(self):
        from rules.validar_produtos import extract_lote_signature
        assert extract_lote_signature({"rastro": [{"data_fabricacao": "2025-01-01"}]}) is None


# =============================================================================
# Cenário end-to-end NEGATIVO: tudo errado → tudo FAIL
# =============================================================================

class TestAllRulesFail:
    """Process where every field is wrong → every rule should FAIL."""

    def test_completely_wrong_metadata(self):
        from rules.validar_cnpj_fornecedor import validate as v_cnpj_forn
        from rules.validar_cnpj_destinatario import validate as v_cnpj_dest
        from rules.validar_serie import validate as v_serie
        from rules.validar_numero_nota import validate as v_num_nota
        from rules.validar_data_emissao import validate as v_data
        from rules.validar_numero_pedido import validate as v_pedido

        danfe = _danfe()
        doc = _pedido_doc()
        doc["cnpjEmitente"] = "99999999000100"
        doc["requestBody"]["cnpjEmitente"] = "99999999000100"
        doc["cnpjDestinatario"] = "88888888000100"
        doc["requestBody"]["cnpjDestinatario"] = "88888888000100"
        doc["serie"] = "9"
        doc["documento"] = "111111"
        doc["dataEmissao"] = "2020-01-01"
        doc["itens"] = [{
            "codigoProduto": "ZZZZZ",
            "produto": "PRODUTO INEXISTENTE NO XML",
            "pedidoDeCompra": {"pedidoErp": "XXXXXX", "itemPedidoErp": "9999"},
        }]

        with patch("rules.validar_numero_nota.compare_with_bedrock") as m_nn, \
             patch("rules.validar_numero_nota.bedrock_compare_status", return_value="MISMATCH"), \
             patch("rules.utils.compare_with_bedrock") as m_utils:
            m_nn.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}
            m_utils.return_value = {"status": "MISMATCH", "bedrock": {"explicacao": "diff"}}

            results = {
                "cnpj_forn": v_cnpj_forn(danfe, [doc]),
                "cnpj_dest": v_cnpj_dest(danfe, [doc]),
                "serie": v_serie(danfe, [doc]),
                "numero_nota": v_num_nota(danfe, [doc]),
                "data_emissao": v_data(danfe, [doc]),
                "pedido": v_pedido(danfe, [doc]),
            }

            for name, r in results.items():
                assert r["status"] == "FAILED", \
                    f"Rule {name} should FAIL with wrong data but got {r['status']}: {r.get('message')}"


class TestRejectMultipleDocsOneFails:
    """2 docs de pedido — um correto, um errado. Regra deve FAIL se qualquer um falha."""

    def test_cnpj_one_ok_one_wrong_fails(self):
        from rules.validar_cnpj_fornecedor import validate
        doc_ok = _pedido_doc()
        doc_wrong = _pedido_doc({"cnpjEmitente": "99999999000100",
                                  "requestBody": {"cnpjEmitente": "99999999000100"}})
        r = validate(_danfe(), [doc_ok, doc_wrong])
        assert r["status"] == "FAILED"
        statuses = [c["status"] for c in r["comparisons"]]
        assert "MATCH" in statuses, "First doc should MATCH"
        assert "MISMATCH" in statuses, "Second doc should MISMATCH"

    def test_serie_one_ok_one_wrong_fails(self):
        from rules.validar_serie import validate
        doc_ok = _pedido_doc()
        doc_ok["serie"] = "1"
        doc_wrong = _pedido_doc()
        doc_wrong["serie"] = "9"
        r = validate(_danfe(), [doc_ok, doc_wrong])
        assert r["status"] == "FAILED"
