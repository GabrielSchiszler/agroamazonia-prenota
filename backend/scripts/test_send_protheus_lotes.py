#!/usr/bin/env python3
"""
Script de teste para validar a geração de payload com lotes no send_to_protheus.

Testa 3 cenários:
1. Lote nos rastros (XML estruturado)
2. Lote no texto do produto (info_adicional do produto - IA)
3. Lote no texto adicional da NF (info_adicional da NF - IA)
"""

import json
import sys
import os
from datetime import datetime

# Adicionar o diretório do handler ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambdas/send_to_protheus'))

# Mock do Bedrock e boto3 antes de importar
import unittest.mock as mock

# Criar mock do Bedrock
class MockBedrock:
    def invoke_model(self, modelId, body):
        body_dict = json.loads(body)
        prompt = body_dict['messages'][0]['content'][0]['text']
        
        if 'LOTE:331/25' in prompt or 'LOTE:ABC123' in prompt:
            response_text = '''{
  "lotes": [
    {
      "numero": "331/25",
      "dataFabricacao": "2025-06-12",
      "dataValidade": "2026-12-12",
      "quantidade": 20.0
    },
    {
      "numero": "332/25",
      "dataFabricacao": "2025-06-12",
      "dataValidade": "2026-12-12",
      "quantidade": 20.0
    }
  ]
}'''
        elif 'LOTE:XYZ789' in prompt:
            response_text = '''{
  "lotes": [
    {
      "numero": "XYZ789",
      "dataFabricacao": "2025-01-15",
      "dataValidade": "2026-01-15",
      "quantidade": null
    }
  ]
}'''
        else:
            response_text = '{"lotes": []}'
        
        class MockResponse:
            def __init__(self, text):
                self._text = text.encode()
            def read(self):
                return self._text
        
        return {'body': MockResponse(response_text)}

# Mock do boto3
mock_boto3 = mock.MagicMock()
mock_boto3.client.return_value = MockBedrock()
mock_boto3.resource.return_value = mock.MagicMock()

# Substituir boto3 antes de importar
sys.modules['boto3'] = mock_boto3

# Definir variáveis de ambiente necessárias
os.environ['TABLE_NAME'] = 'test-table'

# Importar funções do handler
try:
    from handler import convert_rastros_to_lotes, process_produtos_with_lotes, extract_lotes_with_ai
except ImportError as e:
    print(f"ERRO ao importar handler: {e}")
    print("Certifique-se de estar executando o script a partir do diretório backend/scripts/")
    sys.exit(1)

# Mock da função extract_lotes_with_ai para não precisar de Bedrock real
def mock_extract_lotes_with_ai(text):
    """Mock da função extract_lotes_with_ai para testes"""
    if 'LOTE:331/25' in text or 'LOTE:ABC123' in text:
        return [
            {
                'numero': '331/25',
                'dataFabricacao': '2025-06-12',
                'dataValidade': '2026-12-12',
                'quantidade': 20.0
            },
            {
                'numero': '332/25',
                'dataFabricacao': '2025-06-12',
                'dataValidade': '2026-12-12',
                'quantidade': 20.0
            }
        ]
    elif 'LOTE:XYZ789' in text:
        return [
            {
                'numero': 'XYZ789',
                'dataFabricacao': '2025-01-15',
                'dataValidade': '2026-01-15',
                'quantidade': None
            }
        ]
    return []

# Substituir a função no módulo handler
import handler
handler.extract_lotes_with_ai = mock_extract_lotes_with_ai

def print_separator(title):
    """Imprime um separador visual"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def test_cenario_1_rastros():
    """CENÁRIO 1: Lote nos rastros (XML estruturado)"""
    print_separator("CENÁRIO 1: Lote nos RASTROS (XML estruturado)")
    
    # Dados simulados
    produto_xml = {
        'descricao': 'FERTILIZANTE TOP PHOS 280 HP B1',
        'quantidade': 40.0,
        'valor_unitario': 4228.0,
        'rastro': [
            {
                'lote': '331/25',
                'data_fabricacao': '12/06/2025',
                'data_validade': '12/12/2026',
                'quantidade': '20.0'
            },
            {
                'lote': '332/25',
                'data_fabricacao': '12/06/2025',
                'data_validade': '12/12/2026',
                'quantidade': '20.0'
            }
        ],
        'info_adicional': ''  # Vazio para garantir que não use IA
    }
    
    xml_data = {
        'info_adicional': ''  # Vazio para garantir que não use IA
    }
    
    request_body_data = {}
    
    pedido_de_compra = {
        'pedidoErp': '582992',
        'itemPedidoErp': '0001'
    }
    
    produtos_filtrados = [
        (0, produto_xml, pedido_de_compra, '26480')
    ]
    
    print("📦 Dados de entrada:")
    print(f"   Produto: {produto_xml['descricao']}")
    print(f"   Quantidade total: {produto_xml['quantidade']}")
    print(f"   Rastros encontrados: {len(produto_xml['rastro'])}")
    for i, rastro in enumerate(produto_xml['rastro'], 1):
        print(f"     Rastro {i}: Lote={rastro['lote']}, Qtd={rastro['quantidade']}, Fab={rastro['data_fabricacao']}, Valid={rastro['data_validade']}")
    
    # Processar
    produtos_processados = process_produtos_with_lotes(produtos_filtrados, xml_data, request_body_data)
    
    print(f"\n✅ Resultado: {len(produtos_processados)} item(s) processado(s)")
    for i, produto in enumerate(produtos_processados, 1):
        print(f"\n   Item {i}:")
        print(f"     Código: {produto['codigo_produto']}")
        print(f"     Quantidade: {produto['quantidade']}")
        if produto['lote']:
            print(f"     Lote: {produto['lote']['numero']}")
            print(f"     Data Fabricação: {produto['lote'].get('dataFabricacao')}")
            print(f"     Data Validade: {produto['lote'].get('dataValidade')}")
        else:
            print(f"     Lote: Nenhum")
    
    # Gerar payload de exemplo
    payload_items = []
    for produto in produtos_processados:
        item = {
            "codigoProduto": produto['codigo_produto'],
            "produto": produto['produto_xml']['descricao'],
            "quantidade": produto['quantidade'],
            "valorUnitario": produto['produto_xml']['valor_unitario'],
            "valorTotal": produto['quantidade'] * produto['produto_xml']['valor_unitario'],
            "unidadeMedida": "TON",
            "pedidoDeCompra": produto['pedido_de_compra']
        }
        if produto['lote']:
            item["lote"] = {
                "numero": produto['lote']['numero'],
                "dataValidade": produto['lote'].get('dataValidade'),
                "dataFabricacao": produto['lote'].get('dataFabricacao')
            }
        payload_items.append(item)
    
    print(f"\n📋 Payload gerado (itens):")
    print(json.dumps(payload_items, indent=2, ensure_ascii=False))
    
    return produtos_processados

def test_cenario_2_info_produto():
    """CENÁRIO 2: Lote no texto do produto (info_adicional do produto - IA)"""
    print_separator("CENÁRIO 2: Lote no TEXTO DO PRODUTO (info_adicional - IA)")
    
    # Dados simulados
    produto_xml = {
        'descricao': 'FERTILIZANTE TOP PHOS 280 HP B1',
        'quantidade': 40.0,
        'valor_unitario': 4228.0,
        'rastro': None,  # Sem rastros
        'info_adicional': 'RS 000155-0.000048 FERTILIZANTE MINERAL COMPLEXO. LOTE:331/25 FABRIC:06/12/2025 VALID:18 MESES LOTE:332/25 FABRIC:06/12/2025 VALID:18 MESES'
    }
    
    xml_data = {
        'info_adicional': ''  # Vazio para garantir que use info_adicional do produto
    }
    
    request_body_data = {}
    
    pedido_de_compra = {
        'pedidoErp': '582992',
        'itemPedidoErp': '0001'
    }
    
    produtos_filtrados = [
        (0, produto_xml, pedido_de_compra, '26480')
    ]
    
    print("📦 Dados de entrada:")
    print(f"   Produto: {produto_xml['descricao']}")
    print(f"   Quantidade total: {produto_xml['quantidade']}")
    print(f"   Rastros: Nenhum")
    print(f"   Info adicional do produto: {produto_xml['info_adicional'][:100]}...")
    
    # Processar
    produtos_processados = process_produtos_with_lotes(produtos_filtrados, xml_data, request_body_data)
    
    print(f"\n✅ Resultado: {len(produtos_processados)} item(s) processado(s)")
    for i, produto in enumerate(produtos_processados, 1):
        print(f"\n   Item {i}:")
        print(f"     Código: {produto['codigo_produto']}")
        print(f"     Quantidade: {produto['quantidade']}")
        if produto['lote']:
            print(f"     Lote: {produto['lote']['numero']}")
            print(f"     Data Fabricação: {produto['lote'].get('dataFabricacao')}")
            print(f"     Data Validade: {produto['lote'].get('dataValidade')}")
        else:
            print(f"     Lote: Nenhum")
    
    # Gerar payload de exemplo
    payload_items = []
    for produto in produtos_processados:
        item = {
            "codigoProduto": produto['codigo_produto'],
            "produto": produto['produto_xml']['descricao'],
            "quantidade": produto['quantidade'],
            "valorUnitario": produto['produto_xml']['valor_unitario'],
            "valorTotal": produto['quantidade'] * produto['produto_xml']['valor_unitario'],
            "unidadeMedida": "TON",
            "pedidoDeCompra": produto['pedido_de_compra']
        }
        if produto['lote']:
            item["lote"] = {
                "numero": produto['lote']['numero'],
                "dataValidade": produto['lote'].get('dataValidade'),
                "dataFabricacao": produto['lote'].get('dataFabricacao')
            }
        payload_items.append(item)
    
    print(f"\n📋 Payload gerado (itens):")
    print(json.dumps(payload_items, indent=2, ensure_ascii=False))
    
    return produtos_processados

def test_cenario_3_info_nf():
    """CENÁRIO 3: Lote no texto adicional da NF (info_adicional da NF - IA)"""
    print_separator("CENÁRIO 3: Lote no TEXTO ADICIONAL DA NF (info_adicional da NF - IA)")
    
    # Dados simulados
    produto_xml = {
        'descricao': 'FERTILIZANTE TOP PHOS 280 HP B1',
        'quantidade': 40.0,
        'valor_unitario': 4228.0,
        'rastro': None,  # Sem rastros
        'info_adicional': ''  # Vazio no produto
    }
    
    xml_data = {
        'info_adicional': 'LOTE:XYZ789 FABRIC:15/01/2025 VALID:12 MESES COD INTERNO:JDJ4I94 BASE DE CALCULO REDUZIDA'
    }
    
    request_body_data = {}
    
    pedido_de_compra = {
        'pedidoErp': '582992',
        'itemPedidoErp': '0001'
    }
    
    produtos_filtrados = [
        (0, produto_xml, pedido_de_compra, '26480')
    ]
    
    print("📦 Dados de entrada:")
    print(f"   Produto: {produto_xml['descricao']}")
    print(f"   Quantidade total: {produto_xml['quantidade']}")
    print(f"   Rastros: Nenhum")
    print(f"   Info adicional do produto: Vazio")
    print(f"   Info adicional da NF: {xml_data['info_adicional']}")
    
    # Processar
    produtos_processados = process_produtos_with_lotes(produtos_filtrados, xml_data, request_body_data)
    
    print(f"\n✅ Resultado: {len(produtos_processados)} item(s) processado(s)")
    for i, produto in enumerate(produtos_processados, 1):
        print(f"\n   Item {i}:")
        print(f"     Código: {produto['codigo_produto']}")
        print(f"     Quantidade: {produto['quantidade']}")
        if produto['lote']:
            print(f"     Lote: {produto['lote']['numero']}")
            print(f"     Data Fabricação: {produto['lote'].get('dataFabricacao')}")
            print(f"     Data Validade: {produto['lote'].get('dataValidade')}")
        else:
            print(f"     Lote: Nenhum")
    
    # Gerar payload de exemplo
    payload_items = []
    for produto in produtos_processados:
        item = {
            "codigoProduto": produto['codigo_produto'],
            "produto": produto['produto_xml']['descricao'],
            "quantidade": produto['quantidade'],
            "valorUnitario": produto['produto_xml']['valor_unitario'],
            "valorTotal": produto['quantidade'] * produto['produto_xml']['valor_unitario'],
            "unidadeMedida": "TON",
            "pedidoDeCompra": produto['pedido_de_compra']
        }
        if produto['lote']:
            item["lote"] = {
                "numero": produto['lote']['numero'],
                "dataValidade": produto['lote'].get('dataValidade'),
                "dataFabricacao": produto['lote'].get('dataFabricacao')
            }
        payload_items.append(item)
    
    print(f"\n📋 Payload gerado (itens):")
    print(json.dumps(payload_items, indent=2, ensure_ascii=False))
    
    return produtos_processados

def main():
    """Executa todos os testes"""
    print("\n" + "="*80)
    print("  TESTE DE GERAÇÃO DE PAYLOAD COM LOTES - send_to_protheus")
    print("="*80)
    
    try:
        # Teste 1: Rastros
        resultado1 = test_cenario_1_rastros()
        assert len(resultado1) == 2, f"Esperado 2 itens, obtido {len(resultado1)}"
        assert resultado1[0]['lote']['numero'] == '331/25', "Lote 1 incorreto"
        assert resultado1[1]['lote']['numero'] == '332/25', "Lote 2 incorreto"
        print("\n✅ CENÁRIO 1: PASSOU")
        
        # Teste 2: Info adicional do produto
        resultado2 = test_cenario_2_info_produto()
        assert len(resultado2) == 2, f"Esperado 2 itens, obtido {len(resultado2)}"
        assert resultado2[0]['lote']['numero'] == '331/25', "Lote 1 incorreto"
        assert resultado2[1]['lote']['numero'] == '332/25', "Lote 2 incorreto"
        print("\n✅ CENÁRIO 2: PASSOU")
        
        # Teste 3: Info adicional da NF
        resultado3 = test_cenario_3_info_nf()
        assert len(resultado3) == 1, f"Esperado 1 item, obtido {len(resultado3)}"
        assert resultado3[0]['lote']['numero'] == 'XYZ789', "Lote incorreto"
        assert resultado3[0]['quantidade'] == 40.0, "Quantidade incorreta (deve usar total quando lote único sem qtd)"
        print("\n✅ CENÁRIO 3: PASSOU")
        
        print("\n" + "="*80)
        print("  ✅ TODOS OS TESTES PASSARAM!")
        print("="*80 + "\n")
        
    except AssertionError as e:
        print(f"\n❌ ERRO NO TESTE: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO INESPERADO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

