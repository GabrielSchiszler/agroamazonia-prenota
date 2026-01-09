#!/usr/bin/env python3
"""
Script para popular a tabela Chave x CFOP com os dados iniciais
"""
import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.cfop_operation_service import CfopOperationService

# Todos os dados da planilha
ALL_DATA = [
    {
        'chave': '1B',
        'descricao': 'E-033-COMPRA PARA COMERCIALIZACAO',
        'cfop': '5101 6101 5102 6102 5105 6105 5106 6106',
        'operacao': '1B',
        'regra': 'Utilizar quando a nota fiscal se referir à entrada de mercadoria para revenda. Aplicável às entradas normais de mercadoria, conforme CFOPs listados.',
        'observacao': 'Tipo de operação mais utilizado para entrada de mercadoria para revenda ("Entradas Normais"). Utilizar quando a nota fiscal se referir à entrada de mercadoria para revenda. Não utilizar para devoluções ou entradas simbólicas.',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '3I',
        'descricao': 'E-156-COMPRA MERC. - SIMPLES NACIONAL/NAO TRANSITAR',
        'cfop': '5105 6105 5106 6106',
        'operacao': '3I',
        'regra': 'Utilizar quando a nota fiscal não possuir destaque de ICMS ou se tratar de fornecedor optante Simples Nacional, é a operação equivalente à compra para comercialização (1B) que não gerar cálculo de imposto.',
        'observacao': 'Utilizar apenas para registro de notas com os CFOPs mencionados que não possuam destaque de ICMS.',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '1H',
        'descricao': 'E-047-BONIFICACAO RECEBIDA',
        'cfop': '5910 6910',
        'operacao': '1H',
        'regra': 'Utilizar quando a mercadoria for recebida a título de bonificação e tiver finalidade de revenda.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Doação e Brinde. Avaliar sempre a finalidade da mercadoria para definir o tipo de operação correto.',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '1L',
        'descricao': 'E-061-DOAC-O RECEBIDA',
        'cfop': '5910 6910',
        'operacao': '1L',
        'regra': 'Utilizar quando a mercadoria for recebida a título de doação, sem finalidade de revenda.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Bonificação e Brinde. Confirmar a finalidade da mercadoria antes da classificação.',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '1M',
        'descricao': 'E-062-BRINDE RECEBIDO',
        'cfop': '5910 6910',
        'operacao': '1M',
        'regra': 'Utilizar quando a mercadoria for recebida como brinde, sem finalidade de revenda - O produto deve ser do grupo BRINDE.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Bonificação e Doação. A definição do tipo de operação depende da finalidade da mercadoria.',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '2X',
        'descricao': 'E-141-COMPRA P/ COMERC EM VENDA A ORDEM JA RECEBIDA',
        'cfop': '5920 6920',
        'operacao': '2X',
        'regra': 'Utilizar para registro da nota fiscal da transação comercial em operações de venda à ordem, quando a empresa for o destinatário final e não houver trânsito físico da mercadoria. Esta operação movimenta apenas o financeiro.',
        'observacao': 'Deve ser utilizada em conjunto com a operação 1A (CFOP 5923/6923) para formalização da operação triangular ou venda à ordem. Aplica-se, por exemplo, a notas de sementes recebidas em venda à ordem.',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '1A',
        'descricao': 'E-031-REMESSA P/ CONTA E ORDEM DE TERCEIROS',
        'cfop': '5923 6923',
        'operacao': '1A',
        'regra': 'Utilizar para registro da nota fiscal de remessa que acoberta o transporte físico da mercadoria em operações de venda à ordem ou operações triangulares.',
        'observacao': 'Deve ser utilizada em conjunto com a operação 2X (CFOP 5920/6920). Esta nota acompanha o trânsito da mercadoria.',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '1R',
        'descricao': 'E-088-COMPRA PARA COMERCIALIZACAO COM SUBST.TRIBUTARIA',
        'cfop': '',
        'operacao': '1R',
        'regra': '',
        'observacao': '',
        'pedido_compra': True,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1N',
        'descricao': 'E-079-OUTRAS ENTRADAS - SOBRAS DE ESTOQUE',
        'cfop': '',
        'operacao': '1N',
        'regra': '',
        'observacao': 'Registro realizado via formulário próprio, demanda análise da autorização do departamento de Compliance e Controles Internos (Auditoria) onde deve ser mencionado o número da pré-nota a ser classificada e conter a explícita autorização do auditor para a classificação.',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1I',
        'descricao': 'E-049-RETORNO MERC REMETIDA PARA TROCA',
        'cfop': '',
        'operacao': '1I',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1O',
        'descricao': 'E-081-ENTRADA DE AMOSTRA GRATIS',
        'cfop': '',
        'operacao': '1O',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1Q',
        'descricao': 'E-086-ENTRADA DE BEM P/ CONTA DE CONTRATO COMODATO',
        'cfop': '',
        'operacao': '1Q',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1S',
        'descricao': 'E-089-COMPLEMENTO DE ICMS - ENTRADA',
        'cfop': '',
        'operacao': '1S',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '2A',
        'descricao': 'E-096-COMPRA P/ COMERCIALIZACAO - VENDA ORDEM',
        'cfop': '5118 5119',
        'operacao': '2A',
        'regra': '',
        'observacao': 'Não atualiza Estoque',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '3J',
        'descricao': 'E-157-COMPRA P/ COMERCIALIZACAO - VENDA ORDEM',
        'cfop': '5118 5119',
        'operacao': '3J',
        'regra': '',
        'observacao': 'Atualiza Estoque',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '2K',
        'descricao': 'E-115-RETORNO DE MERC OU BEM REMETIDO P/ CONSERTO/REPAR',
        'cfop': '',
        'operacao': '2K',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '2L',
        'descricao': 'E-116-ENTRADA DE VASILHAME OU SACARIA',
        'cfop': '',
        'operacao': '2L',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '2V',
        'descricao': 'E-140-RET. DE MERC. REMETIDA P/IND. POR ENCOMENDA',
        'cfop': '',
        'operacao': '2V',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '3H',
        'descricao': 'E-154-COMPRA DE MAQUINA USADA',
        'cfop': '',
        'operacao': '3H',
        'regra': '',
        'observacao': '',
        'pedido_compra': True,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '3K',
        'descricao': 'E-160-RETORNO DE ARMAZENAGEM',
        'cfop': '',
        'operacao': '3K',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '1C',
        'descricao': 'E-034-COMPRA PARA ENTREGA FUTURA',
        'cfop': '5922 6922',
        'operacao': '1C',
        'regra': '',
        'observacao': 'A remessa (Operação 96) movimenta estoque',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '96',
        'descricao': 'E-018-REMESSA PARA ENTREGA FUTURA',
        'cfop': '1117 2117 1116 2116',
        'operacao': '96',
        'regra': '',
        'observacao': 'Esta operação movimenta o estoque da filial',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '3Q',
        'descricao': 'E-165-COMPRA SIMBOLICA A ORDEM',
        'cfop': '5922 6922',
        'operacao': '3Q',
        'regra': '',
        'observacao': 'A remessa (Operação 3R) não movimenta estoque',
        'pedido_compra': True,
        'ativo': True
    },
    {
        'chave': '3R',
        'descricao': 'E-166-REMESSA COMPRA SIMBOLICA A ORDEM',
        'cfop': '1117 2117 1116 2116',
        'operacao': '3R',
        'regra': '',
        'observacao': 'Esta operação não movimenta o estoque da filial, utilizada para remessas de entrega fura já entregue em venda a ordem.',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '3U',
        'descricao': 'E-168-RETORNO DE VASILHAME OU SACARIA',
        'cfop': '',
        'operacao': '3U',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '5G',
        'descricao': 'E-XXX-ENTRADA COMPL COMPRA PARA COMERCIALIZACAO - VARIA',
        'cfop': '',
        'operacao': '5G',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '6X',
        'descricao': 'E-06X-NOTA FISCAL COMPLEMENTO (PRECO) S/ESTOQUE',
        'cfop': '',
        'operacao': '6X',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    },
    {
        'chave': '95',
        'descricao': 'E-016-OUTRAS ENTRADAS',
        'cfop': '5949 6949',
        'operacao': '95',
        'regra': '',
        'observacao': '',
        'pedido_compra': False,
        'ativo': True
    },
    {
        'chave': '98',
        'descricao': 'E-024-RETORNO DE EXPOSICAO',
        'cfop': '',
        'operacao': '98',
        'regra': '',
        'observacao': 'Registro realizado via formulário próprio, demanda análise da \'Natureza da Operação\' da NF de Origem.',
        'pedido_compra': False,
        'ativo': False  # Incompleto - falta CFOP
    }
]

def seed_cfop_operations():
    """Popula a tabela Chave x CFOP com dados iniciais"""
    service = CfopOperationService()
    
    print("🌱 Iniciando população da tabela Chave x CFOP...")
    print(f"📋 Total de registros a inserir: {len(ALL_DATA)}\n")
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    for data in ALL_DATA:
        try:
            result = service.create(
                chave=data['chave'],
                descricao=data['descricao'],
                cfop=data['cfop'],
                operacao=data['operacao'],
                regra=data.get('regra', ''),
                observacao=data.get('observacao', ''),
                pedido_compra=data['pedido_compra'],
                ativo=data['ativo']
            )
            status = "✅" if data['ativo'] else "⚠️ "
            print(f"{status} {data['chave']} - {data['descricao']} {'(ATIVO)' if data['ativo'] else '(INATIVO - incompleto)'}")
            success_count += 1
        except ValueError as e:
            if "já existe" in str(e):
                print(f"⏭️  {data['chave']} - Já existe, pulando...")
                skipped_count += 1
            else:
                print(f"❌ {data['chave']} - Erro: {e}")
                error_count += 1
        except Exception as e:
            print(f"❌ {data['chave']} - Erro: {e}")
            error_count += 1
    
    print(f"\n📊 Resumo:")
    print(f"   ✅ Inseridos: {success_count}")
    print(f"   ⏭️  Já existiam: {skipped_count}")
    print(f"   ❌ Erros: {error_count}")
    print("\n✨ Concluído!")

if __name__ == '__main__':
    seed_cfop_operations()
