#!/usr/bin/env python3
"""
Script para atualizar registros existentes na tabela Chave x CFOP
Adiciona o campo 'regra' aos registros que já existem
"""
import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.cfop_operation_service import CfopOperationService

# Mapeamento de chave -> regra (texto descritivo) e observacao
REGRA_OBSERVACAO_MAP = {
    '1B': {
        'regra': 'Utilizar quando a nota fiscal se referir à entrada de mercadoria para revenda. Aplicável às entradas normais de mercadoria, conforme CFOPs listados.',
        'observacao': 'Tipo de operação mais utilizado para entrada de mercadoria para revenda ("Entradas Normais"). Utilizar quando a nota fiscal se referir à entrada de mercadoria para revenda. Não utilizar para devoluções ou entradas simbólicas.'
    },
    '3I': {
        'regra': 'Utilizar quando a nota fiscal não possuir destaque de ICMS ou se tratar de fornecedor optante Simples Nacional, é a operação equivalente à compra para comercialização (1B) que não gerar cálculo de imposto.',
        'observacao': 'Utilizar apenas para registro de notas com os CFOPs mencionados que não possuam destaque de ICMS.'
    },
    '1H': {
        'regra': 'Utilizar quando a mercadoria for recebida a título de bonificação e tiver finalidade de revenda.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Doação e Brinde. Avaliar sempre a finalidade da mercadoria para definir o tipo de operação correto.'
    },
    '1L': {
        'regra': 'Utilizar quando a mercadoria for recebida a título de doação, sem finalidade de revenda.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Bonificação e Brinde. Confirmar a finalidade da mercadoria antes da classificação.'
    },
    '1M': {
        'regra': 'Utilizar quando a mercadoria for recebida como brinde, sem finalidade de revenda - O produto deve ser do grupo BRINDE.',
        'observacao': 'O CFOP 5910/6910 também é utilizado para Bonificação e Doação. A definição do tipo de operação depende da finalidade da mercadoria.'
    },
    '2X': {
        'regra': 'Utilizar para registro da nota fiscal da transação comercial em operações de venda à ordem, quando a empresa for o destinatário final e não houver trânsito físico da mercadoria. Esta operação movimenta apenas o financeiro.',
        'observacao': 'Deve ser utilizada em conjunto com a operação 1A (CFOP 5923/6923) para formalização da operação triangular ou venda à ordem. Aplica-se, por exemplo, a notas de sementes recebidas em venda à ordem.'
    },
    '1A': {
        'regra': 'Utilizar para registro da nota fiscal de remessa que acoberta o transporte físico da mercadoria em operações de venda à ordem ou operações triangulares.',
        'observacao': 'Deve ser utilizada em conjunto com a operação 2X (CFOP 5920/6920). Esta nota acompanha o trânsito da mercadoria.'
    },
    '2A': {
        'regra': '',
        'observacao': 'Não atualiza Estoque'
    },
    '3J': {
        'regra': '',
        'observacao': 'Atualiza Estoque'
    },
    '1C': {
        'regra': '',
        'observacao': 'A remessa (Operação 96) movimenta estoque'
    },
    '96': {
        'regra': '',
        'observacao': 'Esta operação movimenta o estoque da filial'
    },
    '3Q': {
        'regra': '',
        'observacao': 'A remessa (Operação 3R) não movimenta estoque'
    },
    '3R': {
        'regra': '',
        'observacao': 'Esta operação não movimenta o estoque da filial, utilizada para remessas de entrega fura já entregue em venda a ordem.'
    },
    '1N': {
        'regra': '',
        'observacao': 'Registro realizado via formulário próprio, demanda análise da autorização do departamento de Compliance e Controles Internos (Auditoria) onde deve ser mencionado o número da pré-nota a ser classificada e conter a explícita autorização do auditor para a classificação.'
    },
    '98': {
        'regra': '',
        'observacao': 'Registro realizado via formulário próprio, demanda análise da \'Natureza da Operação\' da NF de Origem.'
    }
}

def update_existing_records():
    """Atualiza registros existentes adicionando o campo 'regra'"""
    service = CfopOperationService()
    
    print("🔄 Iniciando atualização dos registros existentes...")
    print("📋 Buscando registros na tabela...\n")
    
    # Buscar todos os registros
    all_rules = service.list_all()
    
    if not all_rules:
        print("⚠️  Nenhum registro encontrado na tabela.")
        return
    
    print(f"📊 Total de registros encontrados: {len(all_rules)}\n")
    
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    for rule in all_rules:
        chave = rule.get('chave', '')
        regra_atual = rule.get('regra', '')
        observacao_atual = rule.get('observacao', '')
        
        # Buscar dados no mapa
        dados = REGRA_OBSERVACAO_MAP.get(chave, {})
        regra_nova = dados.get('regra', '')
        observacao_nova = dados.get('observacao', '')
        
        # Se não tem dados no mapa, pular
        if not dados:
            print(f"⚠️  {chave} - Dados não encontrados no mapa, pulando...")
            skipped_count += 1
            continue
        
        # Verificar se precisa atualizar
        precisa_atualizar = False
        if regra_nova and regra_atual != regra_nova:
            precisa_atualizar = True
        if observacao_nova and observacao_atual != observacao_nova:
            precisa_atualizar = True
        
        if not precisa_atualizar:
            print(f"⏭️  {chave} - Já está atualizado, pulando...")
            skipped_count += 1
            continue
        
        try:
            # Atualizar regra e observacao
            service.update(
                mapping_id=rule['id'],
                regra=regra_nova if regra_nova else None,
                observacao=observacao_nova if observacao_nova else None
            )
            print(f"✅ {chave} - Atualizado (regra: {'sim' if regra_nova else 'não'}, observacao: {'sim' if observacao_nova else 'não'})")
            updated_count += 1
        except Exception as e:
            print(f"❌ {chave} - Erro: {e}")
            error_count += 1
    
    print(f"\n📊 Resumo:")
    print(f"   ✅ Atualizados: {updated_count}")
    print(f"   ⏭️  Pulados: {skipped_count}")
    print(f"   ❌ Erros: {error_count}")
    print("\n✨ Concluído!")

if __name__ == '__main__':
    update_existing_records()

