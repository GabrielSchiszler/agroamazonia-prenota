import sys
sys.path.insert(0, '../src')

from services.rules_service import RulesService

def test_sementes_workflow_approved():
    """Testa workflow de sementes com dados válidos"""
    data = {
        'imposto_total': 500,
        'limite_imposto_sementes': 1000,
        'certificado_fitossanitario': True
    }
    
    result = RulesService.validate('SEMENTES', data)
    assert result['status'] == 'APPROVED'

def test_sementes_workflow_rejected_imposto():
    """Testa rejeição por imposto alto"""
    data = {
        'imposto_total': 1500,
        'limite_imposto_sementes': 1000,
        'certificado_fitossanitario': True
    }
    
    result = RulesService.validate('SEMENTES', data)
    assert result['status'] == 'REJECTED'
    assert result['rule'] == 'IMPOSTO_INCORRETO'

def test_agroquimicos_workflow_rejected_licenca():
    """Testa rejeição por falta de licença"""
    data = {
        'licenca_ibama': False,
        'valor_total': 1000,
        'valor_esperado': 1000
    }
    
    result = RulesService.validate('AGROQUIMICOS', data)
    assert result['status'] == 'REJECTED'
    assert result['rule'] == 'LICENCA_AUSENTE'

def test_fertilizantes_workflow_rejected():
    """Testa rejeição por falta de laudo"""
    data = {
        'laudo_composicao': False
    }
    
    result = RulesService.validate('FERTILIZANTES', data)
    assert result['status'] == 'REJECTED'
    assert result['rule'] == 'LAUDO_AUSENTE'

if __name__ == '__main__':
    test_sementes_workflow_approved()
    test_sementes_workflow_rejected_imposto()
    test_agroquimicos_workflow_rejected_licenca()
    test_fertilizantes_workflow_rejected()
    print("✓ Todos os testes passaram")
