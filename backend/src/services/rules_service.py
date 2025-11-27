from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable

class Rule(ABC):
    """Regra base para Chain of Responsibility"""
    def __init__(self):
        self.next_rule: Optional[Rule] = None
    
    def set_next(self, rule: "Rule") -> "Rule":
        self.next_rule = rule
        return rule
    
    @abstractmethod
    def check(self, data: Dict[str, Any]) -> bool:
        """Retorna True se a condição da regra for atendida (erro encontrado)"""
        pass
    
    @abstractmethod
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Executa ação quando regra é verdadeira"""
        pass
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Processa a regra e passa para próxima se validação OK"""
        if self.check(data):
            return self.execute_action(data)
        
        if self.next_rule:
            return self.next_rule.process(data)
        
        return {"status": "APPROVED", "message": "Todas as validações passaram"}

# Regras para SEMENTES
class SementesImpostoRule(Rule):
    def check(self, data: Dict[str, Any]) -> bool:
        imposto = data.get('imposto_total', 0)
        return imposto > data.get('limite_imposto_sementes', 1000)
    
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "REJECTED", "rule": "IMPOSTO_INCORRETO", "message": "Imposto acima do limite para sementes"}

class SementesDocumentacaoRule(Rule):
    def check(self, data: Dict[str, Any]) -> bool:
        return not data.get('certificado_fitossanitario')
    
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "PENDING", "rule": "DOCUMENTACAO_FALTANTE", "message": "Certificado fitossanitário ausente"}

# Regras para AGROQUIMICOS
class AgroquimicosLicencaRule(Rule):
    def check(self, data: Dict[str, Any]) -> bool:
        return not data.get('licenca_ibama')
    
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "REJECTED", "rule": "LICENCA_AUSENTE", "message": "Licença IBAMA obrigatória"}

class AgroquimicosValorRule(Rule):
    def check(self, data: Dict[str, Any]) -> bool:
        valor = data.get('valor_total', 0)
        return valor != data.get('valor_esperado', 0)
    
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "PENDING", "rule": "DIVERGENCIA_VALOR", "message": "Valor divergente do esperado"}

# Regras para FERTILIZANTES
class FertilizantesComposicaoRule(Rule):
    def check(self, data: Dict[str, Any]) -> bool:
        return not data.get('laudo_composicao')
    
    def execute_action(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "REJECTED", "rule": "LAUDO_AUSENTE", "message": "Laudo de composição obrigatório"}

class RulesService:
    """Serviço de regras dinâmicas por tipo de processo"""
    
    @staticmethod
    def get_rules_info(process_type: str) -> list[Dict[str, Any]]:
        """Retorna informações sobre as regras de um processo"""
        rules_map = {
            "SEMENTES": [
                {"name": "Validação de Imposto", "description": "Verifica se imposto está dentro do limite", "action": "REJECT"},
                {"name": "Verificação de Documentação", "description": "Valida presença de Certificado Fitossanitário", "action": "PENDING"}
            ],
            "AGROQUIMICOS": [
                {"name": "Validação de Licença IBAMA", "description": "Verifica presença de licença IBAMA", "action": "REJECT"},
                {"name": "Verificação de Valor", "description": "Compara valor total com esperado", "action": "PENDING"}
            ],
            "FERTILIZANTES": [
                {"name": "Validação de Laudo de Composição", "description": "Verifica presença de laudo", "action": "REJECT"}
            ]
        }
        
        if process_type not in rules_map:
            raise ValueError(f"Process type não suportado: {process_type}")
        
        return rules_map[process_type]
    
    @staticmethod
    def get_workflow(process_type: str) -> Rule:
        """Retorna cadeia de regras para o tipo de processo"""
        if process_type == "SEMENTES":
            rule1 = SementesImpostoRule()
            rule2 = SementesDocumentacaoRule()
            rule1.set_next(rule2)
            return rule1
        
        elif process_type == "AGROQUIMICOS":
            rule1 = AgroquimicosLicencaRule()
            rule2 = AgroquimicosValorRule()
            rule1.set_next(rule2)
            return rule1
        
        elif process_type == "FERTILIZANTES":
            return FertilizantesComposicaoRule()
        
        raise ValueError(f"Process type não suportado: {process_type}")
    
    @staticmethod
    def validate(process_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Executa validação completa para o tipo de processo"""
        workflow = RulesService.get_workflow(process_type)
        return workflow.process(data)
