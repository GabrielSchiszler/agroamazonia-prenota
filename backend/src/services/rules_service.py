import os
import logging
from typing import Dict, Any, List
from src.repositories.dynamodb_repository import DynamoDBRepository

logger = logging.getLogger(__name__)

class RulesService:
    def __init__(self):
        self.repository = DynamoDBRepository()
    
    def list_rules(self, process_type: str) -> List[Dict[str, Any]]:
        """Lista regras de um tipo de processo"""
        pk = f"RULES#{process_type}"
        items = self.repository.query_by_pk_and_sk_prefix(pk, 'RULE#')
        
        rules = []
        for item in items:
            rules.append({
                'rule_name': item.get('RULE_NAME'),
                'order': item.get('ORDER'),
                'enabled': item.get('ENABLED', True)
            })
        
        # Ordenar tratando None como valor alto (999)
        rules.sort(key=lambda x: x['order'] if x['order'] is not None else 999)
        return rules
    
    def create_rule(self, process_type: str, rule_name: str, order: int, enabled: bool = True) -> Dict[str, Any]:
        """Cria nova regra"""
        pk = f"RULES#{process_type}"
        sk = f"RULE#{rule_name}"
        
        self.repository.put_item(pk, sk, {
            'RULE_NAME': rule_name,
            'ORDER': order,
            'ENABLED': enabled
        })
        
        return {'status': 'created', 'rule_name': rule_name}
    
    def update_rule(self, process_type: str, rule_name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Atualiza regra"""
        pk = f"RULES#{process_type}"
        sk = f"RULE#{rule_name}"
        
        update_data = {}
        if 'order' in updates:
            update_data['ORDER'] = updates['order']
        if 'enabled' in updates:
            update_data['ENABLED'] = updates['enabled']
        
        self.repository.update_item(pk, sk, update_data)
        
        return {'status': 'updated', 'rule_name': rule_name}
    
    def delete_rule(self, process_type: str, rule_name: str) -> Dict[str, Any]:
        """Remove regra"""
        pk = f"RULES#{process_type}"
        sk = f"RULE#{rule_name}"
        
        self.repository.delete_item(pk, sk)
        
        return {'status': 'deleted', 'rule_name': rule_name}
