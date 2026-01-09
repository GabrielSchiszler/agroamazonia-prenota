import logging
from typing import Dict, Any, List
from src.repositories.dynamodb_repository import DynamoDBRepository

logger = logging.getLogger(__name__)

class CfopOperationService:
    def __init__(self):
        self.repository = DynamoDBRepository()
        self.pk = "CFOP_OPERATION"
    
    def list_all(self) -> List[Dict[str, Any]]:
        """Lista todas as regras Chave x CFOP"""
        items = self.repository.query_by_pk_and_sk_prefix(self.pk, "MAPPING#")
        
        rules = []
        for item in items:
            rules.append({
                'id': item.get('SK', '').replace('MAPPING#', ''),
                'chave': item.get('CHAVE', ''),
                'descricao': item.get('DESCRICAO', ''),
                'cfop': item.get('CFOP', ''),
                'operacao': item.get('OPERACAO', ''),  # Código para Protheus (mesmo da chave)
                'regra': item.get('REGRA', ''),  # Texto descritivo de quando usar
                'observacao': item.get('OBSERVACAO', ''),
                'pedido_compra': item.get('PEDIDO_COMPRA', False),
                'ativo': item.get('ATIVO', True)  # Por padrão ativo se não especificado
            })
        
        # Ordenar por Chave
        rules.sort(key=lambda x: x.get('chave', ''))
        return rules
    
    def get_by_cfop(self, cfop: str) -> Dict[str, Any]:
        """Busca operação por CFOP usando query direta no DynamoDB
        
        Args:
            cfop: Código CFOP a ser buscado (ex: '5101', '6101')
            
        Returns:
            Dict com os dados da regra encontrada ou None se não encontrado
        """
        if not cfop:
            return None
            
        # Normalizar CFOP (remover espaços e caracteres especiais)
        cfop_normalized = str(cfop).strip()
        
        # Buscar diretamente pelo registro CFOP#{cfop}
        cfop_sk = f"CFOP#{cfop_normalized}"
        cfop_item = self.repository.get_item(self.pk, cfop_sk)
        
        if not cfop_item:
            return None
        
        # Verificar se está ativo
        if not cfop_item.get('ATIVO', True):
            return None
        
        # Buscar o registro principal usando o mapping_id
        mapping_id = cfop_item.get('MAPPING_ID')
        if not mapping_id:
            # Se não tem MAPPING_ID, tentar usar o primeiro de MAPPING_IDS
            mapping_ids = cfop_item.get('MAPPING_IDS', [])
            if not mapping_ids:
                return None
            mapping_id = mapping_ids[0]
        
        # Buscar registro principal
        mapping_sk = f"MAPPING#{mapping_id}"
        mapping_item = self.repository.get_item(self.pk, mapping_sk)
        
        if not mapping_item:
            return None
        
        # Verificar se o registro principal está ativo
        if not mapping_item.get('ATIVO', True):
            return None
        
        # Retornar dados do registro principal
        return {
            'id': mapping_id,
            'chave': mapping_item.get('CHAVE', ''),
            'descricao': mapping_item.get('DESCRICAO', ''),
            'cfop': mapping_item.get('CFOP', ''),  # Retorna formato original para exibição
            'operacao': mapping_item.get('OPERACAO', ''),
            'regra': mapping_item.get('REGRA', ''),
            'observacao': mapping_item.get('OBSERVACAO', ''),
            'pedido_compra': mapping_item.get('PEDIDO_COMPRA', False),
            'ativo': mapping_item.get('ATIVO', True)
        }
    
    def create(self, chave: str, descricao: str, cfop: str, operacao: str, regra: str = '', observacao: str = '', pedido_compra: bool = False, ativo: bool = True) -> Dict[str, Any]:
        """Cria nova regra Chave x CFOP"""
        # Verificar se já existe Chave
        items = self.repository.query_by_pk_and_sk_prefix(self.pk, "MAPPING#")
        for item in items:
            if item.get('CHAVE', '') == chave:
                raise ValueError(f"Chave {chave} já existe")
        
        # Gerar ID único baseado na chave
        mapping_id = chave.replace('.', '').replace('-', '').replace(' ', '').upper()
        sk = f"MAPPING#{mapping_id}"
        
        # Separar CFOPs individuais (separados por espaço)
        cfop_list = [c.strip() for c in cfop.split() if c.strip()] if cfop else []
        
        # Criar registro principal com todos os dados
        self.repository.put_item(self.pk, sk, {
            'CHAVE': chave,
            'DESCRICAO': descricao,
            'CFOP': cfop,  # Mantém formato original para exibição no frontend
            'CFOP_LIST': cfop_list,  # Lista de CFOPs individuais para busca
            'OPERACAO': operacao,
            'REGRA': regra,
            'OBSERVACAO': observacao,
            'PEDIDO_COMPRA': pedido_compra,
            'ATIVO': ativo
        })
        
        # Criar registros individuais por CFOP para busca rápida
        # PK: CFOP_OPERATION, SK: CFOP#{cfop_individual}
        for cfop_item in cfop_list:
            cfop_sk = f"CFOP#{cfop_item}"
            # Verificar se já existe registro para este CFOP
            existing = self.repository.get_item(self.pk, cfop_sk)
            if existing:
                # Se já existe, atualizar para apontar para o novo mapping_id
                # (permite múltiplas chaves para o mesmo CFOP, mas prioriza o ativo)
                if not existing.get('MAPPING_IDS'):
                    existing['MAPPING_IDS'] = []
                if mapping_id not in existing.get('MAPPING_IDS', []):
                    existing['MAPPING_IDS'].append(mapping_id)
                self.repository.put_item(self.pk, cfop_sk, {
                    'CFOP': cfop_item,
                    'MAPPING_ID': mapping_id,  # Principal (último criado)
                    'MAPPING_IDS': existing.get('MAPPING_IDS', []) + [mapping_id] if mapping_id not in existing.get('MAPPING_IDS', []) else existing.get('MAPPING_IDS', []),
                    'CHAVE': chave,
                    'ATIVO': ativo
                })
            else:
                # Criar novo registro para este CFOP
                self.repository.put_item(self.pk, cfop_sk, {
                    'CFOP': cfop_item,
                    'MAPPING_ID': mapping_id,
                    'MAPPING_IDS': [mapping_id],
                    'CHAVE': chave,
                    'ATIVO': ativo
                })
        
        return {
            'id': mapping_id,
            'chave': chave,
            'descricao': descricao,
            'cfop': cfop,
            'operacao': operacao,
            'regra': regra,
            'observacao': observacao,
            'pedido_compra': pedido_compra,
            'ativo': ativo
        }
    
    def update(self, mapping_id: str, chave: str = None, descricao: str = None, cfop: str = None, 
              operacao: str = None, regra: str = None, observacao: str = None, pedido_compra: bool = None, ativo: bool = None) -> Dict[str, Any]:
        """Atualiza regra existente"""
        sk = f"MAPPING#{mapping_id}"
        item = self.repository.get_item(self.pk, sk)
        
        if not item:
            raise ValueError(f"Regra {mapping_id} não encontrada")
        
        # Se Chave está sendo alterada, verificar se não existe outra com mesma chave
        if chave and chave != item.get('CHAVE', ''):
            items = self.repository.query_by_pk_and_sk_prefix(self.pk, "MAPPING#")
            for existing_item in items:
                if existing_item.get('CHAVE', '') == chave and existing_item.get('SK', '') != sk:
                    raise ValueError(f"Chave {chave} já existe em outra regra")
        
        # Se CFOP está sendo alterado, precisamos atualizar os registros individuais
        old_cfop_list = item.get('CFOP_LIST', [])
        if cfop is not None:
            # Separar novos CFOPs
            new_cfop_list = [c.strip() for c in cfop.split() if c.strip()] if cfop else []
            
            # Remover registros CFOP#{cfop} que não estão mais na lista
            cfops_to_remove = set(old_cfop_list) - set(new_cfop_list)
            for cfop_to_remove in cfops_to_remove:
                cfop_sk = f"CFOP#{cfop_to_remove}"
                cfop_item = self.repository.get_item(self.pk, cfop_sk)
                if cfop_item:
                    # Remover mapping_id da lista
                    mapping_ids = cfop_item.get('MAPPING_IDS', [])
                    if mapping_id in mapping_ids:
                        mapping_ids.remove(mapping_id)
                        if mapping_ids:
                            # Atualizar com nova lista e novo principal
                            new_principal = mapping_ids[0]
                            self.repository.put_item(self.pk, cfop_sk, {
                                'CFOP': cfop_to_remove,
                                'MAPPING_ID': new_principal,
                                'MAPPING_IDS': mapping_ids,
                                'CHAVE': cfop_item.get('CHAVE', ''),
                                'ATIVO': cfop_item.get('ATIVO', True)
                            })
                        else:
                            # Se não há mais mapping_ids, deletar registro
                            self.repository.delete_item(self.pk, cfop_sk)
            
            # Adicionar novos registros CFOP#{cfop}
            cfops_to_add = set(new_cfop_list) - set(old_cfop_list)
            for cfop_to_add in cfops_to_add:
                cfop_sk = f"CFOP#{cfop_to_add}"
                existing = self.repository.get_item(self.pk, cfop_sk)
                if existing:
                    # Adicionar mapping_id à lista
                    mapping_ids = existing.get('MAPPING_IDS', [])
                    if mapping_id not in mapping_ids:
                        mapping_ids.append(mapping_id)
                    self.repository.put_item(self.pk, cfop_sk, {
                        'CFOP': cfop_to_add,
                        'MAPPING_ID': mapping_id,  # Atualizar principal
                        'MAPPING_IDS': mapping_ids,
                        'CHAVE': chave or item.get('CHAVE', ''),
                        'ATIVO': ativo if ativo is not None else existing.get('ATIVO', True)
                    })
                else:
                    # Criar novo registro
                    self.repository.put_item(self.pk, cfop_sk, {
                        'CFOP': cfop_to_add,
                        'MAPPING_ID': mapping_id,
                        'MAPPING_IDS': [mapping_id],
                        'CHAVE': chave or item.get('CHAVE', ''),
                        'ATIVO': ativo if ativo is not None else item.get('ATIVO', True)
                    })
        
        # Preparar atualizações
        updates = {}
        if chave is not None:
            updates['CHAVE'] = chave
        if descricao is not None:
            updates['DESCRICAO'] = descricao
        if cfop is not None:
            updates['CFOP'] = cfop
            # Atualizar CFOP_LIST também
            new_cfop_list = [c.strip() for c in cfop.split() if c.strip()] if cfop else []
            updates['CFOP_LIST'] = new_cfop_list
        if operacao is not None:
            updates['OPERACAO'] = operacao
        if regra is not None:
            updates['REGRA'] = regra
        if observacao is not None:
            updates['OBSERVACAO'] = observacao
        if pedido_compra is not None:
            updates['PEDIDO_COMPRA'] = pedido_compra
        if ativo is not None:
            updates['ATIVO'] = ativo
            # Atualizar também nos registros CFOP individuais
            if cfop is not None:
                new_cfop_list = [c.strip() for c in cfop.split() if c.strip()] if cfop else []
            else:
                new_cfop_list = item.get('CFOP_LIST', [])
            for cfop_item in new_cfop_list:
                cfop_sk = f"CFOP#{cfop_item}"
                cfop_record = self.repository.get_item(self.pk, cfop_sk)
                if cfop_record:
                    self.repository.put_item(self.pk, cfop_sk, {
                        'CFOP': cfop_item,
                        'MAPPING_ID': cfop_record.get('MAPPING_ID', mapping_id),
                        'MAPPING_IDS': cfop_record.get('MAPPING_IDS', [mapping_id]),
                        'CHAVE': chave or cfop_record.get('CHAVE', ''),
                        'ATIVO': ativo
                    })
        
        if updates:
            self.repository.update_item(self.pk, sk, updates)
        
        # Retornar item atualizado
        updated_item = self.repository.get_item(self.pk, sk)
        return {
            'id': mapping_id,
            'chave': updated_item.get('CHAVE', ''),
            'descricao': updated_item.get('DESCRICAO', ''),
            'cfop': updated_item.get('CFOP', ''),
            'operacao': updated_item.get('OPERACAO', ''),
            'regra': updated_item.get('REGRA', ''),
            'observacao': updated_item.get('OBSERVACAO', ''),
            'pedido_compra': updated_item.get('PEDIDO_COMPRA', False),
            'ativo': updated_item.get('ATIVO', True)
        }
    
    def delete(self, mapping_id: str) -> Dict[str, Any]:
        """Remove regra e seus registros CFOP individuais"""
        sk = f"MAPPING#{mapping_id}"
        item = self.repository.get_item(self.pk, sk)
        
        if not item:
            raise ValueError(f"Regra {mapping_id} não encontrada")
        
        # Remover registros CFOP individuais
        cfop_list = item.get('CFOP_LIST', [])
        for cfop_item in cfop_list:
            cfop_sk = f"CFOP#{cfop_item}"
            cfop_record = self.repository.get_item(self.pk, cfop_sk)
            if cfop_record:
                mapping_ids = cfop_record.get('MAPPING_IDS', [])
                if mapping_id in mapping_ids:
                    mapping_ids.remove(mapping_id)
                    if mapping_ids:
                        # Atualizar com nova lista e novo principal
                        new_principal = mapping_ids[0]
                        # Buscar chave do novo principal
                        new_principal_item = self.repository.get_item(self.pk, f"MAPPING#{new_principal}")
                        new_chave = new_principal_item.get('CHAVE', '') if new_principal_item else ''
                        self.repository.put_item(self.pk, cfop_sk, {
                            'CFOP': cfop_item,
                            'MAPPING_ID': new_principal,
                            'MAPPING_IDS': mapping_ids,
                            'CHAVE': new_chave,
                            'ATIVO': cfop_record.get('ATIVO', True)
                        })
                    else:
                        # Se não há mais mapping_ids, deletar registro
                        self.repository.delete_item(self.pk, cfop_sk)
        
        # Remover registro principal
        self.repository.delete_item(self.pk, sk)
        
        return {
            'id': mapping_id,
            'message': 'Regra removida com sucesso'
        }

