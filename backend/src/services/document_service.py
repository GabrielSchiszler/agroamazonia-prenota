import os
import json
import boto3
from typing import Dict, Any, List
from src.repositories.dynamodb_repository import DynamoDBRepository
from src.models.domain import DocumentMetadata, DocumentRecord
from src.services.rules_service import RulesService

class DocumentService:
    def __init__(self):
        self.repository = DynamoDBRepository()
        self.sfn_client = boto3.client('stepfunctions')
        self.state_machine_arn = os.environ['STATE_MACHINE_ARN']
    
    def submit_document(self, document_id: str, document_type: str, 
                       process_type: str, s3_path: str) -> Dict[str, Any]:
        """Inicia processamento via Step Functions"""
        input_data = {
            'document_id': document_id,
            'document_type': document_type,
            'process_type': process_type,
            's3_path': s3_path
        }
        
        response = self.sfn_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input=json.dumps(input_data)
        )
        
        return {
            'execution_arn': response['executionArn'],
            'document_id': document_id,
            'status': 'PROCESSING'
        }
    
    def get_document(self, document_id: str) -> Dict[str, Any]:
        """Busca todos os dados de um documento"""
        pk = f"DOC_ID={document_id}"
        items = self.repository.query_by_pk(pk)
        
        if not items:
            raise ValueError(f"Documento {document_id} não encontrado")
        
        return {
            'document_id': document_id,
            'metadata': self._parse_items(items),
            'documents': items
        }
    
    def list_all_documents(self) -> List[Dict[str, Any]]:
        """Lista todos os documentos (scan limitado para desenvolvimento)"""
        # Em produção, usar GSI ou manter índice separado
        # Por ora, retorna lista vazia para evitar scan
        return []
    
    def get_pre_note(self, document_id: str) -> Dict[str, Any]:
        """Busca apenas dados da pré-nota"""
        pk = f"DOC_ID={document_id}"
        items = self.repository.query_by_pk_and_sk_prefix(pk, "METADATA=")
        
        pre_notes = [item for item in items if 'TYPE=PRE_NOTE' in item.get('SK', '')]
        
        if not pre_notes:
            raise ValueError(f"Pré-nota não encontrada para documento {document_id}")
        
        return {
            'document_id': document_id,
            'pre_note_data': self._parse_data_payload(pre_notes[0].get('DataPayload', ''))
        }
    
    def validate_document(self, document_id: str, process_type: str, 
                         data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida documento usando regras do processo"""
        return RulesService.validate(process_type, data)
    
    def _parse_items(self, items: list) -> Dict[str, Any]:
        """Converte lista de itens DDB em estrutura legível"""
        result = {}
        for item in items:
            sk = item.get('SK', '')
            payload = item.get('DataPayload', '')
            result[sk] = self._parse_data_payload(payload)
        return result
    
    def _parse_data_payload(self, payload: str) -> Dict[str, Any]:
        """Converte string CHAVE=VALOR,CHAVE=VALOR em dict"""
        if not payload:
            return {}
        
        result = {}
        for pair in payload.split(','):
            if '=' in pair:
                key, value = pair.split('=', 1)
                result[key] = value
        return result
