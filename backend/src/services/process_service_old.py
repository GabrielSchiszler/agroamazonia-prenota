import os
import json
import boto3
import uuid
import logging
from datetime import datetime
from typing import Dict, Any
from src.repositories.dynamodb_repository import DynamoDBRepository

logger = logging.getLogger(__name__)

class ProcessService:
    def __init__(self):
        logger.info("Initializing ProcessService")
        self.repository = DynamoDBRepository()
        self.s3_client = boto3.client('s3')
        self.sfn_client = boto3.client('stepfunctions')
        self.bucket_name = os.environ['BUCKET_NAME']
        self.state_machine_arn = os.environ['STATE_MACHINE_ARN']
        logger.info(f"Bucket: {self.bucket_name}, StateMachine: {self.state_machine_arn}")
    
    def create_process(self, process_type: str) -> Dict[str, Any]:
        """Cria novo processo"""
        logger.info(f"Creating process with type: {process_type}")
        process_id = str(uuid.uuid4())
        timestamp = int(datetime.now().timestamp())
        
        pk = f"PROCESS={process_id}"
        sk = f"METADATA={timestamp}"
        
        logger.info(f"Saving to DynamoDB: PK={pk}, SK={sk}")
        self.repository.put_item(pk, sk, {
            'STATUS': 'CREATED',
            'PROCESS_TYPE': process_type,
            'TIMESTAMP': timestamp
        })
        
        # Adicionar à lista de processos
        list_pk = 'PROCESS_LIST'
        list_sk = f'PROCESS#{timestamp}#{process_id}'
        self.repository.put_item(list_pk, list_sk, {
            'PROCESS_ID': process_id,
            'TIMESTAMP': timestamp
        })
        
        logger.info(f"Process created successfully: {process_id}")
        return {
            'process_id': process_id,
            'process_type': process_type,
            'status': 'CREATED'
        }
    
    def generate_presigned_url(self, process_id: str, file_name: str, file_type: str, doc_type: str = 'ADDITIONAL', process_type: str = None) -> Dict[str, Any]:
        """Gera URL assinada para upload"""
        import re
        
        # Criar processo se não existir
        pk = f"PROCESS={process_id}"
        items = self.repository.query_by_pk(pk)
        metadata = next((item for item in items if 'METADATA=' in item['SK']), None)
        
        if not metadata:
            # Criar processo automaticamente sem tipo
            timestamp = int(datetime.now().timestamp())
            sk = f"METADATA={timestamp}"
            self.repository.put_item(pk, sk, {
                'STATUS': 'CREATED',
                'TIMESTAMP': timestamp
            })
            
            list_pk = 'PROCESS_LIST'
            list_sk = f'PROCESS#{timestamp}#{process_id}'
            self.repository.put_item(list_pk, list_sk, {
                'PROCESS_ID': process_id,
                'TIMESTAMP': timestamp
            })
            logger.info(f"Process {process_id} created automatically without type")
        
        safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
        
        # Determinar pasta baseado no tipo
        if doc_type == 'DANFE':
            file_key = f"processes/{process_id}/danfe/{safe_name}"
        else:
            file_key = f"processes/{process_id}/docs/{safe_name}"
        
        url = self.s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': file_key,
                'ContentType': file_type
            },
            ExpiresIn=3600
        )
        
        # Registrar arquivo com status PENDING
        timestamp = int(datetime.now().timestamp())
        sk = f"FILE={doc_type}={safe_name}"
        
        self.repository.put_item(pk, sk, {
            'FILE_NAME': safe_name,
            'FILE_KEY': file_key,
            'DOC_TYPE': doc_type,
            'STATUS': 'PENDING',
            'TIMESTAMP': timestamp
        })
        
        return {
            'upload_url': url,
            'file_key': file_key,
            'file_name': safe_name,
            'doc_type': doc_type
        }
    
    def start_process(self, process_id: str, process_type: str = None) -> Dict[str, Any]:
        """Inicia processamento com Step Functions"""
        logger.info(f"Starting process: {process_id}")
        pk = f"PROCESS={process_id}"
        items = self.repository.query_by_pk(pk)
        logger.info(f"Found {len(items)} items for process")
        
        if not items:
            raise ValueError(f"Processo {process_id} não encontrado")
        
        # Buscar metadados do processo
        metadata = next((item for item in items if 'METADATA=' in item['SK']), None)
        logger.info(f"Metadata: {metadata}")
        
        if not metadata:
            raise ValueError("Metadados do processo não encontrados")
        
        # HARDCODED: Sempre usar AGROQUIMICOS
        process_type = 'AGROQUIMICOS'
        self.repository.update_item(pk, metadata['SK'], {'PROCESS_TYPE': process_type})
        logger.info(f"Process type hardcoded to {process_type}")
        
        logger.info(f"Process type: {process_type}")
        
        # Buscar arquivos
        files = [item for item in items if 'FILE=' in item['SK']]
        danfe_files = [f for f in files if f.get('DOC_TYPE') == 'DANFE']
        additional_files = [f for f in files if f.get('DOC_TYPE') == 'ADDITIONAL']
        
        # Validar arquivos obrigatórios
        if not danfe_files:
            raise ValueError("DANFE obrigatório não encontrado")
        
        if not additional_files:
            raise ValueError("Pelo menos um documento adicional é necessário")
        
        # Apenas PDFs para Textract
        pdf_files = [f for f in files if f.get('FILE_NAME', '').lower().endswith('.pdf')]
        logger.info(f"Found {len(files)} files, {len(pdf_files)} PDFs for Textract")
        
        input_data = {
            'process_id': process_id,
            'process_type': process_type,
            'files': [{'FILE_NAME': f.get('FILE_NAME'), 'FILE_KEY': f.get('FILE_KEY'), 'STATUS': f.get('STATUS')} for f in pdf_files]
        }
        logger.info(f"Step Functions input: {json.dumps(input_data)}")
        
        try:
            response = self.sfn_client.start_execution(
                stateMachineArn=self.state_machine_arn,
                input=json.dumps(input_data)
            )
            logger.info(f"Step Functions execution started: {response['executionArn']}")
        except Exception as e:
            logger.error(f"Failed to start Step Functions: {str(e)}")
            raise
        
        # Atualizar status
        sk = metadata['SK']
        logger.info(f"Updating status to PROCESSING for SK: {sk}")
        self.repository.update_item(pk, sk, {'STATUS': 'PROCESSING'})
        
        return {
            'execution_arn': response['executionArn'],
            'process_id': process_id,
            'status': 'PROCESSING'
        }
    
    def update_file_status(self, process_id: str, file_name: str, status: str) -> Dict[str, Any]:
        """Atualiza status do arquivo"""
        pk = f"PROCESS={process_id}"
        items = self.repository.query_by_pk(pk)
        
        # Encontrar arquivo pelo nome
        for item in items:
            if 'FILE=' in item['SK'] and file_name in item['SK']:
                self.repository.update_item(pk, item['SK'], {'STATUS': status})
                return {'status': 'updated'}
        
        return {'status': 'not_found'}
    
    def get_process(self, process_id: str) -> Dict[str, Any]:
        """Busca detalhes do processo"""
        pk = f"PROCESS={process_id}"
        logger.info(f"Querying DynamoDB with PK: {pk}")
        items = self.repository.query_by_pk(pk)
        logger.info(f"Query returned {len(items)} items")
        
        if not items:
            raise ValueError(f"Processo {process_id} não encontrado")
        
        metadata = next((item for item in items if 'METADATA=' in item['SK']), None)
        logger.info(f"Metadata found: {metadata}")
        if not metadata:
            raise ValueError(f"Metadados do processo {process_id} não encontrados")
        
        files = [item for item in items if 'FILE=' in item['SK']]
        
        danfe_files = [f for f in files if f.get('DOC_TYPE') == 'DANFE']
        additional_files = [f for f in files if f.get('DOC_TYPE') == 'ADDITIONAL']
        
        files_data = {
            'danfe': [{
                'file_name': f.get('FILE_NAME'),
                'file_key': f.get('FILE_KEY'),
                'status': f.get('STATUS', 'UNKNOWN')
            } for f in danfe_files],
            'additional': [{
                'file_name': f.get('FILE_NAME'),
                'file_key': f.get('FILE_KEY'),
                'status': f.get('STATUS', 'UNKNOWN')
            } for f in additional_files]
        }
        
        timestamp = metadata.get('TIMESTAMP')
        created_at = str(int(timestamp)) if timestamp else '0'
        
        return {
            'process_id': process_id,
            'process_type': metadata.get('PROCESS_TYPE'),
            'status': metadata.get('STATUS'),
            'files': files_data,
            'created_at': created_at
        }
    
    def list_processes(self) -> list:
        """Lista todos os processos"""
        try:
            # Query lista de processos
            list_items = self.repository.query_by_pk_and_sk_prefix('PROCESS_LIST', 'PROCESS#')
            
            # Ordenar por timestamp (mais recente primeiro)
            list_items.sort(key=lambda x: x.get('TIMESTAMP', 0), reverse=True)
            
            # Buscar metadados de cada processo
            for list_item in list_items:
                process_id = list_item.get('PROCESS_ID')
                if not process_id:
                    continue
                
                pk = f'PROCESS={process_id}'
                items = self.repository.query_by_pk_and_sk_prefix(pk, 'METADATA=')
                
                if items:
                    metadata = items[0]
                    timestamp = metadata.get('TIMESTAMP')
                    processes.append({
                        'process_id': process_id,
                        'process_type': metadata.get('PROCESS_TYPE'),
                        'status': metadata.get('STATUS'),
                        'created_at': str(int(timestamp)) if timestamp else '0'
                    })
            return processes
        except Exception as e:
            logger.error(f"Error listing processes: {e}")
            return []
    
    def get_validation_results(self, process_id: str) -> list:
        """Busca resultados das validações"""
        pk = f"PROCESS={process_id}"
        items = self.repository.query_by_pk_and_sk_prefix(pk, 'VALIDATION=')
        
        if not items:
            return []
        
        latest = max(items, key=lambda x: x.get('TIMESTAMP', 0))
        validation_data = latest.get('VALIDATION_RESULTS')
        
        if not validation_data:
            return []
        
        results = json.loads(validation_data)
        
        # Transformar resultados mantendo estrutura detalhada
        formatted = []
        for result in results:
            rule = result.get('rule')
            formatted_result = {
                'type': rule,
                'danfe_value': result.get('danfe_value'),
                'status': result.get('status'),
                'message': result.get('message'),
                'docs': []
            }
            
            if 'comparisons' in result:
                for comp in result['comparisons']:
                    doc_entry = {
                        'file_name': comp.get('doc_file'),
                        'status': comp.get('status')
                    }
                    # Preservar items detalhados se existir
                    if 'items' in comp:
                        doc_entry['items'] = comp['items']
                    else:
                        doc_entry['value'] = comp.get('doc_value')
                    
                    formatted_result['docs'].append(doc_entry)
            
            formatted.append(formatted_result)
        
        return formatted
    

