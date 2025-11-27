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
        
        logger.info(f"Process created successfully: {process_id}")
        return {
            'process_id': process_id,
            'process_type': process_type,
            'status': 'CREATED'
        }
    
    def generate_presigned_url(self, process_id: str, file_name: str, file_type: str) -> Dict[str, Any]:
        """Gera URL assinada para upload"""
        import re
        safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
        file_key = f"processes/{process_id}/{safe_name}"
        
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
        pk = f"PROCESS={process_id}"
        sk = f"FILE={safe_name}"
        
        self.repository.put_item(pk, sk, {
            'FILE_NAME': safe_name,
            'FILE_KEY': file_key,
            'STATUS': 'PENDING'
        })
        
        return {
            'upload_url': url,
            'file_key': file_key,
            'file_name': safe_name
        }
    
    def start_process(self, process_id: str) -> Dict[str, Any]:
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
        
        process_type = metadata.get('PROCESS_TYPE')
        logger.info(f"Process type: {process_type}")
        
        # Buscar arquivos
        files = [item for item in items if 'FILE=' in item['SK']]
        logger.info(f"Found {len(files)} files")
        
        input_data = {
            'process_id': process_id,
            'process_type': process_type,
            'files': [{'FILE_NAME': f.get('FILE_NAME'), 'FILE_KEY': f.get('FILE_KEY'), 'STATUS': f.get('STATUS')} for f in files]
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
        
        files_data = [{
            'file_name': f.get('FILE_NAME'),
            'file_key': f.get('FILE_KEY'),
            'status': f.get('STATUS', 'UNKNOWN')
        } for f in files]
        
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
        # Usar scan limitado para desenvolvimento
        # Em produção, usar GSI ou índice secundário
        try:
            response = self.repository.table.scan(
                Limit=50,
                FilterExpression='begins_with(SK, :sk_prefix)',
                ExpressionAttributeValues={':sk_prefix': 'METADATA='}
            )
            
            processes = []
            for item in response.get('Items', []):
                process_id = item['PK'].replace('PROCESS=', '')
                timestamp = item.get('TIMESTAMP')
                processes.append({
                    'process_id': process_id,
                    'process_type': item.get('PROCESS_TYPE'),
                    'status': item.get('STATUS'),
                    'created_at': str(int(timestamp)) if timestamp else '0'
                })
            
            return processes
        except Exception as e:
            logger.error(f"Error listing processes: {e}")
            return []
    
    def get_textract_results(self, process_id: str) -> list:
        """Busca resultados do Textract"""
        pk = f"PROCESS={process_id}"
        items = self.repository.query_by_pk_and_sk_prefix(pk, 'TEXTRACT=')
        
        results = []
        for item in items:
            tables_data = item.get('TABLES_DATA')
            tables = json.loads(tables_data) if tables_data else []
            
            results.append({
                'file_name': item.get('FILE_NAME'),
                'file_key': item.get('FILE_KEY'),
                'job_id': item.get('JOB_ID'),
                'table_count': item.get('TABLE_COUNT', 0),
                'tables': tables
            })
        
        return results
    

