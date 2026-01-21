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
        self.repository = DynamoDBRepository()
        self.s3_client = boto3.client('s3')
        self.sfn_client = boto3.client('stepfunctions')
        self.bucket_name = os.environ['BUCKET_NAME']
        self.state_machine_arn = os.environ['STATE_MACHINE_ARN']
    
    def create_process(self, process_type: str) -> Dict[str, Any]:
        process_id = str(uuid.uuid4())
        timestamp = int(datetime.now().timestamp())
        
        # Entrada na lista de processos
        self.repository.put_item('PROCESS', f'PROCESS#{process_id}', {
            'PROCESS_ID': process_id,
            'TIMESTAMP': timestamp
        })
        
        # Metadados do processo
        self.repository.put_item(f'PROCESS#{process_id}', 'METADATA', {
            'STATUS': 'CREATED',
            'PROCESS_TYPE': process_type,
            'TIMESTAMP': timestamp
        })
        
        return {'process_id': process_id, 'process_type': process_type, 'status': 'CREATED'}
    
    def generate_presigned_url(self, process_id: str, file_name: str, file_type: str, doc_type: str = 'ADDITIONAL', metadados: Dict[str, Any] = None) -> Dict[str, Any]:
        import re
        
        # Criar processo se não existir
        pk = f'PROCESS#{process_id}'
        items = self.repository.query_by_pk_and_sk_prefix(pk, 'METADATA')
        
        if not items:
            timestamp = int(datetime.now().timestamp())
            self.repository.put_item('PROCESS', f'PROCESS#{process_id}', {
                'PROCESS_ID': process_id,
                'TIMESTAMP': timestamp
            })
            self.repository.put_item(pk, 'METADATA', {
                'STATUS': 'CREATED',
                'TIMESTAMP': timestamp
            })
        
        safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
        file_key = f"processes/{process_id}/{'danfe' if doc_type == 'DANFE' else 'docs'}/{safe_name}"
        
        url = self.s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': self.bucket_name, 'Key': file_key, 'ContentType': file_type},
            ExpiresIn=3600
        )
        
        # Preparar dados do arquivo
        file_data = {
            'FILE_NAME': safe_name,
            'FILE_KEY': file_key,
            'DOC_TYPE': doc_type,
            'STATUS': 'PENDING'
        }
        
        # Adicionar metadados se fornecidos
        if metadados:
            file_data['METADADOS'] = json.dumps(metadados)
        
        self.repository.put_item(pk, f'FILE#{safe_name}', file_data)
        
        return {'upload_url': url, 'file_key': file_key, 'file_name': safe_name, 'content_type': file_type, 'doc_type': doc_type}
    
    def link_pedido_compra_metadata(self, process_id: str, metadados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Vincula metadados do pedido de compra ao processo (sem arquivo físico).
        
        Cria um registro no DynamoDB com os metadados usando SK diferente de FILE#
        para que não apareça na listagem de arquivos. O Lambda send_to_protheus
        lê esses metadados durante o processamento.
        """
        try:
            # Validar metadados
            if not metadados:
                raise ValueError("Metadados não podem ser vazios")
            
            if not isinstance(metadados, dict):
                raise ValueError("Metadados devem ser um objeto JSON (dict)")
            
            # Criar processo se não existir
            pk = f'PROCESS#{process_id}'
            items = self.repository.query_by_pk_and_sk_prefix(pk, 'METADATA')
            
            if not items:
                timestamp = int(datetime.now().timestamp())
                self.repository.put_item('PROCESS', f'PROCESS#{process_id}', {
                    'PROCESS_ID': process_id,
                    'TIMESTAMP': timestamp
                })
                self.repository.put_item(pk, 'METADATA', {
                    'STATUS': 'CREATED',
                    'TIMESTAMP': timestamp
                })
            
            # Usar SK específica para metadados de pedido de compra (não FILE#)
            # Isso evita que apareça na listagem de arquivos
            sk = 'PEDIDO_COMPRA_METADATA'
            
            # Preparar dados dos metadados
            # Se metadados já for string JSON, usar diretamente; senão, serializar
            if isinstance(metadados, str):
                # Validar se é JSON válido
                try:
                    json.loads(metadados)
                    metadata_json_str = metadados
                except json.JSONDecodeError:
                    raise ValueError("Metadados em formato string não é um JSON válido")
            else:
                metadata_json_str = json.dumps(metadados)
            
            metadata_data = {
                'METADADOS': metadata_json_str,
                'TIMESTAMP': int(datetime.now().timestamp())
            }
            
            # Salvar no DynamoDB com SK diferente de FILE#
            self.repository.put_item(pk, sk, metadata_data)
            
            logger.info(f"Metadados do pedido de compra vinculados ao processo {process_id}")
            
            return {
                'success': True,
                'message': 'Metadados do pedido de compra vinculados com sucesso',
                'process_id': process_id,
                'metadados': metadados if isinstance(metadados, dict) else json.loads(metadata_json_str)
            }
        except ValueError as e:
            logger.error(f"Erro de validação ao vincular metadados: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Erro ao vincular metadados do pedido de compra: {str(e)}")
            logger.exception("Traceback completo:")
            raise
    
    def start_process(self, process_id: str) -> Dict[str, Any]:
        pk = f'PROCESS#{process_id}'
        items = self.repository.query_by_pk(pk)
        
        if not items:
            raise ValueError(f"Processo {process_id} não encontrado")
        
        metadata = next((item for item in items if item['SK'] == 'METADATA'), None)
        if not metadata:
            raise ValueError("Metadados do processo não encontrados")
        
        # HARDCODED: AGROQUIMICOS
        process_type = 'AGROQUIMICOS'
        self.repository.update_item(pk, 'METADATA', {'PROCESS_TYPE': process_type})
        
        files = [item for item in items if item['SK'].startswith('FILE#')]
        danfe_files = [f for f in files if f.get('DOC_TYPE') == 'DANFE']
        
        # Verificar se há metadados de pedido de compra (obrigatório)
        has_pedido_compra_metadata = any(item.get('SK') == 'PEDIDO_COMPRA_METADATA' for item in items)
        
        if not danfe_files:
            raise ValueError("DANFE obrigatório não encontrado")
        
        # Metadados do pedido de compra são obrigatórios (não precisa mais de arquivos adicionais)
        if not has_pedido_compra_metadata:
            raise ValueError("Metadados do pedido de compra são obrigatórios")
        
        # Não precisa mais passar arquivos para o Step Functions - apenas metadados JSON
        input_data = {
            'process_id': process_id,
            'process_type': process_type,
            'files': []  # Não precisa mais de arquivos adicionais
        }
        
        response = self.sfn_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input=json.dumps(input_data)
        )
        
        self.repository.update_item(pk, 'METADATA', {'STATUS': 'PROCESSING'})
        
        return {'execution_arn': response['executionArn'], 'process_id': process_id, 'status': 'PROCESSING'}
    
    def get_process(self, process_id: str) -> Dict[str, Any]:
        pk = f'PROCESS#{process_id}'
        items = self.repository.query_by_pk(pk)
        
        if not items:
            raise ValueError(f"Processo {process_id} não encontrado")
        
        metadata = next((item for item in items if item['SK'] == 'METADATA'), None)
        if not metadata:
            raise ValueError(f"Metadados do processo {process_id} não encontrados")
        
        files = [item for item in items if item['SK'].startswith('FILE#')]
        danfe_files = [f for f in files if f.get('DOC_TYPE') == 'DANFE']
        # Filtrar apenas arquivos adicionais que têm FILE_KEY (arquivos físicos)
        # Metadados apenas (sem arquivo) não devem aparecer na listagem
        additional_files = [f for f in files if f.get('DOC_TYPE') == 'ADDITIONAL' and f.get('FILE_KEY')]
        
        # Buscar resultados de parsing (XML e OCR)
        parsing_results = []
        
        logger.info(f"Total items for process: {len(items)}")
        logger.info(f"SK values: {[item.get('SK') for item in items]}")
        
        # XML parsing
        xml_items = [item for item in items if item.get('SK', '').startswith('PARSED_XML')]
        logger.info(f"Found {len(xml_items)} XML items")
        for item in xml_items:
            logger.info(f"XML item keys: {item.keys()}")
            if item.get('PARSED_DATA'):
                try:
                    parsed_data = json.loads(item['PARSED_DATA'])
                    parsing_results.append({
                        'source': 'XML',
                        'file_name': item.get('FILE_NAME', 'DANFE'),
                        'parsed_data': parsed_data
                    })
                    logger.info(f"Added XML parsing result")
                except Exception as e:
                    logger.error(f"Error parsing XML data: {e}")
        
        # OCR parsing
        ocr_items = [item for item in items if item.get('SK', '').startswith('PARSED_OCR')]
        logger.info(f"Found {len(ocr_items)} OCR items")
        for item in ocr_items:
            logger.info(f"OCR item keys: {item.keys()}")
            if item.get('PARSED_DATA'):
                try:
                    parsed_data = json.loads(item['PARSED_DATA'])
                    parsing_results.append({
                        'source': 'OCR',
                        'file_name': item.get('FILE_NAME', 'OCR Document'),
                        'parsed_data': parsed_data
                    })
                    logger.info(f"Added OCR parsing result")
                except Exception as e:
                    logger.error(f"Error parsing OCR data: {e}")
        
        logger.info(f"Total parsing_results: {len(parsing_results)}")
        
        # Converter sctask_id de Decimal para string se necessário
        sctask_id = metadata.get('sctask_id')
        if sctask_id is not None:
            sctask_id = str(sctask_id)
        
        # Função para processar arquivos com metadados
        def process_file_data(file_item):
            file_data = {
                'file_name': file_item.get('FILE_NAME'),
                'status': file_item.get('STATUS', 'UNKNOWN')
            }
            
            # Adicionar file_key apenas se existir (arquivos físicos)
            if file_item.get('FILE_KEY'):
                file_data['file_key'] = file_item.get('FILE_KEY')
            
            # Adicionar flag metadata_only se existir
            if file_item.get('METADATA_ONLY'):
                file_data['metadata_only'] = True
            
            # Adicionar metadados se existirem
            if file_item.get('METADADOS'):
                try:
                    file_data['metadados'] = json.loads(file_item['METADADOS'])
                except Exception as e:
                    logger.error(f"Erro ao parsear metadados: {e}")
                    file_data['metadados'] = {}
            
            return file_data
        
        # Buscar metadados do pedido de compra (sem arquivo físico)
        pedido_compra_metadata_item = next((item for item in items if item.get('SK') == 'PEDIDO_COMPRA_METADATA'), None)
        pedido_compra_metadata = None
        if pedido_compra_metadata_item and pedido_compra_metadata_item.get('METADADOS'):
            try:
                metadados_str = pedido_compra_metadata_item.get('METADADOS')
                if isinstance(metadados_str, str):
                    pedido_compra_metadata = json.loads(metadados_str)
                else:
                    pedido_compra_metadata = metadados_str
            except Exception as e:
                logger.error(f"Erro ao parsear metadados do pedido de compra: {e}")
        
        # Adicionar metadados do pedido de compra como um "arquivo virtual" na lista de adicionais
        additional_files_list = [process_file_data(f) for f in additional_files]
        if pedido_compra_metadata:
            # Adicionar como um item virtual na lista de arquivos adicionais
            additional_files_list.append({
                'file_name': 'Metadados do Pedido de Compra',
                'status': 'LINKED',
                'metadata_only': True,
                'metadados': pedido_compra_metadata
            })
        
        result = {
            'process_id': process_id,
            'process_type': metadata.get('PROCESS_TYPE'),
            'status': metadata.get('STATUS'),
            'sctask_id': sctask_id,
            'files': {
                'danfe': [process_file_data(f) for f in danfe_files],
                'additional': additional_files_list
            },
            'parsing_results': parsing_results,
            'created_at': str(int(metadata.get('TIMESTAMP', 0)))
        }
        
        logger.info(f"Returning result with {len(result.get('parsing_results', []))} parsing_results")
        return result
    
    def list_processes(self) -> list:
        try:
            items = self.repository.query_by_pk_and_sk_prefix('PROCESS', 'PROCESS#')
            items.sort(key=lambda x: x.get('TIMESTAMP', 0), reverse=True)
            
            processes = []
            for item in items:
                process_id = item.get('PROCESS_ID')
                if not process_id:
                    continue
                
                pk = f'PROCESS#{process_id}'
                metadata_items = self.repository.query_by_pk_and_sk_prefix(pk, 'METADATA')
                
                if metadata_items:
                    metadata = metadata_items[0]
                    processes.append({
                        'process_id': process_id,
                        'process_type': metadata.get('PROCESS_TYPE'),
                        'status': metadata.get('STATUS'),
                        'created_at': str(int(metadata.get('TIMESTAMP', 0)))
                    })
            
            return processes
        except Exception as e:
            logger.error(f"Error listing processes: {e}")
            return []
    
    def get_validation_results(self, process_id: str) -> list:
        pk = f'PROCESS#{process_id}'
        logger.info(f"Querying validations with PK={pk}, SK prefix=VALIDATION")
        items = self.repository.query_by_pk_and_sk_prefix(pk, 'VALIDATION')
        logger.info(f"Found {len(items)} validation items")
        
        if not items:
            logger.warning(f"No validation results found for process {process_id}")
            return []
        
        latest = max(items, key=lambda x: x.get('TIMESTAMP', 0))
        validation_data = latest.get('VALIDATION_RESULTS')
        
        if not validation_data:
            return []
        
        results = json.loads(validation_data)
        
        formatted = []
        for result in results:
            formatted_result = {
                'type': result.get('rule'),
                'danfe_value': result.get('danfe_value'),
                'status': result.get('status'),
                'message': result.get('message'),
                'docs': []
            }
            
            if 'comparisons' in result:
                for comp in result['comparisons']:
                    doc_entry = {'file_name': comp.get('doc_file'), 'status': comp.get('status')}
                    if 'items' in comp:
                        doc_entry['items'] = comp['items']
                    else:
                        doc_entry['value'] = comp.get('doc_value')
                    formatted_result['docs'].append(doc_entry)
            
            formatted.append(formatted_result)
        
        return formatted
    
    def generate_download_url(self, file_key: str) -> str:
        """Gera URL de download para arquivo no S3"""
        url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket_name, 'Key': file_key},
            ExpiresIn=3600
        )
        return url

    def update_file_metadata(self, process_id: str, file_name: str, metadados: Dict[str, Any]) -> Dict[str, Any]:
        """Atualiza metadados JSON de um arquivo"""
        import re
        
        pk = f'PROCESS#{process_id}'
        safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
        sk = f'FILE#{safe_name}'
        
        # Verificar se o arquivo existe
        items = self.repository.query_by_pk(pk)
        file_item = next((item for item in items if item['SK'] == sk), None)
        
        if not file_item:
            raise ValueError(f"Arquivo {file_name} não encontrado no processo {process_id}")
        
        # Atualizar metadados
        self.repository.update_item(pk, sk, {
            'METADADOS': json.dumps(metadados)
        })
        
        logger.info(f"Metadados atualizados para arquivo {file_name} no processo {process_id}")
        
        return {
            'success': True,
            'message': 'Metadados atualizados com sucesso',
            'file_name': safe_name,
            'metadados': metadados
        }