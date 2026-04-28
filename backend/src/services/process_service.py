import os
import json
import boto3
import uuid
import logging
from datetime import datetime
from typing import Dict, Any
from src.repositories.dynamodb_repository import DynamoDBRepository

logger = logging.getLogger(__name__)


def _truthy_pedido_flag(value: Any) -> bool:
    """True se o valor indica flag ligada — mesma regra que isCommodities (bool True ou string 'true')."""
    if value is True:
        return True
    if isinstance(value, str) and value.strip().lower() == "true":
        return True
    return False


def _get_pedido_field(pedido_compra: dict, request_body: dict, field: str) -> Any:
    """Ordem: requestBody → header → raiz do JSON do pedido (metadados ERP)."""
    header = pedido_compra.get("header")
    if not isinstance(header, dict):
        header = {}
    if field in request_body:
        return request_body[field]
    if field in header:
        return header[field]
    return pedido_compra.get(field)

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
        
        logger.info(f"[generate_presigned_url] Iniciando geração de URL pré-assinada")
        logger.info(f"[generate_presigned_url] Parâmetros recebidos:")
        logger.info(f"  - process_id: {process_id} (tipo: {type(process_id)}, length: {len(process_id) if process_id else 0})")
        logger.info(f"  - file_name: {file_name} (tipo: {type(file_name)}, length: {len(file_name) if file_name else 0})")
        logger.info(f"  - file_type: {file_type} (tipo: {type(file_type)})")
        logger.info(f"  - doc_type: {doc_type} (tipo: {type(doc_type)})")
        logger.info(f"  - metadados: {metadados} (tipo: {type(metadados)})")
        
        try:
            # Criar processo se não existir
            pk = f'PROCESS#{process_id}'
            logger.info(f"[generate_presigned_url] Verificando se processo existe: {pk}")
            items = self.repository.query_by_pk_and_sk_prefix(pk, 'METADATA')
            logger.info(f"[generate_presigned_url] Resultado da query: {len(items) if items else 0} itens encontrados")
            
            if not items:
                timestamp = int(datetime.now().timestamp())
                logger.info(f"[generate_presigned_url] Criando novo processo com process_id: {process_id}, timestamp: {timestamp}")
                try:
                    self.repository.put_item('PROCESS', f'PROCESS#{process_id}', {
                        'PROCESS_ID': process_id,
                        'TIMESTAMP': timestamp
                    })
                    logger.info(f"[generate_presigned_url] Item PROCESS criado com sucesso")
                    
                    self.repository.put_item(pk, 'METADATA', {
                        'STATUS': 'CREATED',
                        'TIMESTAMP': timestamp
                    })
                    logger.info(f"[generate_presigned_url] Item METADATA criado com sucesso")
                except Exception as e:
                    logger.error(f"[generate_presigned_url] Erro ao criar processo no DynamoDB: {str(e)}")
                    logger.exception("Traceback completo:")
                    raise
            else:
                logger.info(f"[generate_presigned_url] Processo já existe, não será criado novamente")
            
            logger.info(f"[generate_presigned_url] Processando nome do arquivo: {file_name}")
            safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
            logger.info(f"[generate_presigned_url] Nome seguro gerado: {safe_name}")
            
            file_key = f"processes/{process_id}/{'danfe' if doc_type == 'DANFE' else 'docs'}/{safe_name}"
            logger.info(f"[generate_presigned_url] file_key gerado: {file_key}")
            logger.info(f"[generate_presigned_url] bucket_name: {self.bucket_name}")
            
            logger.info(f"[generate_presigned_url] Gerando URL pré-assinada do S3...")
            try:
                url = self.s3_client.generate_presigned_url(
                    'put_object',
                    Params={'Bucket': self.bucket_name, 'Key': file_key, 'ContentType': file_type},
                    ExpiresIn=3600
                )
                logger.info(f"[generate_presigned_url] URL pré-assinada gerada com sucesso (length: {len(url) if url else 0})")
                logger.info(f"[generate_presigned_url] URL (primeiros 100 chars): {url[:100] if url else 'None'}...")
            except Exception as e:
                logger.error(f"[generate_presigned_url] Erro ao gerar URL pré-assinada do S3: {str(e)}")
                logger.exception("Traceback completo:")
                raise
            
            # Preparar dados do arquivo
            file_data = {
                'FILE_NAME': safe_name,
                'FILE_KEY': file_key,
                'DOC_TYPE': doc_type,
                'STATUS': 'PENDING'
            }
            
            # Adicionar metadados se fornecidos
            if metadados:
                logger.info(f"[generate_presigned_url] Adicionando metadados ao file_data")
                file_data['METADADOS'] = json.dumps(metadados)
                logger.info(f"[generate_presigned_url] Metadados serializados: {file_data['METADADOS']}")
            
            logger.info(f"[generate_presigned_url] Salvando file_data no DynamoDB: {file_data}")
            try:
                self.repository.put_item(pk, f'FILE#{safe_name}', file_data)
                logger.info(f"[generate_presigned_url] File data salvo no DynamoDB com sucesso")
            except Exception as e:
                logger.error(f"[generate_presigned_url] Erro ao salvar file_data no DynamoDB: {str(e)}")
                logger.exception("Traceback completo:")
                raise
            
            result = {
                'upload_url': url,
                'file_key': file_key,
                'file_name': safe_name,
                'content_type': file_type,
                'doc_type': doc_type
            }
            logger.info(f"[generate_presigned_url] Resultado final gerado: {result}")
            logger.info(f"[generate_presigned_url] Geração de URL pré-assinada concluída com sucesso")
            return result
            
        except Exception as e:
            logger.error(f"[generate_presigned_url] Erro durante geração de URL pré-assinada: {str(e)}")
            logger.error(f"[generate_presigned_url] Tipo do erro: {type(e).__name__}")
            logger.exception("[generate_presigned_url] Traceback completo:")
            raise
    
    def generate_presigned_urls_batch(
        self,
        process_id: str,
        files: list[Dict[str, str]],
    ) -> Dict[str, Any]:
        """Generate presigned URLs for multiple files in a single logical operation.

        Each entry in *files* must have keys: file_name, file_type, doc_type.
        Validates MIME types and per-process attachment limit before generating any URL.
        """
        import re
        from src.models.api import MAX_FILES_PER_PROCESS, ALLOWED_CONTENT_TYPES, infer_doc_type_and_folder

        if len(files) > MAX_FILES_PER_PROCESS:
            raise ValueError(
                f"Máximo de {MAX_FILES_PER_PROCESS} arquivos por processo; "
                f"recebidos: {len(files)}"
            )

        for f in files:
            if f["file_type"] not in ALLOWED_CONTENT_TYPES:
                raise ValueError(
                    f"Tipo {f['file_type']} não permitido para {f['file_name']}. "
                    f"Permitidos: {', '.join(sorted(ALLOWED_CONTENT_TYPES))}"
                )

        pk = f"PROCESS#{process_id}"
        items = self.repository.query_by_pk_and_sk_prefix(pk, "METADATA")
        if not items:
            timestamp = int(datetime.now().timestamp())
            self.repository.put_item("PROCESS", f"PROCESS#{process_id}", {
                "PROCESS_ID": process_id,
                "TIMESTAMP": timestamp,
            })
            self.repository.put_item(pk, "METADATA", {
                "STATUS": "CREATED",
                "TIMESTAMP": timestamp,
            })

        existing = self.repository.query_by_pk_and_sk_prefix(pk, "FILE#")
        existing_count = len(existing)
        if existing_count + len(files) > MAX_FILES_PER_PROCESS:
            raise ValueError(
                f"Processo já tem {existing_count} arquivo(s); "
                f"limite total é {MAX_FILES_PER_PROCESS}"
            )

        results: list[Dict[str, str]] = []
        for f in files:
            safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", f["file_name"])
            raw_dt = f.get("doc_type")
            if raw_dt is not None and str(raw_dt).strip() != "":
                doc_type = str(raw_dt).strip().upper()
                if doc_type not in ("DANFE", "ADDITIONAL"):
                    raise ValueError(
                        f"doc_type inválido para {f['file_name']}: {raw_dt!r}. "
                        "Use DANFE, ADDITIONAL ou omita para inferência automática."
                    )
                folder = "danfe" if doc_type == "DANFE" else "docs"
            else:
                doc_type, folder = infer_doc_type_and_folder(
                    f["file_name"], f["file_type"]
                )
            file_key = f"processes/{process_id}/{folder}/{safe_name}"

            url = self.s3_client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": file_key,
                    "ContentType": f["file_type"],
                },
                ExpiresIn=3600,
            )

            file_data = {
                "FILE_NAME": safe_name,
                "FILE_KEY": file_key,
                "DOC_TYPE": doc_type,
                "STATUS": "PENDING",
                "CONTENT_TYPE": f["file_type"],
            }
            self.repository.put_item(pk, f"FILE#{safe_name}", file_data)

            results.append({
                "file_name": safe_name,
                "upload_url": url,
                "file_key": file_key,
                "content_type": f["file_type"],
                "doc_type": doc_type,
            })

        logger.info(
            "[generate_presigned_urls_batch] %d URLs geradas para processo %s",
            len(results), process_id,
        )
        return {"process_id": process_id, "files": results}

    def link_pedido_compra_metadata(self, process_id: str, metadados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Vincula metadados do pedido de compra ao processo (sem arquivo físico).
        
        Cria um registro no DynamoDB com os metadados usando SK diferente de FILE#
        para que não apareça na listagem de arquivos. O Lambda send_to_protheus
        lê esses metadados durante o processamento.
        """
        try:
            # Log do process_id recebido
            logger.info(f"[link_pedido_compra_metadata] process_id recebido: {process_id} (tipo: {type(process_id)}, length: {len(process_id)})")
            
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
                logger.info(f"[link_pedido_compra_metadata] Criando novo processo com process_id: {process_id}")
                self.repository.put_item('PROCESS', f'PROCESS#{process_id}', {
                    'PROCESS_ID': process_id,
                    'TIMESTAMP': timestamp
                })
                self.repository.put_item(pk, 'METADATA', {
                    'STATUS': 'CREATED',
                    'TIMESTAMP': timestamp
                })
                logger.info(f"[link_pedido_compra_metadata] Processo criado com process_id salvo: {process_id}")
            
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
        
        files = [item for item in items if item['SK'].startswith('FILE#')]
        danfe_files = [f for f in files if f.get('DOC_TYPE') == 'DANFE']
        all_files = files
        
        pedido_compra_item = next(
            (item for item in items if item.get('SK') == 'PEDIDO_COMPRA_METADATA'), None
        )
        
        # At least one attachment is required (DANFE or any other file)
        if not all_files:
            raise ValueError("Nenhum arquivo anexado ao processo")
        
        # process_type: pedido requestBody — usoEConsumo → USOCONSUMO; isCommodities → BARTER;
        # senão AGROQUIMICOS; sem PEDIDO_COMPRA_METADATA → DOCUMENTO_ENTRADA
        process_type = 'AGROQUIMICOS'
        
        if pedido_compra_item:
            try:
                metadados_str = pedido_compra_item.get('METADADOS', '{}')
                pedido_compra = json.loads(metadados_str) if isinstance(metadados_str, str) else metadados_str
                
                request_body = pedido_compra.get('requestBody') or {}
                if not isinstance(request_body, dict):
                    request_body = {}

                uso_raw = _get_pedido_field(pedido_compra, request_body, "usoEConsumo")
                if _truthy_pedido_flag(uso_raw):
                    process_type = 'USOCONSUMO'
                    logger.info(
                        f"Processo {process_id}: usoEConsumo ativo, definindo process_type=USOCONSUMO"
                    )
                else:
                    commodities_raw = _get_pedido_field(
                        pedido_compra, request_body, "isCommodities"
                    )
                    if _truthy_pedido_flag(commodities_raw):
                        process_type = 'BARTER'
                        logger.info(
                            f"Processo {process_id}: isCommodities ativo, definindo process_type=BARTER"
                        )
                    else:
                        logger.info(
                            f"Processo {process_id}: isCommodities={commodities_raw}, "
                            "mantendo process_type=AGROQUIMICOS"
                        )
            except Exception as e:
                logger.warning(
                    f"Erro ao derivar process_type (usoEConsumo/isCommodities): {e}. "
                    "Usando process_type=AGROQUIMICOS"
                )
        else:
            process_type = 'DOCUMENTO_ENTRADA'
            logger.info(
                f"Processo {process_id}: sem PEDIDO_COMPRA_METADATA — process_type=DOCUMENTO_ENTRADA"
            )
        
        self.repository.update_item(pk, 'METADATA', {'PROCESS_TYPE': process_type})
        
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
        
        return {'execution_arn': response['executionArn'], 'process_id': process_id, 'process_type': process_type, 'status': 'PROCESSING'}
    
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
        
        # Textract per-file results (multi-anexo)
        textract_items = [item for item in items if item.get('SK', '').startswith('TEXTRACT#')]
        for item in textract_items:
            try:
                tables = json.loads(item.get('TABLES_DATA', '[]'))
            except Exception:
                tables = []
            parsing_results.append({
                'source': 'TEXTRACT',
                'file_name': item.get('FILE_NAME', ''),
                'parsed_data': {
                    'raw_text': item.get('RAW_TEXT', ''),
                    'tables': tables,
                    'job_id': item.get('JOB_ID', ''),
                }
            })
        
        # MERGED_EXTRACTION (canonical JSON from all sources)
        merged_item = next((item for item in items if item.get('SK') == 'MERGED_EXTRACTION'), None)
        if merged_item and merged_item.get('MERGED_DATA'):
            try:
                merged_data = json.loads(merged_item['MERGED_DATA'])
                parsing_results.append({
                    'source': 'MERGED',
                    'file_name': 'merged_extraction',
                    'parsed_data': merged_data
                })
            except Exception as e:
                logger.error(f"Error parsing MERGED_DATA: {e}")
        
        # BEDROCK_EXTRACTION (AI-enriched fields for Protheus)
        bedrock_item = next((item for item in items if item.get('SK') == 'BEDROCK_EXTRACTION'), None)
        if bedrock_item and bedrock_item.get('EXTRACTED_FIELDS'):
            try:
                extracted = json.loads(bedrock_item['EXTRACTED_FIELDS'])
                parsing_results.append({
                    'source': 'BEDROCK_AI',
                    'file_name': 'bedrock_extraction',
                    'parsed_data': extracted
                })
            except Exception as e:
                logger.error(f"Error parsing BEDROCK_EXTRACTION: {e}")

        # Bedrock por arquivo (SK=BEDROCK_EXTRACTION#nome) — JSON estruturado para UI / integração
        bedrock_by_file: list = []
        for item in items:
            sk = item.get('SK') or ''
            if sk.startswith('BEDROCK_EXTRACTION#') and item.get('EXTRACTED_FIELDS'):
                suffix = sk.split('#', 1)[1] if '#' in sk else ''
                try:
                    bedrock_by_file.append({
                        'file_name': item.get('FILE_NAME') or suffix,
                        'parsed_data': json.loads(item['EXTRACTED_FIELDS']),
                    })
                except Exception as e:
                    logger.error(f"Error parsing {sk}: {e}")
        bedrock_by_file.sort(key=lambda x: (x.get('file_name') or ''))
        
        logger.info(f"Total parsing_results: {len(parsing_results)}")

        # Payload HTTP enviado (ou tentado) ao Protheus — salvo em send_to_protheus antes do POST
        protheus_request_payload = None
        prp_raw = metadata.get('protheus_request_payload') or metadata.get('PROTHEUS_REQUEST_PAYLOAD')
        if prp_raw:
            try:
                if isinstance(prp_raw, str):
                    protheus_request_payload = json.loads(prp_raw)
                elif isinstance(prp_raw, dict):
                    protheus_request_payload = prp_raw
            except Exception as e:
                logger.warning(f"Erro ao parsear protheus_request_payload: {e}")
        if protheus_request_payload is None:
            p_info_raw = metadata.get('protheus_request_info') or metadata.get('PROTHEUS_REQUEST_INFO')
            if p_info_raw:
                try:
                    p_info = json.loads(p_info_raw) if isinstance(p_info_raw, str) else p_info_raw
                    if isinstance(p_info, dict) and p_info.get('request_payload') is not None:
                        protheus_request_payload = p_info.get('request_payload')
                except Exception as e:
                    logger.warning(f"Erro ao parsear protheus_request_info: {e}")
        
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
            if file_item.get('DOC_TYPE'):
                file_data['doc_type'] = file_item.get('DOC_TYPE')
            
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
            'bedrock_by_file': bedrock_by_file,
            'protheus_request_payload': protheus_request_payload,
            'created_at': str(int(metadata.get('TIMESTAMP', 0)))
        }
        
        # Adicionar error_info se existir (quando status é FAILED)
        # Verificar diferentes possíveis nomes de campo (DynamoDB pode retornar em diferentes formatos)
        error_info = metadata.get('error_info') or metadata.get('ERROR_INFO')
        if error_info:
            # Se error_info é uma string JSON, fazer parse
            if isinstance(error_info, str):
                try:
                    error_info = json.loads(error_info)
                except Exception as e:
                    logger.warning(f"Erro ao parsear error_info como JSON: {e}")
                    # Se não conseguir parsear, usar como string
                    error_info = {'message': error_info}
            
            result['error_info'] = error_info
            logger.info(f"Adicionado error_info ao resultado: {error_info}")
        else:
            logger.info(f"error_info não encontrado no metadata. Campos disponíveis: {list(metadata.keys())}")
            logger.info(f"Status do processo: {metadata.get('STATUS')}")
        
        logger.info(f"Returning result with {len(result.get('parsing_results', []))} parsing_results")
        logger.info(f"Result keys: {list(result.keys())}")
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