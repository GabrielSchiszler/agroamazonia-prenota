import os
import json
import boto3
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def decimal_to_native(obj):
    """Converte Decimal para tipos nativos"""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    return obj

def handler(event, context):
    """Valida regras de negócio usando dados extraídos"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    process_type = event['process_type']
    
    # Buscar dados parseados
    pk = f"PROCESS#{process_id}"
    items = table.query(
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={':pk': pk}
    )['Items']
    
    danfe_data = None
    docs_data = []
    file_metadata = {}  # Mapear file_name -> metadados JSON
    input_json = None  # Novo formato com header e requestBody
    
    # Buscar INPUT_JSON dos metadados do processo (novo formato)
    metadata_item = next((item for item in items if item['SK'] == 'METADATA'), None)
    if metadata_item:
        input_json_str = metadata_item.get('INPUT_JSON') or metadata_item.get('REQUEST_BODY')
        if input_json_str:
            try:
                if isinstance(input_json_str, str):
                    input_json = json.loads(input_json_str)
                else:
                    input_json = input_json_str
                logger.info(f"Found INPUT_JSON with requestBody.itens: {len(input_json.get('requestBody', {}).get('itens', []))} produtos")
            except Exception as e:
                logger.warning(f"Failed to parse INPUT_JSON: {str(e)}")
    
    # Buscar metadados do pedido de compra (SK: PEDIDO_COMPRA_METADATA) - PRIORIDADE 1
    pedido_compra_item = next((item for item in items if item.get('SK') == 'PEDIDO_COMPRA_METADATA'), None)
    if pedido_compra_item:
        metadados_str = pedido_compra_item.get('METADADOS', '')
        if metadados_str:
            try:
                metadados = json.loads(metadados_str) if isinstance(metadados_str, str) else metadados_str
                
                # Verificar se está no formato do pedido de compra (com header e requestBody)
                if isinstance(metadados, dict):
                    if 'requestBody' in metadados:
                        logger.info(f"[handler] PEDIDO_COMPRA_METADATA - Metadados no formato pedido de compra (tem header e requestBody)")
                        logger.info(f"[handler] PEDIDO_COMPRA_METADATA - requestBody keys: {list(metadados.get('requestBody', {}).keys())}")
                        logger.info(f"[handler] PEDIDO_COMPRA_METADATA - requestBody.cnpjEmitente: {metadados.get('requestBody', {}).get('cnpjEmitente')}")
                        logger.info(f"[handler] PEDIDO_COMPRA_METADATA - requestBody.cnpjDestinatario: {metadados.get('requestBody', {}).get('cnpjDestinatario')}")
                        logger.info(f"[handler] PEDIDO_COMPRA_METADATA - requestBody.itens: {len(metadados.get('requestBody', {}).get('itens', []))} itens")
                    
                    # Salvar como "Metadados do Pedido de Compra" (sem arquivo físico)
                    file_metadata['Metadados do Pedido de Compra'] = metadados
                    logger.info(f"[handler] Metadados do pedido de compra salvos (sem arquivo físico)")
            except Exception as e:
                logger.warning(f"[handler] Falha ao parsear metadados do pedido de compra: {str(e)}")
                import traceback
                traceback.print_exc()
    
    # Buscar metadados dos arquivos (FILE#) - PRIORIDADE 2
    for item in items:
        if 'FILE#' in item['SK']:
            file_name = item.get('FILE_NAME', '')
            metadados_str = item.get('METADADOS', '')
            if metadados_str:
                try:
                    metadados = json.loads(metadados_str) if isinstance(metadados_str, str) else metadados_str
                    
                    # Verificar se está no formato do pedido de compra (com header e requestBody)
                    if isinstance(metadados, dict):
                        if 'requestBody' in metadados:
                            logger.info(f"[handler] Arquivo {file_name} - Metadados no formato pedido de compra (tem header e requestBody)")
                            logger.info(f"[handler] Arquivo {file_name} - requestBody keys: {list(metadados.get('requestBody', {}).keys())}")
                            logger.info(f"[handler] Arquivo {file_name} - requestBody.cnpjEmitente: {metadados.get('requestBody', {}).get('cnpjEmitente')}")
                            logger.info(f"[handler] Arquivo {file_name} - requestBody.cnpjDestinatario: {metadados.get('requestBody', {}).get('cnpjDestinatario')}")
                            logger.info(f"[handler] Arquivo {file_name} - requestBody.itens: {len(metadados.get('requestBody', {}).get('itens', []))} itens")
                        else:
                            logger.info(f"[handler] Arquivo {file_name} - Metadados no formato antigo (sem requestBody)")
                            logger.info(f"[handler] Arquivo {file_name} - Metadados keys: {list(metadados.keys())}")
                    
                    file_metadata[file_name] = metadados
                    logger.info(f"[handler] Metadados salvos para arquivo: {file_name}")
                except Exception as e:
                    logger.warning(f"[handler] Falha ao parsear metadados para {file_name}: {str(e)}")
                    import traceback
                    traceback.print_exc()
    
    for item in items:
        if 'PARSED_XML=' in item['SK']:
            parsed = json.loads(item.get('PARSED_DATA', '{}'))
            danfe_data = {'file_name': item['FILE_NAME'], 'data': parsed}
        # Removido: não processa mais OCR de documentos adicionais
        # elif 'PARSED_OCR=' in item['SK']:
        #     parsed = json.loads(item.get('PARSED_DATA', '{}'))
        #     docs_data.append({'file_name': item['FILE_NAME'], 'data': parsed})
    
    if not danfe_data:
        logger.warning("DANFE XML not found, skipping validation")
        return {
            'process_id': process_id,
            'validation_results': [],
            'status': 'SKIPPED',
            'message': 'DANFE XML not found'
        }
    
    # Buscar regras configuradas para o process_type
    rules = get_rules_for_process_type(process_type)
    results = []
    
    # Preparar dados no formato esperado pelas regras
    # Mesclar metadados JSON com dados OCR, preservando dados OCR originais
    danfe_parsed = danfe_data['data'] if danfe_data else {}
    ocr_docs = []
    
    # PRIORIDADE MÁXIMA: Se tiver INPUT_JSON com requestBody.itens, preparar produtos uma vez
    request_body_itens = None
    if input_json and input_json.get('requestBody', {}).get('itens'):
        request_body_itens = input_json['requestBody']['itens']
        logger.info(f"Found INPUT_JSON with requestBody.itens: {len(request_body_itens)} produtos")
        # Converter formato novo para formato esperado pelas regras
        request_body_itens = [
            {
                'codigoProduto': item.get('codigoProduto', ''),
                'produto': item.get('produto', ''),
                'nomeProduto': item.get('produto', ''),  # Alias para compatibilidade
                'nome': item.get('produto', ''),  # Alias para compatibilidade
                'descricaoProduto': item.get('produto', ''),  # Alias para compatibilidade
                'descricao': item.get('produto', ''),  # Alias para compatibilidade
                'valorUnitario': item.get('valorUnitario', 0),
                'pedidoDeCompra': item.get('pedidoDeCompra', {})
            }
            for item in request_body_itens
        ]
    
    # Se não houver docs_data mas houver metadados do pedido de compra, criar um doc virtual
    if not docs_data and 'Metadados do Pedido de Compra' in file_metadata:
        logger.info(f"[handler] Criando doc virtual para metadados do pedido de compra (sem arquivo físico)")
        docs_data.append({
            'file_name': 'Metadados do Pedido de Compra',
            'data': {}
        })
    
    for doc in docs_data:
        file_name = doc['file_name']
        ocr_data = doc['data']
        metadados = file_metadata.get(file_name, {})
        
        logger.info(f"[handler] Preparando doc {file_name}...")
        logger.info(f"[handler] Doc {file_name} - metadados type: {type(metadados)}")
        logger.info(f"[handler] Doc {file_name} - metadados keys: {list(metadados.keys()) if isinstance(metadados, dict) else 'N/A'}")
        
        # Verificar se metadados estão no formato do pedido de compra (com header e requestBody)
        request_body_from_metadata = None
        if isinstance(metadados, dict):
            # Se metadados têm requestBody diretamente
            if 'requestBody' in metadados:
                request_body_from_metadata = metadados.get('requestBody')
                logger.info(f"[handler] Doc {file_name} - Metadados no formato pedido de compra (tem requestBody)")
                logger.info(f"[handler] Doc {file_name} - requestBody keys: {list(request_body_from_metadata.keys()) if isinstance(request_body_from_metadata, dict) else 'N/A'}")
            # Se metadados são o próprio JSON do pedido de compra (string JSON)
            elif isinstance(metadados, str):
                try:
                    metadados_parsed = json.loads(metadados)
                    if isinstance(metadados_parsed, dict) and 'requestBody' in metadados_parsed:
                        request_body_from_metadata = metadados_parsed.get('requestBody')
                        logger.info(f"[handler] Doc {file_name} - Metadados parseados do formato pedido de compra (tem requestBody)")
                except:
                    pass
        
        # Preparar documento APENAS com metadados do JSON do pedido de compra (NÃO usar OCR)
        doc_prepared = {
            'file_name': file_name,
            '_has_metadata': bool(metadados or request_body_itens or request_body_from_metadata)
            # NÃO incluir dados OCR no doc_prepared - usar apenas JSON do pedido de compra
        }
        
        # PRIORIDADE MÁXIMA: Se tiver INPUT_JSON com requestBody.itens, usar isso
        if request_body_itens:
            doc_prepared['itens'] = request_body_itens
            logger.info(f"[handler] Doc {file_name} - Usando requestBody.itens do INPUT_JSON: {len(doc_prepared['itens'])} produtos")
        
        # PRIORIDADE 2: Se metadados têm requestBody (formato do pedido de compra), mesclar campos do requestBody
        if request_body_from_metadata and isinstance(request_body_from_metadata, dict):
            logger.info(f"[handler] Doc {file_name} - Mesclando campos do requestBody dos metadados...")
            # Mesclar campos do requestBody diretamente no doc_prepared
            for key, value in request_body_from_metadata.items():
                if key == 'itens':
                    # Se já tem itens do INPUT_JSON, não sobrescrever
                    if 'itens' not in doc_prepared:
                        doc_prepared['itens'] = value
                        logger.info(f"[handler] Doc {file_name} - Adicionado {len(value)} itens do requestBody dos metadados")
                else:
                    doc_prepared[key] = value
                    logger.info(f"[handler] Doc {file_name} - Campo '{key}' adicionado do requestBody: {value}")
            
            # Também adicionar requestBody completo para acesso direto
            doc_prepared['requestBody'] = request_body_from_metadata
            logger.info(f"[handler] Doc {file_name} - requestBody completo adicionado ao doc_prepared")
        
        # PRIORIDADE 3: Mesclar outros campos dos metadados (formato antigo)
        elif metadados and isinstance(metadados, dict):
            logger.info(f"[handler] Doc {file_name} - Mesclando metadados (formato antigo)...")
            # Se metadados têm 'itens', usar isso
            if 'itens' in metadados and 'itens' not in doc_prepared:
                doc_prepared['itens'] = metadados['itens']
            # Mesclar outros campos dos metadados
            for key, value in metadados.items():
                if key != 'itens':  # 'itens' já foi tratado acima
                    doc_prepared[key] = value
                    logger.info(f"[handler] Doc {file_name} - Campo '{key}' adicionado dos metadados: {value}")
        
        # Log final do que foi preparado
        logger.info(f"[handler] Doc {file_name} - Preparado:")
        logger.info(f"[handler]   - has_metadata: {doc_prepared['_has_metadata']}")
        logger.info(f"[handler]   - cnpjEmitente: {doc_prepared.get('cnpjEmitente')}")
        logger.info(f"[handler]   - cnpjRemetente: {doc_prepared.get('cnpjRemetente')}")
        logger.info(f"[handler]   - requestBody disponível: {bool(doc_prepared.get('requestBody'))}")
        if doc_prepared.get('requestBody'):
            logger.info(f"[handler]   - requestBody.cnpjEmitente: {doc_prepared.get('requestBody', {}).get('cnpjEmitente')}")
        logger.info(f"[handler]   - itens_count: {len(doc_prepared.get('itens', []))}")
        logger.info(f"[handler]   - Total de keys no doc_prepared: {len(doc_prepared.keys())}")
        
        ocr_docs.append(doc_prepared)
    
    for rule in rules:
        logger.info(f"Executing rule: {rule['rule_name']}")
        
        try:
            module = __import__(f"rules.{rule['rule_name']}", fromlist=['validate'])
            if not hasattr(module, 'validate'):
                results.append({
                    'rule': rule['rule_name'],
                    'status': 'ERROR',
                    'danfe_value': 'null',
                    'message': 'Regra não encontrada'
                })
                continue
            
            result = module.validate(danfe_parsed, ocr_docs)
            logger.info(f"Rule {rule['rule_name']} result: {json.dumps(result, default=str)}")
            results.append(result)
            
            # Aplicar correções se houver
            if result.get('corrections'):
                apply_corrections(process_id, result['corrections'])
            
        except Exception as e:
            logger.error(f"Rule execution failed: {str(e)}")
            results.append({
                'rule': rule['rule_name'],
                'status': 'ERROR',
                'danfe_value': 'null',
                'message': str(e)
            })
    
    # Salvar apenas resultados das validações
    timestamp = int(datetime.now().timestamp())
    sk = f"VALIDATION#{timestamp}"
    
    results_clean = decimal_to_native(results)
    
    # Verificar se há falhas
    has_failures = any(r.get('status') == 'FAILED' for r in results)
    validation_status = 'FAILED' if has_failures else 'PASSED'
    
    # Extrair dados do CFOP para facilitar acesso no Protheus
    cfop_mapping_data = {}
    for result in results:
        if result.get('rule') == 'validar_cfop_chave' and result.get('cfop_data'):
            cfop_mapping_data = result.get('cfop_data', {})
            logger.info(f"CFOP mapping data found: {json.dumps(cfop_mapping_data)}")
            break
    
    item_data = {
        'PK': pk,
        'SK': sk,
        'VALIDATION_RESULTS': json.dumps(results_clean),
        'VALIDATION_STATUS': validation_status,
        'TIMESTAMP': timestamp
    }
    
    # Adicionar dados do CFOP se encontrado
    if cfop_mapping_data:
        item_data['CFOP_MAPPING'] = json.dumps(cfop_mapping_data)
    
    table.put_item(Item=item_data)
    
    return {
        'process_id': process_id,
        'validation_results': results,
        'validation_status': validation_status,
        'status': 'VALIDATED'
    }

def apply_corrections(process_id, corrections):
    """Aplica correções nos dados OCR parseados"""
    pk = f"PROCESS#{process_id}"
    
    for correction in corrections:
        file_name = correction['file_name']
        field = correction['field']
        new_value = correction['new_value']
        
        logger.info(f"Applying correction: {file_name} - {field} = {new_value}")
        
        # Buscar item OCR
        sk = f"PARSED_OCR={file_name}"
        response = table.get_item(Key={'PK': pk, 'SK': sk})
        
        if 'Item' not in response:
            logger.warning(f"Item not found for correction: {sk}")
            continue
        
        item = response['Item']
        parsed_data = json.loads(item.get('PARSED_DATA', '{}'))
        
        # Aplicar correção
        if field in parsed_data:
            parsed_data[field] = new_value
        
        # Atualizar no DynamoDB
        table.update_item(
            Key={'PK': pk, 'SK': sk},
            UpdateExpression='SET PARSED_DATA = :data',
            ExpressionAttributeValues={':data': json.dumps(parsed_data)}
        )
        
        logger.info(f"Correction applied successfully")

def get_rules_for_process_type(process_type):
    """Busca regras configuradas no DynamoDB"""
    try:
        pk = f'RULES#{process_type}'
        logger.info(f"Querying rules with PK={pk}, SK prefix=RULE#")
        
        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
            ExpressionAttributeValues={
                ':pk': pk,
                ':sk': 'RULE#'
            }
        )
        
        logger.info(f"Found {len(response.get('Items', []))} rules")
        
        rules = []
        for item in response.get('Items', []):
            rule = {
                'rule_name': item.get('rule_name') or item.get('RULE_NAME'),
                'order': item.get('order') or item.get('ORDER', 999),
                'enabled': item.get('enabled', item.get('ENABLED', True))
            }
            logger.info(f"Rule found: {rule}")
            rules.append(rule)
        
        # Ordenar por ordem e filtrar habilitadas
        rules = [r for r in rules if r['enabled']]
        rules.sort(key=lambda x: x['order'])
        
        logger.info(f"Returning {len(rules)} enabled rules")
        return rules
    except Exception as e:
        logger.error(f"Failed to load rules: {str(e)}")
        return []
