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
    
    for item in items:
        if 'PARSED_XML=' in item['SK']:
            parsed = json.loads(item.get('PARSED_DATA', '{}'))
            danfe_data = {'file_name': item['FILE_NAME'], 'data': parsed}
        elif 'PARSED_OCR=' in item['SK']:
            parsed = json.loads(item.get('PARSED_DATA', '{}'))
            docs_data.append({'file_name': item['FILE_NAME'], 'data': parsed})
    
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
    danfe_parsed = danfe_data['data'] if danfe_data else {}
    ocr_docs = [{'file_name': doc['file_name'], **doc['data']} for doc in docs_data]
    
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
    
    table.put_item(Item={
        'PK': pk,
        'SK': sk,
        'VALIDATION_RESULTS': json.dumps(results_clean),
        'VALIDATION_STATUS': validation_status,
        'TIMESTAMP': timestamp
    })
    
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
