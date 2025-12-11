import json
import os
import boto3
import requests
import random
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")
    
    # Extrair process_id do Payload se vier do Step Functions
    if 'Payload' in event:
        event = event['Payload']
    
    process_id = event.get('process_id')
    if not process_id:
        print("ERROR: process_id not found in event")
        return {'statusCode': 400, 'error': 'process_id not found'}
    
    validation_results = event.get('validation_results', [])
    error_info = event.get('error', {})
    print(f"Processing failure for process_id: {process_id}")
    
    # Gerar ID único numérico de 6 dígitos
    id_unico = random.randint(100000, 999999)
    
    # Extrair detalhes das validações que falharam
    detalhes = []
    failed_rules = [r for r in validation_results if r.get('status') == 'FAILED']
    
    if failed_rules:
        for rule in failed_rules:
            rule_name = rule.get('rule', 'Desconhecido')
            message = rule.get('message', 'Sem mensagem')
            comparisons = rule.get('comparisons', [])
            
            # Para cada documento que falhou
            for comp in comparisons:
                doc_file = comp.get('doc_file', 'Documento desconhecido')
                
                # Se for validação de produtos, detalhar cada campo que falhou
                if 'items' in comp:
                    for item in comp.get('items', []):
                        if item.get('status') == 'MISMATCH':
                            fields = item.get('fields', {})
                            failed_fields = [f for f, v in fields.items() if v.get('status') == 'MISMATCH']
                            if failed_fields:
                                detalhes.append({
                                    "pagina": 1,
                                    "campo": f"{rule_name} - Item {item.get('item')} - {', '.join(failed_fields)}",
                                    "mensagemErro": f"Documento: {doc_file}. Divergências: " + 
                                                   ", ".join([f"{f}: DANFE={fields[f].get('danfe')} vs DOC={fields[f].get('doc')}" 
                                                              for f in failed_fields])
                                })
                else:
                    # Validação simples (não produtos)
                    detalhes.append({
                        "pagina": 1,
                        "campo": rule_name,
                        "mensagemErro": f"Documento: {doc_file}. {message}. DANFE: {rule.get('danfe_value')}, DOC: {comp.get('doc_value')}"
                    })
    else:
        # Falha técnica (não de validação)
        detalhes.append({
            "pagina": 1,
            "campo": error_info.get('Cause', 'Erro técnico'),
            "mensagemErro": error_info.get('Error', 'Erro no processamento OCR')
        })
    
    # Preparar payload para API externa
    descricao_falha = f"Validação falhou: {len(failed_rules)} regra(s) com divergência" if failed_rules else "Erro no processamento OCR"
    
    payload = {
        "idUnico": id_unico,
        "descricaoFalha": descricao_falha,
        "traceAWS": process_id,
        "detalhes": detalhes
    }
    
    # Enviar para API externa e obter sctask_id
    sctask_id = None
    try:
        api_url = os.environ.get('OCR_FAILURE_API_URL', 'https://virtserver.swaggerhub.com/agroamazonia/fast-ocr/1.0.0/reportar-falha-ocr')
        print(f"Sending to API: {api_url}")
        print(f"Payload: {json.dumps(payload)}")
        response = requests.post(api_url, json=payload, timeout=30)
        response.raise_for_status()
        api_response = response.json()
        print(f"API response: {api_response}")
        # Extrair sctask_id do campo 'tarefa' da resposta
        sctask_id = api_response.get('tarefa')
        print(f"SCTASK ID from API: {sctask_id}")
    except Exception as e:
        print(f"Erro ao reportar falha para API externa: {str(e)}")
        api_response = {'error': str(e)}
    
    # Atualizar status no DynamoDB com sctask_id da API
    try:
        print(f"Updating DynamoDB: PK=PROCESS#{process_id}, SK=METADATA")
        update_expr = 'SET #status = :status, error_info = :error, updated_at = :timestamp'
        expr_values = {
            ':status': 'FAILED',
            ':error': json.dumps(error_info),
            ':timestamp': datetime.utcnow().isoformat()
        }
        if sctask_id:
            update_expr += ', sctask_id = :sctask'
            expr_values[':sctask'] = sctask_id
        
        table.update_item(
            Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={'#status': 'STATUS'},
            ExpressionAttributeValues=expr_values
        )
        print("DynamoDB updated successfully")
    except Exception as e:
        print(f"Erro ao atualizar DynamoDB: {str(e)}")
        import traceback
        traceback.print_exc()
    
    result = {
        'statusCode': 200,
        'process_id': process_id,
        'status': 'FAILED',
        'api_response': api_response
    }
    print(f"Returning: {json.dumps(result)}")
    return result
