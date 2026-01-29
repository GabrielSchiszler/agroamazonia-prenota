import json
import os
import boto3
from datetime import datetime, timezone
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event, context):
    """Atualiza métricas de processamento no DynamoDB"""
    
    print("="*80)
    print("UPDATE_METRICS - INÍCIO")
    print("="*80)
    
    try:
        print(f"UpdateMetrics - Event type: {type(event)}")
        print(f"UpdateMetrics - Event keys: {list(event.keys()) if isinstance(event, dict) else 'N/A'}")
        print(f"UpdateMetrics - Event: {json.dumps(event, default=str)}")
    except Exception as e:
        print(f"ERRO ao fazer dump do event: {e}")
        print(f"Event (repr): {repr(event)}")
    
    # Extrair dados do evento
    process_id = event.get('process_id', '')
    print(f"Process ID extraído: {process_id}")
    
    if not process_id:
        print("ERROR: process_id não fornecido")
        raise ValueError("process_id é obrigatório")
    
    # Buscar start_time do DynamoDB
    start_time_str = None
    process_type = 'UNKNOWN'
    metadata_response = None
    try:
        pk = f"PROCESS#{process_id}"
        metadata_response = table.get_item(
            Key={'PK': pk, 'SK': 'METADATA'}
        )
        
        if 'Item' in metadata_response:
            start_time_str = metadata_response['Item'].get('START_TIME')
            process_type = metadata_response['Item'].get('PROCESS_TYPE', 'UNKNOWN')
            print(f"Found start_time in DynamoDB: {start_time_str}")
            print(f"Found process_type: {process_type}")
        else:
            print(f"WARNING: Process {process_id} METADATA not found in DynamoDB")
            process_type = 'UNKNOWN'
    except Exception as e:
        print(f"ERROR: Failed to get start_time from DynamoDB: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        process_type = 'UNKNOWN'
    
    # process_type vem apenas do DynamoDB (fonte única)
    
    # Determinar status baseado no evento
    event_status = event.get('status', '')
    error_info = event.get('error', {})
    failure_result = event.get('failure_result', {})
    
    # Verificar se error_info não é vazio (pode ser {} quando não há erro)
    # Step Functions passa erro no formato: {Error: "...", Cause: "..."}
    has_error_info = error_info and isinstance(error_info, dict) and (error_info.get('Error') or error_info.get('Cause') or len(error_info) > 0)
    
    # Verificar se failure_result não é vazio (pode ser {} quando não há falha)
    has_failure_result = failure_result and isinstance(failure_result, dict) and len(failure_result) > 0
    
    # Buscar regras que falharam do DynamoDB (sempre buscar, independente do status)
    # Isso permite contar regras que falharam mesmo em processos que eventualmente passaram
    failed_rules = []
    try:
        # Buscar resultados de validação do DynamoDB
        pk = f"PROCESS#{process_id}"
        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
            ExpressionAttributeValues={
                ':pk': pk,
                ':sk': 'VALIDATION#'
            }
        )
        
        items = response.get('Items', [])
        print(f"Buscando regras que falharam - encontrados {len(items)} itens de validação")
        
        if items:
            print(f"Items encontrados: {[item.get('SK', 'N/A') for item in items]}")
            # Ordenar por timestamp e pegar o mais recente
            latest_item = max(items, key=lambda x: x.get('TIMESTAMP', 0))
            print(f"Item mais recente: SK={latest_item.get('SK', 'N/A')}, TIMESTAMP={latest_item.get('TIMESTAMP', 'N/A')}")
            
            validation_results_str = latest_item.get('VALIDATION_RESULTS', '[]')
            print(f"VALIDATION_RESULTS (tipo): {type(validation_results_str)}")
            print(f"VALIDATION_RESULTS (primeiros 200 chars): {str(validation_results_str)[:200]}")
            
            try:
                validation_results = json.loads(validation_results_str) if isinstance(validation_results_str, str) else validation_results_str
                print(f"Resultados de validação parseados: {len(validation_results)} regras")
                print(f"Resultados completos: {json.dumps(validation_results, default=str, indent=2)}")
                
                for idx, rule_result in enumerate(validation_results):
                    rule_status = rule_result.get('status', '')
                    rule_name = rule_result.get('rule', 'UNKNOWN')
                    print(f"[{idx}] Regra: {rule_name}, Status: {rule_status}")
                    
                    # Contar todas as regras que falharam (status = 'FAILED')
                    if rule_status == 'FAILED':
                        failed_rules.append(rule_name)
                        print(f"  ✓ Regra {rule_name} falhou - adicionada à lista")
                    else:
                        print(f"  - Regra {rule_name} passou (status: {rule_status})")
                
                print(f"Total de regras que falharam: {len(failed_rules)} - {failed_rules}")
            except Exception as e:
                print(f"ERRO ao parsear validation_results: {e}")
                import traceback
                print(f"Traceback:\n{traceback.format_exc()}")
        else:
            print("⚠️ Nenhum resultado de validação encontrado no DynamoDB")
            print(f"Query response: {response}")
    except Exception as e:
        print(f"Erro ao buscar regras que falharam: {e}")
        import traceback
        traceback.print_exc()
    
    # Determinar status (fonte única: campo 'status' do evento)
    status = event_status if event_status else 'SUCCESS'
    print(f"Status determinado: {status}")
    
    # Calcular tempo de processamento
    end_time = datetime.now(timezone.utc)
    processing_time = 0
    
    if start_time_str:
        try:
            # Tentar diferentes formatos de data
            print(f"Tentando parsear start_time: '{start_time_str}' (tipo: {type(start_time_str)})")
            
            # Formato ISO com 'Z' no final (ex: 2025-01-26T20:37:40.071Z)
            if isinstance(start_time_str, str) and 'T' in start_time_str:
                # Remover 'Z' e adicionar '+00:00' para timezone UTC
                if start_time_str.endswith('Z'):
                    start_time_str_parsed = start_time_str[:-1] + '+00:00'
                elif '+' in start_time_str or start_time_str.count('-') >= 3:
                    # Já tem timezone ou é formato ISO completo
                    start_time_str_parsed = start_time_str
                else:
                    # Formato ISO sem timezone, assumir UTC
                    start_time_str_parsed = start_time_str + '+00:00'
                
                start_time = datetime.fromisoformat(start_time_str_parsed)
                print(f"Parseado como ISO: {start_time_str_parsed} -> {start_time}")
            elif isinstance(start_time_str, (int, float)) or (isinstance(start_time_str, str) and start_time_str.replace('.', '').isdigit()):
                # Formato timestamp Unix
                start_time = datetime.fromtimestamp(float(start_time_str), tz=timezone.utc)
                print(f"Parseado como timestamp Unix: {start_time_str} -> {start_time}")
            else:
                raise ValueError(f"Formato de data não reconhecido: {start_time_str}")
            
            # Garantir que start_time tem timezone UTC
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
                print(f"Adicionado timezone UTC ao start_time: {start_time}")
            
            processing_time = (end_time - start_time).total_seconds()
            
            # Validar que o tempo não é negativo ou muito grande (indicando erro de parse)
            if processing_time < 0:
                print(f"WARNING: Tempo negativo calculado ({processing_time}s), pode indicar problema de timezone")
                # Se negativo mas pequeno (< 1 hora), pode ser apenas diferença de timezone, usar valor absoluto
                if abs(processing_time) < 3600:
                    processing_time = abs(processing_time)
                    print(f"Ajustado para valor absoluto: {processing_time}s")
                else:
                    raise ValueError(f"Tempo negativo muito grande: {processing_time}s")
            elif processing_time > 86400:  # Mais de 24 horas
                print(f"WARNING: Tempo muito grande ({processing_time}s), pode indicar erro de parse")
                raise ValueError(f"Tempo muito grande: {processing_time}s")
            
            print(f"✓ Tempo calculado com sucesso: {processing_time}s (start: {start_time}, end: {end_time})")
        except Exception as e:
            print(f"ERROR: Erro ao parsear start_time '{start_time_str}': {e}")
            import traceback
            print(f"Traceback completo:\n{traceback.format_exc()}")
            # Usar tempo padrão de 30 segundos se não conseguir calcular
            processing_time = 30
            print(f"Usando tempo padrão de 30s devido ao erro")
    else:
        print("WARNING: start_time não encontrado no DynamoDB, usando tempo padrão de 30s")
        if metadata_response:
            print(f"Response do DynamoDB: Item existe = {'Item' in metadata_response}")
            if 'Item' in metadata_response:
                print(f"Campos disponíveis no Item: {list(metadata_response['Item'].keys())}")
        else:
            print("Nenhuma resposta do DynamoDB disponível")
        processing_time = 30
    
    # Determinar tipo de erro (fonte única: campo 'error' do evento)
    error_type = None
    if status == 'FAILED':
        if has_error_info:
            error_type = 'LAMBDA_ERROR'
        elif has_failure_result:
            error_type = 'VALIDATION_FAILED'
        else:
            error_type = 'PROCESSING_ERROR'
        print(f"Error type: {error_type}")
    
    # Data para agregação
    date_key = end_time.strftime('%Y-%m-%d')
    month_key = end_time.strftime('%Y-%m')
    hour = end_time.hour
    
    print(f"Atualizando métricas: date={date_key}, status={status}, time={processing_time}s")
    print(f"Regras que falharam encontradas: {failed_rules} (total: {len(failed_rules)})")
    print(f"Process type: {process_type}")
    print(f"Error type: {error_type}")
    print(f"Hour: {hour}")
    
    try:
        print("Chamando update_daily_metrics...")
        # Atualizar métricas diárias
        update_daily_metrics(date_key, status, processing_time, error_type, hour, process_type, failed_rules)
        print("✓ update_daily_metrics concluído")
        
        print("Chamando update_monthly_metrics...")
        # Atualizar métricas mensais
        update_monthly_metrics(month_key, status, processing_time, process_type)
        print("✓ update_monthly_metrics concluído")
        
        print("="*80)
        print("UPDATE_METRICS - SUCESSO")
        print("="*80)
        
    except Exception as e:
        print("="*80)
        print("UPDATE_METRICS - ERRO")
        print("="*80)
        print(f"Erro ao atualizar métricas: {e}")
        import traceback
        print(f"Traceback completo:\n{traceback.format_exc()}")
        # Falhar para que Step Functions capture o erro
        raise Exception(f"UpdateMetrics failed: {e}")
    
    return {
        'statusCode': 200,
        'process_id': process_id,
        'metrics_updated': True
    }

def update_daily_metrics(date_key, status, processing_time, error_type, hour, process_type='UNKNOWN', failed_rules=None):
    """Atualiza métricas diárias"""
    
    print(f"\n[update_daily_metrics] Iniciando atualização para {date_key}")
    print(f"[update_daily_metrics] Parâmetros:")
    print(f"  - date_key: {date_key}")
    print(f"  - status: {status}")
    print(f"  - processing_time: {processing_time}")
    print(f"  - error_type: {error_type}")
    print(f"  - hour: {hour}")
    print(f"  - process_type: {process_type}")
    print(f"  - failed_rules: {failed_rules}")
    
    if failed_rules is None:
        failed_rules = []
        print(f"[update_daily_metrics] failed_rules era None, inicializado como lista vazia")
    
    try:
        # Primeiro, tentar buscar o item existente
        print(f"[update_daily_metrics] Buscando item existente: PK=METRICS#{date_key}, SK=SUMMARY")
        response = table.get_item(
            Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'}
        )
        print(f"[update_daily_metrics] Resposta do get_item: Item existe = {'Item' in response}")
        
        if 'Item' not in response:
            # Criar item inicial com todos os mapas vazios
            print(f"[update_daily_metrics] Item não existe, criando novo item inicial")
            table.put_item(
                Item={
                    'PK': f'METRICS#{date_key}',
                    'SK': 'SUMMARY',
                    'total_count': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_time': Decimal('0'),
                    'processes_by_hour': {},
                    'failure_reasons': {},
                    'processes_by_type': {},
                    'failed_rules': {}
                }
            )
            print(f"[update_daily_metrics] ✓ Item inicial criado")
        else:
            existing_item = response['Item']
            print(f"[update_daily_metrics] Item existente encontrado:")
            print(f"  - total_count: {existing_item.get('total_count', 0)}")
            print(f"  - success_count: {existing_item.get('success_count', 0)}")
            print(f"  - failed_count: {existing_item.get('failed_count', 0)}")
            print(f"  - processes_by_type: {existing_item.get('processes_by_type', {})}")
            print(f"  - failed_rules: {existing_item.get('failed_rules', {})}")
            
            # Verificar se os mapas existem (podem não existir em itens criados antes)
            needs_map_init = False
            set_parts = []
            
            if 'processes_by_hour' not in existing_item:
                set_parts.append('processes_by_hour = :empty_map')
                needs_map_init = True
            if 'failure_reasons' not in existing_item:
                set_parts.append('failure_reasons = :empty_map')
                needs_map_init = True
            if 'processes_by_type' not in existing_item:
                set_parts.append('processes_by_type = :empty_map')
                needs_map_init = True
            if 'failed_rules' not in existing_item:
                set_parts.append('failed_rules = :empty_map')
                needs_map_init = True
            
            if needs_map_init:
                # Criar mapas faltantes em uma atualização separada (antes de usar ADD)
                print(f"[update_daily_metrics] Inicializando mapas faltantes: {', '.join(set_parts)}")
                table.update_item(
                    Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'},
                    UpdateExpression='SET ' + ', '.join(set_parts),
                    ExpressionAttributeValues={':empty_map': {}}
                )
                print(f"[update_daily_metrics] ✓ Mapas inicializados")
        
        # Agora fazer o update
        # Usar ADD para campos numéricos simples e SET para mapas aninhados
        expr_names = {}
        expr_values = {
            ':inc': 1,
            ':time': Decimal(str(processing_time))
        }
        
        # Coletar todos os campos ADD em uma única lista
        add_parts = []
        
        # Campos numéricos simples (ADD)
        add_parts.append('total_count :inc')
        add_parts.append('total_time :time')
        
        # Contadores por status (ADD funciona para campos numéricos)
        if status == 'SUCCESS':
            add_parts.append('success_count :succ')
            expr_values[':succ'] = 1
        elif status == 'FAILED':
            add_parts.append('failed_count :fail')
            expr_values[':fail'] = 1
        
        # Para mapas aninhados, usar SET com if_not_exists para evitar sobreposição
        # Separar SET e ADD em seções diferentes para evitar conflitos
        
        set_parts = []
        
        # Contador por hora (usar SET com if_not_exists e depois ADD)
        hour_key = f'#h{hour}'
        hour_path = f'processes_by_hour.{hour_key}'
        set_parts.append(f'{hour_path} = if_not_exists({hour_path}, :zero) + :hour_inc')
        expr_names[hour_key] = str(hour)
        expr_values[':hour_inc'] = 1
        expr_values[':zero'] = 0
        
        # Contador por tipo de erro
        if error_type:
            error_key = f'#e{len(expr_names)}'
            error_path = f'failure_reasons.{error_key}'
            set_parts.append(f'{error_path} = if_not_exists({error_path}, :zero) + :error_inc')
            expr_names[error_key] = error_type
            expr_values[':error_inc'] = 1
        
        # Contador por tipo de processo
        type_key = f'#t{len(expr_names)}'
        type_path = f'processes_by_type.{type_key}'
        set_parts.append(f'{type_path} = if_not_exists({type_path}, :zero) + :type_inc')
        expr_names[type_key] = process_type
        expr_values[':type_inc'] = 1
        
        # Contador por regra que falhou (cada regra que falhou conta +1)
        for idx, rule_name in enumerate(failed_rules):
            rule_key = f'#r{idx}'
            rule_path = f'failed_rules.{rule_key}'
            rule_inc_key = f':rule_inc_{idx}'
            
            set_parts.append(f'{rule_path} = if_not_exists({rule_path}, :zero) + {rule_inc_key}')
            expr_names[rule_key] = rule_name
            expr_values[rule_inc_key] = 1
            print(f"Incrementando contador para regra: {rule_name} (chave: {rule_key}, valor: 1)")
        
        # Construir UpdateExpression: SET para mapas, ADD para campos numéricos
        update_expr_parts = []
        if add_parts:
            update_expr_parts.append('ADD ' + ', '.join(add_parts))
        if set_parts:
            update_expr_parts.append('SET ' + ', '.join(set_parts))
        
        update_expr = ' '.join(update_expr_parts)
        
        print(f"\n[update_daily_metrics] Construindo UpdateExpression...")
        print(f"[update_daily_metrics] UpdateExpression final: {update_expr}")
        print(f"[update_daily_metrics] ExpressionAttributeNames: {json.dumps(expr_names, default=str)}")
        print(f"[update_daily_metrics] ExpressionAttributeValues: {json.dumps({k: str(v) for k, v in expr_values.items()}, default=str)}")
        print(f"[update_daily_metrics] Failed rules to update: {failed_rules} (count: {len(failed_rules)})")
        print(f"[update_daily_metrics] Process type: {process_type}")
        print(f"[update_daily_metrics] Set parts count: {len(set_parts)}")
        print(f"[update_daily_metrics] Add parts count: {len(add_parts)}")
        
        # Atualizar registro
        try:
            print(f"[update_daily_metrics] Executando update_item...")
            table.update_item(
                Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names if expr_names else None,
                ExpressionAttributeValues=expr_values
            )
            print(f"[update_daily_metrics] ✓ Métricas diárias atualizadas com sucesso para {date_key}")
            
            # Verificar se foi salvo corretamente
            verify_response = table.get_item(
                Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'}
            )
            if 'Item' in verify_response:
                verify_item = verify_response['Item']
                print(f"[update_daily_metrics] Verificação pós-update:")
                print(f"  - total_count: {verify_item.get('total_count', 0)}")
                print(f"  - processes_by_type: {verify_item.get('processes_by_type', {})}")
                print(f"  - failed_rules: {verify_item.get('failed_rules', {})}")
        except Exception as update_error:
            print(f"[update_daily_metrics] ✗ ERRO ao atualizar métricas: {update_error}")
            print(f"[update_daily_metrics] UpdateExpression: {update_expr}")
            print(f"[update_daily_metrics] ExpressionAttributeNames: {expr_names}")
            print(f"[update_daily_metrics] ExpressionAttributeValues: {expr_values}")
            import traceback
            print(f"[update_daily_metrics] Traceback:\n{traceback.format_exc()}")
            raise
        
    except Exception as e:
        print(f'Erro específico em update_daily_metrics: {e}')
        raise

def update_monthly_metrics(month_key, status, processing_time, process_type='UNKNOWN'):
    """Atualiza métricas mensais"""
    
    try:
        # Verificar se item existe
        response = table.get_item(
            Key={'PK': f'METRICS#{month_key}', 'SK': 'MONTHLY_SUMMARY'}
        )
        
        if 'Item' not in response:
            # Criar item inicial
            table.put_item(
                Item={
                    'PK': f'METRICS#{month_key}',
                    'SK': 'MONTHLY_SUMMARY',
                    'total_count': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_time': Decimal('0'),
                    'processes_by_type': {}
                }
            )
        
        # Coletar todos os campos ADD em uma única lista
        add_parts = []
        expr_names = {}
        expr_values = {
            ':inc': 1,
            ':time': Decimal(str(processing_time))
        }
        
        # Campos numéricos simples (ADD)
        add_parts.append('total_count :inc')
        add_parts.append('total_time :time')
        
        if status == 'SUCCESS':
            add_parts.append('success_count :succ')
            expr_values[':succ'] = 1
        elif status == 'FAILED':
            add_parts.append('failed_count :fail')
            expr_values[':fail'] = 1
        
        # Contador por tipo de processo
        type_key = f'#t{len(expr_names)}'
        add_parts.append(f'processes_by_type.{type_key} :type_inc')
        expr_names[type_key] = process_type
        expr_values[':type_inc'] = 1
        
        # Construir UpdateExpression com uma única seção ADD
        update_expr = 'ADD ' + ', '.join(add_parts)
        
        table.update_item(
            Key={'PK': f'METRICS#{month_key}', 'SK': 'MONTHLY_SUMMARY'},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names if expr_names else None,
            ExpressionAttributeValues=expr_values
        )
        
    except Exception as e:
        print(f'Erro específico em update_monthly_metrics: {e}')
        raise