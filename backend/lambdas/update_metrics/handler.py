import json
import os
import boto3
from datetime import datetime, timezone
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

# Texto retorno Protheus quando a nota fica como pré-nota (classificação pendente) — alinhado ao PTP / SNS.
PRENOTA_MESSAGE_SNIPPET = "documento de entrada criado como pré-nota"


def _coerce_dict(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    return None


def protheus_response_indicates_prenota(event: dict, metadata_item: dict | None = None) -> bool:
    """True se a resposta Protheus indica documento gravado como pré-nota (mensagem padrão)."""
    texts = []

    def collect_from_obj(obj: dict):
        m = obj.get("message")
        if m is not None:
            texts.append(str(m))
        body = obj.get("body")
        if isinstance(body, dict) and body.get("message") is not None:
            texts.append(str(body["message"]))

    pr = _coerce_dict(event.get("protheus_response"))
    if pr:
        collect_from_obj(pr)

    pr2 = event.get("protheus_result")
    if isinstance(pr2, dict):
        payload = pr2.get("Payload")
        if isinstance(payload, dict):
            inner = _coerce_dict(payload.get("protheus_response"))
            if inner:
                collect_from_obj(inner)

    if metadata_item:
        inner = _coerce_dict(metadata_item.get("protheus_response"))
        if inner:
            collect_from_obj(inner)

    needle = PRENOTA_MESSAGE_SNIPPET.lower()
    for t in texts:
        if needle in t.lower():
            return True
    return False


def _truthy_prenota_flag(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    return str(val).strip().lower() in ("true", "1", "yes", "sim")


def _norm_digits(value: object) -> str:
    s = "".join(ch for ch in str(value or "") if ch.isdigit())
    return s


def _get_json_field(data: dict, *keys):
    cur = data
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _extract_failure_identity(process_id: str, metadata_item: dict | None = None) -> tuple[str, str]:
    """
    Identidade da falha para deduplicação no dashboard.
    Chave base: NF + CnpjFornecedor.
    """
    nf = ""
    cnpj = ""
    pk = f"PROCESS#{process_id}"
    try:
        resp = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': pk}
        )
        items = resp.get("Items", [])
    except Exception:
        items = []

    parsed_xml_items = [it for it in items if str(it.get("SK", "")).startswith("PARSED_XML")]
    if parsed_xml_items:
        parsed_xml_items.sort(key=lambda x: x.get("TIMESTAMP", 0), reverse=True)
        try:
            parsed_data = json.loads(parsed_xml_items[0].get("PARSED_DATA", "{}"))
            nf = str(parsed_data.get("numero_nota") or "").strip()
            cnpj = _norm_digits(_get_json_field(parsed_data, "emitente", "cnpj"))
        except Exception:
            pass

    if not nf:
        bedrock_item = next((it for it in items if it.get("SK") == "BEDROCK_EXTRACTION"), None)
        if bedrock_item and bedrock_item.get("EXTRACTED_FIELDS"):
            try:
                bd = json.loads(bedrock_item["EXTRACTED_FIELDS"])
                nf = str(bd.get("documento") or "").strip()
            except Exception:
                pass

    if not cnpj and metadata_item:
        input_json = metadata_item.get("INPUT_JSON")
        try:
            if isinstance(input_json, str):
                input_json = json.loads(input_json)
            cnpj = _norm_digits(_get_json_field(input_json or {}, "requestBody", "cnpjEmitente"))
        except Exception:
            pass

    if not cnpj:
        pedido_item = next((it for it in items if it.get("SK") == "PEDIDO_COMPRA_METADATA"), None)
        if pedido_item and pedido_item.get("METADADOS"):
            try:
                metadados = pedido_item.get("METADADOS")
                if isinstance(metadados, str):
                    metadados = json.loads(metadados)
                cnpj = _norm_digits(_get_json_field(metadados or {}, "requestBody", "cnpjEmitente"))
            except Exception:
                pass

    nf = nf or "UNKNOWN_NF"
    cnpj = cnpj or "UNKNOWN_CNPJ"
    return nf, cnpj


def _build_failure_keys(nf: str, cnpj: str, failed_rules: list[str], status: str) -> list[str]:
    """Monta chaves NF+CNPJ+tipo_erro (regra específica ou Outros)."""
    if status != "FAILED":
        return []
    error_tags = sorted(set([r for r in (failed_rules or []) if r])) or ["Outros"]
    return [f"{nf}|{cnpj}|{tag}" for tag in error_tags]


def lambda_handler(event, context):
    """
    Atualiza métricas de processamento no DynamoDB.
    
    Suporta deduplicação para reprocessamento:
    - Se o processo já teve métricas registradas (METRICS_STATUS existe no metadata),
      primeiro remove a contagem anterior antes de adicionar a nova.
    - Isso evita métricas duplicadas quando um processo é reprocessado.
    """
    
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
    
    # Buscar start_time e informações de métricas anteriores do DynamoDB
    start_time_str = None
    process_type = 'UNKNOWN'
    metadata_response = None
    previous_metrics_status = None  # Status da métrica anterior (para deduplicação)
    previous_metrics_date = None    # Data em que métricas foram registradas
    previous_failed_rules = []      # Regras que falharam anteriormente
    previous_failure_keys = []      # Chaves deduplicadas (NF|CNPJ|tipo_erro)
    previous_processing_time = 0    # Tempo de processamento anterior (para deduplicação de total_time)
    
    try:
        pk = f"PROCESS#{process_id}"
        metadata_response = table.get_item(
            Key={'PK': pk, 'SK': 'METADATA'}
        )
        
        if 'Item' in metadata_response:
            start_time_str = metadata_response['Item'].get('START_TIME')
            process_type = metadata_response['Item'].get('PROCESS_TYPE', 'UNKNOWN')
            
            # Verificar se já teve métricas registradas (para deduplicação)
            previous_metrics_status = metadata_response['Item'].get('METRICS_STATUS')
            previous_metrics_date = metadata_response['Item'].get('METRICS_DATE')
            previous_failed_rules_str = metadata_response['Item'].get('METRICS_FAILED_RULES', '[]')
            previous_failure_keys_str = metadata_response['Item'].get('METRICS_FAILURE_KEYS', '[]')
            
            try:
                previous_failed_rules = json.loads(previous_failed_rules_str) if isinstance(previous_failed_rules_str, str) else previous_failed_rules_str
            except:
                previous_failed_rules = []
            try:
                previous_failure_keys = json.loads(previous_failure_keys_str) if isinstance(previous_failure_keys_str, str) else previous_failure_keys_str
            except:
                previous_failure_keys = []
            
            # Recuperar tempo de processamento anterior (para decrementar total_time na deduplicação)
            prev_proc_time = metadata_response['Item'].get('METRICS_PROCESSING_TIME', 0)
            try:
                previous_processing_time = float(prev_proc_time) if prev_proc_time else 0
            except (ValueError, TypeError):
                previous_processing_time = 0
            
            print(f"Found start_time in DynamoDB: {start_time_str}")
            print(f"Found process_type: {process_type}")
            print(f"Previous metrics_status: {previous_metrics_status}")
            print(f"Previous metrics_date: {previous_metrics_date}")
            print(f"Previous failed_rules: {previous_failed_rules}")
            print(f"Previous failure_keys: {previous_failure_keys}")
            print(f"Previous processing_time: {previous_processing_time}s")
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

    meta_item = None
    if metadata_response and "Item" in metadata_response:
        meta_item = metadata_response["Item"]

    is_prenota = status == "SUCCESS" and protheus_response_indicates_prenota(event, meta_item)
    print(f"Pré-nota (IA) no sucesso: {is_prenota}")

    previous_is_prenota = False
    if previous_metrics_status == "SUCCESS" and meta_item is not None:
        previous_is_prenota = _truthy_prenota_flag(meta_item.get("METRICS_IS_PRENOTA"))
        print(f"Métricas anteriores eram pré-nota: {previous_is_prenota}")

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

    nf_numero, cnpj_fornecedor = _extract_failure_identity(process_id, meta_item)
    failure_keys = _build_failure_keys(nf_numero, cnpj_fornecedor, failed_rules, status)
    print(f"Failure identity: nf={nf_numero}, cnpj={cnpj_fornecedor}")
    print(f"Failure keys: {failure_keys}")
    
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
        # ============================================
        # DEDUPLICAÇÃO: Remover métricas anteriores se for reprocessamento
        # ============================================
        if previous_metrics_status and previous_metrics_date:
            print(f"\n[DEDUPLICAÇÃO] Processo já teve métricas registradas anteriormente")
            print(f"[DEDUPLICAÇÃO] Status anterior: {previous_metrics_status}, Data: {previous_metrics_date}")
            
            # Extrair date_key e month_key anteriores
            prev_date_key = previous_metrics_date[:10] if len(previous_metrics_date) >= 10 else previous_metrics_date
            prev_month_key = previous_metrics_date[:7] if len(previous_metrics_date) >= 7 else previous_metrics_date
            
            print(f"[DEDUPLICAÇÃO] Removendo métricas do dia {prev_date_key} e mês {prev_month_key}...")
            
            # Decrementar métricas diárias anteriores
            decrement_daily_metrics(
                prev_date_key,
                previous_metrics_status,
                process_type,
                previous_failed_rules,
                previous_failure_keys,
                previous_processing_time,
                previous_is_prenota,
            )
            print(f"✓ Métricas diárias anteriores decrementadas")
            
            # Decrementar métricas mensais anteriores
            decrement_monthly_metrics(
                prev_month_key,
                previous_metrics_status,
                process_type,
                previous_processing_time,
                previous_is_prenota,
            )
            print(f"✓ Métricas mensais anteriores decrementadas")
        
        # ============================================
        # Atualizar métricas com novo status
        # ============================================
        print("Chamando update_daily_metrics...")
        # Atualizar métricas diárias
        update_daily_metrics(
            date_key,
            status,
            processing_time,
            error_type,
            hour,
            process_type,
            failed_rules,
            failure_keys,
            process_id,
            is_prenota,
        )
        print("✓ update_daily_metrics concluído")
        
        print("Chamando update_monthly_metrics...")
        # Atualizar métricas mensais
        update_monthly_metrics(month_key, status, processing_time, process_type, is_prenota)
        print("✓ update_monthly_metrics concluído")
        
        # ============================================
        # Salvar informações de métricas no processo (para deduplicação futura)
        # ============================================
        print("Salvando informações de métricas no processo...")
        save_metrics_status(
            process_id,
            status,
            date_key,
            failed_rules,
            failure_keys,
            processing_time,
            is_prenota,
        )
        print("✓ Informações de métricas salvas no processo")
        
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
        'metrics_updated': True,
        'deduplicated': bool(previous_metrics_status),
        'previous_status': previous_metrics_status
    }

def save_metrics_status(
    process_id,
    status,
    date_key,
    failed_rules,
    failure_keys=None,
    processing_time=0,
    is_prenota=False,
):
    """Salva informações de métricas no processo para suportar deduplicação em reprocessamento"""
    
    pk = f"PROCESS#{process_id}"
    timestamp = datetime.now(timezone.utc).isoformat()
    prenota_flag = bool(is_prenota) if status == "SUCCESS" else False
    
    try:
        table.update_item(
            Key={'PK': pk, 'SK': 'METADATA'},
            UpdateExpression='SET METRICS_STATUS = :status, METRICS_DATE = :date, METRICS_FAILED_RULES = :rules, METRICS_FAILURE_KEYS = :fkeys, METRICS_UPDATED_AT = :timestamp, METRICS_PROCESSING_TIME = :proc_time, METRICS_IS_PRENOTA = :prenota',
            ExpressionAttributeValues={
                ':status': status,
                ':date': date_key,
                ':rules': json.dumps(failed_rules if failed_rules else []),
                ':fkeys': json.dumps(failure_keys if failure_keys else []),
                ':timestamp': timestamp,
                ':proc_time': Decimal(str(round(processing_time, 2))),
                ':prenota': prenota_flag,
            }
        )
        print(
            f"[save_metrics_status] Métricas salvas: status={status}, date={date_key}, "
            f"rules={failed_rules}, failure_keys={failure_keys}, "
            f"processing_time={processing_time}s, prenota={prenota_flag}"
        )
    except Exception as e:
        print(f"[save_metrics_status] Erro ao salvar: {e}")
        # Não falhar o processo por causa disso
        import traceback
        traceback.print_exc()


def decrement_daily_metrics(
    date_key,
    status,
    process_type,
    failed_rules=None,
    failure_keys=None,
    previous_processing_time=0,
    previous_is_prenota=False,
):
    """Decrementa métricas diárias (usado em reprocessamento para evitar duplicação)"""
    
    print(f"\n[decrement_daily_metrics] Decrementando métricas para {date_key}")
    print(
        f"[decrement_daily_metrics] Status: {status}, Process type: {process_type}, "
        f"Failed rules: {failed_rules}, Previous time: {previous_processing_time}s, "
        f"previous_is_prenota: {previous_is_prenota}"
    )
    
    failed_rules = failed_rules or []
    failure_keys = failure_keys or []

    try:
        response = table.get_item(Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'})
        if 'Item' not in response:
            print(f"[decrement_daily_metrics] Item não existe para {date_key}, nada a decrementar")
            return

        item = response['Item']
        item.setdefault('processes_by_type', {})
        item.setdefault('failed_rules', {})
        item.setdefault('failure_reasons', {})
        item.setdefault('failure_dedup_registry', {})

        item['total_count'] = max(int(item.get('total_count', 0) or 0) - 1, 0)
        if previous_processing_time > 0:
            old_total_time = Decimal(str(item.get('total_time', 0) or 0))
            item['total_time'] = max(
                old_total_time - Decimal(str(round(previous_processing_time, 2))),
                Decimal('0'),
            )

        if status == 'SUCCESS':
            item['success_count'] = max(int(item.get('success_count', 0) or 0) - 1, 0)
            if previous_is_prenota:
                item['success_prenota_count'] = max(int(item.get('success_prenota_count', 0) or 0) - 1, 0)

        if process_type:
            current = int((item.get('processes_by_type') or {}).get(process_type, 0) or 0)
            if current > 0:
                item['processes_by_type'][process_type] = current - 1

        # Dedup de falhas por chave NF+CNPJ+tipo_erro
        registry = item.get('failure_dedup_registry') or {}
        if status == 'FAILED':
            for key in failure_keys:
                if key in registry:
                    registry.pop(key, None)
                    item['failed_count'] = max(int(item.get('failed_count', 0) or 0) - 1, 0)
                    error_tag = key.split('|')[-1] if '|' in key else 'Outros'
                    rule_val = int((item.get('failed_rules') or {}).get(error_tag, 0) or 0)
                    if rule_val > 0:
                        item['failed_rules'][error_tag] = rule_val - 1
            item['failure_dedup_registry'] = registry

            # Compatibilidade com métricas antigas sem keys
            if not failure_keys and int(item.get('failed_count', 0) or 0) > 0:
                item['failed_count'] = max(int(item.get('failed_count', 0) or 0) - 1, 0)
                for rule_name in failed_rules:
                    rv = int((item.get('failed_rules') or {}).get(rule_name, 0) or 0)
                    if rv > 0:
                        item['failed_rules'][rule_name] = rv - 1

        table.put_item(Item=item)
        print(f"[decrement_daily_metrics] ✓ Métricas diárias decrementadas para {date_key}")

    except Exception as e:
        print(f"[decrement_daily_metrics] Erro: {e}")
        import traceback
        traceback.print_exc()


def decrement_monthly_metrics(
    month_key, status, process_type, previous_processing_time=0, previous_is_prenota=False
):
    """Decrementa métricas mensais (usado em reprocessamento para evitar duplicação)"""
    
    print(f"\n[decrement_monthly_metrics] Decrementando métricas para {month_key}")
    print(
        f"[decrement_monthly_metrics] Status: {status}, Process type: {process_type}, "
        f"Previous time: {previous_processing_time}s, previous_is_prenota: {previous_is_prenota}"
    )
    
    try:
        # Verificar se o item existe
        response = table.get_item(
            Key={'PK': f'METRICS#{month_key}', 'SK': 'MONTHLY_SUMMARY'}
        )
        
        if 'Item' not in response:
            print(f"[decrement_monthly_metrics] Item não existe para {month_key}, nada a decrementar")
            return
        
        existing_item = response['Item']
        
        # Construir expressão de atualização
        add_parts = []
        expr_names = {}
        expr_values = {
            ':dec': -1
        }
        
        # Decrementar total_count
        add_parts.append('total_count :dec')
        
        # Decrementar total_time com o tempo de processamento anterior
        if previous_processing_time > 0:
            dec_time = Decimal(str(round(previous_processing_time, 2))) * Decimal('-1')
            expr_values[':dec_time'] = dec_time
            add_parts.append('total_time :dec_time')
            print(f"[decrement_monthly_metrics] Decrementando total_time em {previous_processing_time}s")
        
        # Decrementar contador de status
        if status == 'SUCCESS':
            add_parts.append('success_count :dec')
            if previous_is_prenota:
                sp = int(existing_item.get('success_prenota_count', 0) or 0)
                if sp > 0:
                    add_parts.append('success_prenota_count :dec')
        elif status == 'FAILED':
            add_parts.append('failed_count :dec')
        
        # Decrementar por tipo de processo
        if process_type and existing_item.get('processes_by_type', {}).get(process_type):
            type_key = '#t0'
            add_parts.append(f'processes_by_type.{type_key} :dec')
            expr_names[type_key] = process_type
        
        update_expr = 'ADD ' + ', '.join(add_parts)
        
        print(f"[decrement_monthly_metrics] UpdateExpression: {update_expr}")
        
        table.update_item(
            Key={'PK': f'METRICS#{month_key}', 'SK': 'MONTHLY_SUMMARY'},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names if expr_names else None,
            ExpressionAttributeValues=expr_values
        )
        
        print(f"[decrement_monthly_metrics] ✓ Métricas mensais decrementadas para {month_key}")
        
    except Exception as e:
        print(f"[decrement_monthly_metrics] Erro: {e}")
        import traceback
        traceback.print_exc()
        # Não falhar - apenas log


def update_daily_metrics(
    date_key,
    status,
    processing_time,
    error_type,
    hour,
    process_type='UNKNOWN',
    failed_rules=None,
    failure_keys=None,
    process_id=None,
    is_prenota=False,
):
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
    print(f"  - is_prenota: {is_prenota}")
    
    failed_rules = failed_rules or []
    failure_keys = failure_keys or []

    try:
        response = table.get_item(Key={'PK': f'METRICS#{date_key}', 'SK': 'SUMMARY'})
        if 'Item' not in response:
            item = {
                'PK': f'METRICS#{date_key}',
                'SK': 'SUMMARY',
                'total_count': 0,
                'success_count': 0,
                'success_prenota_count': 0,
                'failed_count': 0,
                'total_time': Decimal('0'),
                'processes_by_hour': {},
                'failure_reasons': {},
                'processes_by_type': {},
                'failed_rules': {},
                'failure_dedup_registry': {},
            }
        else:
            item = response['Item']
            item.setdefault('processes_by_hour', {})
            item.setdefault('failure_reasons', {})
            item.setdefault('processes_by_type', {})
            item.setdefault('failed_rules', {})
            item.setdefault('failure_dedup_registry', {})
            if 'success_prenota_count' not in item:
                item['success_prenota_count'] = 0

        item['total_count'] = int(item.get('total_count', 0) or 0) + 1
        item['total_time'] = Decimal(str(item.get('total_time', 0) or 0)) + Decimal(str(processing_time))

        if status == 'SUCCESS':
            item['success_count'] = int(item.get('success_count', 0) or 0) + 1
            if is_prenota:
                item['success_prenota_count'] = int(item.get('success_prenota_count', 0) or 0) + 1

        hour_key = str(hour)
        item['processes_by_hour'][hour_key] = int((item['processes_by_hour'] or {}).get(hour_key, 0) or 0) + 1

        if process_type:
            item['processes_by_type'][process_type] = int((item['processes_by_type'] or {}).get(process_type, 0) or 0) + 1

        if status == 'FAILED' and error_type:
            item['failure_reasons'][error_type] = int((item['failure_reasons'] or {}).get(error_type, 0) or 0) + 1

        if status == 'FAILED':
            registry = item.get('failure_dedup_registry') or {}
            for key in failure_keys:
                if key in registry:
                    continue
                registry[key] = process_id or "unknown_process"
                item['failed_count'] = int(item.get('failed_count', 0) or 0) + 1
                error_tag = key.split('|')[-1] if '|' in key else 'Outros'
                item['failed_rules'][error_tag] = int((item['failed_rules'] or {}).get(error_tag, 0) or 0) + 1
            item['failure_dedup_registry'] = registry

        table.put_item(Item=item)
        print(f"[update_daily_metrics] ✓ Métricas diárias atualizadas com sucesso para {date_key}")
    except Exception as e:
        print(f'Erro específico em update_daily_metrics: {e}')
        raise

def update_monthly_metrics(month_key, status, processing_time, process_type='UNKNOWN', is_prenota=False):
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
                    'success_prenota_count': 0,
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
            if is_prenota:
                add_parts.append('success_prenota_count :pren')
                expr_values[':pren'] = 1
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