#!/usr/bin/env python3
"""
Script para recalcular todas as métricas diárias e mensais a partir dos processos no DynamoDB.

Este script:
1. Busca todos os processos (PK=PROCESS, SK begins_with PROCESS#)
2. Para cada processo, busca o METADATA (status, tipo, timestamps)
3. Recalcula métricas diárias e mensais do zero
4. Substitui os registros de métricas no DynamoDB

Uso:
    python3 fix_metrics.py --table-name <NOME_TABELA> [--region us-east-1] [--dry-run]

Exemplos:
    # Apenas listar o que será feito (sem alterar nada)
    python3 fix_metrics.py --table-name tabela-document-processor-dev --dry-run

    # Executar correção
    python3 fix_metrics.py --table-name tabela-document-processor-dev
"""

import boto3
import argparse
import json
import sys
from collections import defaultdict
from decimal import Decimal
from datetime import datetime, timezone


def get_all_processes(table):
    """Busca todos os processos da tabela"""
    print("📋 Buscando todos os processos...")
    
    response = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={
            ':pk': 'PROCESS',
            ':sk': 'PROCESS#'
        }
    )
    
    items = response.get('Items', [])
    
    # Paginar se necessário
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
            ExpressionAttributeValues={
                ':pk': 'PROCESS',
                ':sk': 'PROCESS#'
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))
    
    print(f"   Encontrados {len(items)} processos")
    return items


def get_process_metadata(table, process_id):
    """Busca metadata de um processo"""
    response = table.get_item(
        Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'}
    )
    return response.get('Item')


def get_validation_results(table, process_id):
    """Busca resultados de validação de um processo"""
    response = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={
            ':pk': f'PROCESS#{process_id}',
            ':sk': 'VALIDATION#'
        }
    )
    
    items = response.get('Items', [])
    if not items:
        return []
    
    # Pegar o mais recente
    latest = max(items, key=lambda x: x.get('TIMESTAMP', 0))
    validation_results_str = latest.get('VALIDATION_RESULTS', '[]')
    
    try:
        results = json.loads(validation_results_str) if isinstance(validation_results_str, str) else validation_results_str
        return results
    except:
        return []


def get_existing_metrics(table):
    """Busca todas as métricas existentes (para listar antes de substituir)"""
    daily_metrics = []
    monthly_metrics = []
    
    # Scan para encontrar todos os registros de métricas
    response = table.scan(
        FilterExpression='begins_with(PK, :prefix)',
        ExpressionAttributeValues={':prefix': 'METRICS#'}
    )
    
    items = response.get('Items', [])
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression='begins_with(PK, :prefix)',
            ExpressionAttributeValues={':prefix': 'METRICS#'},
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))
    
    for item in items:
        pk = item.get('PK', '')
        sk = item.get('SK', '')
        if sk == 'SUMMARY':
            daily_metrics.append(item)
        elif sk == 'MONTHLY_SUMMARY':
            monthly_metrics.append(item)
    
    return daily_metrics, monthly_metrics


def calculate_processing_time(metadata):
    """Calcula o tempo de processamento a partir dos timestamps do metadata"""
    start_time_str = metadata.get('START_TIME')
    
    # Tentar pegar o updated_at como end_time (momento em que o processo terminou)
    updated_at = metadata.get('updated_at') or metadata.get('UPDATED_AT')
    
    if not start_time_str:
        return 30.0  # Padrão se não houver start_time
    
    try:
        # Parse start_time
        if isinstance(start_time_str, str) and 'T' in start_time_str:
            if start_time_str.endswith('Z'):
                start_time_str_parsed = start_time_str[:-1] + '+00:00'
            elif '+' in start_time_str or start_time_str.count('-') >= 3:
                start_time_str_parsed = start_time_str
            else:
                start_time_str_parsed = start_time_str + '+00:00'
            start_time = datetime.fromisoformat(start_time_str_parsed)
        elif isinstance(start_time_str, (int, float)) or (isinstance(start_time_str, str) and start_time_str.replace('.', '').isdigit()):
            start_time = datetime.fromtimestamp(float(start_time_str), tz=timezone.utc)
        else:
            return 30.0
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        
        # Parse end_time
        if updated_at:
            if isinstance(updated_at, str) and 'T' in updated_at:
                if updated_at.endswith('Z'):
                    updated_at_parsed = updated_at[:-1] + '+00:00'
                elif '+' in updated_at or updated_at.count('-') >= 3:
                    updated_at_parsed = updated_at
                else:
                    updated_at_parsed = updated_at + '+00:00'
                end_time = datetime.fromisoformat(updated_at_parsed)
            else:
                end_time = datetime.now(timezone.utc)
        else:
            end_time = datetime.now(timezone.utc)
        
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        processing_time = (end_time - start_time).total_seconds()
        
        if processing_time < 0:
            processing_time = abs(processing_time) if abs(processing_time) < 3600 else 30.0
        elif processing_time > 86400:
            processing_time = 30.0
        
        return round(processing_time, 2)
    except:
        return 30.0


def determine_status(metadata):
    """Determina o status final do processo para métricas"""
    status = metadata.get('STATUS', 'UNKNOWN')
    metrics_status = metadata.get('METRICS_STATUS')
    
    # Se tem METRICS_STATUS, usar como referência (é o mais preciso)
    if metrics_status:
        return metrics_status
    
    # Mapear status do processo para status de métricas
    status_map = {
        'COMPLETED': 'SUCCESS',
        'SUCCESS': 'SUCCESS',
        'VALIDATED': 'SUCCESS',
        'FAILED': 'FAILED',
        'VALIDATION_FAILURE': 'FAILED',
        'PROCESSING': None,  # Não contabilizar
        'CREATED': None,     # Não contabilizar
    }
    
    return status_map.get(status, None)


def main():
    parser = argparse.ArgumentParser(description='Recalcula métricas do DynamoDB a partir dos processos')
    parser.add_argument('--table-name', required=True, help='Nome da tabela DynamoDB')
    parser.add_argument('--region', default='us-east-1', help='Região AWS (default: us-east-1)')
    parser.add_argument('--dry-run', action='store_true', help='Apenas mostrar o que seria feito, sem alterar')
    
    args = parser.parse_args()
    
    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(args.table_name)
    
    print(f"\n{'='*80}")
    print(f"  RECALCULAR MÉTRICAS - {args.table_name}")
    print(f"  Região: {args.region}")
    print(f"  Modo: {'🔍 DRY-RUN (sem alterações)' if args.dry_run else '⚡ EXECUÇÃO REAL'}")
    print(f"{'='*80}\n")
    
    # 1. Buscar métricas existentes (para comparação)
    print("📊 Buscando métricas existentes...")
    existing_daily, existing_monthly = get_existing_metrics(table)
    print(f"   Métricas diárias existentes: {len(existing_daily)}")
    print(f"   Métricas mensais existentes: {len(existing_monthly)}")
    
    for m in sorted(existing_daily, key=lambda x: x.get('PK', '')):
        date = m.get('PK', '').replace('METRICS#', '')
        total = int(m.get('total_count', 0))
        success = int(m.get('success_count', 0))
        failed = int(m.get('failed_count', 0))
        total_time = float(m.get('total_time', 0))
        avg = round(total_time / total, 2) if total > 0 else 0
        print(f"   📅 {date}: total={total}, success={success}, failed={failed}, total_time={total_time:.1f}s, avg={avg:.1f}s")
    
    # 2. Buscar todos os processos
    processes = get_all_processes(table)
    
    # 3. Agregar dados por dia e mês
    # Estrutura: daily_data[date_key] = { total_count, success_count, failed_count, total_time, processes_by_hour, processes_by_type, failed_rules, failure_reasons }
    daily_data = defaultdict(lambda: {
        'total_count': 0,
        'success_count': 0,
        'failed_count': 0,
        'total_time': Decimal('0'),
        'processes_by_hour': defaultdict(int),
        'processes_by_type': defaultdict(int),
        'failed_rules': defaultdict(int),
        'failure_reasons': defaultdict(int)
    })
    
    monthly_data = defaultdict(lambda: {
        'total_count': 0,
        'success_count': 0,
        'failed_count': 0,
        'total_time': Decimal('0'),
        'processes_by_type': defaultdict(int)
    })
    
    processed_count = 0
    skipped_count = 0
    
    print(f"\n🔄 Processando {len(processes)} processos...\n")
    
    for proc in processes:
        process_id = proc.get('PROCESS_ID')
        if not process_id:
            skipped_count += 1
            continue
        
        metadata = get_process_metadata(table, process_id)
        if not metadata:
            print(f"   ⚠️  {process_id}: sem metadata, pulando")
            skipped_count += 1
            continue
        
        # Determinar status para métricas
        metric_status = determine_status(metadata)
        if not metric_status:
            status = metadata.get('STATUS', 'UNKNOWN')
            print(f"   ⏭️  {process_id}: status={status}, não contabilizar")
            skipped_count += 1
            continue
        
        # Determinar data/hora do processo
        process_type = metadata.get('PROCESS_TYPE', 'UNKNOWN')
        timestamp = metadata.get('TIMESTAMP', 0)
        metrics_date = metadata.get('METRICS_DATE')
        
        # Usar METRICS_DATE se existir (é o mais preciso), senão calcular do TIMESTAMP
        if metrics_date and len(metrics_date) >= 10:
            date_key = metrics_date[:10]
        elif timestamp:
            try:
                dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
                date_key = dt.strftime('%Y-%m-%d')
            except:
                date_key = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        else:
            date_key = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        month_key = date_key[:7]
        
        # Calcular hora
        try:
            if timestamp:
                dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
                hour = dt.hour
            else:
                hour = 0
        except:
            hour = 0
        
        # Calcular tempo de processamento
        # Priorizar METRICS_PROCESSING_TIME salvo (mais preciso)
        saved_proc_time = metadata.get('METRICS_PROCESSING_TIME')
        if saved_proc_time is not None:
            try:
                processing_time = float(saved_proc_time)
            except:
                processing_time = calculate_processing_time(metadata)
        else:
            processing_time = calculate_processing_time(metadata)
        
        # Buscar regras que falharam
        failed_rules = []
        if metric_status == 'FAILED':
            # Tentar pegar do METRICS_FAILED_RULES salvo
            saved_rules_str = metadata.get('METRICS_FAILED_RULES', '[]')
            try:
                saved_rules = json.loads(saved_rules_str) if isinstance(saved_rules_str, str) else saved_rules_str
                if saved_rules:
                    failed_rules = saved_rules
            except:
                pass
            
            # Se não tem salvo, buscar do DynamoDB
            if not failed_rules:
                validation_results = get_validation_results(table, process_id)
                for rule in validation_results:
                    if rule.get('status') == 'FAILED':
                        failed_rules.append(rule.get('rule', 'UNKNOWN'))
        
        # Agregar métricas diárias
        daily = daily_data[date_key]
        daily['total_count'] += 1
        daily['total_time'] += Decimal(str(processing_time))
        daily['processes_by_hour'][str(hour)] += 1
        daily['processes_by_type'][process_type] += 1
        
        if metric_status == 'SUCCESS':
            daily['success_count'] += 1
        elif metric_status == 'FAILED':
            daily['failed_count'] += 1
            daily['failure_reasons']['VALIDATION_FAILURE'] += 1
            for rule in failed_rules:
                daily['failed_rules'][rule] += 1
        
        # Agregar métricas mensais
        monthly = monthly_data[month_key]
        monthly['total_count'] += 1
        monthly['total_time'] += Decimal(str(processing_time))
        monthly['processes_by_type'][process_type] += 1
        
        if metric_status == 'SUCCESS':
            monthly['success_count'] += 1
        elif metric_status == 'FAILED':
            monthly['failed_count'] += 1
        
        processed_count += 1
        print(f"   ✅ {process_id[:20]}... | {date_key} | {metric_status:7s} | {process_type:12s} | {processing_time:.1f}s")
    
    # 4. Mostrar resultado
    print(f"\n{'='*80}")
    print(f"  RESUMO DA RECALCULAÇÃO")
    print(f"{'='*80}")
    print(f"  Processos encontrados: {len(processes)}")
    print(f"  Processos contabilizados: {processed_count}")
    print(f"  Processos ignorados: {skipped_count}")
    print(f"  Dias com métricas: {len(daily_data)}")
    print(f"  Meses com métricas: {len(monthly_data)}")
    
    print(f"\n📊 MÉTRICAS DIÁRIAS RECALCULADAS:")
    print(f"{'─'*80}")
    
    for date_key in sorted(daily_data.keys()):
        d = daily_data[date_key]
        total = d['total_count']
        success = d['success_count']
        failed = d['failed_count']
        total_time = float(d['total_time'])
        avg = round(total_time / total, 2) if total > 0 else 0
        rate = round((success / total * 100), 2) if total > 0 else 0
        types = dict(d['processes_by_type'])
        rules = dict(d['failed_rules'])
        
        print(f"  📅 {date_key}:")
        print(f"     Total: {total} | Sucesso: {success} | Falha: {failed} | Taxa sucesso: {rate}%")
        print(f"     Tempo total: {total_time:.1f}s | Tempo médio: {avg:.1f}s")
        print(f"     Tipos: {types}")
        if rules:
            print(f"     Regras falhas: {rules}")
    
    print(f"\n📊 MÉTRICAS MENSAIS RECALCULADAS:")
    print(f"{'─'*80}")
    
    for month_key in sorted(monthly_data.keys()):
        m = monthly_data[month_key]
        total = m['total_count']
        success = m['success_count']
        failed = m['failed_count']
        total_time = float(m['total_time'])
        avg = round(total_time / total, 2) if total > 0 else 0
        rate = round((success / total * 100), 2) if total > 0 else 0
        types = dict(m['processes_by_type'])
        
        print(f"  📅 {month_key}:")
        print(f"     Total: {total} | Sucesso: {success} | Falha: {failed} | Taxa sucesso: {rate}%")
        print(f"     Tempo total: {total_time:.1f}s | Tempo médio: {avg:.1f}s")
        print(f"     Tipos: {types}")
    
    # Calcular totais gerais
    grand_total = sum(d['total_count'] for d in daily_data.values())
    grand_success = sum(d['success_count'] for d in daily_data.values())
    grand_failed = sum(d['failed_count'] for d in daily_data.values())
    grand_time = sum(float(d['total_time']) for d in daily_data.values())
    grand_avg = round(grand_time / grand_total, 2) if grand_total > 0 else 0
    grand_rate = round((grand_success / grand_total * 100), 2) if grand_total > 0 else 0
    
    print(f"\n📊 TOTAIS GERAIS:")
    print(f"{'─'*80}")
    print(f"  Total: {grand_total} | Sucesso: {grand_success} | Falha: {grand_failed}")
    print(f"  Taxa de sucesso: {grand_rate}%")
    print(f"  Tempo médio: {grand_avg}s")
    
    # 5. Identificar métricas que serão deletadas (existem no DynamoDB mas não nos dados recalculados)
    existing_daily_dates = set()
    for m in existing_daily:
        date = m.get('PK', '').replace('METRICS#', '')
        existing_daily_dates.add(date)
    
    existing_monthly_dates = set()
    for m in existing_monthly:
        date = m.get('PK', '').replace('METRICS#', '')
        existing_monthly_dates.add(date)
    
    stale_daily = existing_daily_dates - set(daily_data.keys())
    stale_monthly = existing_monthly_dates - set(monthly_data.keys())
    
    if stale_daily:
        print(f"\n🗑️  MÉTRICAS DIÁRIAS OBSOLETAS (serão removidas):")
        for date in sorted(stale_daily):
            print(f"   ❌ {date} (não tem processos associados)")
    
    if stale_monthly:
        print(f"\n🗑️  MÉTRICAS MENSAIS OBSOLETAS (serão removidas):")
        for date in sorted(stale_monthly):
            print(f"   ❌ {date} (não tem processos associados)")
    
    # 6. Aplicar (ou não, se dry-run)
    if args.dry_run:
        print(f"\n🔍 DRY-RUN: Nenhuma alteração foi feita. Execute sem --dry-run para aplicar.")
        return
    
    print(f"\n⚡ Aplicando métricas recalculadas...")
    
    # Deletar métricas diárias obsoletas
    for date in sorted(stale_daily):
        table.delete_item(Key={'PK': f'METRICS#{date}', 'SK': 'SUMMARY'})
        print(f"   🗑️  Métricas diárias {date} removidas (obsoleta)")
    
    # Deletar métricas mensais obsoletas
    for date in sorted(stale_monthly):
        table.delete_item(Key={'PK': f'METRICS#{date}', 'SK': 'MONTHLY_SUMMARY'})
        print(f"   🗑️  Métricas mensais {date} removidas (obsoleta)")
    
    # Escrever métricas diárias
    for date_key in sorted(daily_data.keys()):
        d = daily_data[date_key]
        
        item = {
            'PK': f'METRICS#{date_key}',
            'SK': 'SUMMARY',
            'total_count': d['total_count'],
            'success_count': d['success_count'],
            'failed_count': d['failed_count'],
            'total_time': d['total_time'],
            'processes_by_hour': {k: v for k, v in d['processes_by_hour'].items()},
            'processes_by_type': {k: v for k, v in d['processes_by_type'].items()},
            'failed_rules': {k: v for k, v in d['failed_rules'].items()},
            'failure_reasons': {k: v for k, v in d['failure_reasons'].items()}
        }
        
        table.put_item(Item=item)
        print(f"   ✅ Métricas diárias {date_key} atualizadas")
    
    # Escrever métricas mensais
    for month_key in sorted(monthly_data.keys()):
        m = monthly_data[month_key]
        
        item = {
            'PK': f'METRICS#{month_key}',
            'SK': 'MONTHLY_SUMMARY',
            'total_count': m['total_count'],
            'success_count': m['success_count'],
            'failed_count': m['failed_count'],
            'total_time': m['total_time'],
            'processes_by_type': {k: v for k, v in m['processes_by_type'].items()}
        }
        
        table.put_item(Item=item)
        print(f"   ✅ Métricas mensais {month_key} atualizadas")
    
    # Atualizar METRICS_PROCESSING_TIME de todos os processos (para deduplicação futura funcionar)
    print(f"\n🔄 Atualizando METRICS_PROCESSING_TIME nos processos...")
    updated_proc = 0
    for proc in processes:
        process_id = proc.get('PROCESS_ID')
        if not process_id:
            continue
        
        metadata = get_process_metadata(table, process_id)
        if not metadata:
            continue
        
        metric_status = determine_status(metadata)
        if not metric_status:
            continue
        
        # Se não tem METRICS_PROCESSING_TIME, calcular e salvar
        if metadata.get('METRICS_PROCESSING_TIME') is None:
            processing_time = calculate_processing_time(metadata)
            
            # Determinar date_key
            metrics_date = metadata.get('METRICS_DATE')
            timestamp = metadata.get('TIMESTAMP', 0)
            if metrics_date and len(metrics_date) >= 10:
                date_key = metrics_date[:10]
            elif timestamp:
                try:
                    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
                    date_key = dt.strftime('%Y-%m-%d')
                except:
                    date_key = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            else:
                date_key = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            # Buscar failed_rules
            failed_rules = []
            saved_rules_str = metadata.get('METRICS_FAILED_RULES', '[]')
            try:
                saved_rules = json.loads(saved_rules_str) if isinstance(saved_rules_str, str) else saved_rules_str
                if saved_rules:
                    failed_rules = saved_rules
            except:
                pass
            
            try:
                table.update_item(
                    Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
                    UpdateExpression='SET METRICS_STATUS = :status, METRICS_DATE = :date, METRICS_PROCESSING_TIME = :proc_time, METRICS_UPDATED_AT = :timestamp',
                    ExpressionAttributeValues={
                        ':status': metric_status,
                        ':date': date_key,
                        ':proc_time': Decimal(str(round(processing_time, 2))),
                        ':timestamp': datetime.now(timezone.utc).isoformat()
                    }
                )
                updated_proc += 1
            except Exception as e:
                print(f"   ⚠️  Erro ao atualizar {process_id}: {e}")
    
    print(f"   ✅ {updated_proc} processos atualizados com METRICS_PROCESSING_TIME")
    
    print(f"\n{'='*80}")
    print(f"  ✅ MÉTRICAS RECALCULADAS COM SUCESSO!")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()

