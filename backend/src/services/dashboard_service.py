import os
import boto3
from datetime import datetime, timedelta
from decimal import Decimal

class DashboardService:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ.get('TABLE_NAME', 'DocumentProcessorTable'))
    
    def get_dashboard_metrics(self, start_date=None, end_date=None):
        """Retorna métricas completas para dashboard
        
        Args:
            start_date: Data inicial no formato YYYY-MM-DD (opcional)
            end_date: Data final no formato YYYY-MM-DD (opcional)
        """
        if start_date and end_date:
            # Buscar por período específico
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            days_diff = (end - start).days + 1
            
            period_days = []
            total_time_period = 0
            total_count_period = 0
            
            for i in range(days_diff):
                date = (start + timedelta(days=i)).strftime('%Y-%m-%d')
                daily_metrics = self.get_metrics_by_date(date)
                if daily_metrics:
                    total_count = daily_metrics.get('total_count', 0)
                    avg_time = daily_metrics.get('avg_processing_time', 0)
                    total_time_period += avg_time * total_count  # Acumular tempo total
                    total_count_period += total_count
                    
                    period_days.append({
                        'date': date,
                        'total': total_count,
                        'success': daily_metrics.get('success_count', 0),
                        'failed': daily_metrics.get('failed_count', 0),
                        'success_rate': daily_metrics.get('success_rate', 0),
                        'avg_processing_time': avg_time,
                        'failed_rules': daily_metrics.get('failed_rules', {}),
                        'processes_by_type': daily_metrics.get('processes_by_type', {})
                    })
                else:
                    period_days.append({
                        'date': date,
                        'total': 0,
                        'success': 0,
                        'failed': 0,
                        'success_rate': 0,
                        'avg_processing_time': 0,
                        'failed_rules': {},
                        'processes_by_type': {}
                    })
            
            # Calcular métricas agregadas do período
            processes_by_type_period = {}
            failed_rules_period = {}
            
            for daily in period_days:
                # Agregar por tipo
                for proc_type, count in daily.get('processes_by_type', {}).items():
                    processes_by_type_period[proc_type] = processes_by_type_period.get(proc_type, 0) + count
                
                # Agregar regras que falharam
                for rule, count in daily.get('failed_rules', {}).items():
                    failed_rules_period[rule] = failed_rules_period.get(rule, 0) + count
            
            # Calcular tempo médio do período
            avg_processing_time_period = (total_time_period / total_count_period) if total_count_period > 0 else 0
            
            return {
                'period': period_days,
                'summary': {
                    'total': sum(d['total'] for d in period_days),
                    'success': sum(d['success'] for d in period_days),
                    'failed': sum(d['failed'] for d in period_days),
                    'success_rate': round((sum(d['success'] for d in period_days) / sum(d['total'] for d in period_days) * 100) if sum(d['total'] for d in period_days) > 0 else 0, 2),
                    'avg_processing_time': round(avg_processing_time_period, 2)
                },
                'processes_by_type': processes_by_type_period,
                'failed_rules': failed_rules_period,
                'start_date': start_date,
                'end_date': end_date
            }
        else:
            # Comportamento padrão: hoje e últimos 7 dias
            today = datetime.now().strftime('%Y-%m-%d')
        today_metrics = self.get_metrics_by_date(today)
        
        # Últimos 7 dias
        last_7_days = []
        total_time_week = 0
        total_count_week = 0
        
        for i in range(7):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            daily_metrics = self.get_metrics_by_date(date)
            if daily_metrics:
                total_count = daily_metrics.get('total_count', 0)
                avg_time = daily_metrics.get('avg_processing_time', 0)
                total_time_week += avg_time * total_count
                total_count_week += total_count
                
                last_7_days.append({
                    'date': date,
                    'total': total_count,
                    'success': daily_metrics.get('success_count', 0),
                    'failed': daily_metrics.get('failed_count', 0),
                    'success_rate': daily_metrics.get('success_rate', 0),
                    'avg_processing_time': avg_time,
                    'failed_rules': daily_metrics.get('failed_rules', {}),
                    'processes_by_type': daily_metrics.get('processes_by_type', {})
                })
        
        # Calcular métricas agregadas por tipo
        processes_by_type_week = {}
        failed_rules_week = {}
        
        for daily in last_7_days:
            # Agregar por tipo
            for proc_type, count in daily.get('processes_by_type', {}).items():
                processes_by_type_week[proc_type] = processes_by_type_week.get(proc_type, 0) + count
            
            # Agregar regras que falharam
            for rule, count in daily.get('failed_rules', {}).items():
                failed_rules_week[rule] = failed_rules_week.get(rule, 0) + count
        
        # Calcular tempo médio da semana
        avg_processing_time_week = (total_time_week / total_count_week) if total_count_week > 0 else 0
        
        return {
            'today': today_metrics,
            'last_7_days': last_7_days,
            'summary': {
                'total_week': sum(d['total'] for d in last_7_days),
                'success_week': sum(d['success'] for d in last_7_days),
                'failed_week': sum(d['failed'] for d in last_7_days),
                'success_rate_week': round((sum(d['success'] for d in last_7_days) / sum(d['total'] for d in last_7_days) * 100) if sum(d['total'] for d in last_7_days) > 0 else 0, 2),
                'avg_processing_time': round(avg_processing_time_week, 2)
            },
            'processes_by_type_week': processes_by_type_week,
            'failed_rules_week': failed_rules_week
        }
    
    def _get_raw_hourly(self, date):
        """Busca processes_by_hour raw (UTC) de uma data específica"""
        try:
            response = self.table.get_item(
                Key={'PK': f'METRICS#{date}', 'SK': 'SUMMARY'}
            )
            if 'Item' not in response:
                return {}
            item = response['Item']
            raw = {}
            if 'processes_by_hour' in item:
                for hour, count in item['processes_by_hour'].items():
                    try:
                        raw[int(hour)] = int(count)
                    except (ValueError, TypeError):
                        pass
            return raw
        except Exception:
            return {}
    
    def _convert_hourly_utc_to_brt(self, date):
        """
        Monta a distribuição por hora em BRT para uma data específica.
        
        Para montar um dia BRT completo:
        - Horas 3-23 UTC do mesmo dia → BRT 0-20
        - Horas 0-2 UTC do dia seguinte → BRT 21-23
        """
        BRT_OFFSET = -3
        
        # Buscar dados raw UTC do dia atual e do dia seguinte
        current_date = datetime.strptime(date, '%Y-%m-%d')
        next_date = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        raw_current = self._get_raw_hourly(date)
        raw_next = self._get_raw_hourly(next_date)
        
        processes_by_hour = {}
        
        # Do dia UTC atual: horas 3-23 → BRT 0-20 (pertencem ao mesmo dia BRT)
        for hour_utc, count in raw_current.items():
            hour_brt = hour_utc + BRT_OFFSET
            if hour_brt >= 0:  # Só incluir se ainda pertence ao mesmo dia BRT
                key = str(hour_brt)
                processes_by_hour[key] = processes_by_hour.get(key, 0) + count
        
        # Do dia UTC seguinte: horas 0-2 → BRT 21-23 (pertencem a este dia BRT)
        for hour_utc, count in raw_next.items():
            hour_brt = hour_utc + BRT_OFFSET
            if hour_brt < 0:  # Só incluir se pertence ao dia BRT anterior (nosso dia alvo)
                hour_brt += 24
                key = str(hour_brt)
                processes_by_hour[key] = processes_by_hour.get(key, 0) + count
        
        return processes_by_hour

    def get_metrics_by_date(self, date):
        """Retorna métricas de uma data específica"""
        try:
            response = self.table.get_item(
                Key={'PK': f'METRICS#{date}', 'SK': 'SUMMARY'}
            )
            
            if 'Item' not in response:
                return {
                    'date': date,
                    'total_count': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'success_rate': 0,
                    'avg_processing_time': 0,
                    'processes_by_hour': {},
                    'failure_reasons': {},
                    'processes_by_type': {},
                    'failed_rules': {}
                }
            
            item = response['Item']
            
            # Converter Decimal para float
            total_count = int(item.get('total_count', 0))
            success_count = int(item.get('success_count', 0))
            failed_count = int(item.get('failed_count', 0))
            total_time = float(item.get('total_time', 0))
            
            # Calcular métricas derivadas
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            avg_time = (total_time / total_count) if total_count > 0 else 0
            
            # Converter horas UTC para BRT buscando dados do dia atual e seguinte
            processes_by_hour = self._convert_hourly_utc_to_brt(date)
            
            failure_reasons = {}
            if 'failure_reasons' in item:
                for reason, count in item['failure_reasons'].items():
                    failure_reasons[reason] = int(count)
            
            processes_by_type = {}
            if 'processes_by_type' in item:
                processes_by_type_dict = item['processes_by_type']
                print(f"DEBUG: processes_by_type raw: {processes_by_type_dict}, type: {type(processes_by_type_dict)}")
                if isinstance(processes_by_type_dict, dict):
                    for proc_type, count in processes_by_type_dict.items():
                        # Converter Decimal para int
                        try:
                            if isinstance(count, Decimal):
                                processes_by_type[proc_type] = int(count)
                            elif isinstance(count, (int, float)):
                                processes_by_type[proc_type] = int(count)
                            else:
                                processes_by_type[proc_type] = int(count) if str(count).isdigit() else 0
                        except Exception as e:
                            print(f"Erro ao converter processes_by_type[{proc_type}]: {e}")
                            processes_by_type[proc_type] = 0
                print(f"DEBUG: processes_by_type converted: {processes_by_type}")
            
            failed_rules = {}
            if 'failed_rules' in item:
                failed_rules_dict = item['failed_rules']
                print(f"DEBUG: failed_rules raw: {failed_rules_dict}, type: {type(failed_rules_dict)}")
                if isinstance(failed_rules_dict, dict):
                    for rule, count in failed_rules_dict.items():
                        # Converter Decimal para int
                        try:
                            if isinstance(count, Decimal):
                                failed_rules[rule] = int(count)
                            elif isinstance(count, (int, float)):
                                failed_rules[rule] = int(count)
                            else:
                                failed_rules[rule] = int(count) if str(count).isdigit() else 0
                        except Exception as e:
                            print(f"Erro ao converter failed_rules[{rule}]: {e}")
                            failed_rules[rule] = 0
                print(f"DEBUG: failed_rules converted: {failed_rules}")
            
            # Calcular taxa de sucesso por tipo
            success_by_type = {}
            failed_by_type = {}
            # Nota: Para calcular taxa por tipo, precisaríamos de mais dados
            # Por enquanto, apenas retornamos os totais por tipo
            
            return {
                'date': date,
                'total_count': total_count,
                'success_count': success_count,
                'failed_count': failed_count,
                'success_rate': round(success_rate, 2),
                'avg_processing_time': round(avg_time, 2),
                'processes_by_hour': processes_by_hour,
                'failure_reasons': failure_reasons,
                'processes_by_type': processes_by_type,
                'failed_rules': failed_rules
            }
            
        except Exception as e:
            print(f"Erro ao buscar métricas para {date}: {e}")
            return None