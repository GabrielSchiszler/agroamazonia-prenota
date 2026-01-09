#!/usr/bin/env python3
"""
Script para limpar TODOS os processos, resultados, validações, etc. do DynamoDB
ATENÇÃO: Esta operação é irreversível!
"""
import os
import sys
import boto3

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configurar DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = os.environ.get('TABLE_NAME', 'DocumentProcessorTable')
table = dynamodb.Table(table_name)

def scan_all_processes():
    """Busca todos os PKs que começam com PROCESS#"""
    print("🔍 Buscando todos os processos...")
    
    processes = []
    last_evaluated_key = None
    
    while True:
        if last_evaluated_key:
            response = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan()
        
        # Filtrar apenas itens que começam com PROCESS#
        for item in response.get('Items', []):
            if item['PK'].startswith('PROCESS#'):
                processes.append(item)
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    # Agrupar por PK
    processes_by_pk = {}
    for item in processes:
        pk = item['PK']
        if pk not in processes_by_pk:
            processes_by_pk[pk] = []
        processes_by_pk[pk].append(item)
    
    return processes_by_pk

def scan_all_metrics():
    """Busca todos os registros de métricas"""
    print("🔍 Buscando registros de métricas...")
    
    metrics = []
    last_evaluated_key = None
    
    while True:
        if last_evaluated_key:
            response = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan()
        
        # Filtrar apenas itens que começam com METRICS#
        for item in response.get('Items', []):
            if item['PK'].startswith('METRICS#'):
                metrics.append(item)
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    return metrics

def clean_all_processes():
    """Limpa todos os processos e resultados"""
    print("="*80)
    print("🧹 LIMPEZA DE PROCESSOS E RESULTADOS")
    print("="*80)
    print("\n⚠️  ATENÇÃO: Esta operação irá DELETAR:")
    print("   - Todos os processos (PROCESS#*)")
    print("   - Todos os metadados de processos")
    print("   - Todos os arquivos (FILE#*)")
    print("   - Todos os resultados de parsing (PARSED_XML, PARSED_OCR)")
    print("   - Todas as validações (VALIDATION#*)")
    print("   - Todos os resultados do Textract (TEXTRACT#*)")
    print("   - Todas as métricas (METRICS#*)")
    print("\n⚠️  Esta operação é IRREVERSÍVEL!\n")
    
    resposta = input("Digite 'LIMPAR TUDO' para confirmar: ")
    
    if resposta != 'LIMPAR TUDO':
        print("❌ Operação cancelada.")
        return
    
    print("\n" + "="*80)
    print("🗑️  Iniciando limpeza...")
    print("="*80 + "\n")
    
    # 1. Limpar processos
    print("📋 [1/2] Limpando processos...")
    processes_by_pk = scan_all_processes()
    
    if not processes_by_pk:
        print("   ℹ️  Nenhum processo encontrado.")
    else:
        print(f"   📊 Total de processos encontrados: {len(processes_by_pk)}")
        
        deleted_processes = 0
        deleted_items = 0
        error_count = 0
        
        for pk, items in processes_by_pk.items():
            try:
                process_id = pk.replace('PROCESS#', '')
                print(f"\n   🗑️  Deletando processo: {process_id}")
                print(f"      Itens a deletar: {len(items)}")
                
                # Deletar todos os itens deste processo
                for item in items:
                    try:
                        table.delete_item(
                            Key={
                                'PK': item['PK'],
                                'SK': item['SK']
                            }
                        )
                        deleted_items += 1
                    except Exception as e:
                        print(f"      ❌ Erro ao deletar {item['SK']}: {e}")
                        error_count += 1
                
                deleted_processes += 1
                print(f"      ✅ Processo deletado ({len(items)} itens)")
                
            except Exception as e:
                print(f"   ❌ Erro ao processar {pk}: {e}")
                error_count += 1
        
        print(f"\n   📊 Resumo de processos:")
        print(f"      ✅ Processos deletados: {deleted_processes}")
        print(f"      ✅ Itens deletados: {deleted_items}")
        if error_count > 0:
            print(f"      ❌ Erros: {error_count}")
    
    # 2. Limpar métricas
    print("\n📊 [2/2] Limpando métricas...")
    metrics = scan_all_metrics()
    
    if not metrics:
        print("   ℹ️  Nenhuma métrica encontrada.")
    else:
        print(f"   📊 Total de registros de métricas encontrados: {len(metrics)}")
        
        deleted_metrics = 0
        error_count = 0
        
        for item in metrics:
            try:
                table.delete_item(
                    Key={
                        'PK': item['PK'],
                        'SK': item['SK']
                    }
                )
                deleted_metrics += 1
            except Exception as e:
                print(f"   ❌ Erro ao deletar métrica {item['SK']}: {e}")
                error_count += 1
        
        print(f"\n   📊 Resumo de métricas:")
        print(f"      ✅ Métricas deletadas: {deleted_metrics}")
        if error_count > 0:
            print(f"      ❌ Erros: {error_count}")
    
    print("\n" + "="*80)
    print("✨ Limpeza concluída!")
    print("="*80)
    print("\n💡 O ambiente está limpo e pronto para novos processamentos.")
    print("   Configurações (regras, CFOP mappings) foram preservadas.")

if __name__ == '__main__':
    try:
        clean_all_processes()
    except KeyboardInterrupt:
        print("\n\n❌ Operação interrompida pelo usuário.")
    except Exception as e:
        print(f"\n\n❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()

