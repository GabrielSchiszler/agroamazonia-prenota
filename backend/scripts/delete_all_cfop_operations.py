#!/usr/bin/env python3
"""
Script para deletar TODOS os registros Chave x CFOP da tabela
ATENÇÃO: Esta operação é irreversível!
"""
import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.cfop_operation_service import CfopOperationService

def delete_all_cfop_operations():
    """Deleta todos os registros Chave x CFOP"""
    service = CfopOperationService()
    
    print("⚠️  ATENÇÃO: Esta operação irá DELETAR TODOS os registros Chave x CFOP!")
    print("⚠️  Esta operação é IRREVERSÍVEL!\n")
    
    resposta = input("Digite 'CONFIRMAR' para continuar: ")
    
    if resposta != 'CONFIRMAR':
        print("❌ Operação cancelada.")
        return
    
    print("\n🗑️  Buscando registros para deletar...")
    
    # Buscar todos os registros
    all_rules = service.list_all()
    
    if not all_rules:
        print("ℹ️  Nenhum registro encontrado na tabela.")
        return
    
    print(f"📋 Total de registros encontrados: {len(all_rules)}\n")
    
    deleted_count = 0
    error_count = 0
    
    for rule in all_rules:
        try:
            service.delete(rule['id'])
            print(f"✅ {rule.get('chave', 'N/A')} - Deletado")
            deleted_count += 1
        except Exception as e:
            print(f"❌ {rule.get('chave', 'N/A')} - Erro: {e}")
            error_count += 1
    
    print(f"\n📊 Resumo:")
    print(f"   ✅ Deletados: {deleted_count}")
    print(f"   ❌ Erros: {error_count}")
    print("\n✨ Concluído!")

if __name__ == '__main__':
    delete_all_cfop_operations()

