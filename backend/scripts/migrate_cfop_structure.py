#!/usr/bin/env python3
"""
Script para migrar estrutura de CFOPs para permitir busca individual
Cria registros CFOP#{cfop} para cada CFOP individual, mantendo o registro principal
"""
import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.cfop_operation_service import CfopOperationService

def migrate_cfop_structure():
    """Migra estrutura de CFOPs para nova forma com registros individuais"""
    service = CfopOperationService()
    
    print("🔄 Iniciando migração da estrutura de CFOPs...")
    print("   Criando registros individuais CFOP#{cfop} para cada CFOP\n")
    
    # Buscar todas as regras
    all_rules = service.list_all()
    
    print(f"📋 Encontradas {len(all_rules)} regras para migrar\n")
    
    migrated_count = 0
    error_count = 0
    
    for rule in all_rules:
        try:
            mapping_id = rule['id']
            cfop = rule.get('cfop', '')
            
            if not cfop:
                print(f"⚠️  Regra {mapping_id} não tem CFOP, pulando...")
                continue
            
            # Separar CFOPs individuais
            cfop_list = [c.strip() for c in cfop.split() if c.strip()]
            
            if not cfop_list:
                print(f"⚠️  Regra {mapping_id} tem CFOP vazio, pulando...")
                continue
            
            print(f"📝 Processando regra {mapping_id} ({rule.get('chave', 'N/A')})")
            print(f"   CFOPs: {cfop_list}")
            
            # Atualizar registro principal com CFOP_LIST
            from src.repositories.dynamodb_repository import DynamoDBRepository
            repository = DynamoDBRepository()
            pk = "CFOP_OPERATION"
            sk = f"MAPPING#{mapping_id}"
            
            # Verificar se já tem CFOP_LIST
            item = repository.get_item(pk, sk)
            if item and 'CFOP_LIST' in item:
                print(f"   ✓ Já tem CFOP_LIST, verificando registros individuais...")
            else:
                # Adicionar CFOP_LIST ao registro principal
                repository.update_item(pk, sk, {'CFOP_LIST': cfop_list})
                print(f"   ✓ CFOP_LIST adicionado ao registro principal")
            
            # Criar/atualizar registros individuais CFOP#{cfop}
            for cfop_item in cfop_list:
                cfop_sk = f"CFOP#{cfop_item}"
                existing = repository.get_item(pk, cfop_sk)
                
                if existing:
                    # Verificar se mapping_id já está na lista
                    mapping_ids = existing.get('MAPPING_IDS', [])
                    if mapping_id not in mapping_ids:
                        mapping_ids.append(mapping_id)
                        repository.put_item(pk, cfop_sk, {
                            'CFOP': cfop_item,
                            'MAPPING_ID': mapping_id,  # Atualizar principal
                            'MAPPING_IDS': mapping_ids,
                            'CHAVE': rule.get('chave', ''),
                            'ATIVO': rule.get('ativo', True)
                        })
                        print(f"   ✓ Registro CFOP#{cfop_item} atualizado (adicionado mapping_id)")
                    else:
                        print(f"   ✓ Registro CFOP#{cfop_item} já existe com este mapping_id")
                else:
                    # Criar novo registro
                    repository.put_item(pk, cfop_sk, {
                        'CFOP': cfop_item,
                        'MAPPING_ID': mapping_id,
                        'MAPPING_IDS': [mapping_id],
                        'CHAVE': rule.get('chave', ''),
                        'ATIVO': rule.get('ativo', True)
                    })
                    print(f"   ✓ Registro CFOP#{cfop_item} criado")
            
            migrated_count += 1
            print()
            
        except Exception as e:
            print(f"❌ Erro ao migrar regra {rule.get('id', 'N/A')}: {e}")
            import traceback
            traceback.print_exc()
            error_count += 1
            print()
    
    print("="*60)
    print(f"📊 Resumo da migração:")
    print(f"   ✅ Migradas com sucesso: {migrated_count}")
    print(f"   ❌ Erros: {error_count}")
    print("="*60)
    
    if error_count == 0:
        print("\n✨ Migração concluída com sucesso!")
        print("   Agora é possível buscar CFOPs individuais usando query direta.")
    else:
        print(f"\n⚠️  Migração concluída com {error_count} erro(s).")

if __name__ == '__main__':
    migrate_cfop_structure()

