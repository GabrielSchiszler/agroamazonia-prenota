#!/usr/bin/env python3
"""
Script para adicionar API Key ao AWS Secrets Manager.

Uso:
    python add_api_key.py --env stg --api-key agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx

Ou definir variáveis de ambiente:
    export AWS_REGION=us-east-1
    export ENV=stg
    python add_api_key.py
"""

import boto3
import json
import os
import sys
import argparse
from datetime import datetime

def get_secret_name(env: str) -> str:
    """Retorna o nome do secret baseado no ambiente."""
    return f"secret-agroamazonia-api-keys-{env}"

def add_api_key(env: str, api_key: str, client_name: str = "frontend"):
    """Adiciona ou atualiza uma API key no Secrets Manager."""
    
    region = os.environ.get('AWS_REGION', 'us-east-1')
    secret_name = get_secret_name(env)
    
    print(f"🔑 Adicionando API Key ao Secrets Manager")
    print(f"   Environment: {env}")
    print(f"   Secret Name: {secret_name}")
    print(f"   API Key: {api_key[:20]}...{api_key[-10:]}")
    print(f"   Region: {region}")
    print()
    
    try:
        secrets_client = boto3.client('secretsmanager', region_name=region)
        
        # Tentar obter o secret existente
        try:
            response = secrets_client.get_secret_value(SecretId=secret_name)
            current_secrets = json.loads(response['SecretString'])
            print(f"✓ Secret encontrado. Atualizando...")
        except secrets_client.exceptions.ResourceNotFoundException:
            print(f"⚠ Secret não encontrado. Criando novo...")
            current_secrets = {}
        
        # Adicionar ou atualizar a API key
        if api_key not in current_secrets:
            current_secrets[api_key] = {
                "status": "active",
                "client_name": client_name,
                "created_at": datetime.utcnow().isoformat() + "Z"
            }
            print(f"✓ Nova API key adicionada")
        else:
            current_secrets[api_key]["status"] = "active"
            current_secrets[api_key]["client_name"] = client_name
            current_secrets[api_key]["updated_at"] = datetime.utcnow().isoformat() + "Z"
            print(f"✓ API key existente atualizada")
        
        # Atualizar o secret
        secrets_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(current_secrets, indent=2)
        )
        
        print()
        print(f"✅ API Key adicionada/atualizada com sucesso!")
        print(f"   Total de API keys no secret: {len(current_secrets)}")
        print()
        print("📋 API Keys ativas:")
        for key, info in current_secrets.items():
            if info.get('status') == 'active':
                print(f"   - {key[:30]}... ({info.get('client_name', 'unknown')})")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao adicionar API key: {e}")
        return False

def list_api_keys(env: str):
    """Lista todas as API keys cadastradas."""
    
    region = os.environ.get('AWS_REGION', 'us-east-1')
    secret_name = get_secret_name(env)
    
    print(f"📋 Listando API Keys")
    print(f"   Environment: {env}")
    print(f"   Secret Name: {secret_name}")
    print(f"   Region: {region}")
    print()
    
    try:
        secrets_client = boto3.client('secretsmanager', region_name=region)
        response = secrets_client.get_secret_value(SecretId=secret_name)
        api_keys = json.loads(response['SecretString'])
        
        print(f"Total de API keys: {len(api_keys)}")
        print()
        
        for key, info in api_keys.items():
            status = info.get('status', 'unknown')
            client_name = info.get('client_name', 'unknown')
            created_at = info.get('created_at', 'N/A')
            
            status_icon = "✅" if status == "active" else "❌"
            print(f"{status_icon} {key}")
            print(f"   Client: {client_name}")
            print(f"   Status: {status}")
            print(f"   Created: {created_at}")
            print()
        
        return True
        
    except secrets_client.exceptions.ResourceNotFoundException:
        print(f"⚠ Secret não encontrado: {secret_name}")
        return False
    except Exception as e:
        print(f"❌ Erro ao listar API keys: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Gerenciar API Keys no AWS Secrets Manager')
    parser.add_argument('--env', type=str, default=os.environ.get('ENV', 'dev'),
                       help='Ambiente (dev, stg, prd)')
    parser.add_argument('--api-key', type=str, 
                       default='agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx',
                       help='API Key a ser adicionada')
    parser.add_argument('--client-name', type=str, default='frontend',
                       help='Nome do cliente/aplicação')
    parser.add_argument('--list', action='store_true',
                       help='Listar todas as API keys cadastradas')
    
    args = parser.parse_args()
    
    if args.list:
        success = list_api_keys(args.env)
    else:
        success = add_api_key(args.env, args.api_key, args.client_name)
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()

