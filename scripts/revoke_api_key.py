#!/usr/bin/env python3
import boto3
import json
import sys

def revoke_api_key(api_key):
    secrets_client = boto3.client('secretsmanager')
    secret_name = 'agroamazonia/api-keys'
    
    try:
        # Get current keys
        response = secrets_client.get_secret_value(SecretId=secret_name)
        api_keys = json.loads(response['SecretString'])
        
        if api_key not in api_keys:
            print(f"❌ API Key não encontrada")
            sys.exit(1)
        
        client_name = api_keys[api_key].get('client_name', 'N/A')
        
        # Update status
        api_keys[api_key]['status'] = 'revoked'
        
        # Update secret
        secrets_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(api_keys)
        )
        
        print(f"✅ API Key revogada com sucesso!")
        print(f"Cliente: {client_name}")
        print(f"Status: revoked")
        
    except Exception as e:
        print(f"❌ Erro ao revogar API Key: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python revoke_api_key.py <API_KEY>")
        sys.exit(1)
    
    api_key = sys.argv[1]
    revoke_api_key(api_key)
