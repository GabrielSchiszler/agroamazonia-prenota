#!/usr/bin/env python3
import boto3
import json
import secrets
import sys

def generate_api_key():
    code = secrets.token_urlsafe(24)
    return f"agroamazonia_key_{code}"

def create_api_key(client_name):
    secrets_client = boto3.client('secretsmanager')
    secret_name = 'agroamazonia/api-keys'
    
    # Get current keys
    response = secrets_client.get_secret_value(SecretId=secret_name)
    api_keys = json.loads(response['SecretString'])
    
    # Generate new key
    api_key = generate_api_key()
    
    # Add to keys
    api_keys[api_key] = {
        'client_name': client_name,
        'status': 'active'
    }
    
    # Update secret
    secrets_client.update_secret(
        SecretId=secret_name,
        SecretString=json.dumps(api_keys)
    )
    
    print(f"✅ API Key criada com sucesso!")
    print(f"Cliente: {client_name}")
    print(f"API Key: {api_key}")
    print(f"\nUso:")
    print(f"curl -H 'x-api-key: {api_key}' https://your-api.com/endpoint")
    
    return api_key

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python create_api_key.py <NOME_CLIENTE>")
        print("Exemplo: python create_api_key.py 'Cliente ABC'")
        sys.exit(1)
    
    client_name = sys.argv[1]
    create_api_key(client_name)
