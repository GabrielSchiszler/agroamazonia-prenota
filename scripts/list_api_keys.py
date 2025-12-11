#!/usr/bin/env python3
import boto3
import json

def list_api_keys():
    secrets_client = boto3.client('secretsmanager')
    secret_name = 'agroamazonia/api-keys'
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    api_keys = json.loads(response['SecretString'])
    
    if not api_keys or api_keys.get('placeholder'):
        print("Nenhuma API Key encontrada")
        return
    
    print(f"\n{'Cliente':<30} {'Status':<10} {'API Key (parcial)'}")
    print("-" * 80)
    
    for api_key, info in api_keys.items():
        if api_key == 'placeholder':
            continue
        client = info.get('client_name', 'N/A')
        status = info.get('status', 'N/A')
        key_preview = f"{api_key[:16]}...{api_key[-8:]}"
        
        print(f"{client:<30} {status:<10} {key_preview}")
    
    print(f"\nTotal: {len([k for k in api_keys.keys() if k != 'placeholder'])} chaves")

if __name__ == '__main__':
    list_api_keys()
