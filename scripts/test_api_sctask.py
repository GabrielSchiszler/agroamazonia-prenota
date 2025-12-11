#!/usr/bin/env python3
import requests
import sys
import os

if len(sys.argv) < 2:
    print("Uso: python3 test_api_sctask.py <process_id>")
    sys.exit(1)

process_id = sys.argv[1]
api_url = os.environ.get('API_URL', 'https://your-api-url.com')
api_key = os.environ.get('API_KEY', 'your-api-key')

try:
    response = requests.get(
        f"{api_url}/api/process/{process_id}",
        headers={'x-api-key': api_key}
    )
    
    if response.ok:
        data = response.json()
        print(f"Status: {data.get('status')}")
        print(f"SCTASK ID: {data.get('sctask_id', 'NÃO RETORNADO PELA API')}")
        print(f"\nResposta completa:")
        print(response.text)
    else:
        print(f"Erro: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"Erro: {e}")
