#!/usr/bin/env python3
"""
Script para testar autenticação OAuth2 do Protheus (Client Credentials Grant)

Uso:
    python3 test_protheus_oauth2.py --token-url <URL> --client-id <ID> --client-secret <SECRET>
    
Ou criar um arquivo .env com:
    PROTHEUS_AUTH_URL=https://seu-token-url.com/oauth/token
    PROTHEUS_CLIENT_ID=seu_client_id
    PROTHEUS_CLIENT_SECRET=seu_client_secret
"""

import requests
import base64
import json
import sys
import os
from urllib.parse import urlencode

def test_oauth2_client_credentials(token_url, client_id, client_secret, scope=None):
    """
    Testa diferentes abordagens de OAuth2 Client Credentials Grant
    """
    print("="*80)
    print("TESTE DE OAUTH2 CLIENT CREDENTIALS - PROTHEUS")
    print("="*80)
    print(f"\nToken URL: {token_url}")
    print(f"Client ID: {client_id[:10]}..." if len(client_id) > 10 else f"Client ID: {client_id}")
    print(f"Client Secret: {'*' * len(client_secret)}")
    if scope:
        print(f"Scope: {scope}")
    print()
    
    # Preparar credenciais para Basic Auth
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    # Abordagem 1: Basic Auth no header (padrão OAuth2)
    headers_basic = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # Abordagem 2: client_id e client_secret no body
    headers_body = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # Abordagem 3: client_id e client_secret no body + Basic Auth (alguns servidores exigem ambos)
    headers_both = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # Preparar dados para diferentes abordagens
    approaches = []
    
    # Abordagem 1: Basic Auth + grant_type=client_credentials
    data_basic = {'grant_type': 'client_credentials'}
    if scope:
        data_basic['scope'] = scope
    approaches.append({
        'name': 'Basic Auth + client_credentials',
        'headers': headers_basic,
        'data': data_basic
    })
    
    # Abordagem 2: Body Auth + client_credentials (com client_id e client_secret no body)
    data_body = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    if scope:
        data_body['scope'] = scope
    approaches.append({
        'name': 'Body Auth + client_credentials (com credenciais no body)',
        'headers': headers_body,
        'data': data_body
    })
    
    # Abordagem 3: Basic Auth + credenciais no body também
    approaches.append({
        'name': 'Basic Auth + Body Auth (ambos)',
        'headers': headers_both,
        'data': data_body
    })
    
    # Abordagem 4: Body Auth sem client_secret (alguns servidores não exigem)
    data_body_no_secret = {
        'grant_type': 'client_credentials',
        'client_id': client_id
    }
    if scope:
        data_body_no_secret['scope'] = scope
    approaches.append({
        'name': 'Body Auth + client_credentials (sem client_secret)',
        'headers': headers_body,
        'data': data_body_no_secret
    })
    
    # Abordagem 5: JSON ao invés de form-urlencoded
    headers_json = {
        'Content-Type': 'application/json'
    }
    data_json = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    if scope:
        data_json['scope'] = scope
    approaches.append({
        'name': 'JSON Body + client_credentials',
        'headers': headers_json,
        'data': json.dumps(data_json)  # JSON string
    })
    
    results = []
    
    for idx, approach in enumerate(approaches, 1):
        print(f"\n{'='*80}")
        print(f"ABORDAGEM {idx}: {approach['name']}")
        print(f"{'='*80}")
        
        try:
            # Preparar request
            if isinstance(approach['data'], str):
                # JSON
                response = requests.post(
                    token_url,
                    data=approach['data'],
                    headers=approach['headers'],
                    timeout=30
                )
            else:
                # Form-urlencoded
                response = requests.post(
                    token_url,
                    data=approach['data'],
                    headers=approach['headers'],
                    timeout=30
                )
            
            print(f"\nStatus Code: {response.status_code}")
            print(f"Headers enviados:")
            for key, value in approach['headers'].items():
                if key == 'Authorization':
                    print(f"  {key}: Basic {encoded_credentials[:20]}...")
                else:
                    print(f"  {key}: {value}")
            
            print(f"\nBody enviado:")
            if isinstance(approach['data'], str):
                print(f"  {approach['data']}")
            else:
                print(f"  {urlencode(approach['data'])}")
            
            print(f"\nResponse Headers:")
            for key, value in response.headers.items():
                print(f"  {key}: {value}")
            
            print(f"\nResponse Body:")
            try:
                response_json = response.json()
                print(json.dumps(response_json, indent=2, ensure_ascii=False))
                
                # Verificar se tem access_token
                access_token = (
                    response_json.get('access_token') or 
                    response_json.get('accessToken') or
                    response_json.get('token')
                )
                
                if access_token:
                    print(f"\n✅ SUCESSO! Token obtido:")
                    print(f"   Token (primeiros 30 chars): {access_token[:30]}...")
                    print(f"   Token completo: {access_token}")
                    
                    # Verificar outros campos
                    if 'token_type' in response_json:
                        print(f"   Token Type: {response_json['token_type']}")
                    if 'expires_in' in response_json:
                        print(f"   Expires In: {response_json['expires_in']} segundos")
                    if 'scope' in response_json:
                        print(f"   Scope: {response_json['scope']}")
                    
                    results.append({
                        'approach': approach['name'],
                        'success': True,
                        'token': access_token,
                        'response': response_json
                    })
                else:
                    print(f"\n❌ FALHOU: Resposta não contém access_token")
                    print(f"   Campos disponíveis: {list(response_json.keys())}")
                    results.append({
                        'approach': approach['name'],
                        'success': False,
                        'error': 'No access_token in response',
                        'response': response_json
                    })
            except ValueError:
                # Não é JSON, tentar como texto
                print(response.text[:500])
                results.append({
                    'approach': approach['name'],
                    'success': False,
                    'error': 'Response is not JSON',
                    'response_text': response.text[:500]
                })
            
        except requests.exceptions.RequestException as e:
            print(f"\n❌ ERRO na requisição: {str(e)}")
            results.append({
                'approach': approach['name'],
                'success': False,
                'error': str(e)
            })
        except Exception as e:
            print(f"\n❌ ERRO inesperado: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append({
                'approach': approach['name'],
                'success': False,
                'error': str(e)
            })
    
    # Resumo final
    print(f"\n{'='*80}")
    print("RESUMO DOS TESTES")
    print(f"{'='*80}")
    
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    
    print(f"\n✅ Sucessos: {len(successful)}")
    for r in successful:
        print(f"   - {r['approach']}")
        print(f"     Token: {r['token'][:30]}...")
    
    print(f"\n❌ Falhas: {len(failed)}")
    for r in failed:
        print(f"   - {r['approach']}")
        print(f"     Erro: {r.get('error', 'Unknown error')}")
    
    if successful:
        print(f"\n🎉 PELO MENOS UMA ABORDAGEM FUNCIONOU!")
        print(f"\nUse esta configuração no Lambda:")
        print(f"   Abordagem: {successful[0]['approach']}")
        return successful[0]
    else:
        print(f"\n⚠️  NENHUMA ABORDAGEM FUNCIONOU")
        print(f"\nVerifique:")
        print(f"   1. URL do token está correta?")
        print(f"   2. Client ID e Client Secret estão corretos?")
        print(f"   3. O servidor aceita Client Credentials Grant?")
        print(f"   4. Há algum firewall/proxy bloqueando?")
        return None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Testa autenticação OAuth2 do Protheus')
    parser.add_argument('--token-url', help='URL do endpoint OAuth2 (Token URL)')
    parser.add_argument('--client-id', help='Client ID')
    parser.add_argument('--client-secret', help='Client Secret')
    parser.add_argument('--scope', help='Scope (opcional)')
    parser.add_argument('--env-file', default='.env', help='Arquivo .env para carregar variáveis (padrão: .env)')
    
    args = parser.parse_args()
    
    # Tentar carregar do arquivo .env se existir
    token_url = args.token_url
    client_id = args.client_id
    client_secret = args.client_secret
    scope = args.scope
    
    if os.path.exists(args.env_file):
        print(f"Carregando variáveis do arquivo {args.env_file}...")
        with open(args.env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    # Remover 'export ' se presente
                    line = line.replace('export ', '')
                    key, value = line.split('=', 1)
                    value = value.strip('"\'')
                    
                    if key == 'PROTHEUS_AUTH_URL' and not token_url:
                        token_url = value
                    elif key == 'PROTHEUS_CLIENT_ID' and not client_id:
                        client_id = value
                    elif key == 'PROTHEUS_CLIENT_SECRET' and not client_secret:
                        client_secret = value
    
    # Verificar se todas as variáveis foram fornecidas
    if not token_url:
        token_url = input("Token URL (Access Token URL): ").strip()
    if not client_id:
        client_id = input("Client ID: ").strip()
    if not client_secret:
        client_secret = input("Client Secret: ").strip()
    
    if not all([token_url, client_id, client_secret]):
        print("ERRO: Token URL, Client ID e Client Secret são obrigatórios!")
        sys.exit(1)
    
    # Executar testes
    result = test_oauth2_client_credentials(token_url, client_id, client_secret, scope)
    
    if result:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()


