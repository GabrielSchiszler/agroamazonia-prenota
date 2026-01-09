#!/usr/bin/env python3
"""
Script para testar a criação do token OAuth2 para a API externa de reporte de falhas OCR.

Uso:
    # Via variáveis de ambiente:
    export OCR_FAILURE_AUTH_URL='...'
    export OCR_FAILURE_CLIENT_ID='...'
    export OCR_FAILURE_CLIENT_SECRET='...'
    export OCR_FAILURE_USERNAME='...'
    export OCR_FAILURE_PASSWORD='...'
    python3 test_oauth2_token.py

    # Via argumentos:
    python3 test_oauth2_token.py --client-id '...' --client-secret '...' --username '...' --password '...'

    # Via arquivo .env:
    python3 test_oauth2_token.py --env-file .env
"""
import os
import sys
import base64
import requests
import json
import argparse
from urllib.parse import parse_qs

# Tentar carregar python-dotenv se disponível
try:
    from dotenv import load_dotenv
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

def get_oauth2_token(auth_url=None, client_id=None, client_secret=None, username=None, password=None):
    """
    Obtém token de acesso OAuth2 usando password credentials grant.
    Retorna o access_token ou None em caso de erro.
    
    Args:
        auth_url: URL do endpoint OAuth2 (ou None para usar env var)
        client_id: Client ID (ou None para usar env var)
        client_secret: Client Secret (ou None para usar env var)
        username: Username (ou None para usar env var)
        password: Password (ou None para usar env var)
    """
    # Usar argumentos se fornecidos, senão usar variáveis de ambiente
    auth_url = auth_url or os.environ.get('OCR_FAILURE_AUTH_URL')
    client_id = client_id or os.environ.get('OCR_FAILURE_CLIENT_ID')
    client_secret = client_secret or os.environ.get('OCR_FAILURE_CLIENT_SECRET')
    username = username or os.environ.get('OCR_FAILURE_USERNAME')
    password = password or os.environ.get('OCR_FAILURE_PASSWORD')
    
    print("="*80)
    print("TESTE DE OAuth2 TOKEN")
    print("="*80)
    print(f"\nVariáveis de ambiente:")
    print(f"  OCR_FAILURE_AUTH_URL: {'SET' if auth_url else 'NOT SET'}")
    if auth_url:
        print(f"    Valor: {auth_url}")
    print(f"  OCR_FAILURE_CLIENT_ID: {'SET' if client_id else 'NOT SET'}")
    if client_id:
        print(f"    Valor: {client_id[:10]}..." if len(client_id) > 10 else f"    Valor: {client_id}")
    print(f"  OCR_FAILURE_CLIENT_SECRET: {'SET' if client_secret else 'NOT SET'}")
    if client_secret:
        print(f"    Valor: {'*' * len(client_secret)} (oculto)")
    print(f"  OCR_FAILURE_USERNAME: {'SET' if username else 'NOT SET'}")
    if username:
        print(f"    Valor: {username}")
    print(f"  OCR_FAILURE_PASSWORD: {'SET' if password else 'NOT SET'}")
    if password:
        print(f"    Valor: {'*' * len(password)} (oculto)")
    print()
    
    if not all([auth_url, client_id, client_secret, username, password]):
        missing = []
        if not auth_url: missing.append('OCR_FAILURE_AUTH_URL')
        if not client_id: missing.append('OCR_FAILURE_CLIENT_ID')
        if not client_secret: missing.append('OCR_FAILURE_CLIENT_SECRET')
        if not username: missing.append('OCR_FAILURE_USERNAME')
        if not password: missing.append('OCR_FAILURE_PASSWORD')
        print(f"❌ ERRO: Variáveis de ambiente faltando: {', '.join(missing)}")
        print("\nPor favor, defina as variáveis de ambiente:")
        print("  export OCR_FAILURE_AUTH_URL='https://agroamazoniad.service-now.com/oauth_token.do'")
        print("  export OCR_FAILURE_CLIENT_ID='seu_client_id'")
        print("  export OCR_FAILURE_CLIENT_SECRET='seu_client_secret'")
        print("  export OCR_FAILURE_USERNAME='seu_username'")
        print("  export OCR_FAILURE_PASSWORD='sua_password'")
        return None
    
    try:
        # ServiceNow OAuth2 pode aceitar client credentials de duas formas:
        # 1. Basic Auth no header (Authorization: Basic base64(client_id:client_secret))
        # 2. client_id e client_secret no body
        # Vamos tentar ambas as abordagens
        
        # Abordagem 1: Basic Auth no header (padrão OAuth2)
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers_basic = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Abordagem 2: client_id e client_secret no body
        headers_body = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Tentar diferentes combinações
        # grant_type pode ser "password" (padrão OAuth2) ou "password_credentials" (ServiceNow)
        approaches = [
            {
                'name': 'Basic Auth + password',
                'headers': headers_basic,
                'data': {
                    'grant_type': 'password',
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Basic Auth + password_credentials',
                'headers': headers_basic,
                'data': {
                    'grant_type': 'password_credentials',
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Body Auth + password',
                'headers': headers_body,
                'data': {
                    'grant_type': 'password',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'username': username,
                    'password': password
                }
            },
            {
                'name': 'Body Auth + password_credentials',
                'headers': headers_body,
                'data': {
                    'grant_type': 'password_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'username': username,
                    'password': password
                }
            }
        ]
        
        response = None
        last_error = None
        successful_approach = None
        
        print("Tentando diferentes abordagens de autenticação OAuth2...\n")
        
        for idx, approach in enumerate(approaches, 1):
            try:
                print(f"[{idx}/4] Tentando: {approach['name']}")
                print(f"  URL: {auth_url}")
                print(f"  Headers:")
                for k, v in approach['headers'].items():
                    if k == 'Authorization' and 'Basic' in v:
                        # Mostrar apenas preview do Basic Auth
                        print(f"    {k}: Basic {v.split(' ')[1][:20]}...")
                    else:
                        print(f"    {k}: {v}")
                print(f"  Body (form-urlencoded):")
                for k, v in approach['data'].items():
                    if k in ['password', 'client_secret']:
                        print(f"    {k}: {'*' * len(str(v))} (oculto)")
                    else:
                        print(f"    {k}: {v}")
                
                # Mostrar exatamente o que será enviado (para debug)
                print(f"  📤 Enviando requisição POST...")
                
                response = requests.post(
                    auth_url, 
                    data=approach['data'], 
                    headers=approach['headers'], 
                    timeout=30
                )
                
                print(f"  📥 Status Code: {response.status_code}")
                print(f"  📥 Response Headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    print(f"  ✅ SUCESSO com abordagem: {approach['name']}")
                    successful_approach = approach['name']
                    break
                else:
                    print(f"  ❌ Falhou com status {response.status_code}")
                    print(f"  Response body (primeiros 500 chars):")
                    print(f"    {response.text[:500]}")
                    if len(response.text) > 500:
                        print(f"    ... (truncado, total: {len(response.text)} chars)")
                    last_error = response
                    
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Exceção: {str(e)}")
                last_error = e
                continue
            except Exception as e:
                print(f"  ❌ Erro inesperado: {str(e)}")
                last_error = e
                continue
            finally:
                print()
        
        if not response or response.status_code != 200:
            print("="*80)
            print("❌ TODAS AS ABORDAGENS FALHARAM")
            print("="*80)
            if last_error:
                if hasattr(last_error, 'status_code'):
                    print(f"Último status code: {last_error.status_code}")
                    print(f"Última resposta: {last_error.text[:500]}")
                else:
                    print(f"Último erro: {str(last_error)}")
            return None
        
        response.raise_for_status()
        
        # ServiceNow geralmente retorna JSON
        try:
            token_response = response.json()
            print(f"✅ Resposta JSON recebida")
            print(f"  Keys na resposta: {list(token_response.keys())}")
        except ValueError:
            # Se não for JSON, tentar parsear como form-urlencoded
            print(f"⚠️  Resposta não é JSON, tentando parsear como form-urlencoded...")
            token_response = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                            for k, v in parse_qs(response.text).items()}
            print(f"  Keys parseadas: {list(token_response.keys())}")
        
        # Tentar diferentes campos possíveis para o access_token
        # ServiceNow pode retornar: access_token, accessToken, token, etc.
        access_token = (
            token_response.get('access_token') or 
            token_response.get('accessToken') or
            token_response.get('token')
        )
        
        if access_token:
            print("="*80)
            print("✅ TOKEN OBTIDO COM SUCESSO!")
            print("="*80)
            print(f"  Abordagem que funcionou: {successful_approach}")
            print(f"  Token preview: {access_token[:30]}...{access_token[-10:]}")
            print(f"  Token length: {len(access_token)} caracteres")
            
            # Mostrar outros campos da resposta se houver
            other_fields = {k: v for k, v in token_response.items() if k not in ['access_token', 'accessToken', 'token']}
            if other_fields:
                print(f"\n  Outros campos na resposta:")
                for k, v in other_fields.items():
                    if isinstance(v, str) and len(v) > 50:
                        print(f"    {k}: {v[:50]}...")
                    else:
                        print(f"    {k}: {v}")
            
            return access_token
        else:
            print("="*80)
            print("❌ ERRO: Token não encontrado na resposta")
            print("="*80)
            print(f"  Resposta completa: {json.dumps(token_response, indent=2, ensure_ascii=False)}")
            return None
            
    except Exception as e:
        print("="*80)
        print(f"❌ ERRO ao obter token OAuth2: {str(e)}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return None

def test_api_call(access_token):
    """Testa uma chamada à API externa com o token obtido"""
    if not access_token:
        print("\n⚠️  Não é possível testar a API sem token")
        return
    
    api_url = os.environ.get('OCR_FAILURE_API_URL', 'https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha')
    
    print("\n" + "="*80)
    print("TESTE DE CHAMADA À API EXTERNA")
    print("="*80)
    print(f"\nURL da API: {api_url}")
    
    # Payload de teste
    test_payload = {
        "idUnico": 999999,
        "descricaoFalha": "Teste de integração OAuth2",
        "traceAWS": "test-oauth2-script",
        "detalhes": [
            {
                "pagina": 1,
                "campo": "teste",
                "mensagemErro": "Este é um teste de integração"
            }
        ]
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        print(f"\nEnviando requisição de teste...")
        print(f"Payload: {json.dumps(test_payload, indent=2, ensure_ascii=False)}")
        
        response = requests.post(api_url, json=test_payload, headers=headers, timeout=30)
        
        print(f"\nStatus Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ API respondeu com sucesso!")
            try:
                response_json = response.json()
                print(f"Response JSON: {json.dumps(response_json, indent=2, ensure_ascii=False)}")
            except:
                print(f"Response Text: {response.text[:500]}")
        else:
            print(f"❌ API respondeu com erro")
            print(f"Response Text: {response.text[:500]}")
            response.raise_for_status()
            
    except requests.exceptions.HTTPError as http_err:
        print(f"\n❌ Erro HTTP: {http_err}")
        if hasattr(http_err, 'response') and http_err.response:
            print(f"Status: {http_err.response.status_code}")
            print(f"Body: {http_err.response.text[:500]}")
    except Exception as e:
        print(f"\n❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Testa criação de token OAuth2 para API externa')
    parser.add_argument('--auth-url', help='URL do endpoint OAuth2', default=None)
    parser.add_argument('--client-id', help='Client ID', default=None)
    parser.add_argument('--client-secret', help='Client Secret', default=None)
    parser.add_argument('--username', help='Username', default=None)
    parser.add_argument('--password', help='Password', default=None)
    parser.add_argument('--env-file', help='Arquivo .env para carregar variáveis', default=None)
    parser.add_argument('--api-url', help='URL da API para testar chamada', default=None)
    parser.add_argument('--skip-api-test', action='store_true', help='Pular teste de chamada à API')
    
    args = parser.parse_args()
    
    # Carregar arquivo .env se fornecido
    if args.env_file:
        if HAS_DOTENV:
            load_dotenv(args.env_file)
            print(f"✅ Arquivo .env carregado: {args.env_file}\n")
        else:
            print("⚠️  python-dotenv não instalado. Instale com: pip install python-dotenv\n")
            # Tentar carregar manualmente
            try:
                with open(args.env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key.strip()] = value.strip().strip('"').strip("'")
                print(f"✅ Variáveis carregadas de {args.env_file}\n")
            except Exception as e:
                print(f"❌ Erro ao carregar {args.env_file}: {e}\n")
    
    print("\n" + "="*80)
    print("SCRIPT DE TESTE - OAuth2 Token para API Externa")
    print("="*80)
    print("\nEste script testa a criação do token OAuth2 e uma chamada de teste à API.\n")
    
    # Obter token (usar argumentos se fornecidos, senão usar env vars)
    token = get_oauth2_token(
        auth_url=args.auth_url,
        client_id=args.client_id,
        client_secret=args.client_secret,
        username=args.username,
        password=args.password
    )
    
    if token:
        if not args.skip_api_test:
            # Testar chamada à API
            if args.api_url:
                os.environ['OCR_FAILURE_API_URL'] = args.api_url
            test_api_call(token)
        print("\n" + "="*80)
        print("✅ TESTE CONCLUÍDO COM SUCESSO")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("❌ TESTE FALHOU - Não foi possível obter o token")
        print("="*80)
        print("\n💡 Dica: Você pode passar as credenciais como argumentos:")
        print("  python3 test_oauth2_token.py --client-id '...' --client-secret '...' --username '...' --password '...'")
        sys.exit(1)

