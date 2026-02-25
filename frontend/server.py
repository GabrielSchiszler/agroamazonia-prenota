#!/usr/bin/env python3
"""
Servidor HTTP simples para desenvolvimento do frontend
Gera config.js automaticamente a partir de variáveis de ambiente
"""
import http.server
import socketserver
import os
from pathlib import Path

PORT = 8080

def load_env_file():
    """Carrega variáveis de ambiente de um arquivo .env"""
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        env_vars = {}
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    # Remover 'export ' se presente
                    line = line.replace('export ', '')
                    key, value = line.split('=', 1)
                    value = value.strip('"\'')
                    env_vars[key] = value
        return env_vars
    return {}

def generate_config_js():
    """Gera config.js a partir de variáveis de ambiente"""
    # Carregar do arquivo .env se existir
    env_vars = load_env_file()
    
    # Prioridade: variáveis de ambiente do sistema > arquivo .env > valores padrão
    api_url = os.environ.get('API_URL') or env_vars.get('API_URL') or 'http://localhost:8001'
    api_key = os.environ.get('API_KEY') or env_vars.get('API_KEY') or 'dev'
    
    # OAuth2 Frontend Config
    oauth2_token_url = os.environ.get('OAUTH2_FRONTEND_TOKEN_URL') or env_vars.get('OAUTH2_FRONTEND_TOKEN_URL') or 'https://api-auth-hml.agroamazonia.io/oauth2/token'
    oauth2_client_id = os.environ.get('OAUTH2_FRONTEND_CLIENT_ID') or env_vars.get('OAUTH2_FRONTEND_CLIENT_ID') or ''
    oauth2_client_secret = os.environ.get('OAUTH2_FRONTEND_CLIENT_SECRET') or env_vars.get('OAUTH2_FRONTEND_CLIENT_SECRET') or ''
    oauth2_scope = os.environ.get('OAUTH2_FRONTEND_SCOPE') or env_vars.get('OAUTH2_FRONTEND_SCOPE') or 'App_Fast/HML'
    
    config_content = f'''// Configuração da API - gerada automaticamente a partir de variáveis de ambiente
// Para alterar, edite o arquivo .env ou defina as variáveis de ambiente
window.ENV = {{
    API_URL: '{api_url}',
    API_KEY: '{api_key}',
    OAUTH2_FRONTEND_TOKEN_URL: '{oauth2_token_url}',
    OAUTH2_FRONTEND_CLIENT_ID: '{oauth2_client_id}',
    OAUTH2_FRONTEND_CLIENT_SECRET: '{oauth2_client_secret}',
    OAUTH2_FRONTEND_SCOPE: '{oauth2_scope}'
}};
'''
    
    config_path = Path(__file__).parent / 'config.js'
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    print(f"✓ config.js gerado:")
    print(f"  API_URL: {api_url}")
    print(f"  API_KEY: {api_key[:20]}..." if len(api_key) > 20 else f"  API_KEY: {api_key}")
    print(f"  OAUTH2_FRONTEND_TOKEN_URL: {oauth2_token_url}")
    print(f"  OAUTH2_FRONTEND_CLIENT_ID: {oauth2_client_id[:20]}..." if len(oauth2_client_id) > 20 else f"  OAUTH2_FRONTEND_CLIENT_ID: {oauth2_client_id}")
    print(f"  OAUTH2_FRONTEND_CLIENT_SECRET: {'*' * 20}..." if oauth2_client_secret else "  OAUTH2_FRONTEND_CLIENT_SECRET: (não definido)")
    print(f"  OAUTH2_FRONTEND_SCOPE: {oauth2_scope}")

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Adicionar headers CORS para desenvolvimento
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Gerar config.js antes de iniciar o servidor
    generate_config_js()
    
    with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
        print(f"\n🚀 Servidor rodando em http://localhost:{PORT}")
        print(f"📁 Servindo arquivos de: {os.getcwd()}")
        print("\n💡 Dica: Para alterar configurações, crie um arquivo .env ou defina variáveis de ambiente:")
        print("   export API_URL='https://sua-api.com/v1'")
        print("   export API_KEY='sua-api-key'")
        print("   export OAUTH2_FRONTEND_TOKEN_URL='https://api-auth-hml.agroamazonia.io/oauth2/token'")
        print("   export OAUTH2_FRONTEND_CLIENT_ID='seu-client-id'")
        print("   export OAUTH2_FRONTEND_CLIENT_SECRET='seu-client-secret'")
        print("   export OAUTH2_FRONTEND_SCOPE='App_Fast/HML'")
        print("\nPressione Ctrl+C para parar")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Servidor encerrado")
