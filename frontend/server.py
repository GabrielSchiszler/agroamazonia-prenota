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
    
    config_content = f'''// Configuração da API - gerada automaticamente a partir de variáveis de ambiente
// Para alterar, edite o arquivo .env ou defina as variáveis de ambiente API_URL e API_KEY
window.ENV = {{
    API_URL: '{api_url}',
    API_KEY: '{api_key}'
}};
'''
    
    config_path = Path(__file__).parent / 'config.js'
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    print(f"✓ config.js gerado:")
    print(f"  API_URL: {api_url}")
    print(f"  API_KEY: {api_key[:20]}..." if len(api_key) > 20 else f"  API_KEY: {api_key}")

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
        print("\n💡 Dica: Para alterar API_URL e API_KEY, crie um arquivo .env ou defina variáveis de ambiente:")
        print("   export API_URL='https://sua-api.com/v1'")
        print("   export API_KEY='sua-api-key'")
        print("\nPressione Ctrl+C para parar")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Servidor encerrado")
