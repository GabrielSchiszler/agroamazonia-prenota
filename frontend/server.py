#!/usr/bin/env python3
"""
Servidor HTTP simples para desenvolvimento do frontend
"""
import http.server
import socketserver
import os

PORT = 8080

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
    
    with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
        print(f"🚀 Servidor rodando em http://localhost:{PORT}")
        print(f"📁 Servindo arquivos de: {os.getcwd()}")
        print("Pressione Ctrl+C para parar")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Servidor encerrado")
