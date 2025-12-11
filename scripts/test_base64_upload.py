#!/usr/bin/env python3
import requests
import json
import base64

API_URL = "https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1"
API_KEY = "agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx"

def test_base64_upload():
    print("🧪 Testando upload com Base64...\n")
    
    import uuid
    process_id = str(uuid.uuid4())
    print(f"📋 Process ID: {process_id}\n")
    
    # Arquivo em base64 (exemplo: XML simples)
    file_content = '<?xml version="1.0"?><test>Hello World</test>'
    file_base64 = base64.b64encode(file_content.encode()).decode()
    
    print(f"📄 Conteúdo original: {file_content}")
    print(f"🔐 Base64: {file_base64}\n")
    
    # Solicitar URL assinada
    print("1️⃣ Solicitando URL assinada...")
    response = requests.post(
        f"{API_URL}/api/process/presigned-url/xml",
        headers={
            "x-api-key": API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "process_id": process_id,
            "file_name": "test.xml",
            "file_type": "application/xml"
        }
    )
    
    if response.status_code != 200:
        print(f"❌ Erro: {response.text}")
        return
    
    data = response.json()
    print(f"✅ URL gerada!")
    
    # DECODIFICAR base64 antes de enviar
    print("\n2️⃣ Decodificando base64 e fazendo upload...")
    file_binary = base64.b64decode(file_base64)
    
    upload_response = requests.put(
        data['upload_url'],
        data=file_binary,  # Enviar como binário, NÃO como base64
        headers={'Content-Type': 'application/xml'}
    )
    
    print(f"Upload Status: {upload_response.status_code}")
    if upload_response.status_code == 200:
        print("✅ Upload realizado com sucesso!")
        print(f"🔑 File Key: {data['file_key']}")
    else:
        print(f"❌ Erro: {upload_response.text}")

if __name__ == "__main__":
    test_base64_upload()
