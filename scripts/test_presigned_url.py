#!/usr/bin/env python3
import requests
import json

API_URL = "https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1"
API_KEY = "agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx"

def test_presigned_url():
    print("🧪 Testando geração de URL assinada...\n")
    
    # Gerar process_id
    import uuid
    process_id = str(uuid.uuid4())
    print(f"📋 Process ID: {process_id}\n")
    
    # Solicitar URL assinada para XML
    print("1️⃣ Solicitando URL assinada para XML...")
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
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ URL gerada com sucesso!")
        print(f"📤 Upload URL: {data['upload_url'][:80]}...")
        print(f"🔑 File Key: {data['file_key']}")
        
        # Testar upload de arquivo
        print("\n📤 Testando upload de arquivo XML...")
        test_xml = b'<?xml version="1.0"?><test>Hello World</test>'
        upload_response = requests.put(
            data['upload_url'],
            data=test_xml,
            headers={'Content-Type': 'application/xml'}
        )
        print(f"Upload Status: {upload_response.status_code}")
        if upload_response.status_code == 200:
            print("✅ Upload realizado com sucesso!")
        else:
            print(f"❌ Erro no upload: {upload_response.text}")
    else:
        print(f"❌ Erro: {response.text}")
        return
    
    print("\n2️⃣ Solicitando URLs assinadas para documentos...")
    response = requests.post(
        f"{API_URL}/api/process/presigned-url/docs",
        headers={
            "x-api-key": API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "process_id": process_id,
            "files": [
                {"file_name": "doc1.pdf", "file_type": "application/pdf"},
                {"file_name": "doc2.pdf", "file_type": "application/pdf"}
            ]
        }
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ {len(data['urls'])} URLs geradas com sucesso!")
        for i, url_data in enumerate(data['urls'], 1):
            print(f"  📄 Doc {i}: {url_data['file_name']}")
            print(f"     🔑 Key: {url_data['file_key']}")
    else:
        print(f"❌ Erro: {response.text}")

if __name__ == "__main__":
    test_presigned_url()
