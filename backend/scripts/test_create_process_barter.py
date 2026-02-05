#!/usr/bin/env python3
"""
Script para criar um processo de teste BARTER (Commodities) com documentos

Uso:
    python3 test_create_process_barter.py --api-url <URL> --api-key <KEY> [--xml-file <arquivo.xml>] [--start]

Ou criar um arquivo .env com:
    API_URL=https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1
    API_KEY=agroamazonia_key_<seu_codigo>
"""

import requests
import uuid
import json
import os
import sys
import argparse
import random
import re
from pathlib import Path
from io import BytesIO

try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
    print("⚠️  reportlab não instalado. Instale com: pip install reportlab")
    print("   O PDF será criado como arquivo vazio (sem biblioteca)")


def create_empty_pdf():
    """Cria um PDF vazio"""
    if HAS_REPORTLAB:
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        # Adicionar uma página vazia
        p.showPage()
        p.save()
        buffer.seek(0)
        return buffer.getvalue()
    else:
        # Retornar um PDF mínimo válido (sem biblioteca)
        # Este é um PDF vazio mínimo válido
        return b'%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n>>\nendobj\nxref\n0 1\ntrailer\n<<\n/Size 1\n/Root 1 0 R\n>>\nstartxref\n9\n%%EOF'


def create_xml_file(xml_path: str):
    """Cria arquivo XML com o XML fornecido para BARTER (SOJA)"""
    
    if os.path.exists(xml_path):
        print(f"ℹ️  Arquivo XML já existe: {xml_path} - será recriado")
    
    xml_content = '''<?xml version="1.0" encoding="utf-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">
<protNFe>
<infProt>
<nProt>131256480060004</nProt>
<digVal>xOz/01esWkMQNotTqnIXmiruA14=</digVal>
<dhRecbto>2025-02-16T13:37:19-03:00</dhRecbto>
<chNFe>31250216907746000113558900466584991781959936</chNFe>
<xMotivo>Autorizado o uso da NF-e</xMotivo>
<cStat>100</cStat>
</infProt>
</protNFe>
<NFe>
<infNFe Id="NFe31250216907746000113558900466584991781959936" versao="4.00">
<avulsa>
<matr>Administrador SIARE</matr>
<UF>MG</UF>
<dEmi>2025-02-16</dEmi>
<nDAR>NT</nDAR>
<xAgente>Administrador SIARE</xAgente>
<vDAR>0.00</vDAR>
<CNPJ>16907746000113</CNPJ>
<xOrgao>Secretaria de Estado de Fazenda de Minas Gerais</xOrgao>
<repEmi>AF2 NIVELITUIUTABA</repEmi>
</avulsa>
<infAdic>
<infAdFisco>Valor do frete: R$1.00||| CONTRATO CC0101920000002 || || EVERTON TIAGO PUGAS; CPF: 298.273.248-37; CAVALO:KEH-1I34; REBOQUE: SUJ-1J12 ||O requerente deverá informar os dados do transportador no verso da NFA. Caso o transporte seja realizado por pessoa física/jurídica não inscrita no cadastro de contribuintes de Minas Gerais, esta Nota Fiscal deverá estar acompanhada do comprovante de recolhimento do ICMS sobre o transporte, se devido. Tipo de Emissão: Normal</infAdFisco>
</infAdic>
<det>
<nItem>1</nItem>
<prod>
<cEAN>SEM GTIN</cEAN>
<cProd>31</cProd>
<qCom>41000.0000</qCom>
<cEANTrib>SEM GTIN</cEANTrib>
<vUnTrib>1.9667</vUnTrib>
<qTrib>41000.0000</qTrib>
<vProd>80634.70</vProd>
<xProd>SOJA/SOJA EM GRAOS</xProd>
<vUnCom>1.9667</vUnCom>
<indTot>1</indTot>
<uTrib>KG</uTrib>
<NCM>12019000</NCM>
<uCom>KG</uCom>
<CFOP>5101</CFOP>
</prod>
<imposto>
<ICMS>
<ICMS40>
<orig>0</orig>
<CST>40</CST>
</ICMS40>
</ICMS>
<COFINS>
<COFINSNT>
<CST>08</CST>
</COFINSNT>
</COFINS>
<PIS>
<PISNT>
<CST>08</CST>
</PISNT>
</PIS>
</imposto>
</det>
<total>
<ICMSTot>
<vICMSUFDest>0.00</vICMSUFDest>
<vICMSUFRemet>0.00</vICMSUFRemet>
<vCOFINS>0</vCOFINS>
<vBCST>0.00</vBCST>
<vICMSDeson>0.00</vICMSDeson>
<vFCPUFDest>0.00</vFCPUFDest>
<vProd>80634.70</vProd>
<vSeg>0.00</vSeg>
<vFCP>0.00</vFCP>
<vFCPST>0.00</vFCPST>
<vNF>80634.70</vNF>
<vPIS>0</vPIS>
<vIPIDevol>0.00</vIPIDevol>
<vBC>0.00</vBC>
<vST>0.00</vST>
<vICMS>0.00</vICMS>
<vII>0.00</vII>
<vFCPSTRet>0.00</vFCPSTRet>
<vDesc>0.00</vDesc>
<vOutro>0.00</vOutro>
<vIPI>0.00</vIPI>
<vFrete>0.00</vFrete>
</ICMSTot>
</total>
<pag>
<vTroco>0.00</vTroco>
<detPag>
<vPag>80634.70</vPag>
<tPag>01</tPag>
</detPag>
</pag>
<Id>NFe31250216907746000113558900466584991781959936</Id>
<ide>
<tpNF>1</tpNF>
<mod>55</mod>
<indPres>0</indPres>
<tpImp>1</tpImp>
<nNF>46658499</nNF>
<cMunFG>3134202</cMunFG>
<procEmi>1</procEmi>
<finNFe>1</finNFe>
<dhEmi>2025-02-16T13:37:19-03:00</dhEmi>
<tpAmb>1</tpAmb>
<indFinal>0</indFinal>
<dhSaiEnt>2025-02-16T13:37:19-03:00</dhSaiEnt>
<idDest>1</idDest>
<tpEmis>1</tpEmis>
<cDV>6</cDV>
<cUF>31</cUF>
<serie>890</serie>
<natOp>VENDA</natOp>
<cNF>78195993</cNF>
<verProc>NFA_VER_1.00</verProc>
</ide>
<emit>
<xNome>ANTONIO SEBASTIAO FRANCO</xNome>
<CRT>3</CRT>
<CPF>51372037691</CPF>
<enderEmit>
<UF>MG</UF>
<xLgr>CAMPO ALEGRE DOS IPES II</xLgr>
<xMun>ITUIUTABA</xMun>
<nro>SN</nro>
<cMun>3134202</cMun>
<xBairro>ZONA RURAL</xBairro>
<CEP>38300000</CEP>
</enderEmit>
<IE>0014299980492</IE>
</emit>
<dest>
<xNome>NATIVA AGRONEGOCIOS REPRESENTACOES LTDA</xNome>
<CNPJ>03856216000141</CNPJ>
<enderDest>
<UF>MG</UF>
<xLgr>JUSCELINO KUBITSCHEK DE OLIVEIRA</xLgr>
<xMun>PATOS DE MINAS</xMun>
<nro>1810</nro>
<cMun>3148004</cMun>
<xBairro>IPANEMA</xBairro>
<CEP>38706491</CEP>
</enderDest>
<IE>4810814470038</IE>
<indIEDest>1</indIEDest>
</dest>
<transp>
<modFrete>0</modFrete>
</transp>
</infNFe>
</NFe>
<versao>4.00</versao>
</nfeProc>
'''
    
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    
    print(f"✓ Arquivo XML criado: {xml_path}")


def get_metadata_json():
    """Retorna o JSON de metadados do pedido de compra para BARTER (Commodities)"""
    return {
        "header": {
            "tenantId": "00,050101"
        },
        "requestBody": {
            "isCommodities": True,
            "itens": [
                {
                    "codigoProduto": "AAK00001KG00600",
                    "produto": "SOJA"
                }
            ],
            "cnpjDestinatario": "03856216000141"
        }
    }


def upload_file_to_s3(presigned_url: str, file_content: bytes, content_type: str):
    """Faz upload de um arquivo para S3 usando presigned URL"""
    response = requests.put(
        presigned_url,
        data=file_content,
        headers={'Content-Type': content_type}
    )
    response.raise_for_status()
    return response


def test_create_process(api_url: str, api_key: str, xml_file: str = None, start_process: bool = False):
    """Cria um processo de teste BARTER com documentos - SEMPRE gera um novo processo único"""
    
    print("="*80)
    print("TESTE DE CRIAÇÃO DE PROCESSO BARTER (COMMODITIES) COM DOCUMENTOS")
    print("="*80)
    print(f"\nAPI URL: {api_url}")
    print(f"API Key: {api_key[:20]}..." if len(api_key) > 20 else f"API Key: {api_key}")
    print()
    
    # SEMPRE gerar um novo process_id único (nunca reutilizar)
    import time
    process_id = str(uuid.uuid4())
    timestamp = int(time.time())
    print(f"✓ Novo Process ID gerado: {process_id}")
    print(f"   Timestamp: {timestamp}")
    print(f"   (Cada execução cria um processo completamente novo e único)")
    print(f"   Tipo: BARTER (isCommodities: true)")
    
    # Preparar arquivo XML com nome único baseado no process_id e timestamp
    if xml_file is None:
        # Usar nome único baseado no process_id e timestamp para garantir unicidade
        import time
        timestamp = int(time.time())
        xml_file = f"test_nfe_barter_{process_id[:8]}_{timestamp}.xml"
    
    # Limpar arquivo XML antigo se existir (para garantir que não há conflitos)
    if os.path.exists(xml_file):
        try:
            os.remove(xml_file)
            print(f"   (Arquivo XML antigo removido para evitar conflitos)")
        except Exception as e:
            print(f"   ⚠️  Aviso: Não foi possível remover arquivo antigo: {e}")
    
    # Sempre criar um novo XML
    print(f"\n📄 Criando arquivo XML: {xml_file}")
    print(f"   (Nome único para evitar conflitos com execuções anteriores)")
    create_xml_file(xml_file)
    
    # Ler XML
    print(f"\n📄 Lendo arquivo XML: {xml_file}")
    with open(xml_file, 'rb') as f:
        xml_content = f.read()
    
    xml_filename = os.path.basename(xml_file)
    print(f"✓ XML carregado ({len(xml_content)} bytes)")
    
    # 0. Verificar se o processo já existe (não deveria, mas vamos validar)
    print(f"\n{'='*80}")
    print("0️⃣  VERIFICANDO SE PROCESSO JÁ EXISTE")
    print(f"{'='*80}")
    print(f"   Process ID: {process_id}")
    
    try:
        check_response = requests.get(
            f"{api_url}/api/process/{process_id}",
            headers={'x-api-key': api_key}
        )
        if check_response.ok:
            existing_data = check_response.json()
            existing_files = existing_data.get('files', {}).get('danfe', [])
            if existing_files:
                print(f"⚠️  AVISO: Processo {process_id} já existe com {len(existing_files)} arquivo(s) DANFE!")
                print(f"   Isso não deveria acontecer - gerando novo Process ID...")
                # Gerar novo process_id se o anterior já existir
                process_id = str(uuid.uuid4())
                print(f"✓ Novo Process ID gerado: {process_id}")
            else:
                print(f"✓ Processo não existe ou está vazio (OK para criar novo)")
        else:
            print(f"✓ Processo não existe (OK para criar novo)")
    except Exception as e:
        print(f"ℹ️  Não foi possível verificar processo existente: {e}")
        print(f"   Continuando com criação do processo...")
    
    # 1. Obter presigned URL para XML (DANFE)
    print(f"\n{'='*80}")
    print("1️⃣  OBTENDO URL PARA UPLOAD DO XML (DANFE)")
    print(f"{'='*80}")
    print(f"   Process ID: {process_id}")
    print(f"   Arquivo: {xml_filename}")
    
    xml_url_response = requests.post(
        f"{api_url}/api/process/presigned-url/xml",
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key
        },
        json={
            'process_id': process_id,
            'file_name': xml_filename,
            'file_type': 'application/xml'
        }
    )
    
    if not xml_url_response.ok:
        print(f"❌ Erro ao obter URL para XML: {xml_url_response.status_code}")
        print(xml_url_response.text)
        return None
    
    xml_url_data = xml_url_response.json()
    print(f"✓ URL obtida: {xml_url_data['upload_url'][:80]}...")
    
    # 2. Fazer upload do XML
    print(f"\n{'='*80}")
    print("2️⃣  FAZENDO UPLOAD DO XML")
    print(f"{'='*80}")
    print(f"   Process ID: {process_id}")
    print(f"   Arquivo: {xml_filename}")
    print(f"   Tamanho: {len(xml_content)} bytes")
    
    try:
        upload_file_to_s3(
            xml_url_data['upload_url'],
            xml_content,
            'application/xml'
        )
        print(f"✓ XML enviado com sucesso para o processo {process_id}")
        print(f"   (Este é um processo NOVO - não reutiliza processos anteriores)")
    except Exception as e:
        print(f"❌ Erro ao fazer upload do XML: {e}")
        return None
    
    # 3. Vincular metadados do pedido de compra (sem arquivo físico)
    print(f"\n{'='*80}")
    print("3️⃣  VINCULANDO METADADOS DO PEDIDO DE COMPRA (BARTER)")
    print(f"{'='*80}")
    print(f"   Process ID: {process_id}")
    print(f"   isCommodities: true (será detectado como BARTER)")
    
    metadata = get_metadata_json()
    
    metadata_response = requests.post(
        f"{api_url}/api/process/metadados/pedido",
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key
        },
        json={
            'process_id': process_id,
            'metadados': metadata
        }
    )
    
    if not metadata_response.ok:
        print(f"❌ Erro ao vincular metadados: {metadata_response.status_code}")
        print(metadata_response.text)
        return None
    
    metadata_data = metadata_response.json()
    print(f"✓ Metadados vinculados com sucesso ao processo {process_id}!")
    print(f"   Nome do documento: {metadata_data.get('file_name')}")
    print(f"   Process ID verificado: {metadata_data.get('process_id')}")
    print(f"   isCommodities: {metadata.get('requestBody', {}).get('isCommodities')}")
    
    # Validar que o process_id retornado é o mesmo que enviamos
    if metadata_data.get('process_id') != process_id:
        print(f"⚠️  AVISO: Process ID retornado difere do enviado!")
        print(f"   Enviado: {process_id}")
        print(f"   Retornado: {metadata_data.get('process_id')}")
    
    # 4. Verificar processo criado
    print(f"\n{'='*80}")
    print("4️⃣  VERIFICANDO PROCESSO CRIADO")
    print(f"{'='*80}")
    
    try:
        process_response = requests.get(
            f"{api_url}/api/process/{process_id}",
            headers={'x-api-key': api_key}
        )
        
        if process_response.ok:
            process_data = process_response.json()
            print(f"✓ Processo verificado:")
            print(f"   Process ID: {process_data.get('process_id')}")
            print(f"   Status: {process_data.get('status')}")
            print(f"   Tipo: {process_data.get('process_type')} (deve ser BARTER após start)")
            
            danfe_files = process_data.get('files', {}).get('danfe', [])
            additional_files = process_data.get('files', {}).get('additional', [])
            
            print(f"   Arquivos DANFE: {len(danfe_files)} arquivo(s)")
            for idx, danfe_file in enumerate(danfe_files, 1):
                print(f"     {idx}. {danfe_file.get('file_name', 'N/A')} - {danfe_file.get('status', 'N/A')}")
            
            print(f"   Arquivos adicionais: {len(additional_files)} arquivo(s)")
            
            # Validar que há apenas 1 arquivo DANFE (o que acabamos de enviar)
            if len(danfe_files) != 1:
                print(f"\n⚠️  AVISO: Esperado 1 arquivo DANFE, mas encontrado {len(danfe_files)}!")
                print(f"   Isso pode indicar que há arquivos de execuções anteriores.")
                print(f"   Process ID atual: {process_id}")
            
            # Mostrar metadados do pedido de compra
            if additional_files:
                for file_info in additional_files:
                    if file_info.get('metadata_only'):
                        if 'metadados' in file_info:
                            print(f"\n   Metadados do pedido de compra:")
                            print(f"   {json.dumps(file_info['metadados'], indent=6, ensure_ascii=False)}")
        else:
            print(f"⚠️  Não foi possível verificar o processo: {process_response.status_code}")
    except Exception as e:
        print(f"⚠️  Erro ao verificar processo: {e}")
    
    # 5. Iniciar processo (opcional)
    if start_process:
        print(f"\n{'='*80}")
        print("5️⃣  INICIANDO PROCESSAMENTO (BARTER)")
        print(f"{'='*80}")
        
        try:
            start_response = requests.post(
                f"{api_url}/api/process/start",
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': api_key
                },
                json={
                    'process_id': process_id
                }
            )
            
            if start_response.ok:
                start_data = start_response.json()
                print(f"✓ Processamento iniciado!")
                print(f"   Execution ARN: {start_data.get('execution_arn')}")
                print(f"   Status: {start_data.get('status')}")
                print(f"   Process Type: {start_data.get('process_type')} (deve ser BARTER)")
            else:
                print(f"❌ Erro ao iniciar processamento: {start_response.status_code}")
                print(start_response.text)
        except Exception as e:
            print(f"❌ Erro ao iniciar processamento: {e}")
    
    # Resumo final
    print(f"\n{'='*80}")
    print("✅ NOVO PROCESSO BARTER CRIADO COM SUCESSO!")
    print(f"{'='*80}")
    print(f"\n📋 Process ID: {process_id}")
    print(f"   (Este é um processo NOVO e ÚNICO - Tipo: BARTER)")
    print(f"\n📄 Arquivos:")
    print(f"   - XML (DANFE): {xml_filename}")
    print(f"   - Metadados do pedido de compra: vinculados (isCommodities: true)")
    print(f"\n🔗 URLs:")
    print(f"   - Ver processo: GET {api_url}/api/process/{process_id}")
    print(f"   - Iniciar processamento: POST {api_url}/api/process/start")
    print(f"     Body: {{\"process_id\": \"{process_id}\"}}")
    print(f"\n💡 Dica: Cada execução deste script cria um processo completamente novo!")
    print(f"💡 Dica: O tipo BARTER será detectado automaticamente ao iniciar o processamento")
    
    return process_id


def main():
    parser = argparse.ArgumentParser(description='Cria um processo de teste BARTER (Commodities) com documentos')
    parser.add_argument('--api-url', help='URL base da API (padrão: https://kv8riifhmh.execute-api.us-east-1.amazonaws.com/v1)')
    parser.add_argument('--api-key', help='Chave de API (padrão: agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx)')
    parser.add_argument('--xml-file', help='Caminho para arquivo XML (padrão: test_nfe_barter.xml)')
    parser.add_argument('--start', action='store_true', help='Iniciar processamento após criar')
    parser.add_argument('--env-file', default='.env', help='Arquivo .env para carregar variáveis (padrão: .env)')
    
    args = parser.parse_args()
    
    # Valores padrão (dev environment)
    default_api_url = 'https://gx3eyeb4i1.execute-api.us-east-1.amazonaws.com/v1'
    default_api_key = 'agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx'
    
    # Tentar carregar do arquivo .env se existir
    api_url = args.api_url
    api_key = args.api_key
    
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
                    
                    if key == 'API_URL' and not api_url:
                        api_url = value
                    elif key == 'API_KEY' and not api_key:
                        api_key = value
    
    # Usar valores padrão se não fornecidos
    if not api_url:
        api_url = default_api_url
        print(f"ℹ️  Usando API URL padrão: {api_url}")
    if not api_key:
        api_key = default_api_key
        print(f"ℹ️  Usando API Key padrão: {api_key[:30]}...")
    
    # Executar teste
    process_id = test_create_process(
        api_url=api_url,
        api_key=api_key,
        xml_file=args.xml_file,
        start_process=args.start
    )
    
    if process_id:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
