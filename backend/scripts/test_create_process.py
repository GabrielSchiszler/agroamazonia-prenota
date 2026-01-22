#!/usr/bin/env python3
"""
Script para criar um processo de teste com documentos

Uso:
    python3 test_create_process.py --api-url <URL> --api-key <KEY> [--xml-file <arquivo.xml>] [--start]

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
    """Sempre cria um novo arquivo XML com número aleatório para cNF"""
    # Gerar número aleatório de 8 dígitos para cNF
    cnf_random = f"{random.randint(0, 99999999):08d}"
    nnf_random = f"{random.randint(0, 99999):08d}"

    if os.path.exists(xml_path):
        print(f"ℹ️  Arquivo XML já existe: {xml_path} - será recriado com novo número aleatório")
    
    # Gerar ID da NFe baseado no cNF aleatório (mantendo estrutura de 44 dígitos)
    # Formato: NFe + 44 dígitos (onde os últimos 8 são o cNF)
    # Exemplo: NFe3125094718062500561055005000016620{cNF}
    # A base deve ter 36 dígitos para que com o cNF de 8 dígitos totalize 44
    nfe_id_base = "3125094718062500561055005000016620"  # 34 dígitos fixos
    # Adicionar 2 dígitos para completar 36 (totalizando 44 com cNF de 8)
    nfe_id_base = nfe_id_base + "00"  # Agora tem 36 dígitos
    nfe_id = f"NFe{nfe_id_base}{cnf_random}"
    ch_nfe = f"{nfe_id_base}{cnf_random}"  # Total: 36 + 8 = 44 dígitos
    
    xml_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">
<NFe xmlns="http://www.portalfiscal.inf.br/nfe">
<infNFe versao="4.00" Id="{nfe_id}">
<ide>
<cUF>31</cUF>
<cNF>{cnf_random}</cNF>
<natOp>Venda merc.adq.receb.de terceiros</natOp>
<mod>55</mod>
<serie>5</serie>
<nNF>{nnf_random}</nNF>
<dhEmi>2025-09-27T13:50:46-03:00</dhEmi>
<dhSaiEnt>2025-09-27T13:50:46-03:00</dhSaiEnt>
<tpNF>1</tpNF>
<idDest>2</idDest>
<cMunFG>3170107</cMunFG>
<tpImp>2</tpImp>
<tpEmis>1</tpEmis>
<cDV>9</cDV>
<tpAmb>1</tpAmb>
<finNFe>1</finNFe>
<indFinal>0</indFinal>
<indPres>9</indPres>
<indIntermed>0</indIntermed>
<procEmi>0</procEmi>
<verProc>SAP CLOUD NFE</verProc>
</ide>
<emit>
<CNPJ>47180625005610</CNPJ>
<xNome>CTVA PROTECAO DE CULTIVOS LTDA.</xNome>
<xFant>CTVA Uberaba</xFant>
<enderEmit>
<xLgr>ROD BR 050</xLgr>
<nro>S/N</nro>
<xCpl>KM 185 SAL</xCpl>
<xBairro>ZONA RURAL</xBairro>
<cMun>3170107</cMun>
<xMun>UBERABA</xMun>
<UF>MG</UF>
<CEP>38001970</CEP>
<xPais>Brasil</xPais>
<fone>1141668034</fone>
</enderEmit>
<IE>0030275780279</IE>
<IM>0</IM>
<CRT>3</CRT>
</emit>
<dest>
<CNPJ>13563680000101</CNPJ>
<xNome>AGRO AMAZONIA PROD.AGROPEC.S.A.</xNome>
<enderDest>
<xLgr>AV.TENENTE CORONEL DUARTE 1777</xLgr>
<nro>1777</nro>
<xBairro>PORTO</xBairro>
<cMun>5103403</cMun>
<xMun>CUIABA</xMun>
<UF>MT</UF>
<CEP>78015500</CEP>
<cPais>1058</cPais>
<xPais>Brasil</xPais>
<fone>556533192000</fone>
</enderDest>
<indIEDest>1</indIEDest>
<IE>134219686</IE>
</dest>
<det nItem="1">
<prod>
<cProd>000000000005020943</cProd>
<cEAN>7898312160344</cEAN>
<xProd>GARLON480BR BOMBONA 20L HERBICIDA</xProd>
<NCM>38089329</NCM>
<CFOP>6102</CFOP>
<uCom>L</uCom>
<qCom>3000.0000</qCom>
<vUnCom>52.0041500000</vUnCom>
<vProd>156012.45</vProd>
<cEANTrib>7898312160344</cEANTrib>
<uTrib>L</uTrib>
<qTrib>3000.0000</qTrib>
<vUnTrib>52.0041500000</vUnTrib>
<indTot>1</indTot>
<xPed>ID 7942</xPed>
<rastro>
<nLote>25I4813004</nLote>
<qLote>3000.000</qLote>
<dFab>2025-09-23</dFab>
<dVal>2028-09-22</dVal>
</rastro>
</prod>
<imposto>
<ICMS>
<ICMS20>
<orig>5</orig>
<CST>20</CST>
<modBC>3</modBC>
<pRedBC>60.0000</pRedBC>
<vBC>62404.98</vBC>
<pICMS>7.0000</pICMS>
<vICMS>4368.35</vICMS>
<vICMSDeson>6552.52</vICMSDeson>
<motDesICMS>3</motDesICMS>
<indDeduzDeson>1</indDeduzDeson>
</ICMS20>
</ICMS>
<IPI>
<cEnq>999</cEnq>
<IPINT>
<CST>53</CST>
</IPINT>
</IPI>
<PIS>
<PISNT>
<CST>06</CST>
</PISNT>
</PIS>
<COFINS>
<COFINSNT>
<CST>06</CST>
</COFINSNT>
</COFINS>
</imposto>
<infAdProd>UN1993, LÍQUIDO INFLAMÁVEL, N.E. (Querosene (petróleo), Isobutanol) ,3 , III , Número de risco 30 POLUENTE MARINHO (Triclopir-2-butoxietil éster)</infAdProd>
</det>
<total>
<ICMSTot>
<vBC>89031.10</vBC>
<vICMS>6232.18</vICMS>
<vICMSDeson>9348.26</vICMSDeson>
<vFCP>0.00</vFCP>
<vBCST>0.00</vBCST>
<vST>0.00</vST>
<vFCPST>0.00</vFCPST>
<vFCPSTRet>0.00</vFCPSTRet>
<vProd>222577.76</vProd>
<vFrete>0.00</vFrete>
<vSeg>0.00</vSeg>
<vDesc>0.00</vDesc>
<vII>0.00</vII>
<vIPI>0.00</vIPI>
<vIPIDevol>0.00</vIPIDevol>
<vPIS>0.00</vPIS>
<vCOFINS>0.00</vCOFINS>
<vOutro>0.00</vOutro>
<vNF>213229.50</vNF>
</ICMSTot>
</total>
<transp>
<modFrete>0</modFrete>
<transporta>
<CNPJ>00950001000105</CNPJ>
<xNome>BRAVO SERVICOS LOGISTICOS LTDA</xNome>
<IE>7019582870070</IE>
<xEnder>ROD BR 050 KM 185 LOJA 08 SN</xEnder>
<xMun>UBERABA</xMun>
<UF>MG</UF>
</transporta>
<vol>
<qVol>4280</qVol>
<esp>Decímetro cúbico</esp>
<pesoL>4622.400</pesoL>
<pesoB>5050.400</pesoB>
</vol>
</transp>
<cobr>
<fat>
<nFat>7900121806</nFat>
<vOrig>222577.76</vOrig>
<vDesc>9348.26</vDesc>
<vLiq>213229.50</vLiq>
</fat>
<dup>
<nDup>001</nDup>
<dVenc>2026-02-05</dVenc>
<vDup>213229.50</vDup>
</dup>
</cobr>
<pag>
<detPag>
<indPag>1</indPag>
<tPag>18</tPag>
<vPag>213229.50</vPag>
</detPag>
</pag>
<infAdic>
<infAdFisco>ICMS - Base de cálculo do ICMS reduzida em 60% nos termos da Parte I do Anexo II do RICMS/MG. COFINS - Alíquota do COFINS reduzida a zero nos termos do art. 1º da Lei 10.925/2004. PIS - Alíquota do PIS reduzida a zero nos termos do art. 1º da Lei 10.925/2004. Desconto concedido conforme convenio 100/97: 133.546,66 X 7,00 % = R$ 9.348,26 .</infAdFisco>
<infCpl>PO: ID 7942 - OV: 1000565147 , DL: 0080839143 FO: 6100743062 Billing: 7900121806 'Declaro que os produtos perigosos estão adequadamente classificados, embalados, identificados, e estivados para suportar os riscos das operações de transporte e que atendem as exigências da regulamentação, Resolução ANTT 5.998/2022'</infCpl>
</infAdic>
<infRespTec>
<CNPJ>74544297000192</CNPJ>
<xContato>SAP Product Engineering</xContato>
<email>responsavel.tecnico@sap.com</email>
<fone>1155032400</fone>
</infRespTec>
</infNFe>
<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
<SignedInfo>
<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>
<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>
<Reference URI="#{nfe_id}">
<Transforms>
<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
<Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>
</Transforms>
<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>
<DigestValue>n1IEITIksnPsVqNEZhtBJaia/wc=</DigestValue>
</Reference>
</SignedInfo>
<SignatureValue>dLneFqMdF18FdSeTIxlmXQ0LelxtJQTYZRWw45YPUoh1png2IVyyaDa0D5kcWCyWM3szg8tzxnn6wi9XM4cG0FpU4M6RxKP8x5EkJdWp/+Ytqv/4NTNHVncDWeQWFSjcwT51e8mC5HubwW5ACywAU/higpdS/1wj9z85s0lT62r7BBkZc5w9MCx+eVaKABi6KUldUbUIuFX4vqp566flmybL82QmHbwcY+vY1Rmx2+E/ahampcr4BW8YUQpL8Eccgg9iGwjGO9hz1uHXartv+tRrCcOA5XheglbjHMmvO3r936CD/4mrUbE5DhopTYUdKr+fF9m11gx6QsKuH9aSLQ==</SignatureValue>
<KeyInfo>
<X509Data>
<X509Certificate>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</X509Certificate>
</X509Data>
</KeyInfo>
</Signature>
</NFe>
<protNFe versao="4.00">
<infProt>
<tpAmb>1</tpAmb>
<verAplic>W-3.3.7</verAplic>
<chNFe>{ch_nfe}</chNFe>
<dhRecbto>2025-09-27T13:51:18-03:00</dhRecbto>
<nProt>131256958571250</nProt>
<digVal>n1IEITIksnPsVqNEZhtBJaia/wc=</digVal>
<cStat>100</cStat>
<xMotivo>Autorizado o uso da NF-e</xMotivo>
</infProt>
</protNFe>
</nfeProc>
'''
    
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    
    print(f"✓ Arquivo XML criado: {xml_path}")


def get_metadata_json():
    """Retorna o JSON de metadados de exemplo"""
    return {
        "header": {
            "tenantId": "00,010101"
        },
        "requestBody": {
            "moeda": "BRL",
            "itens": [
                {
                    "codigoProduto": "DC700001TB00500",
                    "produto": "TORDON ULTRA-S DRMHPE TB 50 LT",
                    "valorUnitario": 1927.452188,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0001"
                    }
                },
                {
                    "codigoProduto": "DC700001TB00500",
                    "produto": "TORDON ULTRA-S DRMHPE TB 50 LT",
                    "valorUnitario": 1927.452188,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0001"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0002"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0002"
                    }
                },
                {
                    "codigoProduto": "DLR00002GL00100",
                    "produto": "DOMINUM XT-S GL 10 LT",
                    "valorUnitario": 848.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0003"
                    }
                },
                {
                    "codigoProduto": "DLR00002GL00100",
                    "produto": "DOMINUM XT-S GL 10 LT",
                    "valorUnitario": 848.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0003"
                    }
                },
                {
                    "codigoProduto": "9BA00001BD00200",
                    "produto": "GARLON 480 BD 20 LT",
                    "valorUnitario": 996.4,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0004"
                    }
                },
                {
                    "codigoProduto": "9BA00001BD00200",
                    "produto": "GARLON 480 BD 20 LT",
                    "valorUnitario": 996.4,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0004"
                    }
                },
                {
                    "codigoProduto": "9OG00001BD00205",
                    "produto": "PADRON HERBICIDE BD 20 LT",
                    "valorUnitario": 959.4,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0005"
                    }
                },
                {
                    "codigoProduto": "9OG00001BD00205",
                    "produto": "PADRON HERBICIDE BD 20 LT",
                    "valorUnitario": 959.4,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0005"
                    }
                },
                {
                    "codigoProduto": "AFG00005BD00203",
                    "produto": "TRUPER BD 20 LT",
                    "valorUnitario": 995.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0006"
                    }
                },
                {
                    "codigoProduto": "AFG00005BD00203",
                    "produto": "TRUPER BD 20 LT",
                    "valorUnitario": 995.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0006"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0007"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0007"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0008"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0008"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0009"
                    }
                },
                {
                    "codigoProduto": "DC700004BD00200",
                    "produto": "TORDON ULTRA-S BTLHPE BD 20 LT",
                    "valorUnitario": 738.6,
                    "pedidoDeCompra": {
                        "pedidoErp": "AA7116",
                        "itemPedidoErp": "0009"
                    }
                }
            ],
            "cnpjEmitente": "47180625006349",
            "cnpjDestinatario": "13563680000101"
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
    """Cria um processo de teste com documentos"""
    
    print("="*80)
    print("TESTE DE CRIAÇÃO DE PROCESSO COM DOCUMENTOS")
    print("="*80)
    print(f"\nAPI URL: {api_url}")
    print(f"API Key: {api_key[:20]}..." if len(api_key) > 20 else f"API Key: {api_key}")
    print()
    
    # Gerar process_id
    process_id = str(uuid.uuid4())
    print(f"✓ Process ID gerado: {process_id}")
    
    # Preparar arquivo XML
    if xml_file is None:
        xml_file = "test_nfe.xml"
    
    # Sempre criar um novo XML com número aleatório
    print(f"\n📄 Criando arquivo XML de exemplo: {xml_file}")
    create_xml_file(xml_file)
    
    # Ler XML
    print(f"\n📄 Lendo arquivo XML: {xml_file}")
    with open(xml_file, 'rb') as f:
        xml_content = f.read()
    
    xml_filename = os.path.basename(xml_file)
    print(f"✓ XML carregado ({len(xml_content)} bytes)")
    
    # 1. Obter presigned URL para XML (DANFE)
    print(f"\n{'='*80}")
    print("1️⃣  OBTENDO URL PARA UPLOAD DO XML (DANFE)")
    print(f"{'='*80}")
    
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
    
    try:
        upload_file_to_s3(
            xml_url_data['upload_url'],
            xml_content,
            'application/xml'
        )
        print(f"✓ XML enviado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao fazer upload do XML: {e}")
        return None
    
    # 3. Vincular metadados do pedido de compra (sem arquivo físico)
    print(f"\n{'='*80}")
    print("3️⃣  VINCULANDO METADADOS DO PEDIDO DE COMPRA")
    print(f"{'='*80}")
    
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
    print(f"✓ Metadados vinculados com sucesso!")
    print(f"   Nome do documento: {metadata_data.get('file_name')}")
    print(f"   Process ID: {metadata_data.get('process_id')}")
    
    # 4. Verificar processo criado
    print(f"\n{'='*80}")
    print("5️⃣  VERIFICANDO PROCESSO CRIADO")
    print(f"{'='*80}")
    
    try:
        process_response = requests.get(
            f"{api_url}/api/process/{process_id}",
            headers={'x-api-key': api_key}
        )
        
        if process_response.ok:
            process_data = process_response.json()
            print(f"✓ Processo encontrado:")
            print(f"   Status: {process_data.get('status')}")
            print(f"   Tipo: {process_data.get('process_type')}")
            print(f"   DANFE: {len(process_data.get('files', {}).get('danfe', []))} arquivo(s)")
            print(f"   Adicionais: {len(process_data.get('files', {}).get('additional', []))} arquivo(s)")
            
            # Mostrar metadados do pedido de compra
            additional_files = process_data.get('files', {}).get('additional', [])
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
        print("5️⃣  INICIANDO PROCESSAMENTO")
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
            else:
                print(f"❌ Erro ao iniciar processamento: {start_response.status_code}")
                print(start_response.text)
        except Exception as e:
            print(f"❌ Erro ao iniciar processamento: {e}")
    
    # Resumo final
    print(f"\n{'='*80}")
    print("✅ PROCESSO CRIADO COM SUCESSO!")
    print(f"{'='*80}")
    print(f"\nProcess ID: {process_id}")
    print(f"XML: {xml_filename}")
    print(f"Metadados do pedido de compra: vinculados (sem arquivo físico)")
    print(f"\nPara verificar o processo:")
    print(f"  GET {api_url}/api/process/{process_id}")
    print(f"\nPara iniciar o processamento:")
    print(f"  POST {api_url}/api/process/start")
    print(f"  Body: {{\"process_id\": \"{process_id}\"}}")
    
    return process_id


def main():
    parser = argparse.ArgumentParser(description='Cria um processo de teste com documentos')
    parser.add_argument('--api-url', help='URL base da API (padrão: https://kv8riifhmh.execute-api.us-east-1.amazonaws.com/v1)')
    parser.add_argument('--api-key', help='Chave de API (padrão: agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx)')
    parser.add_argument('--xml-file', help='Caminho para arquivo XML (padrão: test_nfe.xml)')
    parser.add_argument('--start', action='store_true', help='Iniciar processamento após criar')
    parser.add_argument('--env-file', default='.env', help='Arquivo .env para carregar variáveis (padrão: .env)')
    
    args = parser.parse_args()
    
    # Valores padrão (dev environment)
    default_api_url = 'https://kv8riifhmh.execute-api.us-east-1.amazonaws.com/v1'
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

