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
    """Cria arquivo XML com o XML fornecido do TIMAC AGRO"""
    
    if os.path.exists(xml_path):
        print(f"ℹ️  Arquivo XML já existe: {xml_path} - será recriado")
    
    xml_content = '''<?xml version="1.0" encoding="utf-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">
  <protNFe>
    <infProt>
      <nProt>243250401092571</nProt>
      <digVal>uDFG3AOUbDv+HpkHatR+kzsOcjo</digVal>
      <dhRecbto>2025-12-21T19:13:28-03:00</dhRecbto>
      <chNFe>43251202329713000200550040004742931458693440</chNFe>
      <xMotivo>Autorizado o uso da NF-e</xMotivo>
      <cStat>100</cStat>
    </infProt>
  </protNFe>
  <NFe>
    <infNFe Id="NFe43251202329713000200550040004742931458693440">
      <ide>
        <tpNF>1</tpNF>
        <mod>55</mod>
        <indPres>9</indPres>
        <tpImp>1</tpImp>
        <nNF>474293</nNF>
        <cMunFG>4315602</cMunFG>
        <procEmi>0</procEmi>
        <finNFe>1</finNFe>
        <dhEmi>2025-12-21T19:12:51-03:00</dhEmi>
        <tpAmb>1</tpAmb>
        <indFinal>0</indFinal>
        <dhSaiEnt>2025-12-21T19:12:51-03:00</dhSaiEnt>
        <idDest>2</idDest>
        <tpEmis>1</tpEmis>
        <cDV>0</cDV>
        <cUF>43</cUF>
        <serie>4</serie>
        <natOp>VENDA PROD ESTABELECIMENTO</natOp>
        <cNF>45869344</cNF>
        <verProc>12.1.2410 | 3.0</verProc>
        <indIntermed>0</indIntermed>
      </ide>
      <emit>
        <xNome>TIMAC AGRO INDUSTRIA E COMERCIO DE FERTILIZANTES LTDA</xNome>
        <IM>432060</IM>
        <CRT>3</CRT>
        <xFant>RIO GRANDE</xFant>
        <CNPJ>02329713000200</CNPJ>
        <enderEmit>
          <xCpl>KM 002 CONJ.B</xCpl>
          <fone>5321258100</fone>
          <UF>RS</UF>
          <xPais>BRASIL</xPais>
          <cPais>1058</cPais>
          <xLgr>ALMT. MAXIMIANO FONSECA</xLgr>
          <xMun>RIO GRANDE</xMun>
          <nro>1550</nro>
          <cMun>4315602</cMun>
          <xBairro>Zona Portuaria</xBairro>
          <CEP>96204040</CEP>
        </enderEmit>
        <IE>1000194164</IE>
      </emit>
      <dest>
        <xNome>AGRO AMAZONIA PRODUTOS AGROPECUARIOS S.A.</xNome>
        <IM>1</IM>
        <CNPJ>13563680000365</CNPJ>
        <enderDest>
          <fone>55669938866</fone>
          <UF>MT</UF>
          <xPais>BRASIL</xPais>
          <cPais>1058</cPais>
          <xLgr>AV ITRIO CORREA DA COST</xLgr>
          <xMun>RONDONOPOLIS</xMun>
          <nro>1647</nro>
          <cMun>5107602</cMun>
          <xBairro>VILA SALMEM</xBairro>
          <CEP>78745160</CEP>
        </enderDest>
        <IE>130614270</IE>
        <indIEDest>1</indIEDest>
      </dest>
      <det>
        <nItem>1</nItem>
        <prod>
          <cEAN>SEM GTIN</cEAN>
          <cProd>26480</cProd>
          <qCom>40.0000</qCom>
          <cEANTrib>SEM GTIN</cEANTrib>
          <vUnTrib>4228.0000000000</vUnTrib>
          <qTrib>40.0000</qTrib>
          <vProd>169120.00</vProd>
          <nFCI>ED5E9C4B-8507-4990-B51A-306C1209C47A</nFCI>
          <xProd>FERTILIZANTE TOP PHOS 280 HP B1</xProd>
          <vUnCom>4228.0000000000</vUnCom>
          <indTot>1</indTot>
          <uTrib>TON</uTrib>
          <NCM>31055900</NCM>
          <uCom>TON</uCom>
          <CFOP>6101</CFOP>
        </prod>
        <imposto>
          <IBSCBS>
            <CST>515</CST>
            <cClassTrib>515001</cClassTrib>
            <gIBSCBS>
              <vIBS>0.00</vIBS>
              <gCBS>
                <gDif>
                  <pDif>100.0000</pDif>
                  <vDif>584.48</vDif>
                </gDif>
                <pCBS>0.9000</pCBS>
                <vCBS>0.00</vCBS>
                <gRed>
                  <pAliqEfet>0.3600</pAliqEfet>
                  <pRedAliq>60.0000</pRedAliq>
                </gRed>
              </gCBS>
              <gIBSUF>
                <gDif>
                  <pDif>100.0000</pDif>
                  <vDif>64.94</vDif>
                </gDif>
                <pIBSUF>0.1000</pIBSUF>
                <gRed>
                  <pAliqEfet>0.0400</pAliqEfet>
                  <pRedAliq>60.0000</pRedAliq>
                </gRed>
                <vIBSUF>0.00</vIBSUF>
              </gIBSUF>
              <vBC>162355.54</vBC>
              <gIBSMun>
                <gDif>
                  <pDif>100.0000</pDif>
                  <vDif>0.00</vDif>
                </gDif>
                <pIBSMun>0.0000</pIBSMun>
                <vIBSMun>0.00</vIBSMun>
                <gRed>
                  <pAliqEfet>0.0000</pAliqEfet>
                  <pRedAliq>60.0000</pRedAliq>
                </gRed>
              </gIBSMun>
            </gIBSCBS>
          </IBSCBS>
          <ICMS>
            <ICMS20>
              <modBC>3</modBC>
              <pRedBC>42.8600</pRedBC>
              <orig>5</orig>
              <CST>20</CST>
              <vBC>96635.17</vBC>
              <vICMS>6764.46</vICMS>
              <pICMS>7.0000</pICMS>
            </ICMS20>
          </ICMS>
          <IPI>
            <IPINT>
              <CST>53</CST>
            </IPINT>
            <cEnq>999</cEnq>
          </IPI>
          <COFINS>
            <COFINSNT>
              <CST>06</CST>
            </COFINSNT>
          </COFINS>
          <PIS>
            <PISNT>
              <CST>06</CST>
            </PISNT>
          </PIS>
        </imposto>
        <vItem>169120.00</vItem>
        <infAdProd>RS 000155-0.000048 FERTILIZANTE MINERAL COMPLEXO.7% S-SO4 %N: 3,000 %P2O5 Total: 28,000 %P2O5 SOL CNA + H2O: 22,000 %P2O5 SOL H2O: 18,000%CA: 17,000%S: 7,000 Nat. Fisica: GRANULADO RESOLUCAO DO SENADO FEDERAL 13/2012. NUMERO DA FCI ED5E9C4B-8507-4990-B51A-306C1209C47A, CONTEUDO DA IMPORTACAO: 0,00</infAdProd>
      </det>
      <total>
        <vNFTot>169120.00</vNFTot>
        <ICMSTot>
          <vCOFINS>0.00</vCOFINS>
          <vBCST>0.00</vBCST>
          <vICMSDeson>0.00</vICMSDeson>
          <vProd>169120.00</vProd>
          <vSeg>0.00</vSeg>
          <vFCP>0.00</vFCP>
          <vFCPST>0.00</vFCPST>
          <vNF>169120.00</vNF>
          <vPIS>0.00</vPIS>
          <vIPIDevol>0.00</vIPIDevol>
          <vBC>96635.17</vBC>
          <vST>0.00</vST>
          <vICMS>6764.46</vICMS>
          <vII>0.00</vII>
          <vFCPSTRet>0.00</vFCPSTRet>
          <vDesc>0.00</vDesc>
          <vOutro>0.00</vOutro>
          <vIPI>0.00</vIPI>
          <vFrete>0.00</vFrete>
        </ICMSTot>
        <IBSCBSTot>
          <gCBS>
            <vDevTrib>0.00</vDevTrib>
            <vCredPres>0.00</vCredPres>
            <vCredPresCondSus>0.00</vCredPresCondSus>
            <vCBS>0.00</vCBS>
            <vDif>584.48</vDif>
          </gCBS>
          <vBCIBSCBS>162355.54</vBCIBSCBS>
          <gIBS>
            <vIBS>0.00</vIBS>
            <gIBSUF>
              <vDevTrib>0.00</vDevTrib>
              <vIBSUF>0.00</vIBSUF>
              <vDif>64.94</vDif>
            </gIBSUF>
            <vCredPres>0.00</vCredPres>
            <vCredPresCondSus>0.00</vCredPresCondSus>
            <gIBSMun>
              <vDevTrib>0.00</vDevTrib>
              <vIBSMun>0.00</vIBSMun>
              <vDif>0.00</vDif>
            </gIBSMun>
          </gIBS>
        </IBSCBSTot>
      </total>
      <transp>
        <modFrete>0</modFrete>
        <vol>
          <marca>TOP-PHOS</marca>
          <pesoL>40000.000</pesoL>
          <esp>BIG-BAG 1000KG</esp>
          <qVol>40</qVol>
          <nVol>1/40</nVol>
          <pesoB>40088.000</pesoB>
        </vol>
        <transporta>
          <xNome>COOCATRANS S.A</xNome>
          <UF>RS</UF>
          <xEnder>RUA ANTONIO ARAUJO,1046</xEnder>
          <xMun>PASSO FUNDO</xMun>
          <CNPJ>06308626000570</CNPJ>
          <IE>0910381526</IE>
        </transporta>
      </transp>
      <cobr>
        <fat>
          <vOrig>169120.00</vOrig>
          <nFat>02474293401</nFat>
          <vDesc>0.00</vDesc>
          <vLiq>169120.00</vLiq>
        </fat>
        <dup>
          <dVenc>2026-07-20</dVenc>
          <nDup>001</nDup>
          <vDup>169120.00</vDup>
        </dup>
      </cobr>
      <pag>
        <detPag>
          <vPag>169120.00</vPag>
          <tPag>15</tPag>
          <indPag>1</indPag>
        </detPag>
      </pag>
      <infAdic>
        <infCpl>LOTE:331/25 FABRIC:06/12/2025 VALID:18 MESES COD INTERNO:JDJ4I94 LACRES:656641 ATE 656648 BASE DE CALCULO REDUZIDA CFE RICMS RS LIVRO I, ART 23, INC LXXXIX , ALINEA B. PIS/PASEP E COFINS TRIBUTADOS A ALIQUOTA ZERO PARA USO EXCLUSIVO COMO FER TILIZANTE CFE ART 1 DA LEI 10.925/04. IPI NAO TRIBUTADO CFE CAPITULO 31 DA TIPI END. COBR.: AVENIDA ITRIO CORREA DA COST, 1647 BAIRRO: VILA SALMEM CEP: 78745-160 CIDADE: RONDONOPOLIS UF: MT END. ENTREGA - RAZAO SOCIAL: AGRO AMAZONIA PRODUTOS AGROPECUARIOS ENDERECO: AV ITRIO CORREA DA COST,1647 BAIRRO: VILA SALMEM CEP: 78745-160 CIDADE: RONDONOPOLIS UF: MT CNPJ: 013.563.680/0003-65 INS. ESTADUAL: 130614270 PEDIDO: 582992 NR. ORDEM DE CARREGAMENTO: 653603 DETALHES CALCULO ICMS: PCT ICMS 7,00% PCT RED BASE ICMS 42,86% VALOR BASE ICMS 96635,17 VALOR ICMS 6764,46</infCpl>
      </infAdic>
    </infNFe>
  </NFe>
  <versao>4.00</versao>
</nfeProc>
'''
    
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    
    print(f"✓ Arquivo XML criado: {xml_path}")


def get_metadata_json():
    """Retorna o JSON de metadados do pedido de compra 582992"""
    return {
        "header": {
            "tenantId": "00,010101"
        },
        "requestBody": {
            "cnpjEmitente": "02329713000200",
            "cnpjDestinatario": "13563680000365",
            "itens": [
                {
                    "codigoProduto": "26480",
                    "produto": "FERTILIZANTE TOP PHOS 280 HP B1",
                    "quantidade": 40.0,
                    "valorUnitario": 4228.0,
                    "valorTotal": 169120.0,
                    "unidadeMedida": "TON",
                    "pedidoDeCompra": {
                        "pedidoErp": "582992",
                        "itemPedidoErp": "0001"
                    }
                }
            ]
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
    """Cria um processo de teste com documentos - SEMPRE gera um novo processo único"""
    
    print("="*80)
    print("TESTE DE CRIAÇÃO DE PROCESSO COM DOCUMENTOS")
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
    
    # Preparar arquivo XML com nome único baseado no process_id e timestamp
    if xml_file is None:
        # Usar nome único baseado no process_id e timestamp para garantir unicidade
        import time
        timestamp = int(time.time())
        xml_file = f"test_nfe_{process_id[:8]}_{timestamp}.xml"
    
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
    print("3️⃣  VINCULANDO METADADOS DO PEDIDO DE COMPRA")
    print(f"{'='*80}")
    print(f"   Process ID: {process_id}")
    
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
    
    # Validar que o process_id retornado é o mesmo que enviamos
    if metadata_data.get('process_id') != process_id:
        print(f"⚠️  AVISO: Process ID retornado difere do enviado!")
        print(f"   Enviado: {process_id}")
        print(f"   Retornado: {metadata_data.get('process_id')}")
    
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
            print(f"✓ Processo verificado:")
            print(f"   Process ID: {process_data.get('process_id')}")
            print(f"   Status: {process_data.get('status')}")
            print(f"   Tipo: {process_data.get('process_type')}")
            
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
    print("✅ NOVO PROCESSO CRIADO COM SUCESSO!")
    print(f"{'='*80}")
    print(f"\n📋 Process ID: {process_id}")
    print(f"   (Este é um processo NOVO e ÚNICO)")
    print(f"\n📄 Arquivos:")
    print(f"   - XML (DANFE): {xml_filename}")
    print(f"   - Metadados do pedido de compra: vinculados")
    print(f"\n🔗 URLs:")
    print(f"   - Ver processo: GET {api_url}/api/process/{process_id}")
    print(f"   - Iniciar processamento: POST {api_url}/api/process/start")
    print(f"     Body: {{\"process_id\": \"{process_id}\"}}")
    print(f"\n💡 Dica: Cada execução deste script cria um processo completamente novo!")
    
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

