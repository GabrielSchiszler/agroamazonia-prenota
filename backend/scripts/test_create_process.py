#!/usr/bin/env python3
"""
Script para criar um processo de teste com documentos

Autenticação e HML: mesmo padrão de test_process_multilot_opteraduo_aws.py
  (OAuth2 client_credentials + Bearer; x-api-key opcional; User-Agent estilo navegador).

Uso:
    cd backend/scripts
    python3 test_create_process.py --api-url <URL> [--api-key ...] [--xml-file ...] [--start]

Homologação (igual ao multilot — vars em ../.env.homolog ou export):
    python3 test_create_process.py --homolog [--start]

    # Ou explícito:
    python3 test_create_process.py \\
      --env-file ../.env.homolog \\
      --api-url 'https://api-hml.agroamazonia.com/fast/v1' \\
      --start

Prioridade: argumentos CLI > variáveis no shell (os.environ) > arquivo --env-file
> defaults de dev.

Variáveis úteis: API_URL, API_KEY,
  OAUTH2_FRONTEND_TOKEN_URL, OAUTH2_FRONTEND_CLIENT_ID,
  OAUTH2_FRONTEND_CLIENT_SECRET, OAUTH2_FRONTEND_SCOPE
"""

from __future__ import annotations

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

# HML — mesmo padrão do frontend (app.js) e test_ritm_stg.py: …/fast/v1 + /process/…
HOMOLOG_API_URL = "https://api-hml.agroamazonia.com/fast/v1"
SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = SCRIPT_DIR.parent

_DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _load_env_file(path: Path) -> dict[str, str]:
    """Lê KEY=VAL de um .env (ignora comentários; suporta export). Igual ao multilot."""
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        line = line.replace("export ", "", 1)
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        out[k] = v
    return out


def _cfg_str(
    arg_val: str | None,
    file_env: dict[str, str],
    env_key: str,
    default: str | None = None,
) -> str | None:
    """Prioridade: CLI > variável no shell > arquivo .env > default."""
    if arg_val is not None and str(arg_val).strip():
        return str(arg_val).strip()
    v = os.environ.get(env_key)
    if v is not None and str(v).strip():
        return v.strip()
    v = file_env.get(env_key)
    if v is not None and str(v).strip():
        return str(v).strip()
    return default


def fetch_oauth2_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> str:
    """Mesmo fluxo que frontend/auth.js (grant_type=client_credentials)."""
    r = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"OAuth2 token HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    token = data.get("access_token") or data.get("accessToken") or data.get("token")
    if not token:
        raise RuntimeError(f"Resposta OAuth2 sem access_token: {data}")
    return str(token)


def _browser_like_headers(
    file_env: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    if getattr(args, "no_browser_ua", False):
        return {}
    ua = _cfg_str(
        getattr(args, "user_agent", None),
        file_env,
        "AGRO_API_USER_AGENT",
        _DEFAULT_BROWSER_UA,
    )
    if not ua:
        return {}
    return {
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }


def build_auth_headers(
    file_env: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    """
    Headers JSON: Bearer (OAuth) e/ou x-api-key — alinhado ao multilot / frontend.
    """
    api_key = _cfg_str(args.api_key, file_env, "API_KEY")
    base = {"Content-Type": "application/json", **_browser_like_headers(file_env, args)}

    cid = _cfg_str(args.oauth_client_id, file_env, "OAUTH2_FRONTEND_CLIENT_ID")
    csec = _cfg_str(args.oauth_client_secret, file_env, "OAUTH2_FRONTEND_CLIENT_SECRET")
    if cid and csec:
        token_url = _cfg_str(
            args.oauth_token_url,
            file_env,
            "OAUTH2_FRONTEND_TOKEN_URL",
            "https://api-auth-hml.agroamazonia.io/oauth2/token",
        )
        assert token_url
        scope = _cfg_str(
            args.oauth_scope,
            file_env,
            "OAUTH2_FRONTEND_SCOPE",
            "App_Fast/HML",
        )
        assert scope
        token = fetch_oauth2_token(token_url, cid, csec, scope)
        parts = [f"OAuth2 Bearer ({len(token)} chars)"]
        out = {**base, "Authorization": f"Bearer {token}"}
        if api_key:
            out["x-api-key"] = api_key
            parts.append("x-api-key")
        else:
            parts.append("(sem API_KEY — se 403, exporte API_KEY do config.js)")
        print(f"Autenticação: {' + '.join(parts)}")
        return out

    if api_key:
        print("Autenticação: x-api-key")
        return {**base, "x-api-key": api_key}

    raise RuntimeError(
        "Defina OAuth2 (OAUTH2_FRONTEND_CLIENT_ID + OAUTH2_FRONTEND_CLIENT_SECRET) "
        "ou API_KEY / --api-key. Mesmas variáveis do frontend (config.js / .env.homolog)."
    )


def _process_http_base(api_url: str, path_prefix: str) -> str:
    """Monta a base …/process. path_prefix vazio = {api_url}/process (HML/front com …/v1)."""
    api_url = api_url.rstrip("/")
    p = (path_prefix or "").strip().strip("/")
    if p:
        return f"{api_url}/{p}/process"
    return f"{api_url}/process"


def _normalize_cloudfront_api_url(url: str) -> str:
    """Front/CDK às vezes usam …/fast sem /v1; rotas reais são …/fast/v1/process/…"""
    u = url.rstrip("/")
    if "api-hml.agroamazonia.com" in u and u.endswith("/fast"):
        return f"{u}/v1"
    return url


def _looks_like_url(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    v = value.strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def _describe_auth(auth_headers: dict[str, str]) -> str:
    if "Authorization" in auth_headers and "x-api-key" in auth_headers:
        return "Bearer + x-api-key"
    if "Authorization" in auth_headers:
        return "Bearer"
    if "x-api-key" in auth_headers:
        return "x-api-key"
    return "custom"


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
    """Cria arquivo XML de teste (modelo SP / NF-e 4.00) com vDesc no item para validar parse e envio."""
    
    if os.path.exists(xml_path):
        print(f"ℹ️  Arquivo XML já existe: {xml_path} - será recriado")
    
    # Modelo alinhado à NF Boehringer / TOPLINE (exemplo real); <vDesc> no <prod> para teste valor_desconto → Protheus.
    xml_content = '''<?xml version="1.0" encoding="utf-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">
  <protNFe versao="4.00">
    <infProt Id="Id135261194585475">
      <tpAmb>1</tpAmb>
      <chNFe>35260357600249000155552010004485281516323523</chNFe>
      <dhRecbto>2026-03-30T08:48:51-03:00</dhRecbto>
      <nProt>135261194585475</nProt>
      <digVal>yl9d4vqkbv4ITOy5s6r8oA18sEo=</digVal>
      <cStat>100</cStat>
      <xMotivo>Autorizado o uso da NF-e</xMotivo>
    </infProt>
  </protNFe>
  <NFe xmlns="http://www.portalfiscal.inf.br/nfe">
    <infNFe Id="NFe35260357600249000155552010004485281516323523" versao="4.00">
      <ide>
        <cUF>35</cUF>
        <cNF>51632352</cNF>
        <natOp>VENDA PROD. ESTAB. Ñ DEVA POR ELE TRANSITAR</natOp>
        <mod>55</mod>
        <serie>201</serie>
        <nNF>448528</nNF>
        <dhEmi>2026-03-30T08:48:41-03:00</dhEmi>
        <tpNF>1</tpNF>
        <idDest>2</idDest>
        <cMunFG>3536505</cMunFG>
        <tpImp>1</tpImp>
        <tpEmis>1</tpEmis>
        <cDV>3</cDV>
        <tpAmb>1</tpAmb>
        <finNFe>1</finNFe>
        <indFinal>0</indFinal>
        <indPres>9</indPres>
        <indIntermed>0</indIntermed>
        <procEmi>0</procEmi>
        <verProc>SAP DRC</verProc>
      </ide>
      <emit>
        <CNPJ>57600249000155</CNPJ>
        <xNome>BOEHRINGER INGELHEIM ANIMAL HEALTH DO BRASIL LTDA</xNome>
        <xFant>BOEHRINGER INGELHEIM ANIMAL HE</xFant>
        <enderEmit>
          <xLgr>Avenida Doutor Roberto Moreira</xLgr>
          <nro>5005</nro>
          <xBairro>Recanto dos Pássaros</xBairro>
          <cMun>3536505</cMun>
          <xMun>Paulinia</xMun>
          <UF>SP</UF>
          <CEP>13148914</CEP>
          <cPais>1058</cPais>
          <xPais>Brasil</xPais>
          <fone>551935785000</fone>
        </enderEmit>
        <IE>513002758110</IE>
        <CRT>3</CRT>
      </emit>
      <dest>
        <CNPJ>13563680002813</CNPJ>
        <xNome>AGRO AMAZONIA PRODUTOS AGROPECUARIO S.A.</xNome>
        <enderDest>
          <xLgr>AV CAPITAO SILVIO</xLgr>
          <nro>1481</nro>
          <xBairro>APOIO RODOVIARIO SUL</xBairro>
          <cMun>1100189</cMun>
          <xMun>ARIQUEMES</xMun>
          <UF>RO</UF>
          <CEP>76876728</CEP>
          <cPais>1058</cPais>
          <xPais>Brasil</xPais>
          <fone>8837285942</fone>
        </enderDest>
        <indIEDest>1</indIEDest>
        <IE>00000004619439</IE>
      </dest>
      <det>
        <nItem>1</nItem>
        <prod>
          <cProd>000000000000181509</cProd>
          <cEAN>7898659784753</cEAN>
          <xProd>TOPLINE 5L X4 #BR - S/APLICADOR</xProd>
          <NCM>38089199</NCM>
          <CFOP>6105</CFOP>
          <uCom>PC</uCom>
          <qCom>23.0000</qCom>
          <vUnCom>2012.2200000000</vUnCom>
          <vProd>46281.06</vProd>
          <vDesc>4100.50</vDesc>
          <cEANTrib>7898659784753</cEANTrib>
          <uTrib>PC</uTrib>
          <qTrib>23.0000</qTrib>
          <vUnTrib>2012.2200000000</vUnTrib>
          <indTot>1</indTot>
          <xPed>Pedido Ariqueme</xPed>
          <nFCI>AE46CE13-B7A6-4551-B40C-E4E6FD871C30</nFCI>
          <rastro>
            <nLote>115/25</nLote>
            <qLote>23.000</qLote>
            <dFab>2025-10-07</dFab>
            <dVal>2028-10-07</dVal>
          </rastro>
        </prod>
        <imposto>
          <vTotTrib>1181.06</vTotTrib>
          <ICMS>
            <ICMS20>
              <orig>5</orig>
              <CST>20</CST>
              <modBC>3</modBC>
              <pRedBC>60.0000</pRedBC>
              <vBC>16872.22</vBC>
              <pICMS>7.0000</pICMS>
              <vICMS>1181.06</vICMS>
            </ICMS20>
          </ICMS>
          <IPI>
            <cEnq>999</cEnq>
            <IPINT>
              <CST>51</CST>
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
        <infAdProd>-LT: 11525 -Dt.Prod: 07102025 -Dt.Valid: 07102028</infAdProd>
        <vItem>42180.56</vItem>
      </det>
      <total>
        <ICMSTot>
          <vBC>16872.22</vBC>
          <vICMS>1181.06</vICMS>
          <vICMSDeson>0.00</vICMSDeson>
          <vFCP>0.00</vFCP>
          <vBCST>0.00</vBCST>
          <vST>0.00</vST>
          <vFCPST>0.00</vFCPST>
          <vFCPSTRet>0.00</vFCPSTRet>
          <vProd>46281.06</vProd>
          <vFrete>0.00</vFrete>
          <vSeg>0.00</vSeg>
          <vDesc>4100.50</vDesc>
          <vII>0.00</vII>
          <vIPI>0.00</vIPI>
          <vIPIDevol>0.00</vIPIDevol>
          <vPIS>0.00</vPIS>
          <vCOFINS>0.00</vCOFINS>
          <vOutro>0.00</vOutro>
          <vNF>42180.56</vNF>
          <vTotTrib>1181.06</vTotTrib>
        </ICMSTot>
        <vNFTot>42180.56</vNFTot>
      </total>
      <transp>
        <modFrete>0</modFrete>
        <transporta>
          <CNPJ>02905424000120</CNPJ>
          <xNome>AGV LOGISTICA S.A</xNome>
          <IE>714031345113</IE>
          <xEnder>R EDGAR MARCHIORI 255</xEnder>
          <xMun>VINHEDO</xMun>
          <UF>SP</UF>
        </transporta>
        <vol>
          <qVol>23</qVol>
          <pesoL>517.500</pesoL>
          <pesoB>519.800</pesoB>
        </vol>
      </transp>
      <cobr>
        <fat>
          <nFat>0000448528</nFat>
          <vOrig>46281.06</vOrig>
          <vDesc>4100.50</vDesc>
          <vLiq>42180.56</vLiq>
        </fat>
        <dup>
          <nDup>001</nDup>
          <dVenc>2026-07-28</dVenc>
          <vDup>42180.56</vDup>
        </dup>
      </cobr>
      <pag>
        <detPag>
          <tPag>15</tPag>
          <vPag>42180.56</vPag>
        </detPag>
      </pag>
      <infAdic>
        <infCpl>PED DO CLIENTE: Pedido Ariquemes — XML de teste create_process (vDesc no item).</infCpl>
      </infAdic>
      <compra>
        <xPed>Pedido Ariquemes</xPed>
      </compra>
    </infNFe>
  </NFe>
</nfeProc>
'''
    
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    
    print(f"✓ Arquivo XML criado: {xml_path}")


def get_metadata_json():
    """Metadados alinhados ao XML de teste (Boehringer / TOPLINE, destino Ariquemes)."""
    return {
        "header": {
            "tenantId": "00,010101"
        },
        "requestBody": {
            "cnpjEmitente": "57600249000155",
            "cnpjDestinatario": "13563680002813",
            "itens": [
                {
                    "codigoProduto": "000000000000181509",
                    "produto": "TOPLINE 5L X4 #BR - S/APLICADOR",
                    "quantidade": 23.0,
                    "valorUnitario": 2012.22,
                    "valorTotal": 46281.06,
                    "unidadeMedida": "PC",
                    "pedidoDeCompra": {
                        "pedidoErp": "1131195295",
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


def test_create_process(
    api_url: str,
    auth_headers: dict[str, str],
    xml_file: str | None = None,
    start_process: bool = False,
    path_prefix: str = "",
) -> str | None:
    """Cria um processo de teste com documentos - SEMPRE gera um novo processo único"""
    http_base = _process_http_base(api_url, path_prefix)

    print("="*80)
    print("TESTE DE CRIAÇÃO DE PROCESSO COM DOCUMENTOS")
    print("="*80)
    print(f"\nAPI URL: {api_url}")
    print(f"HTTP base: {http_base}")
    print(f"Auth: {_describe_auth(auth_headers)}")
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
            f"{http_base}/{process_id}",
            headers=auth_headers,
            timeout=60,
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
        f"{http_base}/presigned-url/xml",
        headers=auth_headers,
        json={
            'process_id': process_id,
            'file_name': xml_filename,
            'file_type': 'application/xml'
        },
        timeout=60,
    )
    
    if not xml_url_response.ok:
        print(f"❌ Erro ao obter URL para XML: {xml_url_response.status_code}")
        print(xml_url_response.text)
        if xml_url_response.status_code == 403:
            print(
                "Dica 403: exporte API_KEY (config.js / deploy) junto com OAuth; "
                "WAF pode bloquear sem User-Agent — não use --no-browser-ua.",
                file=sys.stderr,
            )
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
        f"{http_base}/metadados/pedido",
        headers=auth_headers,
        json={
            'process_id': process_id,
            'metadados': metadata
        },
        timeout=60,
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
            f"{http_base}/{process_id}",
            headers=auth_headers,
            timeout=60,
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
                f"{http_base}/start",
                headers=auth_headers,
                json={
                    'process_id': process_id
                },
                timeout=60,
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
    print(f"   - Ver processo: GET {http_base}/{process_id}")
    print(f"   - Iniciar processamento: POST {http_base}/start")
    print(f"     Body: {{\"process_id\": \"{process_id}\"}}")
    print(f"\n💡 Dica: Cada execução deste script cria um processo completamente novo!")
    
    return process_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria um processo de teste com documentos",
        epilog=(
            "Exemplo homolog (OAuth2 + vars em ../.env.homolog), igual ao multilot:\n"
            "  python3 test_create_process.py --env-file ../.env.homolog "
            f"--api-url '{HOMOLOG_API_URL}' --start\n\n"
            "Atalho --homolog (sobrepõe com backend/.env.homolog depois do --env-file):\n"
            "  python3 test_create_process.py --homolog --start\n\n"
            "Só x-api-key:\n"
            f"  python3 test_create_process.py --api-url '{HOMOLOG_API_URL}' "
            "--api-key 'agroamazonia_key_...' --start"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--api-url",
        help="Base URL da API (HML: …/fast/v1 como no frontend; dev execute-api: …/v1/api)",
    )
    parser.add_argument("--api-key", help="Header x-api-key (se não usar só OAuth2)")
    parser.add_argument(
        "--oauth-token-url",
        help="URL do token OAuth2 (default: OAUTH2_FRONTEND_TOKEN_URL ou auth hml)",
    )
    parser.add_argument("--oauth-client-id", help="OAUTH2_FRONTEND_CLIENT_ID")
    parser.add_argument("--oauth-client-secret", help="OAUTH2_FRONTEND_CLIENT_SECRET")
    parser.add_argument("--oauth-scope", help="OAUTH2_FRONTEND_SCOPE")
    parser.add_argument(
        "--user-agent",
        help="User-Agent nas requisições (default: navegador; env: AGRO_API_USER_AGENT)",
    )
    parser.add_argument(
        "--no-browser-ua",
        action="store_true",
        help="Não enviar User-Agent estilo navegador",
    )
    parser.add_argument("--xml-file", help="Caminho para arquivo XML (padrão: nome único test_nfe_*.xml)")
    parser.add_argument("--start", action="store_true", help="Iniciar processamento após criar")
    parser.add_argument(
        "--api-path-prefix",
        default=os.environ.get("PROCESS_API_PATH_PREFIX", ""),
        help=(
            'Segmento extra antes de /process (padrão vazio = …/v1/process, igual ao front). '
            'Use "api" para gateways no estilo …/v1/api/process (ex.: multilot opteraduo AWS).'
        ),
    )
    parser.add_argument(
        "--env-file",
        default=str(SCRIPT_DIR / ".env"),
        help="Arquivo .env; export no shell tem prioridade sobre o arquivo.",
    )
    parser.add_argument(
        "--homolog",
        action="store_true",
        help=(
            f"Depois do --env-file, carrega {BACKEND_ROOT / '.env.homolog'} "
            f"e usa API_URL padrão {HOMOLOG_API_URL} se API_URL não estiver definida"
        ),
    )

    args = parser.parse_args()

    # Execute-api costuma expor …/v1/api/process (prefixo api na URL base)
    default_api_url = "https://gx3eyeb4i1.execute-api.us-east-1.amazonaws.com/v1/api"
    default_api_key = "agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx"

    merged_env: dict[str, str] = {}
    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = Path.cwd() / env_path
    if env_path.is_file():
        print(f"Carregando {env_path}...")
        merged_env.update(_load_env_file(env_path))

    if args.homolog:
        hf = BACKEND_ROOT / ".env.homolog"
        if hf.is_file():
            print(f"Carregando {hf} (--homolog)...")
            merged_env.update(_load_env_file(hf))
        else:
            print(
                f"⚠️  {hf} não encontrado; use --env-file ../.env.homolog ou export das variáveis.",
                file=sys.stderr,
            )

    api_url = _cfg_str(args.api_url, merged_env, "API_URL")
    api_key_only = _cfg_str(args.api_key, merged_env, "API_KEY")

    if api_key_only and _looks_like_url(api_key_only):
        if not api_url:
            api_url = api_key_only.strip()
            args.api_key = None
            api_key_only = _cfg_str(args.api_key, merged_env, "API_KEY")
            print(
                "Aviso: URL em --api-key; usando como API_URL. "
                "Com OAuth2 use OAUTH2_* no .env; com API key use o valor x-api-key.",
                file=sys.stderr,
            )
        else:
            print(
                "ERRO: --api-key parece ser uma URL. Use --api-url para a base.",
                file=sys.stderr,
            )
            sys.exit(1)

    if not api_url and args.homolog:
        api_url = HOMOLOG_API_URL
        print(f"ℹ️  API URL homolog (default): {api_url}")

    if not api_url:
        api_url = default_api_url
        print(f"ℹ️  Usando API URL padrão (dev): {api_url}")

    api_url = _normalize_cloudfront_api_url(api_url)

    try:
        auth_headers = build_auth_headers(merged_env, args)
    except RuntimeError as e:
        if args.homolog:
            print(str(e), file=sys.stderr)
            sys.exit(1)
        auth_headers = {
            "Content-Type": "application/json",
            **_browser_like_headers(merged_env, args),
            "x-api-key": default_api_key,
        }
        print("Autenticação: x-api-key (fallback dev — sem OAuth/API_KEY no .env)")

    process_id = test_create_process(
        api_url=api_url,
        auth_headers=auth_headers,
        xml_file=args.xml_file,
        start_process=args.start,
        path_prefix=args.api_path_prefix,
    )

    sys.exit(0 if process_id else 1)


if __name__ == '__main__':
    main()

