"""
Shared fixtures for all backend tests.

Sets up environment variables and sys.path so that both Lambda handlers
and FastAPI service code can be imported without hitting real AWS.
"""

import os
import sys
import json
import pytest

# ---------------------------------------------------------------------------
# Environment variables expected by Lambdas / services
# ---------------------------------------------------------------------------
os.environ.setdefault("TABLE_NAME", "test-table")
os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:000000000000:stateMachine:test")
os.environ.setdefault("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# sys.path adjustments so imports resolve the same way they do in Lambda
# ---------------------------------------------------------------------------
_backend_root = os.path.join(os.path.dirname(__file__), "..")
_lambdas_root = os.path.join(_backend_root, "lambdas")

for p in [_backend_root, _lambdas_root]:
    abs_p = os.path.abspath(p)
    if abs_p not in sys.path:
        sys.path.insert(0, abs_p)


# ---------------------------------------------------------------------------
# Sample NF-e XML (minimal but valid structure)
# ---------------------------------------------------------------------------
SAMPLE_NFE_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">
  <NFe>
    <infNFe Id="NFe35240112345678000195550010000000011234567890" versao="4.00">
      <ide>
        <nNF>1</nNF>
        <serie>1</serie>
        <mod>55</mod>
        <dhEmi>2024-01-15T10:00:00-03:00</dhEmi>
        <tpNF>1</tpNF>
        <natOp>VENDA</natOp>
        <finNFe>1</finNFe>
        <cNF>12345678</cNF>
        <cDV>0</cDV>
        <tpAmb>1</tpAmb>
        <tpEmis>1</tpEmis>
        <tpImp>1</tpImp>
        <idDest>1</idDest>
        <indFinal>0</indFinal>
        <indPres>0</indPres>
        <verProc>1.0</verProc>
      </ide>
      <emit>
        <CNPJ>12345678000195</CNPJ>
        <xNome>Empresa Teste Ltda</xNome>
        <IE>123456789</IE>
        <CRT>3</CRT>
        <enderEmit>
          <xLgr>Rua Teste</xLgr>
          <nro>100</nro>
          <xBairro>Centro</xBairro>
          <xMun>Sao Paulo</xMun>
          <cMun>3550308</cMun>
          <UF>SP</UF>
          <CEP>01000000</CEP>
          <cPais>1058</cPais>
          <xPais>Brasil</xPais>
        </enderEmit>
      </emit>
      <dest>
        <CNPJ>98765432000199</CNPJ>
        <xNome>Destinatario Teste</xNome>
        <IE>987654321</IE>
        <indIEDest>1</indIEDest>
        <enderDest>
          <xLgr>Av Destino</xLgr>
          <nro>200</nro>
          <xBairro>Industrial</xBairro>
          <xMun>Manaus</xMun>
          <cMun>1302603</cMun>
          <UF>AM</UF>
          <CEP>69000000</CEP>
          <cPais>1058</cPais>
          <xPais>Brasil</xPais>
        </enderDest>
      </dest>
      <det nItem="1">
        <prod>
          <cProd>PROD001</cProd>
          <xProd>Produto Teste</xProd>
          <NCM>12345678</NCM>
          <CFOP>5102</CFOP>
          <uCom>UN</uCom>
          <qCom>10</qCom>
          <vUnCom>100.00</vUnCom>
          <vProd>1000.00</vProd>
          <cEAN>SEM GTIN</cEAN>
          <cEANTrib>SEM GTIN</cEANTrib>
          <uTrib>UN</uTrib>
          <qTrib>10</qTrib>
          <vUnTrib>100.00</vUnTrib>
          <indTot>1</indTot>
        </prod>
        <imposto>
          <ICMS>
            <ICMS00>
              <orig>0</orig>
              <CST>00</CST>
              <vBC>1000.00</vBC>
              <pICMS>18.00</pICMS>
              <vICMS>180.00</vICMS>
            </ICMS00>
          </ICMS>
        </imposto>
      </det>
      <total>
        <ICMSTot>
          <vBC>1000.00</vBC>
          <vICMS>180.00</vICMS>
          <vICMSDeson>0.00</vICMSDeson>
          <vBCST>0.00</vBCST>
          <vST>0.00</vST>
          <vProd>1000.00</vProd>
          <vFrete>0.00</vFrete>
          <vSeg>0.00</vSeg>
          <vDesc>0.00</vDesc>
          <vII>0.00</vII>
          <vIPI>0.00</vIPI>
          <vPIS>0.00</vPIS>
          <vCOFINS>0.00</vCOFINS>
          <vOutro>0.00</vOutro>
          <vNF>1000.00</vNF>
        </ICMSTot>
      </total>
      <transp>
        <modFrete>0</modFrete>
      </transp>
    </infNFe>
  </NFe>
  <protNFe>
    <infProt>
      <nProt>135240000000001</nProt>
      <dhRecbto>2024-01-15T10:05:00-03:00</dhRecbto>
      <cStat>100</cStat>
      <xMotivo>Autorizado o uso da NF-e</xMotivo>
    </infProt>
  </protNFe>
</nfeProc>
"""

SAMPLE_GENERIC_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>Generic XML content</item>
</root>
"""


@pytest.fixture
def nfe_xml_bytes():
    return SAMPLE_NFE_XML


@pytest.fixture
def generic_xml_bytes():
    return SAMPLE_GENERIC_XML
