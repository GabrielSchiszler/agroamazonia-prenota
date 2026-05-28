"""Tests for utils.protheus_hints (Textract → campos únicos estilo Protheus)."""

from utils.protheus_hints import hints_from_textract_text


def test_hints_nfse_after_boleto_prefers_label_prefix_chave():
    snippet = """07790.00116 12068.967392 06776.263383 7 14320000817058
Chave de Acesso da NFS-e
35041072219378380000120000000000005326032643827496
Prestador do Serviço
19.378.380/0001-20
TOMADOR DO SERVIÇO
13.563.680/0033-80
Valor Líquido da NFS-e
R$ 8.170,58
"""
    h = hints_from_textract_text(snippet)
    assert h["chaveAcesso"] == "35041072219378380000120000000000005326032643"
    assert h["cnpjEmitente"] == "19378380000120"
    assert h["cnpjTomador"] == "13563680003380"
    assert h["valorDocumento"] == "8.170,58"
    px = h.get("parsed_xml_style")
    assert isinstance(px, dict)
    assert px["chave_acesso"] == "35041072219378380000120000000000005326032643"
    assert px["emitente"]["cnpj"] == "19378380000120"
    assert px["destinatario"]["cnpj"] == "13563680003380"
    assert px["totais"]["valor_nota"] == "8.170,58"
    assert h.get("serie") == "NFS"
    assert h.get("tipoDocumentoFiscal") == "NFSE"
    assert px.get("serie") == "NFS"
    assert "cnpjs_encontrados" not in h
    assert "valores_rs_sample" not in h


def test_hints_empty():
    assert hints_from_textract_text("") == {}
    assert hints_from_textract_text("   ") == {}
