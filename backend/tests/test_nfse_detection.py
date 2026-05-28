"""Tests for NFS-e detection (utils.nfse_detection)."""

from utils.nfse_detection import (
    NFSE_SERIE_PROTHEUS,
    detect_nfse_from_sources,
    detect_nfse_from_text,
    extract_numero_nfse,
    is_nfse_document_text,
)


NFSE_SNIPPET = """
NFS-e
Nota Fiscal de Serviço Eletrônica
Número: 27
Prestador do Serviço
19.378.380/0001-20
Tomador do Serviço
13.563.680/0033-80
ISSQN
Código de Serviço
Discriminação dos Serviços
Valor Líquido da NFS-e
R$ 8.170,58
"""

DANFE_SNIPPET = """
DANFE
Documento Auxiliar da Nota Fiscal Eletrônica
CFOP 5102
ICMS
Modalidade do frete
"""


def test_is_nfse_true_on_classic_layout():
    assert is_nfse_document_text(NFSE_SNIPPET) is True


def test_is_nfse_false_on_danfe():
    assert is_nfse_document_text(DANFE_SNIPPET) is False


def test_detect_nfse_sets_serie_nfs():
    d = detect_nfse_from_text(NFSE_SNIPPET)
    assert d["is_nfse"] is True
    assert d["serie"] == NFSE_SERIE_PROTHEUS
    assert d["numero_nota"] == "27"


def test_extract_numero_nfse():
    assert extract_numero_nfse("Número da NFS-e: 27") == "27"


def test_detect_nfse_from_sources_ignores_modelo_55():
    d = detect_nfse_from_sources(raw_texts=[NFSE_SNIPPET], xml_modelo="55")
    assert d["is_nfse"] is False


def test_detect_nfse_from_bedrock_serie():
    d = detect_nfse_from_sources(bedrock_fields={"serie": "NFS", "documento": "27"})
    assert d["is_nfse"] is True
    assert d["serie"] == "NFS"


def test_detect_nfse_relaxed_uso_consumo_prestador_issqn():
    text = "Prestador do Serviço\nISSQN\nTomador do Serviço\nNúmero: 27"
    d = detect_nfse_from_sources(raw_texts=[text], uso_e_consumo=True)
    assert d.get("is_nfse") is True
    assert d.get("serie") == "NFS"
