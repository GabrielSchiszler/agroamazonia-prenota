import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.boleto_duplicatas import (  # noqa: E402
    document_source_kind,
    extract_duplicatas_from_document_text,
    extract_duplicatas_from_ocr,
    extract_duplicatas_from_sources,
    resolve_duplicatas_uc,
)
from utils.duplicatas_protheus import build_duplicatas_protheus_payload  # noqa: E402

COBRANCA_SNIPPET = """
SICOOB
Beneficiário SIMPLE AGRO SISTEMAS LTDA 30.190.475/0001-59
Vencimento 25/06/2026
Valor do Documento 5.392,81
PAGAVEL PREFERENCIALMENTE NO SICOOB
Nosso Número 1-5
Ficha de compensação
"""


def test_extract_vencimento_sem_depender_do_nome_arquivo():
    raw = extract_duplicatas_from_document_text(COBRANCA_SNIPPET)
    assert len(raw) == 1
    assert raw[0]["vencimento"] == "2026-06-25"
    assert raw[0]["valorVencimento"] == 5392.81


def test_ocr_varre_todos_anexos_nome_generico():
    ocr = {
        "source_files": ["anexo_cobranca_1287.pdf", "nota_servico.pdf"],
        "raw_text": COBRANCA_SNIPPET + "\n---\nNFS-e 1287",
        "per_document": [
            {
                "file_name": "anexo_cobranca_1287.pdf",
                "raw_text": COBRANCA_SNIPPET,
                "protheus_hints": {"valorDocumento": "5.392,81"},
            },
            {"file_name": "nota_servico.pdf", "raw_text": "NFS-e número 1287 sem vencimento"},
        ],
    }
    dups = extract_duplicatas_from_ocr(ocr)
    assert len(dups) == 1
    assert dups[0]["vencimento"] == "2026-06-25"
    assert dups[0]["valorVencimento"] == 5392.81


def test_ocr_ignora_anexo_sem_vencimento():
    ocr = {
        "per_document": [
            {"file_name": "x.pdf", "raw_text": "NFS-e 1287 valor líquido R$ 100,00"},
        ],
    }
    assert extract_duplicatas_from_ocr(ocr) == []


def test_uc_pipeline_sem_valor_usa_total_nota():
    raw = [{"vencimento": "2026-06-25"}]
    out = build_duplicatas_protheus_payload(raw, uso_consumo=True, valor_total_doc=5746.2)
    assert out == [{"vencimento": "2026-06-25", "valor": 5746.2}]


def test_sources_prefer_bedrock_duplicatas():
    ocr = {
        "per_document": [
            {"file_name": "scan_01.pdf", "raw_text": COBRANCA_SNIPPET},
        ],
    }
    bedrock = {
        "duplicatas": [{"vencimento": "2026-06-25", "valorVencimento": 5392.81}],
    }
    out = extract_duplicatas_from_sources(ocr, bedrock, bedrock_first=True)
    assert len(out) == 1
    assert out[0]["valorVencimento"] == 5392.81


def test_dedupe_repeated_vencimento():
    text = COBRANCA_SNIPPET + "\nVencimento\n25/06/2026\n"
    raw = extract_duplicatas_from_document_text(text)
    assert len(raw) == 1


def test_document_source_kind():
    assert document_source_kind("BOLETO_1287.pdf") == "boleto"
    assert document_source_kind("NF_-_1287.pdf") == "nf_nfs"
    assert document_source_kind("anexo.pdf") == "unknown"


def test_resolve_nf_e_boleto_mesmo_valor_prefere_boleto():
    """NF domingo 28/06 + boleto dia útil 29/06, mesmo valor = total → 1 duplicata."""
    raw = [
        {
            "vencimento": "2026-06-28",
            "valorVencimento": 5392.81,
            "source": "nf_nfs",
        },
        {
            "vencimento": "2026-06-29",
            "valorVencimento": 5392.81,
            "source": "boleto",
        },
    ]
    out = resolve_duplicatas_uc(raw, valor_total_doc=5392.81)
    assert len(out) == 1
    assert out[0]["vencimento"] == "2026-06-29"
    assert out[0]["valorVencimento"] == 5392.81
    assert "source" not in out[0]


def test_resolve_duas_parcelas_reais_mantem_ambas():
    raw = [
        {"vencimento": "2026-04-15", "valorVencimento": 500.0, "source": "boleto"},
        {"vencimento": "2026-05-15", "valorVencimento": 500.0, "source": "boleto"},
    ]
    out = resolve_duplicatas_uc(raw, valor_total_doc=1000.0)
    assert len(out) == 2


def test_resolve_duas_datas_sem_valor_mantem_para_split():
    raw = [
        {"vencimento": "2026-04-15", "source": "nf_nfs"},
        {"vencimento": "2026-05-15", "source": "boleto"},
    ]
    out = resolve_duplicatas_uc(raw, valor_total_doc=1000.0)
    assert len(out) == 2
    payload = build_duplicatas_protheus_payload(out, uso_consumo=True, valor_total_doc=1000.0)
    assert sum(d["valor"] for d in payload) == 1000.0


def test_uc_pipeline_nf_boleto_end_to_end():
    bedrock = {
        "duplicatas": [
            {"vencimento": "2026-06-28", "valorVencimento": 5392.81, "source": "nf_nfs"},
            {"vencimento": "2026-06-29", "valorVencimento": 5392.81, "source": "boleto"},
        ],
    }
    resolved = resolve_duplicatas_uc(
        extract_duplicatas_from_sources(None, bedrock, bedrock_first=True),
        valor_total_doc=5392.81,
    )
    payload = build_duplicatas_protheus_payload(
        resolved, uso_consumo=True, valor_total_doc=5392.81
    )
    assert payload == [{"vencimento": "2026-06-29", "valor": 5392.81}]


def test_merge_vencimentos_de_multiplos_anexos():
    ocr = {
        "per_document": [
            {"file_name": "a.pdf", "raw_text": "Vencimento 15/04/2026"},
            {"file_name": "b.pdf", "raw_text": "Vencimento 15/05/2026 Valor do Documento 500,00"},
        ],
    }
    dups = extract_duplicatas_from_ocr(ocr)
    assert len(dups) == 2
    assert {d["vencimento"] for d in dups} == {"2026-04-15", "2026-05-15"}
