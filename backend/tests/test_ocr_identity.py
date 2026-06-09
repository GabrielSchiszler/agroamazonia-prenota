import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.ocr_identity import (  # noqa: E402
    find_ocr_identity_conflicts,
    pick_matching_ocr_per_document,
)


def test_pick_by_chave_xml():
    xml = {"chave_acesso": "35260357600249000155552010004485281516323523"}
    ocr = {
        "per_document": [
            {
                "file_name": "boleto.pdf",
                "protheus_hints": {
                    "chaveAcesso": "11111111111111111111111111111111111111111111",
                },
            },
            {
                "file_name": "nf.pdf",
                "protheus_hints": {
                    "chaveAcesso": "35260357600249000155552010004485281516323523",
                },
            },
        ]
    }
    picked = pick_matching_ocr_per_document(xml, ocr)
    assert picked["file_name"] == "nf.pdf"


def test_conflicts_detecta_anexo_divergente():
    xml = {
        "numero_nota": "448528",
        "chave_acesso": "35260357600249000155552010004485281516323523",
        "emitente": {"cnpj": "57600249000155"},
    }
    ocr = {
        "per_document": [
            {
                "file_name": "nf.xml.pdf",
                "protheus_hints": {
                    "parsed_xml_style": {
                        "numero_nota": "448528",
                        "chave_acesso": "35260357600249000155552010004485281516323523",
                        "emitente": {"cnpj": "57600249000155"},
                    }
                },
            },
            {
                "file_name": "outra_nf.pdf",
                "protheus_hints": {
                    "numeroNota": "999",
                    "chaveAcesso": "11111111111111111111111111111111111111111111",
                },
            },
        ]
    }
    conflicts = find_ocr_identity_conflicts(xml, ocr, primary_file_name="danfe.xml")
    assert len(conflicts) == 1
    assert conflicts[0]["file_name"] == "outra_nf.pdf"
