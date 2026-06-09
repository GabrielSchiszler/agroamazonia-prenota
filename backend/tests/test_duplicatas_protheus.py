import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))

from utils.duplicatas_protheus import build_duplicatas_protheus_payload  # noqa: E402


def test_uc_usa_valor_vencimento():
    raw = [{"vencimento": "2026-04-15", "valorVencimento": "5746.2"}]
    out = build_duplicatas_protheus_payload(raw, uso_consumo=True, valor_total_doc=5746.2)
    assert out == [{"vencimento": "2026-04-15", "valor": 5746.2}]


def test_uc_sem_valor_usa_total_nota_um_vencimento():
    raw = [{"vencimento": "2026-04-15"}]
    out = build_duplicatas_protheus_payload(raw, uso_consumo=True, valor_total_doc=1000.0)
    assert out == [{"vencimento": "2026-04-15", "valor": 1000.0}]


def test_uc_split_duas_datas_sem_valor():
    raw = [
        {"vencimento": "2026-04-15"},
        {"vencimento": "2026-05-15"},
    ]
    out = build_duplicatas_protheus_payload(raw, uso_consumo=True, valor_total_doc=1000.0)
    assert len(out) == 2
    assert sum(d["valor"] for d in out) == 1000.0
    assert {d["vencimento"] for d in out} == {"2026-04-15", "2026-05-15"}


def test_nao_uc_exige_valor_ou_valor_vencimento():
    raw = [{"vencimento": "2026-04-15", "valor": "100"}]
    out = build_duplicatas_protheus_payload(raw, uso_consumo=False, valor_total_doc=0)
    assert out == [{"vencimento": "2026-04-15", "valor": 100.0}]
