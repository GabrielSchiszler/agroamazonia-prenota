"""Detecção de pré-nota Protheus para métricas de dashboard."""

import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_prenota_detectada_pela_mensagem_top_level():
    from update_metrics.handler import protheus_response_indicates_prenota

    ev = {
        "protheus_response": {
            "message": (
                "Documento de entrada criado como pré-nota devido a erros na classificação. "
                "Verifique o log para mais detalhes."
            )
        }
    }
    assert protheus_response_indicates_prenota(ev, None) is True


def test_prenota_detectada_em_body_aninhado():
    from update_metrics.handler import protheus_response_indicates_prenota

    ev = {
        "protheus_response": {
            "body": {
                "message": "Documento de entrada criado como pré-nota devido a erros na classificação."
            }
        }
    }
    assert protheus_response_indicates_prenota(ev, None) is True


def test_nao_prenota_mensagem_sucesso():
    from update_metrics.handler import protheus_response_indicates_prenota

    ev = {"protheus_response": {"message": "Documento de entrada criado com sucesso."}}
    assert protheus_response_indicates_prenota(ev, None) is False


def test_prenota_fallback_metadata_json_string():
    from update_metrics.handler import protheus_response_indicates_prenota

    meta = {
        "protheus_response": '{"message": "Documento de entrada criado como pré-nota devido a X."}'
    }
    assert protheus_response_indicates_prenota({}, meta) is True
