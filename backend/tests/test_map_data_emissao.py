"""map_data_emissao (send_to_protheus)."""

import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_yyyymmdd():
    from send_to_protheus.handler import map_data_emissao

    assert map_data_emissao("20260414") == "2026-04-14"


def test_already_iso_date():
    from send_to_protheus.handler import map_data_emissao

    assert map_data_emissao("2026-04-16") == "2026-04-16"


def test_iso_datetime():
    from send_to_protheus.handler import map_data_emissao

    assert map_data_emissao("2026-04-16T15:00:00-03:00") == "2026-04-16"
