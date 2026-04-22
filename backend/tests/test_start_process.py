"""
Tests for ProcessService.start_process

Covers:
- Old flow: DANFE + pedido compra → process_type = AGROQUIMICOS
- Old flow: DANFE + pedido compra (isCommodities) → BARTER
- usoEConsumo (requestBody, header ou string 'true') → USOCONSUMO
- Multi-anexo: files without pedido compra → DOCUMENTO_ENTRADA
- Error: no files at all → ValueError
- Error: process not found → ValueError
"""

import json
import pytest
from unittest.mock import patch, MagicMock


def _build_service():
    """Build ProcessService with mocked AWS clients."""
    with patch("src.services.process_service.DynamoDBRepository") as MockRepo, \
         patch("src.services.process_service.boto3") as mock_boto:

        mock_sfn = MagicMock()
        mock_sfn.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:000:execution:test:exec-1"
        }
        mock_boto.client.side_effect = lambda svc, **kw: (
            mock_sfn if svc == "stepfunctions" else MagicMock()
        )

        from src.services.process_service import ProcessService
        service = ProcessService()
        service.sfn_client = mock_sfn

        return service, service.repository


def _metadata_item():
    return {"PK": "PROCESS#p1", "SK": "METADATA", "STATUS": "PENDING", "TIMESTAMP": 1700000000}


def _file_item(name, doc_type="DANFE"):
    return {
        "PK": "PROCESS#p1",
        "SK": f"FILE#{name}",
        "FILE_NAME": name,
        "FILE_KEY": f"processes/p1/danfe/{name}",
        "DOC_TYPE": doc_type,
    }


def _pedido_item(is_commodities=False, uso_e_consumo=False, header=None):
    doc = {
        "requestBody": {
            "isCommodities": is_commodities,
            "usoEConsumo": uso_e_consumo,
            "cnpjEmitente": "111",
        }
    }
    if header is not None:
        doc["header"] = header
    return {
        "PK": "PROCESS#p1",
        "SK": "PEDIDO_COMPRA_METADATA",
        "METADADOS": json.dumps(doc),
    }


def _pedido_item_raw(metadados: dict):
    return {
        "PK": "PROCESS#p1",
        "SK": "PEDIDO_COMPRA_METADATA",
        "METADADOS": json.dumps(metadados),
    }


class TestStartProcess:

    def test_old_flow_agroquimicos(self):
        """DANFE + pedido compra (not commodities) → AGROQUIMICOS."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item(is_commodities=False),
        ]

        result = service.start_process("p1")
        assert result["process_type"] == "AGROQUIMICOS"
        assert result["status"] == "PROCESSING"
        service.sfn_client.start_execution.assert_called_once()

    def test_old_flow_barter(self):
        """DANFE + pedido compra (isCommodities=true) → BARTER."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item(is_commodities=True),
        ]

        result = service.start_process("p1")
        assert result["process_type"] == "BARTER"

    def test_pedido_uso_e_consumo(self):
        """requestBody.usoEConsumo=true → USOCONSUMO (prioridade sobre isCommodities)."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item(is_commodities=True, uso_e_consumo=True),
        ]

        result = service.start_process("p1")
        assert result["process_type"] == "USOCONSUMO"

    def test_uso_e_consumo_string_true(self):
        """requestBody.usoEConsumo 'true' (string) → USOCONSUMO, mesma regra que isCommodities."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item_raw({
                "requestBody": {
                    "usoEConsumo": "true",
                    "isCommodities": False,
                    "cnpjEmitente": "111",
                }
            }),
        ]
        assert service.start_process("p1")["process_type"] == "USOCONSUMO"

    def test_uso_e_consumo_apenas_no_header(self):
        """usoEConsumo só no header (sem no requestBody) → USOCONSUMO."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item_raw({
                "header": {"tenantId": "00,010101", "usoEConsumo": True},
                "requestBody": {"isCommodities": False, "cnpjEmitente": "111"},
            }),
        ]
        assert service.start_process("p1")["process_type"] == "USOCONSUMO"

    def test_is_commodities_no_header(self):
        """isCommodities só no header → BARTER (mesma ordem de leitura que usoEConsumo)."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("nota.xml"),
            _pedido_item_raw({
                "header": {"tenantId": "x", "isCommodities": True},
                "requestBody": {"cnpjEmitente": "111"},
            }),
        ]
        assert service.start_process("p1")["process_type"] == "BARTER"

    def test_multi_anexo_without_pedido(self):
        """Files without pedido compra → DOCUMENTO_ENTRADA."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("fatura.pdf", doc_type="ADDITIONAL"),
            _file_item("boleto.pdf", doc_type="ADDITIONAL"),
        ]

        result = service.start_process("p1")
        assert result["process_type"] == "DOCUMENTO_ENTRADA"

    def test_single_pdf_no_xml_no_pedido(self):
        """Only 1 PDF, no XML, no pedido → should work (DOCUMENTO_ENTRADA)."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
            _file_item("contrato.pdf", doc_type="ADDITIONAL"),
        ]

        result = service.start_process("p1")
        assert result["process_type"] == "DOCUMENTO_ENTRADA"
        assert result["status"] == "PROCESSING"

    def test_error_no_files(self):
        """No files at all → ValueError."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata_item(),
        ]

        with pytest.raises(ValueError, match="Nenhum arquivo anexado"):
            service.start_process("p1")

    def test_error_process_not_found(self):
        """Empty DynamoDB → ValueError."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = []

        with pytest.raises(ValueError, match="não encontrado"):
            service.start_process("p1")

    def test_error_no_metadata(self):
        """Items exist but no METADATA → ValueError."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _file_item("nota.xml"),
        ]

        with pytest.raises(ValueError, match="Metadados"):
            service.start_process("p1")
