"""
Tests for ProcessService.get_process

Covers:
- Old flow: returns XML parsing_results
- Multi-anexo: returns TEXTRACT, MERGED, BEDROCK_AI sources
- Mixed: all sources present together
- Empty parsing: no extraction data at all
"""

import json
import pytest
from unittest.mock import patch, MagicMock


def _build_service():
    with patch("src.services.process_service.DynamoDBRepository") as MockRepo, \
         patch("src.services.process_service.boto3") as mock_boto:
        from src.services.process_service import ProcessService
        service = ProcessService()
        return service, service.repository


def _metadata():
    return {
        "PK": "PROCESS#p1", "SK": "METADATA",
        "STATUS": "COMPLETED", "PROCESS_TYPE": "AGROQUIMICOS",
        "TIMESTAMP": 1700000000,
    }


def _danfe_file():
    return {
        "PK": "PROCESS#p1", "SK": "FILE#nota.xml",
        "FILE_NAME": "nota.xml", "FILE_KEY": "processes/p1/danfe/nota.xml",
        "DOC_TYPE": "DANFE", "STATUS": "UPLOADED",
    }


def _additional_file(name="fatura.pdf"):
    return {
        "PK": "PROCESS#p1", "SK": f"FILE#{name}",
        "FILE_NAME": name, "FILE_KEY": f"processes/p1/docs/{name}",
        "DOC_TYPE": "ADDITIONAL", "STATUS": "EXTRACTED",
    }


def _parsed_xml():
    return {
        "PK": "PROCESS#p1", "SK": "PARSED_XML=nota.xml",
        "FILE_NAME": "nota.xml",
        "PARSED_DATA": json.dumps({"numero_nota": "1", "emitente": {"cnpj": "111"}}),
    }


def _textract_item(name="fatura.pdf"):
    return {
        "PK": "PROCESS#p1", "SK": f"TEXTRACT#{name}",
        "FILE_NAME": name,
        "RAW_TEXT": "Fatura #12345 Total R$1000",
        "TABLES_DATA": json.dumps([{"rows": [["A", "B"]]}]),
        "JOB_ID": "sync",
    }


def _merged_item():
    return {
        "PK": "PROCESS#p1", "SK": "MERGED_EXTRACTION",
        "MERGED_DATA": json.dumps({
            "schema_version": 2, "nfe_xml": {"numero_nota": "1"},
            "nfe_file": "nota.xml", "xml_documents": [], "textract_documents": [],
        }),
    }


def _bedrock_item():
    return {
        "PK": "PROCESS#p1", "SK": "BEDROCK_EXTRACTION",
        "EXTRACTED_FIELDS": json.dumps({
            "tipoDeDocumento": "NF", "documento": "000001",
        }),
    }


class TestGetProcess:

    def test_old_flow_xml_only(self):
        """Old flow: only XML parsing result returned."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _danfe_file(), _parsed_xml(),
        ]

        result = service.get_process("p1")
        sources = [r["source"] for r in result["parsing_results"]]
        assert "XML" in sources
        assert "TEXTRACT" not in sources
        assert "MERGED" not in sources
        assert "BEDROCK_AI" not in sources

    def test_multi_anexo_all_sources(self):
        """Multi-anexo: all extraction sources present."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _danfe_file(), _additional_file(),
            _parsed_xml(), _textract_item(), _merged_item(), _bedrock_item(),
        ]

        result = service.get_process("p1")
        sources = [r["source"] for r in result["parsing_results"]]
        assert "XML" in sources
        assert "TEXTRACT" in sources
        assert "MERGED" in sources
        assert "BEDROCK_AI" in sources

    def test_textract_result_structure(self):
        """TEXTRACT result has raw_text, tables, job_id."""
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _additional_file(), _textract_item(),
        ]

        result = service.get_process("p1")
        textract = next(r for r in result["parsing_results"] if r["source"] == "TEXTRACT")
        assert "raw_text" in textract["parsed_data"]
        assert "tables" in textract["parsed_data"]
        assert textract["parsed_data"]["job_id"] == "sync"

    def test_merged_result_has_schema_version(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _danfe_file(), _merged_item(),
        ]

        result = service.get_process("p1")
        merged = next(r for r in result["parsing_results"] if r["source"] == "MERGED")
        assert merged["parsed_data"]["schema_version"] == 2

    def test_bedrock_result_has_extracted_fields(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _danfe_file(), _bedrock_item(),
        ]

        result = service.get_process("p1")
        bedrock = next(r for r in result["parsing_results"] if r["source"] == "BEDROCK_AI")
        assert bedrock["parsed_data"]["tipoDeDocumento"] == "NF"

    def test_no_parsing_results_when_nothing_extracted(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = [_metadata(), _danfe_file()]

        result = service.get_process("p1")
        assert result["parsing_results"] == []
        assert result.get("bedrock_by_file") == []

    def test_bedrock_by_file_from_prefixed_sk(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(),
            _danfe_file(),
            {
                "PK": "PROCESS#p1",
                "SK": "BEDROCK_EXTRACTION#boleto.pdf",
                "FILE_NAME": "boleto.pdf",
                "EXTRACTED_FIELDS": json.dumps({"documento": "55", "tipoDeDocumento": "BOL"}),
            },
        ]

        result = service.get_process("p1")
        assert len(result["bedrock_by_file"]) == 1
        assert result["bedrock_by_file"][0]["file_name"] == "boleto.pdf"
        assert result["bedrock_by_file"][0]["parsed_data"]["documento"] == "55"

    def test_protheus_request_payload_from_metadata(self):
        service, repo = _build_service()
        meta = dict(_metadata())
        meta["protheus_request_payload"] = json.dumps({
            "documento": "999",
            "itens": [{"codigoProduto": "1"}],
        })
        repo.query_by_pk.return_value = [meta, _danfe_file()]

        result = service.get_process("p1")
        assert result["protheus_request_payload"] is not None
        assert result["protheus_request_payload"]["documento"] == "999"
        assert len(result["protheus_request_payload"]["itens"]) == 1

    def test_protheus_request_payload_fallback_from_request_info(self):
        service, repo = _build_service()
        meta = dict(_metadata())
        meta["protheus_request_info"] = json.dumps({
            "request_payload": {"documento": "777"},
            "response_status_code": 400,
        })
        repo.query_by_pk.return_value = [meta, _danfe_file()]

        result = service.get_process("p1")
        assert result["protheus_request_payload"]["documento"] == "777"

    def test_process_not_found_raises(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = []

        with pytest.raises(ValueError, match="não encontrado"):
            service.get_process("p1")

    def test_files_section_separates_danfe_and_additional(self):
        service, repo = _build_service()
        repo.query_by_pk.return_value = [
            _metadata(), _danfe_file(), _additional_file("comprovante.pdf"),
        ]

        result = service.get_process("p1")
        assert len(result["files"]["danfe"]) == 1
        assert result["files"]["danfe"][0]["file_name"] == "nota.xml"
        assert len(result["files"]["additional"]) == 1
        assert result["files"]["additional"][0]["file_name"] == "comprovante.pdf"
