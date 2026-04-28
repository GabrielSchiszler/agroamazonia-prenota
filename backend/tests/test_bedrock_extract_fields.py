"""
Tests for backend/lambdas/bedrock_extract_fields/handler.py

Covers:
- Happy path: MERGED_EXTRACTION exists, Bedrock returns valid JSON
- Missing MERGED_EXTRACTION → fields_extracted = False
- Bedrock returns empty → fields_extracted = False
- Bedrock returns invalid JSON → saves RAW_RESPONSE + PARSE_ERROR
- _build_prompt: includes NF-e data, Textract data, pedido metadata
"""

import io
import json
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")


def _ocr_merged_backfill_item():
    pd = json.dumps({"raw_text": "ocr", "source_files": ["a.pdf"]})
    return {
        "PK": "PROCESS#p1",
        "SK": "PARSED_OCR=textract_merged",
        "FILE_NAME": "textract_merged",
        "PARSED_DATA": pd,
        "SOURCE": "TEXTRACT",
    }


def _merged_item(nfe_xml=None, textract_docs=None):
    data = {
        "schema_version": 2,
        "nfe_xml": nfe_xml,
        "nfe_file": "nota.xml" if nfe_xml else None,
        "xml_documents": [],
        "textract_documents": textract_docs or [],
    }
    return {
        "PK": "PROCESS#p1",
        "SK": "MERGED_EXTRACTION",
        "MERGED_DATA": json.dumps(data),
    }


def _bedrock_response(text: str):
    body = json.dumps({
        "output": {"message": {"content": [{"text": text}]}}
    }).encode()
    return {"body": io.BytesIO(body)}


VALID_EXTRACTION = json.dumps({
    "tipoDeDocumento": "NF",
    "documento": "000001",
    "serie": "001",
    "dataEmissao": "20240115",
    "chaveAcesso": "35240112345678000195550010000000011234567890",
    "cnpjEmitente": "12345678000195",
    "itens": [{"codigoProduto": "PROD001", "quantidade": 10}],
})


class TestBedrockExtractFieldsHandler:

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_happy_path(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        mock_table.query.return_value = {
            "Items": [_merged_item(nfe_xml={"numero_nota": "1"}), _ocr_merged_backfill_item()]
        }
        mock_invoke.return_value = VALID_EXTRACTION

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is True

        puts = mock_table.put_item.call_args_list
        assert len(puts) == 2
        saved = puts[0][1]["Item"]
        assert saved["SK"] == "BEDROCK_EXTRACTION"
        fields = json.loads(saved["EXTRACTED_FIELDS"])
        assert fields["tipoDeDocumento"] == "NF"
        ocr_saved = puts[1][1]["Item"]
        assert ocr_saved["SK"] == "PARSED_OCR=textract_merged"
        ocr_pd = json.loads(ocr_saved["PARSED_DATA"])
        assert ocr_pd["documento_entrada_protheus"]["tipoDeDocumento"] == "NF"
        assert ocr_pd["_campos_estruturados_fonte"] == "bedrock"

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_missing_merged_extraction(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        mock_table.query.return_value = {
            "Items": [{"PK": "PROCESS#p1", "SK": "METADATA"}]
        }

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is False
        mock_invoke.assert_not_called()

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_bedrock_returns_empty(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        mock_table.query.return_value = {"Items": [_merged_item()]}
        mock_invoke.return_value = None

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is False

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_bedrock_returns_invalid_json(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        mock_table.query.return_value = {"Items": [_merged_item()]}
        mock_invoke.return_value = "This is not JSON at all {{"

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is False

        saved = mock_table.put_item.call_args[1]["Item"]
        assert saved["SK"] == "BEDROCK_EXTRACTION"
        assert "PARSE_ERROR" in saved
        assert saved["RAW_RESPONSE"].startswith("This is not JSON")

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_bedrock_strips_markdown_fences(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        mock_table.query.return_value = {"Items": [_merged_item(), _ocr_merged_backfill_item()]}
        mock_invoke.return_value = f"```json\n{VALID_EXTRACTION}\n```"

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is True
        assert len(mock_table.put_item.call_args_list) == 2

    @patch("bedrock_extract_fields.handler._invoke_bedrock")
    @patch("bedrock_extract_fields.handler.table")
    def test_two_textract_files_saves_per_file_sk_and_merged(self, mock_table, mock_invoke):
        from bedrock_extract_fields.handler import handler

        tex = [
            {"file_name": "a.pdf", "raw_text": "NOTA A", "tables": []},
            {"file_name": "b.pdf", "raw_text": "NOTA B", "tables": []},
        ]
        ocr_pd = json.dumps({
            "raw_text": "x",
            "per_document": [
                {"file_name": "a.pdf", "protheus_hints": {}},
                {"file_name": "b.pdf", "protheus_hints": {}},
            ],
        })
        ocr_item = {
            "PK": "PROCESS#p1",
            "SK": "PARSED_OCR=textract_merged",
            "FILE_NAME": "textract_merged",
            "PARSED_DATA": ocr_pd,
            "SOURCE": "TEXTRACT",
        }
        mock_table.query.return_value = {
            "Items": [_merged_item(nfe_xml={"numero_nota": "9"}, textract_docs=tex), ocr_item],
        }
        ex_a = json.dumps({"tipoDeDocumento": "NF", "documento": "111", "serie": "1"})
        ex_b = json.dumps({"tipoDeDocumento": "NF", "documento": "222", "serie": "2"})
        mock_invoke.side_effect = [ex_a, ex_b]

        result = handler({"process_id": "p1"}, None)
        assert result["fields_extracted"] is True
        assert mock_invoke.call_count == 2

        puts = mock_table.put_item.call_args_list
        sks = [c[1]["Item"]["SK"] for c in puts]
        assert "BEDROCK_EXTRACTION#a.pdf" in sks
        assert "BEDROCK_EXTRACTION#b.pdf" in sks
        assert "BEDROCK_EXTRACTION" in sks
        assert "PARSED_OCR=textract_merged" in sks

        merged_item = next(c[1]["Item"] for c in puts if c[1]["Item"]["SK"] == "BEDROCK_EXTRACTION")
        merged = json.loads(merged_item["EXTRACTED_FIELDS"])
        assert merged["documento"] == "111"

        ocr_saved = next(c[1]["Item"] for c in puts if c[1]["Item"]["SK"] == "PARSED_OCR=textract_merged")
        ocr_out = json.loads(ocr_saved["PARSED_DATA"])
        assert ocr_out["per_document"][0]["documento_entrada_protheus"]["documento"] == "111"
        assert ocr_out["per_document"][1]["documento_entrada_protheus"]["documento"] == "222"


class TestBuildPrompt:

    def test_includes_nfe_xml_section(self):
        from bedrock_extract_fields.handler import _build_prompt

        prompt = _build_prompt(
            {"nfe_xml": {"numero_nota": "1"}, "textract_documents": []},
            None,
        )
        assert "NF-e XML" in prompt
        assert "numero_nota" in prompt

    def test_includes_textract_section(self):
        from bedrock_extract_fields.handler import _build_prompt

        prompt = _build_prompt(
            {
                "nfe_xml": None,
                "textract_documents": [
                    {"file_name": "fatura.pdf", "raw_text": "Fatura #12345", "tables": []}
                ],
            },
            None,
        )
        assert "Texto OCR" in prompt
        assert "fatura.pdf" in prompt
        assert "Fatura #12345" in prompt

    def test_includes_pedido_metadata(self):
        from bedrock_extract_fields.handler import _build_prompt

        prompt = _build_prompt(
            {"nfe_xml": None, "textract_documents": []},
            {"requestBody": {"cnpjEmitente": "111"}},
        )
        assert "pedido de compra" in prompt
        assert "cnpjEmitente" in prompt

    def test_single_doc_prompt_targets_one_file(self):
        from bedrock_extract_fields.handler import _build_prompt_single_doc

        doc = {"file_name": "fatura.pdf", "raw_text": "Total R$ 10", "tables": [{"rows": []}]}
        prompt = _build_prompt_single_doc(
            {"nfe_xml": {"numero_nota": "1"}, "textract_documents": [doc]},
            doc,
            None,
        )
        assert "fatura.pdf" in prompt
        assert "único alvo" in prompt
        assert "Total R$ 10" in prompt
        assert "numero_nota" in prompt
