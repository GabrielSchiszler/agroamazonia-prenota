"""
response_summary para feedback de SUCESSO via Bedrock.

Entrada: a mesma estrutura usada com generate_error_summary_with_bedrock no send_feedback:
  {"process_id": str, "success": true, "details": organized_details}

Todo o JSON é enviado ao modelo (payload_req, response_req, body.message, body.details, etc.).

Não altera bedrock_error_summary.py — módulo separado só para o ramo de sucesso.
"""

import json
import logging
import os
from typing import Optional

import boto3

logger = logging.getLogger()


def build_success_feedback_summary_prompt(feedback_data: dict) -> str:
    """Monta o prompt completo (testes locais / --dry-run)."""
    payload_json = json.dumps(feedback_data, ensure_ascii=False, indent=2, default=str)

    return """Você traduz integrações com ERP Protheus para usuários de negócio.

O JSON abaixo é um feedback com processamento bem-sucedido (success: true). Use TODO o conteúdo: em especial details.payload_req, details.response_req (status_code, body.message, body.details), process_type, status, timestamps, etc.

Regras curtas: HTTP 2xx com mensagem de sucesso pode ser documento de entrada definitivo; se body.message mencionar pré-nota, classificação ou "log", ou existir lista body.details técnica, explique a pendência em linguagem simples. Não repita caminhos de servidor inteiros; extraia o que importa (ex.: chave da NFe, nome do XML). Não cite ao usuário nomes de tabelas internas (SX3, SD1, SF1…); traduza o sentido.

Saída: exatamente 3 parágrafos em texto corrido — (1) o que ocorreu; (2) impacto ou pendências; (3) o que conferir ou fazer a seguir. Apenas fatos presentes no JSON. Sem markdown, asteriscos ou listas numeradas.

Entrada (JSON completo):

""" + payload_json + """

Retorne APENAS o texto dos três parágrafos, em texto puro."""


def _invoke_bedrock(prompt: str) -> Optional[str]:
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model_id = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")
    response = bedrock_client.invoke_model(
        modelId=model_id,
        body=json.dumps(
            {
                "messages": [{"role": "user", "content": [{"text": prompt}]}],
                "inferenceConfig": {
                    "maxTokens": 2000,
                    "temperature": 0.3,
                    "topP": 0.9,
                },
            }
        ),
    )
    body_bytes = response["body"].read()
    if not body_bytes:
        logger.error("Bedrock returned empty response (success summary)")
        return None
    result = json.loads(body_bytes)
    if "output" in result and "message" in result["output"]:
        content = result["output"]["message"].get("content", [])
        if content:
            text_content = content[0].get("text", "")
            text_content = (
                text_content.replace("**", "")
                .replace("*", "")
                .replace("__", "")
                .replace("_", "")
            )
            return text_content.strip()
    logger.warning("Bedrock success summary: unexpected response shape")
    return None


def generate_success_feedback_summary_with_bedrock(feedback_data: dict) -> Optional[str]:
    """
    Gera response_summary para success: true com o mesmo objeto que o erro recebe
    (process_id, success, details organizados).
    """
    if not isinstance(feedback_data, dict):
        return None
    if feedback_data.get("success") is not True:
        logger.warning("bedrock_success_summary: expected success=True in feedback_data")
        return None
    try:
        prompt = build_success_feedback_summary_prompt(feedback_data)
        return _invoke_bedrock(prompt)
    except Exception as e:
        logger.error("Erro ao gerar success summary com Bedrock: %s", str(e))
        import traceback

        traceback.print_exc()
        return None
