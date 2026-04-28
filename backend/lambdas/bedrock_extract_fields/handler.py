"""
Lambda: bedrock_extract_fields

Uses Amazon Bedrock (Nova Pro) to extract structured fields from the canonical
MERGED_EXTRACTION record.  The output is a JSON that maps directly to the Protheus
"documento de entrada" payload consumed by send_to_protheus.

Storage:
- SK=BEDROCK_EXTRACTION — merged JSON (compatível com send_to_protheus)
- SK=BEDROCK_EXTRACTION#<file_name> — um JSON por arquivo Textract (OCR), para UI e leitura futura

Quando há textract_documents com conteúdo, a Lambda chama o modelo **uma vez por arquivo**
e agrega os resultados em BEDROCK_EXTRACTION. Sem PDF/imagem OCR, mantém-se uma única
chamada com o prompt completo (ex.: só XML).

Input:  { "process_id": "..." }
Output: { "process_id": "...", "fields_extracted": true/false }
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])


PROTHEUS_FIELD_SCHEMA = """\
Campos do payload "documento de entrada" Protheus que precisam ser extraídos.
Se o valor não puder ser determinado com confiança, use null.

{
  "tipoDeDocumento": "string — ex: NF, NFS",
  "documento": "string — número do documento / nota",
  "serie": "string — série da nota",
  "dataEmissao": "string — formato YYYYMMDD",
  "especie": "string — ex: SPED",
  "chaveAcesso": "string — 44 dígitos da NFe ou null",
  "tipoFrete": "string — C, F, T, D, S, N",
  "moeda": "int — 1=BRL, 2=USD, 3=EUR",
  "taxaCambio": "number",
  "cnpjEmitente": "string — 14 dígitos, apenas números",
  "cpfEmitente": "string — 11 dígitos (se PF) ou null",
  "ieEmitente": "string ou null",
  "itens": [
    {
      "codigoProduto": "string",
      "produto": "string — descrição",
      "quantidade": "number",
      "valorUnitario": "number",
      "unidadeMedida": "string",
      "cfop": "string"
    }
  ]
}
"""


def _build_prompt(merged_data: dict, pedido_metadata: Optional[dict]) -> str:
    parts = [
        "Você é um especialista em documentos fiscais brasileiros e integração com ERP Protheus.",
        "",
        "A partir dos dados extraídos abaixo (XML de NF-e estruturado e/ou texto OCR de documentos "
        "complementares), preencha o JSON de saída no schema indicado.",
        "",
        "Se o XML NF-e está disponível, **priorize** seus campos (são estruturados e confiáveis).",
        "Use o texto OCR apenas para complementar campos ausentes no XML.",
        "",
        "### Schema de saída esperado",
        PROTHEUS_FIELD_SCHEMA,
        "",
    ]

    if pedido_metadata:
        parts.append("### Metadados do pedido de compra (referência)")
        parts.append(json.dumps(pedido_metadata, ensure_ascii=False, indent=2, default=str))
        parts.append("")

    parts.append("### Dados extraídos (MERGED_EXTRACTION)")
    nfe = merged_data.get("nfe_xml")
    textract_docs = merged_data.get("textract_documents", [])

    if nfe:
        parts.append("#### NF-e XML (estruturado)")
        parts.append(json.dumps(nfe, ensure_ascii=False, indent=2, default=str)[:12000])

    if textract_docs:
        parts.append("#### Texto OCR (Textract)")
        for doc in textract_docs[:5]:
            parts.append(f"--- Arquivo: {doc.get('file_name', '?')} ---")
            parts.append((doc.get("raw_text") or "")[:4000])
            if doc.get("tables"):
                parts.append(f"Tabelas ({len(doc['tables'])}):")
                parts.append(json.dumps(doc["tables"][:3], ensure_ascii=False, default=str)[:3000])

    parts.append("")
    parts.append(
        "Retorne APENAS o JSON válido (sem markdown, sem explicação, sem ```). "
        "Se um campo não puder ser determinado, use null."
    )

    return "\n".join(parts)


def _build_prompt_single_doc(
    merged_data: dict,
    doc: dict,
    pedido_metadata: Optional[dict],
) -> str:
    """Um único arquivo Textract + XML NF-e opcional como referência."""
    parts = [
        "Você é um especialista em documentos fiscais brasileiros e integração com ERP Protheus.",
        "",
        "Analise **apenas** o documento indicado abaixo (um arquivo). "
        "Extraia os campos do schema para esse documento.",
        "",
        "Se existir XML NF-e de referência, use-o para priorizar campos estruturados da nota; "
        "o texto/tabelas deste arquivo servem para o que for específico deste anexo.",
        "",
        "### Schema de saída esperado",
        PROTHEUS_FIELD_SCHEMA,
        "",
    ]

    if pedido_metadata:
        parts.append("### Metadados do pedido de compra (referência)")
        parts.append(json.dumps(pedido_metadata, ensure_ascii=False, indent=2, default=str))
        parts.append("")

    nfe = merged_data.get("nfe_xml")
    if nfe:
        parts.append("#### NF-e XML (referência — processo inteiro)")
        parts.append(json.dumps(nfe, ensure_ascii=False, indent=2, default=str)[:12000])
        parts.append("")

    fn = doc.get("file_name", "?")
    parts.append(f"#### Documento (único alvo): {fn}")
    parts.append((doc.get("raw_text") or "")[:8000])
    if doc.get("tables"):
        parts.append(f"Tabelas ({len(doc['tables'])}):")
        parts.append(json.dumps(doc["tables"][:5], ensure_ascii=False, default=str)[:6000])

    parts.append("")
    parts.append(
        "Retorne APENAS o JSON válido (sem markdown, sem explicação, sem ```). "
        "Se um campo não puder ser determinado, use null."
    )
    return "\n".join(parts)


def _textract_docs_with_content(merged_data: dict) -> list[dict]:
    out: list[dict] = []
    for d in merged_data.get("textract_documents") or []:
        if not isinstance(d, dict):
            continue
        text = (d.get("raw_text") or "").strip()
        tables = d.get("tables") or []
        if text or tables:
            out.append(d)
    return out


def _parse_bedrock_json(raw_response: str) -> Optional[dict[str, Any]]:
    if not raw_response or not raw_response.strip():
        return None
    try:
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
        if cleaned.rstrip().endswith("```"):
            cleaned = cleaned.rstrip()[:-3]
        extracted = json.loads(cleaned.strip())
        return extracted if isinstance(extracted, dict) else None
    except json.JSONDecodeError:
        return None


def _merge_bedrock_extractions(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    """Agrega vários JSONs Bedrock num único dict (primeiro não-nulo por campo; itens concatenados)."""
    merged: dict[str, Any] = {}
    all_itens: list[Any] = []
    for ext in extractions:
        if not isinstance(ext, dict):
            continue
        for k, v in ext.items():
            if k == "itens" and isinstance(v, list):
                all_itens.extend(v)
            elif v is not None:
                if k not in merged or merged[k] is None:
                    merged[k] = v
    if all_itens:
        merged["itens"] = all_itens
    return merged


def _invoke_bedrock(prompt: str) -> Optional[str]:
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model_id = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")

    response = bedrock_client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {
                "maxTokens": 4096,
                "temperature": 0.1,
            },
        }),
    )

    result = json.loads(response["body"].read())
    text = result.get("output", {}).get("message", {}).get("content", [{}])[0].get("text", "")
    return text.strip() if text else None


def handler(event, context):
    process_id = event["process_id"]
    pk = f"PROCESS#{process_id}"

    logger.info("bedrock_extract_fields start: process_id=%s", process_id)

    items = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )["Items"]
    items_by_sk = {it["SK"]: it for it in items}

    # Load MERGED_EXTRACTION
    merged_item = items_by_sk.get("MERGED_EXTRACTION")
    if not merged_item or not merged_item.get("MERGED_DATA"):
        logger.warning("MERGED_EXTRACTION not found — skipping Bedrock extraction")
        return {"process_id": process_id, "fields_extracted": False}

    merged_data = json.loads(merged_item["MERGED_DATA"])

    # Load pedido de compra metadata (optional enrichment)
    pedido_item = items_by_sk.get("PEDIDO_COMPRA_METADATA")
    pedido_metadata = None
    if pedido_item and pedido_item.get("METADADOS"):
        try:
            pedido_metadata = json.loads(pedido_item["METADADOS"])
        except Exception:
            pass

    ts = int(datetime.now().timestamp())
    docs_for_llm = _textract_docs_with_content(merged_data)
    per_file_extracted: list[tuple[str, dict[str, Any]]] = []
    extracted: Optional[dict[str, Any]] = None
    raw_response: Optional[str] = None

    if docs_for_llm:
        logger.info("Bedrock per-file: %d documento(s) Textract", len(docs_for_llm))
        for doc in docs_for_llm:
            sprompt = _build_prompt_single_doc(merged_data, doc, pedido_metadata)
            raw = _invoke_bedrock(sprompt)
            parsed = _parse_bedrock_json(raw or "")
            fn = (doc.get("file_name") or "").strip() or "sem_nome"
            if parsed:
                table.put_item(Item={
                    "PK": pk,
                    "SK": f"BEDROCK_EXTRACTION#{fn}",
                    "FILE_NAME": fn,
                    "EXTRACTED_FIELDS": json.dumps(parsed, ensure_ascii=False, default=str),
                    "TIMESTAMP": ts,
                })
                per_file_extracted.append((fn, parsed))
            else:
                logger.warning("Bedrock per-file inválido ou vazio: %s", fn)

        if per_file_extracted:
            extracted = _merge_bedrock_extractions([d for _, d in per_file_extracted])
        else:
            logger.warning("Todas as extrações por arquivo falharam — prompt agregado")
            prompt = _build_prompt(merged_data, pedido_metadata)
            logger.info("Bedrock prompt length (fallback): %d chars", len(prompt))
            raw_response = _invoke_bedrock(prompt)
            extracted = _parse_bedrock_json(raw_response or "")
    else:
        prompt = _build_prompt(merged_data, pedido_metadata)
        logger.info("Bedrock prompt length: %d chars", len(prompt))
        raw_response = _invoke_bedrock(prompt)
        extracted = _parse_bedrock_json(raw_response or "")

    if extracted is None:
        if raw_response:
            logger.error("Bedrock response is not valid JSON\nRaw: %s", raw_response[:500])
            table.put_item(Item={
                "PK": pk,
                "SK": "BEDROCK_EXTRACTION",
                "RAW_RESPONSE": raw_response[:10000],
                "PARSE_ERROR": "JSON inválido",
                "TIMESTAMP": ts,
            })
        else:
            logger.error("Bedrock returned empty response")
        return {"process_id": process_id, "fields_extracted": False}

    table.put_item(Item={
        "PK": pk,
        "SK": "BEDROCK_EXTRACTION",
        "EXTRACTED_FIELDS": json.dumps(extracted, ensure_ascii=False, default=str),
        "TIMESTAMP": ts,
    })

    logger.info(
        "BEDROCK_EXTRACTION saved (%d keys in extracted JSON)",
        len(extracted) if isinstance(extracted, dict) else 0,
    )

    # Espelha em PARSED_OCR=textract_merged: consolidado + por arquivo em per_document
    ocr_merged = items_by_sk.get("PARSED_OCR=textract_merged")
    if ocr_merged and ocr_merged.get("PARSED_DATA"):
        try:
            pd = json.loads(ocr_merged["PARSED_DATA"])
            if isinstance(pd, dict):
                pd["documento_entrada_protheus"] = extracted
                pd["_campos_estruturados_fonte"] = "bedrock"
                by_fn = {fn: d for fn, d in per_file_extracted}
                for row in pd.get("per_document") or []:
                    if not isinstance(row, dict):
                        continue
                    rfn = row.get("file_name")
                    if rfn and rfn in by_fn:
                        row["documento_entrada_protheus"] = by_fn[rfn]
                table.put_item(Item={
                    "PK": pk,
                    "SK": "PARSED_OCR=textract_merged",
                    "FILE_NAME": ocr_merged.get("FILE_NAME", "textract_merged"),
                    "PARSED_DATA": json.dumps(pd, ensure_ascii=False, default=str),
                    "SOURCE": ocr_merged.get("SOURCE", "TEXTRACT"),
                    "TIMESTAMP": ts,
                })
                logger.info("PARSED_OCR=textract_merged updated with documento_entrada_protheus")
        except Exception as e:
            logger.warning("Could not mirror Bedrock JSON into PARSED_OCR: %s", e)

    return {"process_id": process_id, "fields_extracted": True}
