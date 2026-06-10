"""
Lambda: merge_extractions

Reads PARSED_XML=* and TEXTRACT#* items for a process, merges them into a single
canonical JSON record (SK=MERGED_EXTRACTION), and *also* writes a PARSED_OCR record
so that send_to_protheus (which reads PARSED_OCR) keeps working without changes.

Schema (MERGED_EXTRACTION):
{
    "schema_version": 2,
    "nfe_xml": { ... NF-e principal (mesmo item IS_PRIMARY / melhor score) ... } | null,
    "nfe_file": "filename.xml" | null,
    "xml_documents": [ { "file_name": "...", "parsed_data": { ... } } ],
    "textract_documents": [
        {
            "file_name": "...",
            "raw_text": "...",
            "tables": [{"rows": [[...]]}],
            "job_id": "...",
            "protheus_hints": { ... , "parsed_xml_style": { ... } }  // flat + espelho tipo PARSED_DATA XML
        }
    ]
}

PARSED_OCR=textract_merged / PARSED_DATA (JSON):
    raw_text, source_files, per_document (hints/tabelas por ficheiro).
    Após Bedrock, bedrock_extract_fields acrescenta documento_entrada_protheus (mesmo shape do item BEDROCK_EXTRACTION).

Input:  { "process_id": "..." }
Output: { "process_id": "..." }
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import boto3

from utils.primary_xml import iter_parsed_xml_items, pick_best_parsed_xml_item

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

SCHEMA_VERSION = 2


def handler(event, context):
    process_id = event["process_id"]
    pk = f"PROCESS#{process_id}"
    timestamp = int(datetime.now().timestamp())

    logger.info("merge_extractions start: process_id=%s", process_id)

    items = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )["Items"]

    items_by_sk = {it["SK"]: it for it in items}

    # ---- Todos os XMLs parseados ----
    xml_documents = [
        {"file_name": fn, "parsed_data": data}
        for _, fn, data in iter_parsed_xml_items(items)
    ]

    best_xml = pick_best_parsed_xml_item(items)
    nfe_xml_data = None
    nfe_file = None
    if best_xml and best_xml.get("PARSED_DATA"):
        try:
            nfe_xml_data = json.loads(best_xml["PARSED_DATA"])
            nfe_file = best_xml.get("FILE_NAME")
            logger.info("Primary PARSED_XML: %s", best_xml.get("SK"))
        except Exception as e:
            logger.warning("Could not parse primary PARSED_DATA: %s", e)

    # ---- Textract results ----
    textract_docs: list[dict] = []
    combined_raw_text_parts: list[str] = []
    for sk, it in items_by_sk.items():
        if not sk.startswith("TEXTRACT#"):
            continue
        suffix = sk[len("TEXTRACT#") :] if sk.startswith("TEXTRACT#") else ""
        doc: dict = {
            "file_name": it.get("FILE_NAME", ""),
            "file_upload_id": suffix or None,
            "raw_text": it.get("RAW_TEXT", ""),
            "tables": [],
            "job_id": it.get("JOB_ID", ""),
        }
        try:
            doc["tables"] = json.loads(it.get("TABLES_DATA", "[]"))
        except Exception:
            pass
        hints_raw = it.get("PROTHEUS_HINTS")
        if hints_raw:
            try:
                doc["protheus_hints"] = json.loads(hints_raw)
            except Exception:
                pass
        doc["timestamp"] = it.get("TIMESTAMP")
        textract_docs.append(doc)
        if doc["raw_text"]:
            combined_raw_text_parts.append(doc["raw_text"])

    try:
        from utils.extraction_dedup import dedupe_textract_documents

        before = len(textract_docs)
        textract_docs = dedupe_textract_documents(textract_docs)
        if len(textract_docs) < before:
            logger.info(
                "Textract deduped: %d -> %d documento(s)",
                before,
                len(textract_docs),
            )
        combined_raw_text_parts = [
            d["raw_text"] for d in textract_docs if d.get("raw_text")
        ]
    except ImportError:
        pass

    logger.info(
        "Merging: nfe_xml=%s, textract_docs=%d",
        bool(nfe_xml_data),
        len(textract_docs),
    )

    # ---- Write MERGED_EXTRACTION (canonical) ----
    merged = {
        "schema_version": SCHEMA_VERSION,
        "nfe_xml": nfe_xml_data,
        "nfe_file": nfe_file,
        "xml_documents": xml_documents,
        "textract_documents": textract_docs,
    }
    merged_json = json.dumps(merged, ensure_ascii=False, default=str)

    table.put_item(Item={
        "PK": pk,
        "SK": "MERGED_EXTRACTION",
        "MERGED_DATA": merged_json,
        "TIMESTAMP": timestamp,
    })
    logger.info("MERGED_EXTRACTION written (%d bytes)", len(merged_json))

    # ---- Backfill PARSED_OCR for send_to_protheus compatibility ----
    # send_to_protheus reads the *first* PARSED_OCR item and uses PARSED_DATA (JSON)
    # with optional keys like 'moeda'.  We combine all Textract raw text into a single
    # PARSED_OCR record so the existing handler picks it up.
    if textract_docs:
        combined_text = "\n---\n".join(combined_raw_text_parts)
        ocr_compat = {
            "raw_text": combined_text,
            "source_files": [d["file_name"] for d in textract_docs],
            # Por ficheiro: protheus_hints (regex + parsed_xml_style como parse_xml) e depois
            # documento_entrada_protheus no mesmo registo (Bedrock).
            "per_document": [
                {
                    "file_name": d.get("file_name", ""),
                    "file_upload_id": d.get("file_upload_id"),
                    "raw_text": d.get("raw_text") or "",
                    "protheus_hints": d.get("protheus_hints") or {},
                    "tables_count": len(d.get("tables") or []),
                    "job_id": d.get("job_id") or "",
                }
                for d in textract_docs
            ],
        }
        ocr_json = json.dumps(ocr_compat, ensure_ascii=False, default=str)
        table.put_item(Item={
            "PK": pk,
            "SK": "PARSED_OCR=textract_merged",
            "FILE_NAME": "textract_merged",
            "PARSED_DATA": ocr_json,
            "SOURCE": "TEXTRACT",
            "TIMESTAMP": timestamp,
        })
        logger.info("PARSED_OCR backfill written for send_to_protheus compat")

    return {"process_id": process_id}
