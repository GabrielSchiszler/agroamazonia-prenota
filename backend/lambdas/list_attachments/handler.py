"""
Lista FILE# com FILE_KEY para o Step Functions Map: cada item tem handler xml | textract | skip.
Arquivos .txt usam o mesmo ramo textract (extract_documents lê UTF-8 do S3, sem API Textract).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from utils.extraction_dedup import dedupe_file_items_by_content_hash

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

TEXTRACT_SUPPORTED = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}
PLAIN_TEXT_EXTENSIONS = {".txt"}


def _ext(name: str) -> str:
    dot = name.rfind(".")
    return name[dot:].lower() if dot != -1 else ""


def handler(event, context):
    process_id = event["process_id"]
    pk = f"PROCESS#{process_id}"
    logger.info("list_attachments process_id=%s", process_id)

    items = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )["Items"]

    file_items = [
        it
        for it in items
        if str(it.get("SK", "")).startswith("FILE#") and it.get("FILE_KEY")
    ]
    file_items = dedupe_file_items_by_content_hash(file_items)

    attachments: list[dict] = []
    for it in file_items:
        sk = it.get("SK", "")
        fname = it.get("FILE_NAME", "")
        lower = fname.lower()
        if lower.endswith(".xml"):
            attachments.append(
                {
                    "file_sk": sk,
                    "file_name": fname,
                    "file_key": it["FILE_KEY"],
                    "handler": "xml",
                }
            )
            continue
        ext = _ext(fname)
        if ext in TEXTRACT_SUPPORTED or ext in PLAIN_TEXT_EXTENSIONS:
            attachments.append(
                {
                    "file_sk": sk,
                    "file_name": fname,
                    "file_key": it["FILE_KEY"],
                    "handler": "textract",
                }
            )
        else:
            attachments.append(
                {
                    "file_sk": sk,
                    "file_name": fname,
                    "file_key": it["FILE_KEY"],
                    "handler": "skip",
                    "reason": f"Extensão {ext} não suportada pelo Textract (ex.: DOCX).",
                }
            )

    out = {
        "process_id": process_id,
        "attachments": attachments,
        "attachment_count": len(attachments),
    }
    logger.info("list_attachments: %s", json.dumps(out, default=str))
    return out
