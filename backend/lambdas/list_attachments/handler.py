"""
Lista FILE# com FILE_KEY para o Step Functions Map: cada item tem handler xml | textract | skip.
"""

from __future__ import annotations

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

TEXTRACT_SUPPORTED = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}


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

    attachments: list[dict] = []
    for it in items:
        sk = it.get("SK", "")
        if not sk.startswith("FILE#"):
            continue
        if not it.get("FILE_KEY"):
            continue
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
        if ext in TEXTRACT_SUPPORTED:
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
