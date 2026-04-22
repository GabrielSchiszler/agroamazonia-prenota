"""
Marca FILE# como REJECTED (Map: handler skip — sem Textract).
"""

from __future__ import annotations

import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])


def handler(event, context):
    process_id = event["process_id"]
    att = event["attachment"]
    pk = f"PROCESS#{process_id}"
    file_sk = att["file_sk"]
    fname = att["file_name"]
    reason = att.get("reason", "unsupported")

    logger.info("reject_attachment %s %s", file_sk, reason)

    table.update_item(
        Key={"PK": pk, "SK": file_sk},
        UpdateExpression="SET #st = :st, rejection_reason = :rr",
        ExpressionAttributeNames={"#st": "STATUS"},
        ExpressionAttributeValues={
            ":st": "REJECTED",
            ":rr": reason[:500],
        },
    )

    return {
        "process_id": process_id,
        "file_name": fname,
        "status": "rejected",
    }
