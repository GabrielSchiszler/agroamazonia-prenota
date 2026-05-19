"""
Lambda: extract_documents

Runs Amazon Textract on every non-XML attachment of a process.
Persists each result as SK=TEXTRACT#{file_name} (same schema as processor.py).

Input (from Step Functions):
  { "process_id": "..." }

Behaviour:
  1. Query all FILE# items for the process.
  2. Skip files already classified as NF-e XML (ends with .xml — parse_xml handles those).
  3. Para PDF / IMAGE: Textract AnalyzeDocument (TABLES+FORMS) ou StartDocumentAnalysis; se o PDF
     for rejeitado com UnsupportedDocumentException, tenta DetectDocumentText (só texto, sem tabelas).
  4. Antes do Textract, PDFs são inspecionados em bytes (heurística XFA/Encrypt etc.) e o resultado vai
     para os logs CloudWatch para comparar com outros anexos que funcionam.
  5. Para .txt: lê UTF-8 do S3 (sem Textract), grava mesmo schema TEXTRACT# (merge + Bedrock iguais ao PDF).
  6. DOCX: marcado REJECTED (Textract não suporta DOCX nativamente).

Output:
  { "process_id": "...", "extracted_count": N, "rejected": [...] }
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from utils.pdf_textract_precheck import diagnose_pdf_bytes
from utils.protheus_hints import hints_from_textract_text

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Textract não está disponível em todas as regiões (ex.: não há endpoint em sa-east-1).
# Usar região explícita (padrão us-east-1) e AnalyzeDocument com Bytes após get_object no S3 local.
TEXTRACT_REGION = os.environ.get("TEXTRACT_REGION", "us-east-1")
TEXTRACT_ASYNC_STAGING_BUCKET = os.environ.get("TEXTRACT_ASYNC_STAGING_BUCKET", "").strip()

s3 = boto3.client("s3")
textract = boto3.client("textract", region_name=TEXTRACT_REGION)
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

TEXTRACT_SUPPORTED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}
PLAIN_TEXT_EXTENSIONS = {".txt"}
TEXTRACT_MAX_SYNC_BYTES = 10 * 1024 * 1024  # 10 MB sync limit
PLAIN_TEXT_MAX_BYTES = TEXTRACT_MAX_SYNC_BYTES


def _textract_result_sk(file_sk: str, fname: str) -> str:
    """SK único por anexo; compatível com FILE#<nome legado>."""
    if file_sk.startswith("FILE#"):
        return f"TEXTRACT#{file_sk[5:]}"
    return f"TEXTRACT#{fname}"


def _ext(name: str) -> str:
    dot = name.rfind(".")
    return name[dot:].lower() if dot != -1 else ""


def _run_textract_sync(bucket: str, key: str):
    """Textract AnalyzeDocument (sync) — tables + forms + text via Bytes.

    O bucket de documentos costuma estar em sa-east-1; o endpoint Textract em outra região
    não aceita S3Object cross-region — por isso baixamos o objeto e enviamos Bytes.

    Se AnalyzeDocument devolver UnsupportedDocumentException (PDF XFA, encriptado,
    formato exótico), tenta DetectDocumentText (só texto, sem tabelas) como fallback.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    if len(body) > TEXTRACT_MAX_SYNC_BYTES:
        raise ValueError(
            f"Arquivo excede {TEXTRACT_MAX_SYNC_BYTES} bytes para AnalyzeDocument síncrono; "
            "use cópia para bucket de staging (TEXTRACT_ASYNC_STAGING_BUCKET) ou reduza o PDF."
        )

    diag: dict | None = None
    if _ext(key) == ".pdf" or body[:5] == b"%PDF-":
        diag = diagnose_pdf_bytes(body)
        logger.info(
            "PDF pre-Textract key=%s diagnóstico=%s",
            key,
            json.dumps(diag, ensure_ascii=False, default=str),
        )

    try:
        resp = textract.analyze_document(
            Document={"Bytes": body},
            FeatureTypes=["TABLES", "FORMS"],
        )
        resp["_textract_mode"] = "analyze_document"
        return resp
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code", "")
        if code != "UnsupportedDocumentException":
            raise
        logger.warning(
            "AnalyzeDocument UnsupportedDocumentException key=%s; "
            "tentando DetectDocumentText. diag=%s",
            key,
            json.dumps(diag, ensure_ascii=False, default=str) if diag else "n/a",
        )
        try:
            resp2 = textract.detect_document_text(Document={"Bytes": body})
            resp2["_textract_mode"] = "detect_document_text_fallback"
            logger.info(
                "DetectDocumentText OK após falha AnalyzeDocument key=%s blocks=%s",
                key,
                len(resp2.get("Blocks") or []),
            )
            return resp2
        except ClientError as e2:
            code2 = (e2.response or {}).get("Error", {}).get("Code", "")
            logger.error(
                "DetectDocumentText também falhou key=%s code=%s diag=%s",
                key,
                code2,
                json.dumps(diag, ensure_ascii=False, default=str) if diag else "n/a",
            )
            raise RuntimeError(
                f"Textract rejeitou o PDF (AnalyzeDocument e DetectDocumentText). "
                f"AnalyzeDocument={code}; DetectDocumentText={code2}. "
                f"Diagnóstico: {json.dumps(diag, ensure_ascii=False, default=str) if diag else 'n/a'}"
            ) from e2


def _run_textract_async(bucket: str, key: str):
    """StartDocumentAnalysis → poll until done. Requer bucket de staging na mesma região do Textract."""
    if not TEXTRACT_ASYNC_STAGING_BUCKET:
        raise RuntimeError(
            "PDF maior que o limite síncrono: defina TEXTRACT_ASYNC_STAGING_BUCKET "
            f"(bucket na região {TEXTRACT_REGION}, mesma do Textract) para análise assíncrona."
        )
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    staging_key = f"textract-staging/{int(time.time())}-{os.path.basename(key)}"
    s3_tx = boto3.client("s3", region_name=TEXTRACT_REGION)
    s3_tx.put_object(Bucket=TEXTRACT_ASYNC_STAGING_BUCKET, Key=staging_key, Body=body)
    start = textract.start_document_analysis(
        DocumentLocation={
            "S3Object": {"Bucket": TEXTRACT_ASYNC_STAGING_BUCKET, "Name": staging_key}
        },
        FeatureTypes=["TABLES", "FORMS"],
    )
    job_id = start["JobId"]
    logger.info("Textract async job started: %s for %s", job_id, key)

    while True:
        time.sleep(3)
        result = textract.get_document_analysis(JobId=job_id)
        status = result["JobStatus"]
        if status == "SUCCEEDED":
            return job_id, result
        if status == "FAILED":
            raise RuntimeError(f"Textract job {job_id} failed: {result.get('StatusMessage')}")


def _extract_text_and_tables(blocks: list[dict]) -> tuple[str, list[dict]]:
    """Pull raw text (LINE blocks) and table structures from Textract response."""
    lines: list[str] = []
    tables: list[dict] = []

    block_map = {b["Id"]: b for b in blocks}

    for b in blocks:
        if b["BlockType"] == "LINE":
            lines.append(b.get("Text", ""))
        elif b["BlockType"] == "TABLE":
            rows: list[list[str]] = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    cells = [block_map[cid] for cid in rel["Ids"] if cid in block_map]
                    for cell in sorted(cells, key=lambda c: (c.get("RowIndex", 0), c.get("ColumnIndex", 0))):
                        ri = cell.get("RowIndex", 1) - 1
                        while len(rows) <= ri:
                            rows.append([])
                        cell_text = ""
                        for crel in cell.get("Relationships", []):
                            if crel["Type"] == "CHILD":
                                cell_text = " ".join(
                                    block_map[wid].get("Text", "")
                                    for wid in crel["Ids"]
                                    if wid in block_map
                                )
                        rows[ri].append(cell_text)
            tables.append({"rows": rows})

    return "\n".join(lines), tables


def _persist_extraction_like_textract(
    pk: str,
    file_sk: str,
    fname: str,
    fkey: str,
    raw_text: str,
    tables_data: list,
    job_id: str,
    timestamp: int,
    textract_mode: str | None = None,
) -> None:
    """Grava TEXTRACT#* + STATUS EXTRACTED no FILE# — mesmo formato que Textract (merge/Bedrock)."""
    sk = _textract_result_sk(file_sk, fname)
    hints = hints_from_textract_text(raw_text)
    textract_item: dict = {
        "PK": pk,
        "SK": sk,
        "FILE_NAME": fname,
        "FILE_KEY": fkey,
        "JOB_ID": job_id,
        "TABLE_COUNT": len(tables_data),
        "TABLES_DATA": json.dumps(tables_data),
        "RAW_TEXT": raw_text,
        "TIMESTAMP": timestamp,
    }
    if textract_mode:
        textract_item["TEXTRACT_MODE"] = textract_mode
    if hints:
        textract_item["PROTHEUS_HINTS"] = json.dumps(hints, ensure_ascii=False)
    table.put_item(Item=textract_item)
    table.update_item(
        Key={"PK": pk, "SK": file_sk},
        UpdateExpression="SET #st = :st",
        ExpressionAttributeNames={"#st": "STATUS"},
        ExpressionAttributeValues={":st": "EXTRACTED"},
    )


def _extract_plain_text_from_s3(bucket: str, fkey: str, max_bytes: int) -> str:
    head = s3.head_object(Bucket=bucket, Key=fkey)
    size = head["ContentLength"]
    if size > max_bytes:
        raise ValueError(
            f"Arquivo texto excede {max_bytes} bytes; reduza o .txt ou aumente o limite."
        )
    obj = s3.get_object(Bucket=bucket, Key=fkey)
    raw_bytes = obj["Body"].read()
    return raw_bytes.decode("utf-8", errors="replace")


def handle_single_textract(event, context):
    """Um anexo não-XML: .txt (UTF-8) ou Textract para PDF/imagem (Step Functions Map)."""
    process_id = event["process_id"]
    bucket = os.environ["BUCKET_NAME"]
    pk = f"PROCESS#{process_id}"
    timestamp = int(datetime.now().timestamp())
    fname = event["file_name"]
    fkey = event["file_key"]
    fi_sk = event["file_sk"]

    logger.info("extract_documents single: %s", fname)

    ext = _ext(fname)

    if ext in PLAIN_TEXT_EXTENSIONS:
        try:
            raw_text = _extract_plain_text_from_s3(bucket, fkey, PLAIN_TEXT_MAX_BYTES)
            _persist_extraction_like_textract(
                pk, fi_sk, fname, fkey, raw_text, [], "plaintext", timestamp
            )
            logger.info("Plain text extracted: %s (%d chars)", fname, len(raw_text))
            return {
                "process_id": process_id,
                "file_name": fname,
                "extracted_count": 1,
                "rejected": [],
            }
        except Exception as exc:
            logger.error("Plain text extract failed for %s: %s", fname, exc, exc_info=True)
            table.update_item(
                Key={"PK": pk, "SK": fi_sk},
                UpdateExpression="SET #st = :st, extraction_error = :err",
                ExpressionAttributeNames={"#st": "STATUS"},
                ExpressionAttributeValues={
                    ":st": "EXTRACTION_FAILED",
                    ":err": str(exc)[:500],
                },
            )
            raise

    if ext not in TEXTRACT_SUPPORTED_EXTENSIONS:
        logger.warning("Unsupported for Textract: %s (ext=%s)", fname, ext)
        table.update_item(
            Key={"PK": pk, "SK": fi_sk},
            UpdateExpression="SET #st = :st, rejection_reason = :rr",
            ExpressionAttributeNames={"#st": "STATUS"},
            ExpressionAttributeValues={
                ":st": "REJECTED",
                ":rr": f"Extensão {ext} não suportada pelo Textract (DOCX requer conversão para PDF).",
            },
        )
        return {
            "process_id": process_id,
            "file_name": fname,
            "extracted_count": 0,
            "rejected": [fname],
        }

    try:
        head = s3.head_object(Bucket=bucket, Key=fkey)
        size = head["ContentLength"]

        if size <= TEXTRACT_MAX_SYNC_BYTES:
            resp = _run_textract_sync(bucket, fkey)
            mode = resp.pop("_textract_mode", None)
            raw_text, tables_data = _extract_text_and_tables(resp.get("Blocks", []))
            job_id = "sync"
        else:
            job_id, resp = _run_textract_async(bucket, fkey)
            mode = resp.pop("_textract_mode", None)
            raw_text, tables_data = _extract_text_and_tables(resp.get("Blocks", []))

        _persist_extraction_like_textract(
            pk, fi_sk, fname, fkey, raw_text, tables_data, job_id, timestamp, textract_mode=mode
        )

        return {
            "process_id": process_id,
            "file_name": fname,
            "extracted_count": 1,
            "rejected": [],
        }
    except Exception as exc:
        logger.error("Textract failed for %s: %s", fname, exc, exc_info=True)
        table.update_item(
            Key={"PK": pk, "SK": fi_sk},
            UpdateExpression="SET #st = :st, extraction_error = :err",
            ExpressionAttributeNames={"#st": "STATUS"},
            ExpressionAttributeValues={
                ":st": "EXTRACTION_FAILED",
                ":err": str(exc)[:500],
            },
        )
        raise


def handler(event, context):
    process_id = event["process_id"]
    bucket = os.environ["BUCKET_NAME"]
    pk = f"PROCESS#{process_id}"
    timestamp = int(datetime.now().timestamp())

    logger.info("extract_documents start: process_id=%s", process_id)

    if event.get("file_name") and event.get("file_key") and event.get("file_sk"):
        return handle_single_textract(event, context)

    items = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )["Items"]

    file_items = [
        it for it in items
        if it.get("SK", "").startswith("FILE#")
        and it.get("FILE_KEY")
        and not it.get("FILE_NAME", "").lower().endswith(".xml")
    ]

    logger.info("Non-XML files to process: %d", len(file_items))

    extracted_count = 0
    rejected: list[str] = []

    for fi in file_items:
        fname = fi["FILE_NAME"]
        fkey = fi["FILE_KEY"]
        fi_sk = fi["SK"]
        ext = _ext(fname)

        if ext in PLAIN_TEXT_EXTENSIONS:
            logger.info("Reading plain text: %s (%s)", fname, fkey)
            try:
                raw_text = _extract_plain_text_from_s3(bucket, fkey, PLAIN_TEXT_MAX_BYTES)
                _persist_extraction_like_textract(
                    pk, fi_sk, fname, fkey, raw_text, [], "plaintext", timestamp
                )
                extracted_count += 1
                logger.info("Plain text done for %s (%d chars)", fname, len(raw_text))
            except Exception as exc:
                logger.error("Plain text failed for %s: %s", fname, exc, exc_info=True)
                table.update_item(
                    Key={"PK": pk, "SK": fi_sk},
                    UpdateExpression="SET #st = :st, extraction_error = :err",
                    ExpressionAttributeNames={"#st": "STATUS"},
                    ExpressionAttributeValues={
                        ":st": "EXTRACTION_FAILED",
                        ":err": str(exc)[:500],
                    },
                )
            continue

        if ext not in TEXTRACT_SUPPORTED_EXTENSIONS:
            logger.warning("Unsupported for Textract: %s (ext=%s)", fname, ext)
            rejected.append(fname)
            table.update_item(
                Key={"PK": pk, "SK": fi_sk},
                UpdateExpression="SET #st = :st, rejection_reason = :rr",
                ExpressionAttributeNames={"#st": "STATUS"},
                ExpressionAttributeValues={
                    ":st": "REJECTED",
                    ":rr": f"Extensão {ext} não suportada pelo Textract (DOCX requer conversão para PDF).",
                },
            )
            continue

        logger.info("Running Textract on %s (%s)", fname, fkey)

        try:
            head = s3.head_object(Bucket=bucket, Key=fkey)
            size = head["ContentLength"]

            if size <= TEXTRACT_MAX_SYNC_BYTES:
                resp = _run_textract_sync(bucket, fkey)
                mode = resp.pop("_textract_mode", None)
                raw_text, tables_data = _extract_text_and_tables(resp.get("Blocks", []))
                job_id = "sync"
            else:
                job_id, resp = _run_textract_async(bucket, fkey)
                mode = resp.pop("_textract_mode", None)
                raw_text, tables_data = _extract_text_and_tables(resp.get("Blocks", []))

            _persist_extraction_like_textract(
                pk, fi_sk, fname, fkey, raw_text, tables_data, job_id, timestamp, textract_mode=mode
            )

            extracted_count += 1
            logger.info(
                "Textract done for %s (job=%s, lines=%d, tables=%d)",
                fname,
                job_id,
                raw_text.count("\n") + 1,
                len(tables_data),
            )

        except Exception as exc:
            logger.error("Textract failed for %s: %s", fname, exc, exc_info=True)
            table.update_item(
                Key={"PK": pk, "SK": fi_sk},
                UpdateExpression="SET #st = :st, extraction_error = :err",
                ExpressionAttributeNames={"#st": "STATUS"},
                ExpressionAttributeValues={
                    ":st": "EXTRACTION_FAILED",
                    ":err": str(exc)[:500],
                },
            )

    result = {
        "process_id": process_id,
        "extracted_count": extracted_count,
        "rejected": rejected,
    }
    logger.info("extract_documents done: %s", json.dumps(result))
    return result
