"""Logs seguros para fluxo de URLs pré-assinadas (S3 SigV4).

A query string contém credenciais temporárias — nunca logar URL completa.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse


def emit_presigned_line(
    logger_instance: logging.Logger,
    msg: str,
    *args,
    is_error: bool = False,
    exc_info: bool = False,
) -> None:
    """Imprime em stdout (CloudWatch no Lambda) e repete no logger.

    No Lambda, ``logging.basicConfig`` pode não alterar o root; ``print`` garante visibilidade.
    """
    try:
        line = msg % args if args else msg
    except Exception:
        line = f"{msg} {args}"
    print(line, flush=True)
    if is_error:
        logger_instance.error(msg, *args, exc_info=exc_info)
    else:
        logger_instance.info(msg, *args)


def safe_presigned_url_preview(url: Optional[str], max_path: int = 160) -> str:
    """Host + path truncado, sem query (onde vivem X-Amz-* e assinatura)."""
    if not url:
        return "(vazia)"
    try:
        p = urlparse(url)
        path = p.path or ""
        tail = "…" if len(path) > max_path else ""
        short = path[:max_path]
        return f"{p.scheme}://{p.netloc}{short}{tail}"
    except Exception:
        return "(url inválida)"


def presigned_put_response_for_log(result: Dict[str, Any]) -> Dict[str, Any]:
    """Payload de resposta para log: sem upload_url bruta; preview + tamanho."""
    out: Dict[str, Any] = {k: v for k, v in result.items() if k != "upload_url"}
    u = result.get("upload_url")
    if isinstance(u, str):
        out["upload_url_preview"] = safe_presigned_url_preview(u)
        out["upload_url_chars"] = len(u)
    return out


def presigned_batch_response_for_log(body: Dict[str, Any]) -> Dict[str, Any]:
    """Resposta batch: lista de ficheiros com preview em vez de URL completa."""
    files = body.get("files") or []
    slim = []
    for i, f in enumerate(files):
        if not isinstance(f, dict):
            slim.append({"index": i, "raw": str(f)[:200]})
            continue
        entry = {
            "index": i,
            "file_name": f.get("file_name"),
            "file_key": f.get("file_key"),
            "doc_type": f.get("doc_type"),
            "content_type": f.get("content_type"),
        }
        u = f.get("upload_url")
        if isinstance(u, str):
            entry["upload_url_preview"] = safe_presigned_url_preview(u)
            entry["upload_url_chars"] = len(u)
        slim.append(entry)
    return {"process_id": body.get("process_id"), "files_count": len(slim), "files": slim}
