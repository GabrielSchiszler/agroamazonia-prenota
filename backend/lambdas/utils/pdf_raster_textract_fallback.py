"""
Rasteriza páginas de PDF para PNG e chama Textract DetectDocumentText (bytes) por página.

Usado quando AnalyzeDocument e DetectDocumentText falham com UnsupportedDocumentException
no PDF original (ex.: JPEG2000/JBIG2 internos, PDF frágil de NFSe).
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def textract_line_blocks_from_pdf_via_raster(
    textract_client: Any,
    pdf_bytes: bytes,
    key_log: str,
    *,
    max_sync_bytes: int,
    log: logging.Logger | None = None,
) -> list[dict]:
    """
    Devolve lista de blocos sintéticos só LINE, compatível com _extract_text_and_tables.
    """
    lg = log or logger
    try:
        import fitz  # PyMuPDF
    except ImportError as e:
        raise RuntimeError(
            "PyMuPDF (pymupdf) não está instalado; inclua extract_documents/requirements.txt no bundle."
        ) from e

    dpi = float(os.environ.get("TEXTRACT_RASTER_DPI", "200"))
    max_pages = int(os.environ.get("TEXTRACT_RASTER_MAX_PAGES", "15"))

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    synthetic: list[dict] = []
    line_idx = 0
    try:
        total = len(doc)
        n = min(total, max_pages)
        if total > max_pages:
            lg.warning(
                "[raster_textract] PDF %s: %d páginas; processando só as primeiras %d (TEXTRACT_RASTER_MAX_PAGES)",
                key_log,
                total,
                max_pages,
            )
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        for page_no in range(n):
            page = doc.load_page(page_no)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            png = pix.tobytes("png")
            if len(png) > max_sync_bytes:
                raise ValueError(
                    f"Página {page_no + 1} rasterizada ({len(png)} bytes) excede limite síncrono Textract ({max_sync_bytes})"
                )
            lg.info(
                "[raster_textract] key=%s page=%d/%d png_bytes=%d dpi=%s",
                key_log,
                page_no + 1,
                n,
                len(png),
                dpi,
            )
            resp = textract_client.detect_document_text(Document={"Bytes": png})
            for b in resp.get("Blocks") or []:
                if b.get("BlockType") != "LINE":
                    continue
                txt = (b.get("Text") or "").strip()
                if not txt:
                    continue
                synthetic.append(
                    {
                        "Id": f"r{page_no}-L{line_idx}",
                        "BlockType": "LINE",
                        "Text": txt,
                    }
                )
                line_idx += 1
    finally:
        doc.close()

    if not synthetic:
        raise RuntimeError(
            f"Raster fallback: Textract não devolveu linhas de texto nas imagens (key={key_log})"
        )
    return synthetic
