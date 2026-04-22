"""
Classifica arquivos por extensão + Content-Type declarado.

Categorias:
  NFE_XML   – XML de NF-e (namespace portalfiscal)
  OTHER_XML – XML genérico (não NF-e)
  PDF       – Textract-ready
  DOCX      – Requer conversão ou extração alternativa (risco MVP)
  IMAGE     – PNG/JPEG/TIFF — Textract-ready
  REJECTED  – extensão/MIME não suportada
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from enum import Enum
from typing import Optional


class DocumentKind(str, Enum):
    NFE_XML = "NFE_XML"
    OTHER_XML = "OTHER_XML"
    PDF = "PDF"
    DOCX = "DOCX"
    IMAGE = "IMAGE"
    REJECTED = "REJECTED"


_NFE_NS = "http://www.portalfiscal.inf.br/nfe"
_NFE_ROOT_TAGS = {"nfeProc", "NFe", "enviNFe"}

_EXT_MAP: dict[str, DocumentKind] = {
    ".xml": DocumentKind.OTHER_XML,
    ".pdf": DocumentKind.PDF,
    ".docx": DocumentKind.DOCX,
    ".png": DocumentKind.IMAGE,
    ".jpg": DocumentKind.IMAGE,
    ".jpeg": DocumentKind.IMAGE,
    ".tiff": DocumentKind.IMAGE,
    ".tif": DocumentKind.IMAGE,
}

_MIME_MAP: dict[str, DocumentKind] = {
    "application/xml": DocumentKind.OTHER_XML,
    "text/xml": DocumentKind.OTHER_XML,
    "application/pdf": DocumentKind.PDF,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DocumentKind.DOCX,
    "image/png": DocumentKind.IMAGE,
    "image/jpeg": DocumentKind.IMAGE,
    "image/tiff": DocumentKind.IMAGE,
}


def classify_by_name_and_mime(
    file_name: str,
    content_type: Optional[str] = None,
) -> DocumentKind:
    """Classify based on extension and MIME type (no content inspection)."""
    ext = ""
    dot = file_name.rfind(".")
    if dot != -1:
        ext = file_name[dot:].lower()

    kind = _EXT_MAP.get(ext)
    if kind is None and content_type:
        kind = _MIME_MAP.get(content_type.lower().split(";")[0].strip())
    return kind if kind is not None else DocumentKind.REJECTED


def is_nfe_xml(xml_bytes: bytes) -> bool:
    """Check if XML content is a Brazilian NF-e by namespace/root tag.

    Lightweight: parses only the root element (no full tree).
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return False

    tag = root.tag
    ns = ""
    local = tag
    if tag.startswith("{"):
        ns, local = tag[1:].split("}", 1)

    if ns == _NFE_NS and local in _NFE_ROOT_TAGS:
        return True

    for child in root:
        ctag = child.tag
        if ctag.startswith("{"):
            cns, clocal = ctag[1:].split("}", 1)
            if cns == _NFE_NS and clocal in _NFE_ROOT_TAGS:
                return True

    return False


def classify_xml_content(xml_bytes: bytes) -> DocumentKind:
    """Promote OTHER_XML → NFE_XML if the content is a valid NF-e."""
    return DocumentKind.NFE_XML if is_nfe_xml(xml_bytes) else DocumentKind.OTHER_XML
