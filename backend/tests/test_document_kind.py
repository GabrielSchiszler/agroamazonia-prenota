"""
Tests for backend/lambdas/utils/document_kind.py

Validates the file-classification logic that decides how each attachment
is processed (NF-e XML → parse_xml, PDF/image → Textract, DOCX → rejected).
"""

import pytest
from utils.document_kind import (
    DocumentKind,
    classify_by_name_and_mime,
    is_nfe_xml,
    classify_xml_content,
)


# =====================================================================
# classify_by_name_and_mime
# =====================================================================

class TestClassifyByNameAndMime:
    """Extension + MIME-based classification (no file content)."""

    @pytest.mark.parametrize("file_name, expected", [
        ("nota.xml", DocumentKind.OTHER_XML),
        ("NOTA.XML", DocumentKind.OTHER_XML),
        ("fatura.pdf", DocumentKind.PDF),
        ("contrato.PDF", DocumentKind.PDF),
        ("foto.png", DocumentKind.IMAGE),
        ("foto.jpg", DocumentKind.IMAGE),
        ("foto.jpeg", DocumentKind.IMAGE),
        ("scan.tiff", DocumentKind.IMAGE),
        ("scan.tif", DocumentKind.IMAGE),
        ("relatorio.docx", DocumentKind.DOCX),
    ])
    def test_known_extensions(self, file_name, expected):
        assert classify_by_name_and_mime(file_name) == expected

    @pytest.mark.parametrize("file_name", [
        "arquivo.exe",
        "planilha.xlsx",
        "dados.csv",
        "readme.txt",
        "sem_extensao",
    ])
    def test_unknown_extension_no_mime_returns_rejected(self, file_name):
        assert classify_by_name_and_mime(file_name) == DocumentKind.REJECTED

    def test_unknown_extension_but_known_mime_uses_mime(self):
        result = classify_by_name_and_mime("file.unknown", content_type="application/pdf")
        assert result == DocumentKind.PDF

    def test_mime_with_charset_is_stripped(self):
        result = classify_by_name_and_mime("data.unknown", content_type="application/xml; charset=utf-8")
        assert result == DocumentKind.OTHER_XML

    def test_extension_takes_precedence_over_mime(self):
        result = classify_by_name_and_mime("nota.xml", content_type="application/pdf")
        assert result == DocumentKind.OTHER_XML

    def test_case_insensitive_extension(self):
        assert classify_by_name_and_mime("FOTO.JPEG") == DocumentKind.IMAGE
        assert classify_by_name_and_mime("Doc.Pdf") == DocumentKind.PDF


# =====================================================================
# is_nfe_xml
# =====================================================================

class TestIsNfeXml:
    """Content-level NF-e detection by namespace/root tag."""

    def test_valid_nfe_proc(self, nfe_xml_bytes):
        assert is_nfe_xml(nfe_xml_bytes) is True

    def test_minimal_nfe_root(self):
        xml = b'<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe"><NFe/></nfeProc>'
        assert is_nfe_xml(xml) is True

    def test_nfe_as_child_element(self):
        xml = (
            b'<root xmlns:nfe="http://www.portalfiscal.inf.br/nfe">'
            b'<nfe:NFe/></root>'
        )
        assert is_nfe_xml(xml) is True

    def test_generic_xml_is_not_nfe(self, generic_xml_bytes):
        assert is_nfe_xml(generic_xml_bytes) is False

    def test_empty_root_is_not_nfe(self):
        assert is_nfe_xml(b"<root/>") is False

    def test_invalid_xml_returns_false(self):
        assert is_nfe_xml(b"not xml at all") is False

    def test_partial_xml_returns_false(self):
        assert is_nfe_xml(b"<nfeProc>") is False


# =====================================================================
# classify_xml_content
# =====================================================================

class TestClassifyXmlContent:
    """Promotes OTHER_XML → NFE_XML based on content inspection."""

    def test_nfe_content_returns_nfe_xml(self, nfe_xml_bytes):
        assert classify_xml_content(nfe_xml_bytes) == DocumentKind.NFE_XML

    def test_generic_content_returns_other_xml(self, generic_xml_bytes):
        assert classify_xml_content(generic_xml_bytes) == DocumentKind.OTHER_XML

    def test_broken_xml_returns_other_xml(self):
        assert classify_xml_content(b"broken") == DocumentKind.OTHER_XML
