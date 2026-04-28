"""Inferência de DOC_TYPE/pasta S3 no batch (sem doc_type no cliente)."""

from src.models.api import infer_doc_type_and_folder


def test_infer_xml_by_mime():
    dt, folder = infer_doc_type_and_folder("nota.xml", "application/xml")
    assert dt == "DANFE"
    assert folder == "danfe"


def test_infer_xml_by_extension_when_mime_generic():
    dt, folder = infer_doc_type_and_folder("nota.xml", "application/octet-stream")
    assert dt == "DANFE"
    assert folder == "danfe"


def test_infer_pdf_additional_docs():
    dt, folder = infer_doc_type_and_folder("boleto.pdf", "application/pdf")
    assert dt == "ADDITIONAL"
    assert folder == "docs"


def test_infer_image():
    dt, folder = infer_doc_type_and_folder("scan.png", "image/png")
    assert dt == "ADDITIONAL"
    assert folder == "docs"
