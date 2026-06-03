import logging
logger = logging.getLogger()

from utils.nfse_detection import NFSE_SERIE_PROTHEUS, is_nfse_danfe_data

from .ocr_utils import are_similar_with_ocr_tolerance

def normalize_value(value):
    if not value:
        return ""
    return str(value).strip().lstrip('0') or '0'


def _normalize_serie_compare(value, *, nfse_context: bool = False) -> str:
    s = str(value or "").strip().upper()
    if nfse_context and s in ("", NFSE_SERIE_PROTHEUS, "NFSE", "NFS-E"):
        return NFSE_SERIE_PROTHEUS
    if s == NFSE_SERIE_PROTHEUS:
        return NFSE_SERIE_PROTHEUS
    return normalize_value(value)


def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_serie.py] Starting validation with {len(ocr_docs)} docs")
    nfse_doc = is_nfse_danfe_data(danfe_data)
    danfe_serie = danfe_data.get('serie', '') or (NFSE_SERIE_PROTHEUS if nfse_doc else '')
    normalized_danfe = _normalize_serie_compare(danfe_serie, nfse_context=nfse_doc)
    
    comparisons = []
    all_match = True
    corrections = []
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        has_metadata = doc.get('_has_metadata', False)
        
        # USAR APENAS DADOS DO JSON DO PEDIDO DE COMPRA (NÃO usar OCR)
        doc_serie = None
        
        if has_metadata:
            # Buscar série nos metadados do JSON do pedido de compra
            doc_serie = doc.get('serie') or doc.get('serieDocumento')
        
        normalized_doc = _normalize_serie_compare(doc_serie, nfse_context=nfse_doc) if doc_serie else ''
        corrected_value = None
        
        if not doc_serie:
            if nfse_doc:
                status = 'MATCH'
                logger.info(
                    f"[validar_serie] Doc {doc_file} - NFS-e: série NFS inferida do documento "
                    f"(pedido sem serie/serieDocumento)"
                )
            else:
                status = 'MISMATCH'
                all_match = False
                logger.warning(f"[validar_serie] Doc {doc_file} - Série não encontrada no JSON do pedido de compra")
        elif normalized_danfe == normalized_doc:
            status = 'MATCH'
        else:
            status = 'MISMATCH'
            all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': doc_serie or 'NÃO ENCONTRADO NO JSON DO PEDIDO DE COMPRA',
            'status': status
        })
    
    return {
        'rule': 'validar_serie',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_serie,
        'message': 'Série validada em todos os pedidos de compra' if all_match else 'Divergência na série',
        'comparisons': comparisons,
        'corrections': corrections
    }
