import logging
logger = logging.getLogger()

from .ocr_utils import are_similar_with_ocr_tolerance

def normalize_value(value):
    if not value:
        return ""
    return str(value).strip().lstrip('0') or '0'

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_serie.py] Starting validation with {len(ocr_docs)} docs")
    danfe_serie = danfe_data.get('serie', '')
    normalized_danfe = normalize_value(danfe_serie)
    
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
        
        normalized_doc = normalize_value(doc_serie) if doc_serie else ''
        corrected_value = None
        
        if not doc_serie:
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
        'message': 'Série validada em todos os documentos' if all_match else 'Divergência na série',
        'comparisons': comparisons,
        'corrections': corrections
    }
