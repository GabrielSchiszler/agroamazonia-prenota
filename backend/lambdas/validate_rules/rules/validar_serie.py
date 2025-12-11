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
        doc_serie = doc.get('serie', '')
        normalized_doc = normalize_value(doc_serie)
        corrected_value = None
        
        # Comparar com tolerância a erros de OCR (1 vs I, 0 vs O, etc)
        if normalized_danfe == normalized_doc:
            status = 'MATCH'
        elif are_similar_with_ocr_tolerance(normalized_danfe, normalized_doc):
            status = 'MATCH'
            # Corrigir valor OCR para o valor correto do DANFE
            corrected_value = danfe_serie
            corrections.append({
                'file_name': doc_file,
                'field': 'serie',
                'old_value': doc_serie,
                'new_value': danfe_serie
            })
        else:
            status = 'MISMATCH'
            all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': corrected_value or doc_serie,
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
