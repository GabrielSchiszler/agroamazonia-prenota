import logging
logger = logging.getLogger()

from .utils import compare_with_bedrock

def normalize_numero(val):
    if not val:
        return ""
    return str(val).strip().lstrip('0') or '0'

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_numero_nota.py] Starting validation with {len(ocr_docs)} docs")
    danfe_numero = danfe_data.get('numero_nota', '')
    normalized_danfe = normalize_numero(danfe_numero)
    
    comparisons = []
    all_match = True
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        # Novo campo: documento
        doc_numero = doc.get('documento') or doc.get('numero_nota', '')
        normalized_doc = normalize_numero(doc_numero)
        
        if normalized_danfe == normalized_doc:
            status = 'MATCH'
        else:
            bedrock_result = compare_with_bedrock(normalized_danfe, normalized_doc, 'número da nota')
            status = bedrock_result
            if status == 'MISMATCH':
                all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': doc_numero,
            'status': status
        })
    
    return {
        'rule': 'validar_numero_nota',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_numero,
        'message': 'Número da nota validado em todos os documentos' if all_match else 'Divergência no número da nota',
        'comparisons': comparisons
    }
