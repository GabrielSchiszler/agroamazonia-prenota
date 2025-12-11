import logging
logger = logging.getLogger()

from .utils import compare_with_bedrock

def normalize_date(date_str):
    """Extrai YYYY-MM-DD de qualquer formato de data"""
    if not date_str:
        return ''
    s = str(date_str)
    # Pega só os primeiros 10 caracteres se tiver timestamp: 2025-10-17T17:27:00-03:00 -> 2025-10-17
    if 'T' in s:
        return s.split('T')[0]
    # Se já está no formato YYYY-MM-DD
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    return s

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_data_emissao.py] Starting validation with {len(ocr_docs)} docs")
    danfe_data_emissao = danfe_data.get('data_emissao', '')
    danfe_date_normalized = normalize_date(danfe_data_emissao)
    logger.info(f"DANFE date raw: '{danfe_data_emissao}' -> normalized: '{danfe_date_normalized}'")
    
    comparisons = []
    all_match = True
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        doc_data = doc.get('dataEmissao') or doc.get('data_emissao', '')
        doc_date_normalized = normalize_date(doc_data)
        logger.info(f"DOC date raw: '{doc_data}' -> normalized: '{doc_date_normalized}'")
        logger.info(f"Comparing: '{danfe_date_normalized}' == '{doc_date_normalized}' -> {danfe_date_normalized == doc_date_normalized}")
        
        if danfe_date_normalized == doc_date_normalized:
            status = 'MATCH'
        else:
            status = 'MISMATCH'
            all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': doc_data,
            'status': status
        })
    
    return {
        'rule': 'validar_data_emissao',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_data_emissao,
        'message': 'Data de emissão validada' if all_match else 'Divergência na data de emissão',
        'comparisons': comparisons
    }
