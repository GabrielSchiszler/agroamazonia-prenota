import logging
logger = logging.getLogger()

from .utils import compare_with_bedrock

def normalize_cnpj(cnpj):
    if not cnpj:
        return ""
    return ''.join(filter(str.isdigit, str(cnpj)))

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_cnpj_fornecedor.py] Starting validation with {len(ocr_docs)} docs")
    danfe_cnpj = danfe_data.get('emitente', {}).get('cnpj', '')
    normalized_danfe = normalize_cnpj(danfe_cnpj)
    
    comparisons = []
    all_match = True
    corrections = []
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        doc_cnpj = doc.get('cnpjRemetente') or doc.get('emitente', {}).get('cnpj', '')
        normalized_doc = normalize_cnpj(doc_cnpj)
        corrected_value = None
        
        if normalized_danfe == normalized_doc:
            status = 'MATCH'
        else:
            # Usar Bedrock para validar CNPJs próximos (diferença de 1 dígito)
            bedrock_result = compare_with_bedrock(normalized_danfe, normalized_doc, 'CNPJ do fornecedor')
            status = bedrock_result
            if status == 'MATCH':
                # Corrigir valor OCR para o valor correto do DANFE
                corrected_value = danfe_cnpj
                corrections.append({
                    'file_name': doc_file,
                    'field': 'cnpjRemetente',
                    'old_value': doc_cnpj,
                    'new_value': danfe_cnpj
                })
            else:
                all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': corrected_value or doc_cnpj,
            'status': status
        })
    
    return {
        'rule': 'validar_cnpj_fornecedor',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_cnpj,
        'message': 'CNPJ do fornecedor validado' if all_match else 'Divergência no CNPJ do fornecedor',
        'comparisons': comparisons,
        'corrections': corrections
    }
