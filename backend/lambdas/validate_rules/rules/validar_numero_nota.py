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
        has_metadata = doc.get('_has_metadata', False)
        
        # USAR APENAS DADOS DO JSON DO PEDIDO DE COMPRA (NÃO usar OCR)
        doc_numero = None
        
        if has_metadata:
            # Buscar número da nota nos metadados do JSON do pedido de compra
            doc_numero = doc.get('documento') or doc.get('numero_nota') or doc.get('numeroNota')
        
        normalized_doc = normalize_numero(doc_numero) if doc_numero else ''
        
        if not doc_numero:
            status = 'MISMATCH'
            all_match = False
            logger.warning(f"[validar_numero_nota] Doc {doc_file} - Número da nota não encontrado no JSON do pedido de compra")
        elif normalized_danfe == normalized_doc:
            status = 'MATCH'
        else:
            bedrock_result = compare_with_bedrock(normalized_danfe, normalized_doc, 'número da nota')
            status = bedrock_result
            if status == 'MISMATCH':
                all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': doc_numero or 'NÃO ENCONTRADO NO JSON DO PEDIDO DE COMPRA',
            'status': status
        })
    
    return {
        'rule': 'validar_numero_nota',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_numero,
        'message': 'Número da nota validado em todos os documentos' if all_match else 'Divergência no número da nota',
        'comparisons': comparisons
    }
