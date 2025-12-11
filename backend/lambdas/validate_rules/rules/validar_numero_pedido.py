import logging

logger = logging.getLogger()

def validate(danfe_data, ocr_docs):
    logger.info(f"[validar_numero_pedido.py] Starting validation with {len(ocr_docs)} docs")
    
    info_adicional = danfe_data.get('info_adicional', '') or ''
    
    comparisons = []
    all_match = True
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        doc_pedido = doc.get('numeroPedido') or doc.get('numero_pedido', '')
        
        if doc_pedido and str(doc_pedido) in info_adicional:
            status = 'MATCH'
        elif not doc_pedido:
            status = 'MATCH'
        else:
            status = 'MISMATCH'
            all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': doc_pedido or 'NÃO ENCONTRADO',
            'status': status
        })
    
    return {
        'rule': 'validar_numero_pedido',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': info_adicional if info_adicional else 'NÃO ENCONTRADO',
        'message': 'Número do pedido validado' if all_match else 'Divergência no número do pedido',
        'comparisons': comparisons
    }
