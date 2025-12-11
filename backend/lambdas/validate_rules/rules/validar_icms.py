import logging
logger = logging.getLogger()

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_icms.py] Starting validation with {len(ocr_docs)} docs")
    emitente_uf = danfe_data.get('emitente', {}).get('uf', '')
    destinatario_uf = danfe_data.get('destinatario', {}).get('uf', '')
    is_internal = emitente_uf == destinatario_uf
    
    danfe_icms = danfe_data.get('totais', {}).get('valor_icms', danfe_data.get('totais', {}).get('icms', '0'))
    
    comparisons = []
    all_match = True
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        
        # Verificar se dados de impostos foram extraídos
        impostos = doc.get('impostos', {})
        totais = doc.get('totais', {})
        
        # Se não tem estrutura de impostos, considerar como não extraído
        if not impostos and not totais:
            comparisons.append({
                'doc_file': doc_file,
                'doc_value': 'NÃO EXTRAÍDO',
                'status': 'MISMATCH'
            })
            all_match = False
            continue
        
        doc_icms = totais.get('valor_icms', totais.get('icms'))
        
        # Se ICMS não foi extraído (None ou vazio), falhar
        if doc_icms is None or doc_icms == '':
            comparisons.append({
                'doc_file': doc_file,
                'doc_value': 'NÃO EXTRAÍDO',
                'status': 'MISMATCH'
            })
            all_match = False
            continue
        
        # Converter para float para comparação
        try:
            danfe_icms_float = float(str(danfe_icms).replace(',', '.'))
            doc_icms_float = float(str(doc_icms).replace(',', '.'))
        except:
            comparisons.append({
                'doc_file': doc_file,
                'doc_value': 'INVÁLIDO',
                'status': 'MISMATCH'
            })
            all_match = False
            continue
        
        # Operação interna: ICMS deve ser zero
        if is_internal:
            if danfe_icms_float == 0 and doc_icms_float == 0:
                status = 'MATCH'
            else:
                status = 'MISMATCH'
                all_match = False
        else:
            # Operação interestadual: validar se valores conferem
            if abs(danfe_icms_float - doc_icms_float) < 0.01:
                status = 'MATCH'
            else:
                status = 'MISMATCH'
                all_match = False
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': str(doc_icms),
            'status': status
        })
    
    operation_type = 'interna' if is_internal else 'interestadual'
    return {
        'rule': 'validar_icms',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': f'{danfe_icms} (operação {operation_type})',
        'message': f'ICMS validado para operação {operation_type}' if all_match else f'Divergência no ICMS (operação {operation_type})',
        'comparisons': comparisons
    }
