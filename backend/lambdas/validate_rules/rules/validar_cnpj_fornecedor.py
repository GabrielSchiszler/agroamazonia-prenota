import logging
logger = logging.getLogger()

from .utils import compare_with_bedrock

def normalize_cnpj(cnpj):
    if not cnpj:
        return ""
    return ''.join(filter(str.isdigit, str(cnpj)))

def get_cnpj_root(cnpj):
    """Extrai os 8 primeiros dígitos do CNPJ (raiz/matriz) para comparação"""
    normalized = normalize_cnpj(cnpj)
    if len(normalized) >= 8:
        return normalized[:8]
    return normalized

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_cnpj_fornecedor.py] Starting validation with {len(ocr_docs)} docs")
    danfe_cnpj = danfe_data.get('emitente', {}).get('cnpj', '')
    normalized_danfe = normalize_cnpj(danfe_cnpj)
    danfe_root = get_cnpj_root(danfe_cnpj)  # Apenas 8 primeiros dígitos (raiz/matriz)
    logger.info(f"[validar_cnpj_fornecedor] DANFE CNPJ completo: {normalized_danfe}, Raiz (8 dígitos): {danfe_root}")
    
    comparisons = []
    all_match = True
    corrections = []
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        has_metadata = doc.get('_has_metadata', False)
        
        # PASSO 1: Tentar validar primeiro com dados do OCR
        doc_cnpj_ocr = doc.get('cnpjRemetente') or doc.get('emitente', {}).get('cnpj', '')
        normalized_doc_ocr = normalize_cnpj(doc_cnpj_ocr)
        corrected_value = None
        status = None
        source_used = None
        
        # Validar com OCR primeiro (se houver CNPJ no OCR)
        # Comparar apenas os 8 primeiros dígitos (raiz/matriz do CNPJ)
        if normalized_doc_ocr:
            doc_root_ocr = get_cnpj_root(doc_cnpj_ocr)
            logger.info(f"[validar_cnpj_fornecedor] Doc {doc_file} - CNPJ OCR completo: {normalized_doc_ocr}, Raiz (8 dígitos): {doc_root_ocr}")
            
            if danfe_root == doc_root_ocr:
                status = 'MATCH'
                source_used = 'OCR'
                logger.info(f"[validar_cnpj_fornecedor] Doc {doc_file} - MATCH via OCR (raiz): {doc_cnpj_ocr} (raiz: {doc_root_ocr})")
            else:
                # Não precisa mais usar Bedrock, pois estamos comparando apenas a raiz
                # Se as raízes são diferentes, não é match
                status = None  # Ainda não validado, tentar metadados se disponíveis
                source_used = None
                logger.info(f"[validar_cnpj_fornecedor] Doc {doc_file} - Raiz diferente (DANFE: {danfe_root}, DOC: {doc_root_ocr}), tentando metadados")
        else:
            # OCR não tem CNPJ, tentar metadados se disponíveis
            status = None
            source_used = None
        
        # PASSO 2: Se OCR não deu match (ou não tem CNPJ) e há metadados, tentar validar com metadados JSON
        if status != 'MATCH' and has_metadata:
            # Buscar CNPJ nos metadados (prioridade: cnpjRemetente > cnpjFornecedor > fornecedor.cnpj > cnpj)
            # cnpjRemetente é o campo mais comum nos metadados JSON fornecidos (ex: {"cnpjRemetente": "03869628000116"})
            doc_cnpj_metadata = doc.get('cnpjRemetente')
            metadata_field_used = 'cnpjRemetente' if doc_cnpj_metadata else None
            
            if not doc_cnpj_metadata:
                doc_cnpj_metadata = doc.get('cnpjFornecedor')
                metadata_field_used = 'cnpjFornecedor' if doc_cnpj_metadata else metadata_field_used
            
            if not doc_cnpj_metadata:
                fornecedor_obj = doc.get('fornecedor')
                if isinstance(fornecedor_obj, dict):
                    doc_cnpj_metadata = fornecedor_obj.get('cnpj')
                    metadata_field_used = 'fornecedor.cnpj' if doc_cnpj_metadata else metadata_field_used
            
            if not doc_cnpj_metadata:
                doc_cnpj_metadata = doc.get('cnpj')
                metadata_field_used = 'cnpj' if doc_cnpj_metadata else metadata_field_used
            
            normalized_doc_metadata = normalize_cnpj(doc_cnpj_metadata) if doc_cnpj_metadata else ''
            
            if normalized_doc_metadata:
                doc_root_metadata = get_cnpj_root(doc_cnpj_metadata)
                logger.info(f"[validar_cnpj_fornecedor] Doc {doc_file} - CNPJ encontrado nos metadados (campo: {metadata_field_used}): {doc_cnpj_metadata} (raiz: {doc_root_metadata})")
                
                # Comparar apenas os 8 primeiros dígitos (raiz/matriz do CNPJ)
                if danfe_root == doc_root_metadata:
                    status = 'MATCH'
                    source_used = 'METADADOS JSON'
                    logger.info(f"[validar_cnpj_fornecedor] Doc {doc_file} - ✅ MATCH via METADADOS (raiz): {doc_cnpj_metadata} (raiz: {doc_root_metadata})")
                else:
                    # Raízes diferentes, não é match
                    logger.warning(f"[validar_cnpj_fornecedor] Doc {doc_file} - ❌ Raiz diferente (DANFE: {danfe_root}, METADADOS: {doc_root_metadata})")
            else:
                logger.warning(f"[validar_cnpj_fornecedor] Doc {doc_file} - Nenhum CNPJ encontrado nos metadados (campos verificados: cnpjRemetente, cnpjFornecedor, fornecedor.cnpj, cnpj)")
        
        # Se ainda não validou, considerar como falha
        if status != 'MATCH':
            status = 'MISMATCH'
            all_match = False
            source_used = source_used or 'OCR' if normalized_doc_ocr else ('METADADOS' if has_metadata else 'NENHUM')
            logger.warning(f"[validar_cnpj_fornecedor] Doc {doc_file} - MISMATCH (tentou: {source_used})")
        
        # Determinar valor a exibir (prioridade: corrigido > metadados > OCR)
        display_value = corrected_value
        if not display_value and has_metadata:
            # Priorizar cnpjRemetente dos metadados (campo mais comum)
            display_value = doc.get('cnpjRemetente')
            if not display_value:
                display_value = doc.get('cnpjFornecedor')
            if not display_value:
                fornecedor_obj = doc.get('fornecedor')
                if isinstance(fornecedor_obj, dict):
                    display_value = fornecedor_obj.get('cnpj')
            if not display_value:
                display_value = doc.get('cnpj')
        if not display_value:
            display_value = doc_cnpj_ocr or 'NÃO ENCONTRADO'
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': display_value,
            'status': status,
            'source': source_used  # Adicionar informação sobre a fonte usada
        })
    
    return {
        'rule': 'validar_cnpj_fornecedor',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': f"{danfe_cnpj} (raiz: {danfe_root})",
        'message': 'CNPJ do fornecedor validado (raiz/matriz)' if all_match else 'Divergência na raiz do CNPJ do fornecedor (8 primeiros dígitos)',
        'comparisons': comparisons,
        'corrections': corrections
    }
