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
    logger.info(f"[validar_cnpj_destinatario.py] Starting validation with {len(ocr_docs)} docs")
    danfe_cnpj = danfe_data.get('destinatario', {}).get('cnpj', '')
    normalized_danfe = normalize_cnpj(danfe_cnpj)
    danfe_root = get_cnpj_root(danfe_cnpj)  # Apenas 8 primeiros dígitos (raiz/matriz)
    logger.info(f"[validar_cnpj_destinatario] DANFE CNPJ completo: {normalized_danfe}, Raiz (8 dígitos): {danfe_root}")
    
    comparisons = []
    all_match = True
    corrections = []
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        has_metadata = doc.get('_has_metadata', False)
        
        # USAR APENAS DADOS DO JSON DO PEDIDO DE COMPRA (NÃO usar OCR)
        status = None
        source_used = None
        
        # Buscar CNPJ APENAS nos metadados do JSON do pedido de compra
        if has_metadata:
            logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - Buscando CNPJ nos metadados...")
            logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - Keys disponíveis no doc: {list(doc.keys())[:20]}...")  # Mostrar primeiras 20 keys
            
            # Verificar se metadados estão no formato do pedido de compra (com header e requestBody)
            request_body = None
            if 'requestBody' in doc:
                request_body = doc.get('requestBody')
                logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - Metadados no formato pedido de compra (tem requestBody)")
            elif isinstance(doc.get('_metadata'), dict) and 'requestBody' in doc.get('_metadata', {}):
                request_body = doc.get('_metadata', {}).get('requestBody')
                logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - Metadados no formato pedido de compra (tem _metadata.requestBody)")
            
            # Buscar CNPJ nos metadados (prioridade: requestBody.cnpjDestinatario > cnpjDestinatario > destinatario.cnpj)
            doc_cnpj_metadata = None
            metadata_field_used = None
            
            # PRIORIDADE 1: requestBody.cnpjDestinatario (formato do pedido de compra)
            if request_body and isinstance(request_body, dict):
                doc_cnpj_metadata = request_body.get('cnpjDestinatario')
                if doc_cnpj_metadata:
                    metadata_field_used = 'requestBody.cnpjDestinatario'
                    logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - CNPJ encontrado em requestBody.cnpjDestinatario: {doc_cnpj_metadata}")
            
            # PRIORIDADE 2: Campos diretos no doc (formato antigo)
            if not doc_cnpj_metadata:
                doc_cnpj_metadata = doc.get('cnpjDestinatario')
                metadata_field_used = 'cnpjDestinatario' if doc_cnpj_metadata else None
                if doc_cnpj_metadata:
                    logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - CNPJ encontrado em cnpjDestinatario: {doc_cnpj_metadata}")
            
            if not doc_cnpj_metadata:
                destinatario_obj = doc.get('destinatario')
                if isinstance(destinatario_obj, dict):
                    doc_cnpj_metadata = destinatario_obj.get('cnpj')
                    metadata_field_used = 'destinatario.cnpj' if doc_cnpj_metadata else metadata_field_used
                    if doc_cnpj_metadata:
                        logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - CNPJ encontrado em destinatario.cnpj: {doc_cnpj_metadata}")
            
            normalized_doc_metadata = normalize_cnpj(doc_cnpj_metadata) if doc_cnpj_metadata else ''
            
            if normalized_doc_metadata:
                doc_root_metadata = get_cnpj_root(doc_cnpj_metadata)
                logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - CNPJ encontrado nos metadados (campo: {metadata_field_used}): {doc_cnpj_metadata} (raiz: {doc_root_metadata})")
                
                # Comparar apenas os 8 primeiros dígitos (raiz/matriz do CNPJ)
                if danfe_root == doc_root_metadata:
                    status = 'MATCH'
                    source_used = 'METADADOS JSON'
                    logger.info(f"[validar_cnpj_destinatario] Doc {doc_file} - ✅ MATCH via METADADOS (raiz): {doc_cnpj_metadata} (raiz: {doc_root_metadata})")
                else:
                    # Raízes diferentes, não é match
                    logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - ❌ Raiz diferente (DANFE: {danfe_root}, METADADOS: {doc_root_metadata})")
            else:
                logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - Nenhum CNPJ encontrado nos metadados")
                logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - Campos verificados: requestBody.cnpjDestinatario, cnpjDestinatario, destinatario.cnpj")
                logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - requestBody disponível: {bool(request_body)}")
                if request_body:
                    logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - requestBody keys: {list(request_body.keys())}")
                    logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - requestBody.cnpjDestinatario: {request_body.get('cnpjDestinatario')}")
                logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - doc.cnpjDestinatario: {doc.get('cnpjDestinatario')}")
                logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - doc.keys() sample: {list(doc.keys())[:30]}")
        
        # Se ainda não validou, considerar como falha
        if status != 'MATCH':
            status = 'MISMATCH'
            all_match = False
            source_used = source_used or ('METADADOS JSON' if has_metadata else 'NENHUM')
            logger.warning(f"[validar_cnpj_destinatario] Doc {doc_file} - MISMATCH (tentou: {source_used})")
        
        # Determinar valor a exibir (APENAS dos metadados do JSON do pedido de compra)
        display_value = None
        if has_metadata:
            # Buscar no requestBody primeiro (formato do pedido de compra)
            request_body = doc.get('requestBody')
            if request_body and isinstance(request_body, dict):
                display_value = request_body.get('cnpjDestinatario')
            # Fallback para campos diretos
            if not display_value:
                display_value = doc.get('cnpjDestinatario')
            if not display_value:
                destinatario_obj = doc.get('destinatario')
                if isinstance(destinatario_obj, dict):
                    display_value = destinatario_obj.get('cnpj')
            if not display_value:
                display_value = doc.get('cnpj')
        if not display_value:
            display_value = 'NÃO ENCONTRADO NO JSON DO PEDIDO DE COMPRA'
        
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': display_value,
            'status': status,
            'source': source_used  # Adicionar informação sobre a fonte usada
        })
    
    return {
        'rule': 'validar_cnpj_destinatario',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': f"{danfe_cnpj} (raiz: {danfe_root})",
        'message': 'CNPJ do destinatário validado (raiz/matriz)' if all_match else 'Divergência na raiz do CNPJ do destinatário (8 primeiros dígitos)',
        'comparisons': comparisons,
        'corrections': corrections
    }

