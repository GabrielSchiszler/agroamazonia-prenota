import json

def normalize_number(val):
    """Normaliza número removendo formatação e convertendo para float"""
    try:
        # Se já é numérico, retornar diretamente
        if isinstance(val, (int, float)):
            return round(float(val), 2)
        
        s = str(val).strip()
        # Se tem espaço, pega só a parte antes do espaço
        if ' ' in s:
            s = s.split(' ')[0]
        # Formato brasileiro: 3.200,00 -> remove ponto, troca vírgula por ponto
        # Formato americano: 3,200.00 -> remove vírgula
        if ',' in s and '.' in s:
            # Tem ambos - verifica qual vem depois
            if s.rindex(',') > s.rindex('.'):
                # Vírgula depois = formato BR: 3.200,00
                s = s.replace('.', '').replace(',', '.')
            else:
                # Ponto depois = formato US: 3,200.00
                s = s.replace(',', '')
        elif ',' in s:
            # Só vírgula = decimal BR: 3200,00
            s = s.replace(',', '.')
        # Só ponto ou nada = já está ok
        return round(float(s), 2)
    except Exception as e:
        import logging
        logging.getLogger().warning(f"Erro ao normalizar número '{val}': {e}")
        return 0

def normalize_codigo(codigo):
    """Normaliza código removendo espaços, caracteres especiais e zeros à esquerda"""
    import re
    if not codigo:
        return ''
    
    # Remove TODOS os espaços e caracteres especiais (pontos, hífens, etc)
    normalized = re.sub(r'[^A-Z0-9]', '', str(codigo).upper().strip())
    
    # SEMPRE remove zeros à esquerda, mesmo que tenha letras depois
    # Exemplos:
    # "000000010000013136" -> "10000013136"
    # "000123ABC" -> "123ABC" (remove zeros à esquerda)
    # "ABC000123" -> "ABC000123" (não remove zeros no meio)
    # "000" -> "0" (mantém pelo menos um zero)
    
    if normalized:
        # Remove zeros à esquerda de toda a string
        normalized = normalized.lstrip('0') or '0'  # Mantém pelo menos um '0' se for tudo zero
    
    return normalized

def codes_are_similar(code1, code2, max_diff=1):
    """Verifica se dois códigos são similares considerando erros comuns de OCR"""
    if code1 == code2:
        return True
    
    # Erros comuns de OCR: 1<->I, 0<->O, 5<->S, 8<->B, etc
    ocr_errors = {
        '1': 'I', 'I': '1',
        '0': 'O', 'O': '0',
        '5': 'S', 'S': '5',
        '8': 'B', 'B': '8',
        '2': 'Z', 'Z': '2',
        '6': 'G', 'G': '6',
        'A': '4', '4': 'A'
    }
    
    if len(code1) != len(code2):
        return False
    
    diff_count = 0
    for c1, c2 in zip(code1, code2):
        if c1 != c2:
            # Verifica se é um erro comum de OCR
            if ocr_errors.get(c1) == c2 or ocr_errors.get(c2) == c1:
                diff_count += 0.5  # Erro comum conta menos
            else:
                diff_count += 1
    
    return diff_count <= max_diff

def make_product_key(prod, is_danfe=True):
    """Cria chave única para produto - apenas codigo e quantidade"""
    if is_danfe:
        codigo = normalize_codigo(prod.get('codigo', ''))
        qtd = normalize_number(prod.get('quantidade', 0))
    else:
        codigo = normalize_codigo(prod.get('codigoProduto') or prod.get('codigo', ''))
        # Para documentos, quantidade pode estar em diferentes campos
        # Verificar todos os campos possíveis (em ordem de prioridade)
        qtd_raw = None
        for field in ['quantidade', 'quantidadeProduto', 'qtd', 'quantidadeItem', 'qCom', 'qTrib']:
            if field in prod and prod[field] is not None:
                qtd_raw = prod[field]
                break
        
        # Log para debug se quantidade for None ou 0
        if qtd_raw is None or (isinstance(qtd_raw, (int, float)) and qtd_raw == 0):
            import logging
            logger = logging.getLogger()
            logger.warning(f"[make_product_key] Quantidade não encontrada no produto. Campos disponíveis: {list(prod.keys())}")
            logger.warning(f"[make_product_key] Produto completo: {json.dumps(prod, default=str)[:500]}")
        
        qtd = normalize_number(qtd_raw or 0)
    
    return (codigo, qtd)

def find_matching_product(danfe_prod, doc_produtos, used_indices):
    """Encontra produto correspondente no documento usando apenas nome/descrição"""
    import logging
    logger = logging.getLogger()
    
    danfe_desc = danfe_prod.get('descricao', '').strip()
    danfe_nome = danfe_prod.get('nome', '').strip() or danfe_desc
    
    logger.info(f"[validar_produtos] Buscando match para produto DANFE:")
    logger.info(f"  Nome/Descrição: '{danfe_nome}' / '{danfe_desc}'")
    
    # PRIORIDADE 1: Match exato por descrição (case-insensitive)
    danfe_desc_upper = danfe_desc.upper()
    danfe_nome_upper = danfe_nome.upper()
    
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        
        doc_desc = (doc_prod.get('descricaoProduto') or doc_prod.get('descricao') or doc_prod.get('nome', '')).strip()
        doc_desc_upper = doc_desc.upper()
        
        # Match exato
        if danfe_desc_upper == doc_desc_upper or danfe_nome_upper == doc_desc_upper:
            logger.info(f"  ✓ MATCH EXATO por descrição no índice {i}")
            return i, doc_prod
        
        # Match parcial (uma contém a outra)
        if (danfe_desc_upper in doc_desc_upper or doc_desc_upper in danfe_desc_upper) and len(danfe_desc_upper) > 3:
            logger.info(f"  ✓ MATCH PARCIAL por descrição no índice {i} (DANFE: '{danfe_desc}', DOC: '{doc_desc}')")
            return i, doc_prod
    
    logger.warning(f"  ✗ Nenhum match encontrado para produto (descricao: '{danfe_desc}')")
    return None, None

def validate_products_comparison(danfe_produtos, doc_produtos, doc_file, source_type, doc_root=None):
    """Valida e compara produtos, retorna resultado da validação
    
    Args:
        danfe_produtos: Lista de produtos do DANFE
        doc_produtos: Lista de produtos do documento (OCR ou metadados)
        doc_file: Nome do arquivo
        source_type: "OCR" ou "METADADOS JSON"
        doc_root: Objeto doc completo (para buscar moeda no nível raiz se necessário)
    """
    import logging
    logger = logging.getLogger()
    
    items_detail = []
    used_indices = set()
    unmatched_danfe = []
    all_match = True
    
    # Tentar parear cada produto DANFE com DOC
    for danfe_idx, danfe_prod in enumerate(danfe_produtos):
        doc_idx, doc_prod = find_matching_product(danfe_prod, doc_produtos, used_indices)
        
        if doc_prod is not None:
            used_indices.add(doc_idx)
            # Match encontrado - validar apenas nome e descrição
            fields = {}
            
            # Nome
            danfe_nome = danfe_prod.get('nome', '').strip() or danfe_prod.get('descricao', '').strip()
            doc_nome = (doc_prod.get('nomeProduto') or doc_prod.get('nome') or doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).strip()
            
            if not danfe_nome:
                danfe_nome = danfe_prod.get('descricao', '').strip()
            
            nome_status = 'MATCH'
            if danfe_nome.upper() == doc_nome.upper():
                nome_status = 'MATCH'
            elif danfe_nome.upper() in doc_nome.upper() or doc_nome.upper() in danfe_nome.upper():
                nome_status = 'MATCH'
            else:
                from .utils import compare_with_bedrock
                nome_status = compare_with_bedrock(danfe_nome, doc_nome, 'nome do produto')
            
            fields['nome'] = {
                'danfe': danfe_nome,
                'doc': doc_nome,
                'status': nome_status
            }
            
            # Descrição
            danfe_desc = danfe_prod.get('descricao', '').strip()
            doc_desc = (doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).strip()
            
            desc_status = 'MATCH'
            if danfe_desc.upper() == doc_desc.upper():
                desc_status = 'MATCH'
            elif danfe_desc.upper() in doc_desc.upper() or doc_desc.upper() in danfe_desc.upper():
                desc_status = 'MATCH'
            else:
                from .utils import compare_with_bedrock
                desc_status = compare_with_bedrock(danfe_desc, doc_desc, 'descrição do produto')
            
            fields['descricao'] = {
                'danfe': danfe_desc,
                'doc': doc_desc,
                'status': desc_status
            }
            
            item_has_mismatch = any(f['status'] == 'MISMATCH' for f in fields.values())
            if item_has_mismatch:
                all_match = False
            
            items_detail.append({
                'item': danfe_idx + 1,
                'danfe_position': danfe_idx + 1,
                'doc_position': doc_idx + 1,
                'fields': fields,
                'status': 'MISMATCH' if item_has_mismatch else 'MATCH'
            })
        else:
            unmatched_danfe.append((danfe_idx, danfe_prod))
            all_match = False
    
    # Produtos do DANFE não pareados = MISMATCH
    for danfe_idx, danfe_prod in unmatched_danfe:
        danfe_nome = danfe_prod.get('nome', '').strip() or danfe_prod.get('descricao', '').strip()
        danfe_desc = danfe_prod.get('descricao', '').strip()
        
        items_detail.append({
            'item': danfe_idx + 1,
            'danfe_position': danfe_idx + 1,
            'doc_position': None,
            'fields': {
                'nome': {
                    'danfe': danfe_nome,
                    'doc': 'NÃO ENCONTRADO',
                    'status': 'MISMATCH'
                },
                'descricao': {
                    'danfe': danfe_desc,
                    'doc': 'NÃO ENCONTRADO',
                    'status': 'MISMATCH'
                }
            },
            'status': 'MISMATCH'
        })
    
    # Produtos do documento que não estão no DANFE = também é erro
    unmatched_doc = []
    for doc_idx, doc_prod in enumerate(doc_produtos):
        if doc_idx not in used_indices:
            unmatched_doc.append((doc_idx, doc_prod))
            all_match = False
            logger.warning(f"[validar_produtos] Produto no documento (índice {doc_idx}) não encontrado no DANFE: {doc_prod.get('codigoProduto') or doc_prod.get('codigo', 'N/A')}")
    
    # Adicionar produtos do documento não pareados como erros
    for doc_idx, doc_prod in unmatched_doc:
        doc_nome = (doc_prod.get('nomeProduto') or doc_prod.get('nome') or doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).strip()
        doc_desc = (doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).strip()
        
        items_detail.append({
            'item': len(danfe_produtos) + doc_idx + 1,  # Continuar numeração após produtos do DANFE
            'danfe_position': None,  # Não está no DANFE
            'doc_position': doc_idx + 1,
            'fields': {
                'nome': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_nome,
                    'status': 'MISMATCH'
                },
                'descricao': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_desc,
                    'status': 'MISMATCH'
                }
            },
            'status': 'MISMATCH'
        })
    
    # Ordenar por posição DANFE primeiro, depois por posição DOC
    items_detail.sort(key=lambda x: (x['danfe_position'] if x['danfe_position'] is not None else 9999, x['doc_position'] if x['doc_position'] is not None else 9999))
        
    return {
        'all_match': all_match,
        'comparison': {
            'doc_file': doc_file,
            'items': items_detail,
            'status': 'MATCH' if all_match else 'MISMATCH',
            'source': source_type
        }
    }

def validate(danfe_data, ocr_docs):
    import logging
    logger = logging.getLogger()
    logger.info(f"[validar_produtos.py] Starting validation with {len(ocr_docs)} docs")
    danfe_produtos = danfe_data.get('produtos', [])
    comparisons = []
    all_match = True
    
    logger.info(f"[validar_produtos] DANFE tem {len(danfe_produtos)} produtos")
    
    for doc in ocr_docs:
        doc_file = doc.get('file_name', 'unknown')
        has_metadata = doc.get('_has_metadata', False)
        
        # PASSO 1: Buscar produtos nos metadados JSON PRIMEIRO (prioridade)
        doc_produtos_metadata = []
        if has_metadata:
            # Buscar 'itens' dos metadados (que foram mesclados no handler)
            doc_produtos_metadata = doc.get('itens', [])
            logger.info(f"[validar_produtos] Doc {doc_file} - Metadados têm 'itens': {len(doc_produtos_metadata)} produtos")
            if doc_produtos_metadata:
                # Log do primeiro produto para debug
                first_prod = doc_produtos_metadata[0]
                logger.info(f"[validar_produtos] Primeiro produto metadados (completo): {json.dumps(first_prod, default=str)}")
                logger.info(f"[validar_produtos] Primeiro produto metadados: codigo={first_prod.get('codigoProduto') or first_prod.get('codigo')}, qtd={first_prod.get('quantidade')} (tipo: {type(first_prod.get('quantidade'))})")
        
        # PASSO 2: Buscar produtos do OCR (fallback se não tiver metadados)
        ocr_data_original = doc.get('_ocr_data', {})
        doc_produtos_ocr = ocr_data_original.get('itens') or ocr_data_original.get('produtos', [])
        logger.info(f"[validar_produtos] Doc {doc_file} - OCR tem produtos: {len(doc_produtos_ocr)}")
        if doc_produtos_ocr:
            first_ocr = doc_produtos_ocr[0]
            logger.info(f"[validar_produtos] Primeiro produto OCR: codigo={first_ocr.get('codigoProduto') or first_ocr.get('codigo')}, qtd={first_ocr.get('quantidade')}")
        
        logger.info(f"[validar_produtos] Doc {doc_file} - Metadados: {len(doc_produtos_metadata)} produtos, OCR: {len(doc_produtos_ocr)} produtos")
        logger.info(f"[validar_produtos] DANFE tem {len(danfe_produtos)} produtos")
        if danfe_produtos:
            first_danfe = danfe_produtos[0]
            logger.info(f"[validar_produtos] Primeiro produto DANFE: codigo={first_danfe.get('codigo')}, qtd={first_danfe.get('quantidade')}")
        
        # PRIORIDADE: Se metadados JSON tem produtos, usar metadados primeiro
        if has_metadata and doc_produtos_metadata and len(doc_produtos_metadata) > 0:
            logger.info(f"[validar_produtos] Doc {doc_file} - Metadados JSON tem {len(doc_produtos_metadata)} produtos, usando METADADOS JSON (prioridade)")
            result = validate_products_comparison(danfe_produtos, doc_produtos_metadata, doc_file, "METADADOS JSON", doc)
            comparisons.append(result['comparison'])
            if not result['all_match']:
                all_match = False
            continue
        
        # Se metadados não tem produtos, tentar OCR como fallback
        if doc_produtos_ocr and len(doc_produtos_ocr) > 0:
            logger.info(f"[validar_produtos] Doc {doc_file} - Metadados não tem produtos, usando OCR como fallback ({len(doc_produtos_ocr)} produtos)")
            result = validate_products_comparison(danfe_produtos, doc_produtos_ocr, doc_file, "OCR", doc)
            comparisons.append(result['comparison'])
            if not result['all_match']:
                all_match = False
            continue
        
        # Se chegou aqui, não tem produtos em nenhuma fonte
        logger.warning(f"[validar_produtos] Doc {doc_file} - ❌ Nenhum produto encontrado no OCR nem metadados")
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': f'0 produtos encontrados (esperado {len(danfe_produtos)})',
            'status': 'MISMATCH'
        })
        all_match = False
    
    return {
        'rule': 'validar_produtos',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': f'{len(danfe_produtos)} produtos',
        'message': 'Produtos validados' if all_match else 'Divergência nos produtos',
        'comparisons': comparisons
    }
