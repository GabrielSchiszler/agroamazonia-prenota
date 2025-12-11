def normalize_number(val):
    try:
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
    except:
        return 0

def normalize_codigo(codigo):
    """Normaliza código removendo espaços, caracteres especiais e zeros à esquerda"""
    import re
    # Remove TODOS os espaços e caracteres especiais
    normalized = re.sub(r'[^A-Z0-9]', '', str(codigo).upper().strip())
    # Remove zeros à esquerda apenas se o código for totalmente numérico
    if normalized.isdigit():
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
        qtd = normalize_number(prod.get('quantidade', 0))
    
    return (codigo, qtd)

def find_matching_product(danfe_prod, doc_produtos, used_indices):
    """Encontra produto correspondente no doc (ignora ordem)"""
    import logging
    logger = logging.getLogger()
    
    danfe_key = make_product_key(danfe_prod, is_danfe=True)
    logger.info(f"Buscando match para DANFE key: {danfe_key}")
    logger.info(f"  DANFE raw: codigo={danfe_prod.get('codigo')}, qtd={danfe_prod.get('quantidade')}")
    logger.info(f"  Indices já usados: {used_indices}")
    
    # Tentar match por código + quantidade (exato)
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_key = make_product_key(doc_prod, is_danfe=False)
        if danfe_key == doc_key:
            logger.info(f"  ✓ MATCH exato no índice {i}")
            return i, doc_prod
    
    # Tentar match com tolerância a erros de OCR
    danfe_codigo, danfe_qtd = danfe_key
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_codigo, doc_qtd = make_product_key(doc_prod, is_danfe=False)
        
        # Quantidade deve ser igual, código pode ter erro de OCR
        if abs(danfe_qtd - doc_qtd) <= 0.01 and codes_are_similar(danfe_codigo, doc_codigo):
            logger.info(f"  ✓ MATCH com tolerância OCR no índice {i} (DANFE: {danfe_codigo}, DOC: {doc_codigo})")
            return i, doc_prod
    
    # Fallback: tentar match por descrição
    logger.info(f"  Tentando match por descrição...")
    danfe_desc = str(danfe_prod.get('descricao', '')).upper().strip()
    
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_desc = str(doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).upper().strip()
        
        # Verifica se a descrição da DANFE está contida na descrição do OCR
        if danfe_desc and doc_desc and danfe_desc in doc_desc:
            logger.info(f"  ✓ MATCH por descrição no índice {i}")
            return i, doc_prod
    
    logger.warning(f"  ✗ Nenhum match encontrado para {danfe_key}")
    return None, None

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
        doc_produtos = doc.get('itens') or doc.get('produtos', [])
        logger.info(f"[validar_produtos] Doc {doc_file} tem {len(doc_produtos)} produtos")
        
        if len(danfe_produtos) != len(doc_produtos):
            comparisons.append({
                'doc_file': doc_file,
                'doc_value': f'{len(doc_produtos)} produtos (esperado {len(danfe_produtos)})',
                'status': 'MISMATCH'
            })
            all_match = False
            continue
        
        items_detail = []
        used_indices = set()
        unmatched_danfe = []
        
        # Tentar parear cada produto DANFE com DOC
        for danfe_idx, danfe_prod in enumerate(danfe_produtos):
            doc_idx, doc_prod = find_matching_product(danfe_prod, doc_produtos, used_indices)
            
            if doc_prod is not None:
                used_indices.add(doc_idx)
                # Match encontrado - validar cada campo
                fields = {}
                
                # Código
                danfe_cod = danfe_prod.get('codigo', '')
                doc_cod = doc_prod.get('codigoProduto') or doc_prod.get('codigo', '')
                
                # Normalizar e comparar
                danfe_cod_norm = normalize_codigo(danfe_cod)
                doc_cod_norm = normalize_codigo(doc_cod)
                corrected_cod = doc_cod
                
                if danfe_cod_norm == doc_cod_norm:
                    cod_status = 'MATCH'
                elif codes_are_similar(danfe_cod_norm, doc_cod_norm):
                    cod_status = 'MATCH'
                    corrected_cod = danfe_cod  # Corrigir para valor DANFE
                else:
                    # Tentar com Bedrock se falhar
                    from .utils import compare_with_bedrock
                    cod_status = compare_with_bedrock(danfe_cod, doc_cod, 'código do produto')
                    if cod_status == 'MATCH':
                        corrected_cod = danfe_cod  # Corrigir para valor DANFE
                
                fields['codigo'] = {
                    'danfe': danfe_cod,
                    'doc': corrected_cod,
                    'status': cod_status
                }
                
                # Descrição
                danfe_desc = danfe_prod.get('descricao', '')
                doc_desc = doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')
                corrected_desc = doc_desc
                
                if str(danfe_desc).upper() in str(doc_desc).upper() or str(doc_desc).upper() in str(danfe_desc).upper():
                    desc_status = 'MATCH'
                else:
                    # Usar Bedrock para validar se são similares
                    from .utils import compare_with_bedrock
                    desc_status = compare_with_bedrock(danfe_desc, doc_desc, 'descrição do produto')
                    if desc_status == 'MATCH':
                        corrected_desc = danfe_desc  # Corrigir para valor DANFE
                
                fields['descricao'] = {
                    'danfe': danfe_desc,
                    'doc': corrected_desc,
                    'status': desc_status
                }
                
                # Unidade
                danfe_un = danfe_prod.get('unidade', '')
                doc_un = doc_prod.get('unidadeMedida') or doc_prod.get('unidade', '')
                fields['unidade'] = {
                    'danfe': danfe_un,
                    'doc': doc_un,
                    'status': 'MATCH' if str(danfe_un).upper() == str(doc_un).upper() else 'MISMATCH'
                }
                
                # Quantidade
                danfe_qtd = normalize_number(danfe_prod.get('quantidade', 0))
                doc_qtd = normalize_number(doc_prod.get('quantidade', 0))
                fields['quantidade'] = {
                    'danfe': danfe_qtd,
                    'doc': doc_qtd,
                    'status': 'MATCH' if abs(danfe_qtd - doc_qtd) <= 0.01 else 'MISMATCH'
                }
                
                # Valor unitário
                danfe_vunit = normalize_number(danfe_prod.get('valor_unitario', 0))
                doc_vunit = normalize_number(doc_prod.get('valorUnitario') or doc_prod.get('valor_unitario', 0))
                fields['valor_unitario'] = {
                    'danfe': danfe_vunit,
                    'doc': doc_vunit,
                    'status': 'MATCH' if abs(danfe_vunit - doc_vunit) <= 0.01 else 'MISMATCH'
                }
                
                # Valor total
                danfe_vtotal = normalize_number(danfe_prod.get('valor_total', 0))
                doc_vtotal = normalize_number(doc_prod.get('valorTotal') or doc_prod.get('valor_total', 0))
                fields['valor_total'] = {
                    'danfe': danfe_vtotal,
                    'doc': doc_vtotal,
                    'status': 'MATCH' if abs(danfe_vtotal - doc_vtotal) <= 0.01 else 'MISMATCH'
                }
                
                item_has_mismatch = any(f['status'] == 'MISMATCH' for f in fields.values())
                if item_has_mismatch:
                    has_mismatch = True
                
                items_detail.append({
                    'item': danfe_idx + 1,
                    'danfe_position': danfe_idx + 1,
                    'doc_position': doc_idx + 1,
                    'fields': fields,
                    'status': 'MISMATCH' if item_has_mismatch else 'MATCH'
                })
            else:
                unmatched_danfe.append((danfe_idx, danfe_prod))
        
        # Produtos não pareados = MISMATCH
        for danfe_idx, danfe_prod in unmatched_danfe:
            all_match = False
            items_detail.append({
                'item': danfe_idx + 1,
                'danfe_position': danfe_idx + 1,
                'doc_position': None,
                'fields': {
                    'codigo': {
                        'danfe': danfe_prod.get('codigo', ''),
                        'doc': 'NÃO ENCONTRADO',
                        'status': 'MISMATCH'
                    },
                    'descricao': {
                        'danfe': danfe_prod.get('descricao', ''),
                        'doc': 'NÃO ENCONTRADO',
                        'status': 'MISMATCH'
                    },
                    'unidade': {
                        'danfe': danfe_prod.get('unidade', ''),
                        'doc': '-',
                        'status': 'MISMATCH'
                    },
                    'quantidade': {
                        'danfe': normalize_number(danfe_prod.get('quantidade', 0)),
                        'doc': 0,
                        'status': 'MISMATCH'
                    },
                    'valor_unitario': {
                        'danfe': normalize_number(danfe_prod.get('valor_unitario', 0)),
                        'doc': 0,
                        'status': 'MISMATCH'
                    },
                    'valor_total': {
                        'danfe': normalize_number(danfe_prod.get('valor_total', 0)),
                        'doc': 0,
                        'status': 'MISMATCH'
                    }
                },
                'status': 'MISMATCH'
            })
        
        # Ordenar por posição DANFE
        items_detail.sort(key=lambda x: x['danfe_position'])
        
        has_mismatch = any(item['status'] == 'MISMATCH' for item in items_detail)
        
        comparisons.append({
            'doc_file': doc_file,
            'items': items_detail,
            'status': 'MISMATCH' if has_mismatch else 'MATCH'
        })
        
        if has_mismatch:
            all_match = False
    
    return {
        'rule': 'validar_produtos',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': f'{len(danfe_produtos)} produtos',
        'message': 'Produtos validados' if all_match else 'Divergência nos produtos',
        'comparisons': comparisons
    }
