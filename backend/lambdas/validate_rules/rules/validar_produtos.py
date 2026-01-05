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
    """Encontra produto correspondente no doc (ignora ordem)"""
    import logging
    logger = logging.getLogger()
    
    danfe_key = make_product_key(danfe_prod, is_danfe=True)
    danfe_cod_raw = danfe_prod.get('codigo', '')
    danfe_qtd_raw = danfe_prod.get('quantidade', 0)
    logger.info(f"Buscando match para DANFE:")
    logger.info(f"  Raw: codigo='{danfe_cod_raw}', qtd={danfe_qtd_raw}")
    logger.info(f"  Key (normalizado): codigo='{danfe_key[0]}', qtd={danfe_key[1]}")
    logger.info(f"  Indices já usados: {used_indices}")
    logger.info(f"  Total de produtos no doc para comparar: {len(doc_produtos)}")
    
    # PRIORIDADE 1: Tentar match por código (ignorando quantidade inicialmente)
    # Se o código fizer match, usar esse produto mesmo que a quantidade esteja diferente
    # A quantidade será comparada depois nos campos individuais
    danfe_codigo = danfe_key[0]
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            logger.info(f"  Pulando doc[{i}] (já usado)")
            continue
        doc_key = make_product_key(doc_prod, is_danfe=False)
        doc_codigo = doc_key[0]
        doc_raw_cod = doc_prod.get('codigoProduto') or doc_prod.get('codigo', '')
        doc_raw_qtd = doc_prod.get('quantidade') or doc_prod.get('quantidadeProduto') or doc_prod.get('qtd') or 0
        logger.info(f"  Comparando doc[{i}]:")
        logger.info(f"    Produto completo: {json.dumps(doc_prod, default=str)[:500]}")
        logger.info(f"    Raw: codigo='{doc_raw_cod}', qtd={doc_raw_qtd} (tipo: {type(doc_raw_qtd)})")
        logger.info(f"    Key (normalizado): codigo='{doc_key[0]}', qtd={doc_key[1]}")
        
        # Primeiro verificar se o código faz match (com tolerância a erros de OCR)
        cod_match = (danfe_codigo == doc_codigo) or codes_are_similar(danfe_codigo, doc_codigo)
        
        if cod_match:
            logger.info(f"  ✓✓✓ MATCH POR CÓDIGO no índice {i} ✓✓✓")
            logger.info(f"    Códigos normalizados: '{danfe_codigo}' == '{doc_codigo}'")
            logger.info(f"    Quantidades serão comparadas depois: DANFE={danfe_key[1]}, DOC={doc_key[1]}")
            return i, doc_prod
        else:
            logger.info(f"  ✗ Código não match no índice {i}: '{danfe_codigo}' != '{doc_codigo}'")
    
    # PRIORIDADE 2: Tentar match por código + quantidade (exato)
    # A normalização já remove zeros à esquerda em make_product_key
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_key = make_product_key(doc_prod, is_danfe=False)
        if danfe_key == doc_key:
            logger.info(f"  ✓✓✓ MATCH EXATO (código + quantidade) no índice {i} ✓✓✓")
            logger.info(f"    Códigos normalizados: '{danfe_key[0]}' == '{doc_key[0]}'")
            logger.info(f"    Quantidades: {danfe_key[1]} == {doc_key[1]}")
            return i, doc_prod
    
    # PRIORIDADE 3: Tentar match com tolerância a erros de OCR (código + quantidade)
    danfe_codigo, danfe_qtd = danfe_key
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_codigo, doc_qtd = make_product_key(doc_prod, is_danfe=False)
        
        # Quantidade deve ser igual (ou muito próxima)
        qtd_match = abs(danfe_qtd - doc_qtd) <= 0.01
        # Código pode ter erro de OCR ou diferenças de zeros à esquerda (já normalizado)
        # Se códigos normalizados são iguais, é match
        cod_match = (danfe_codigo == doc_codigo) or codes_are_similar(danfe_codigo, doc_codigo)
        
        if qtd_match and cod_match:
            logger.info(f"  ✓ MATCH com tolerância OCR no índice {i} (DANFE: {danfe_codigo}, DOC: {doc_codigo})")
            return i, doc_prod
        else:
            logger.info(f"  ✗ Não match no índice {i}: qtd_match={qtd_match} (DANFE: {danfe_qtd}, DOC: {doc_qtd}), cod_match={cod_match} (DANFE: {danfe_codigo}, DOC: {doc_codigo})")
    
    # Fallback: tentar match por descrição (se quantidade também bater)
    logger.info(f"  Tentando match por descrição...")
    danfe_desc = str(danfe_prod.get('descricao', '')).upper().strip()
    danfe_qtd = danfe_key[1]
    
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        doc_desc = str(doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')).upper().strip()
        doc_qtd = normalize_number(doc_prod.get('quantidade', 0))
        
        # Verifica se a descrição da DANFE está contida na descrição do OCR ou vice-versa
        # E quantidade deve ser igual (ou muito próxima)
        desc_match = (danfe_desc and doc_desc and (danfe_desc in doc_desc or doc_desc in danfe_desc))
        qtd_match = abs(danfe_qtd - doc_qtd) <= 0.01
        
        if desc_match and qtd_match:
            logger.info(f"  ✓ MATCH por descrição no índice {i} (DANFE: '{danfe_desc}', DOC: '{doc_desc}')")
            return i, doc_prod
    
    logger.warning(f"  ✗ Nenhum match encontrado para {danfe_key} (codigo DANFE: {danfe_prod.get('codigo')}, descricao: '{danfe_desc}')")
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
            # Match encontrado - validar cada campo
            fields = {}
            
            # Código
            danfe_cod = danfe_prod.get('codigo', '')
            doc_cod = doc_prod.get('codigoProduto') or doc_prod.get('codigo', '')
            
            # Normalizar e comparar (sempre normalizar para ignorar zeros à esquerda)
            danfe_cod_norm = normalize_codigo(danfe_cod)
            doc_cod_norm = normalize_codigo(doc_cod)
            
            logger.info(f"[validar_produtos] Comparando códigos - DANFE: '{danfe_cod}' (normalizado: '{danfe_cod_norm}') vs DOC: '{doc_cod}' (normalizado: '{doc_cod_norm}')")
            
            corrected_cod = doc_cod
            
            if danfe_cod_norm == doc_cod_norm:
                cod_status = 'MATCH'
            elif codes_are_similar(danfe_cod_norm, doc_cod_norm):
                cod_status = 'MATCH'
                corrected_cod = danfe_cod
            else:
                from .utils import compare_with_bedrock
                cod_status = compare_with_bedrock(danfe_cod, doc_cod, 'código do produto')
                if cod_status == 'MATCH':
                    corrected_cod = danfe_cod
            
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
                from .utils import compare_with_bedrock
                desc_status = compare_with_bedrock(danfe_desc, doc_desc, 'descrição do produto')
                if desc_status == 'MATCH':
                    corrected_desc = danfe_desc
            
            fields['descricao'] = {
                'danfe': danfe_desc,
                'doc': corrected_desc,
                'status': desc_status
            }
            
            # Unidade - desconsiderar se não encontrada no documento
            danfe_un = danfe_prod.get('unidade', '')
            doc_un = doc_prod.get('unidadeMedida') or doc_prod.get('unidade', '')
            # Se não encontrou unidade no documento, considerar MATCH (desconsiderar)
            if not doc_un or doc_un.strip() == '':
                unidade_status = 'MATCH'
                doc_un = 'NÃO ENCONTRADO (DESCONSIDERADO)'
            else:
                unidade_status = 'MATCH' if str(danfe_un).upper() == str(doc_un).upper() else 'MISMATCH'
            
            fields['unidade'] = {
                'danfe': danfe_un,
                'doc': doc_un,
                'status': unidade_status
            }
            
            # Quantidade - verificar todos os campos possíveis (igual ao make_product_key)
            danfe_qtd = normalize_number(danfe_prod.get('quantidade', 0))
            # Verificar todos os campos possíveis para quantidade no documento
            doc_qtd_raw = None
            for field in ['quantidade', 'quantidadeProduto', 'qtd', 'quantidadeItem', 'qCom', 'qTrib']:
                if field in doc_prod and doc_prod[field] is not None:
                    doc_qtd_raw = doc_prod[field]
                    break
            doc_qtd = normalize_number(doc_qtd_raw or 0)
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
            
            # Moeda (sempre BRL para DANFE)
            danfe_moeda = 'BRL'
            doc_moeda = doc_prod.get('moeda', '')
            
            # Se não encontrou moeda no item, buscar no doc raiz (pode estar nos metadados no nível raiz)
            if not doc_moeda and doc_root:
                doc_moeda = doc_root.get('moeda', '')
            
            # Se não encontrou moeda mas valores são iguais, considerar OK
            if not doc_moeda and abs(danfe_vtotal - doc_vtotal) <= 0.01:
                moeda_status = 'MATCH'
                doc_moeda = 'NÃO ENCONTRADO PORÉM VALOR IGUAL'
            elif doc_moeda and (doc_moeda.upper() == 'BRL' or doc_moeda.upper() == danfe_moeda):
                moeda_status = 'MATCH'
            else:
                moeda_status = 'MISMATCH'
            
            fields['moeda'] = {
                'danfe': danfe_moeda,
                'doc': doc_moeda,
                'status': moeda_status
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
                },
                'moeda': {
                    'danfe': 'BRL',
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
        doc_cod = doc_prod.get('codigoProduto') or doc_prod.get('codigo', '')
        doc_desc = doc_prod.get('descricaoProduto') or doc_prod.get('descricao', '')
        doc_qtd = normalize_number(doc_prod.get('quantidade', 0))
        doc_vtotal = normalize_number(doc_prod.get('valorTotal') or doc_prod.get('valor_total', 0))
        
        items_detail.append({
            'item': len(danfe_produtos) + doc_idx + 1,  # Continuar numeração após produtos do DANFE
            'danfe_position': None,  # Não está no DANFE
            'doc_position': doc_idx + 1,
            'fields': {
                'codigo': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_cod,
                    'status': 'MISMATCH'
                },
                'descricao': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_desc,
                    'status': 'MISMATCH'
                },
                'unidade': {
                    'danfe': '-',
                    'doc': doc_prod.get('unidadeMedida') or doc_prod.get('unidade', ''),
                    'status': 'MISMATCH'
                },
                'quantidade': {
                    'danfe': 0,
                    'doc': doc_qtd,
                    'status': 'MISMATCH'
                },
                'valor_unitario': {
                    'danfe': 0,
                    'doc': normalize_number(doc_prod.get('valorUnitario') or doc_prod.get('valor_unitario', 0)),
                    'status': 'MISMATCH'
                },
                'valor_total': {
                    'danfe': 0,
                    'doc': doc_vtotal,
                    'status': 'MISMATCH'
                },
                'moeda': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_prod.get('moeda', '') or (doc_root.get('moeda', '') if doc_root else ''),
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
