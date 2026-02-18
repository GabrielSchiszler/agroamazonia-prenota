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

def normalize_code_separators(text):
    """Normaliza separadores em códigos numéricos (pontos, traços, espaços) para facilitar comparação"""
    import re
    # Estratégia: substituir todos os separadores (., -, espaço) por espaço único
    # Exemplo: "15.15.15" -> "15 15 15", "30-00-20" -> "30 00 20"
    # Isso permite comparar códigos equivalentes independente do separador usado
    
    # Primeiro, substituir pontos e traços por espaços
    text = re.sub(r'[.\-]', ' ', text)
    # Depois, normalizar múltiplos espaços consecutivos em um único espaço
    text = re.sub(r'\s+', ' ', text)
    # Remover espaços no início e fim
    return text.strip()

def find_matching_product(danfe_prod, doc_produtos, used_indices):
    """Encontra produto correspondente no documento usando apenas nome, sem depender de código/ID
    Retorna: (doc_idx, doc_prod, has_equivalent_code) onde has_equivalent_code indica se houve match por código numérico equivalente"""
    import logging
    logger = logging.getLogger()
    
    # Buscar nome do produto no DANFE (prioridade: produto > nome > descricao)
    danfe_nome = (danfe_prod.get('produto', '').strip() or 
                  danfe_prod.get('nome', '').strip() or 
                  danfe_prod.get('descricao', '').strip())
    
    logger.info(f"[validar_produtos] Buscando match para produto DANFE:")
    logger.info(f"  Nome: '{danfe_nome}'")
    
    # PRIORIDADE 1: Match exato por nome (case-insensitive)
    danfe_nome_upper = danfe_nome.upper()
    
    # Tentar match exato primeiro
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        
        # Buscar nome do produto no documento (prioridade: produto > nomeProduto > nome > descricaoProduto > descricao)
        doc_nome = (doc_prod.get('produto') or 
                   doc_prod.get('nomeProduto') or 
                   doc_prod.get('nome') or 
                   doc_prod.get('descricaoProduto') or 
                   doc_prod.get('descricao', '')).strip()
        doc_nome_upper = doc_nome.upper()
        
        # Match exato
        if danfe_nome_upper == doc_nome_upper:
            logger.info(f"  ✓ MATCH EXATO por nome no índice {i} (DANFE: '{danfe_nome}', DOC: '{doc_nome}')")
            return i, doc_prod, False
    
    # PRIORIDADE 1.5: Match com normalização de códigos (pontos vs traços)
    # Extrair códigos numéricos e comparar separadamente
    import re
    danfe_codes = re.findall(r'\b\d+[.\- ]\d+[.\- ]\d+\b', danfe_nome_upper)
    danfe_nome_normalized = normalize_code_separators(danfe_nome_upper)
    
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        
        doc_nome = (doc_prod.get('produto') or 
                   doc_prod.get('nomeProduto') or 
                   doc_prod.get('nome') or 
                   doc_prod.get('descricaoProduto') or 
                   doc_prod.get('descricao', '')).strip()
        doc_nome_upper = doc_nome.upper()
        doc_nome_normalized = normalize_code_separators(doc_nome_upper)
        doc_codes = re.findall(r'\b\d+[.\- ]\d+[.\- ]\d+\b', doc_nome_upper)
        
        # Se ambos têm códigos numéricos, verificar se são equivalentes após normalização
        if danfe_codes and doc_codes:
            danfe_code_normalized = normalize_code_separators(danfe_codes[0])
            doc_code_normalized = normalize_code_separators(doc_codes[0])
            if danfe_code_normalized == doc_code_normalized:
                logger.info(f"  ✓ MATCH por código numérico equivalente no índice {i}")
                logger.info(f"    DANFE: '{danfe_nome}' (código: '{danfe_codes[0]}' -> '{danfe_code_normalized}')")
                logger.info(f"    DOC: '{doc_nome}' (código: '{doc_codes[0]}' -> '{doc_code_normalized}')")
                return i, doc_prod, True
        
        # Match após normalização completa do texto
        if danfe_nome_normalized == doc_nome_normalized:
            logger.info(f"  ✓ MATCH EXATO após normalização de códigos no índice {i}")
            logger.info(f"    DANFE: '{danfe_nome}' (normalizado: '{danfe_nome_normalized}')")
            logger.info(f"    DOC: '{doc_nome}' (normalizado: '{doc_nome_normalized}')")
            return i, doc_prod, False
    
    # PRIORIDADE 2: Match parcial (uma contém a outra) - mais permissivo
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        
        doc_nome = (doc_prod.get('produto') or 
                   doc_prod.get('nomeProduto') or 
                   doc_prod.get('nome') or 
                   doc_prod.get('descricaoProduto') or 
                   doc_prod.get('descricao', '')).strip()
        doc_nome_upper = doc_nome.upper()
        
        # Match parcial (uma contém a outra) - mais permissivo
        if len(danfe_nome_upper) > 3 and len(doc_nome_upper) > 3:
            # Verificar se há palavras-chave em comum (pelo menos 3 caracteres)
            # Extrair palavras significativas (mais de 2 caracteres)
            danfe_words = [w for w in danfe_nome_upper.split() if len(w) > 2]
            doc_words = [w for w in doc_nome_upper.split() if len(w) > 2]
            
            # Se houver pelo menos 2 palavras em comum, considerar match
            common_words = set(danfe_words) & set(doc_words)
            if len(common_words) >= 2:
                logger.info(f"  ✓ MATCH PARCIAL por palavras-chave no índice {i} (palavras comuns: {common_words})")
                logger.info(f"    DANFE: '{danfe_nome}', DOC: '{doc_nome}'")
                return i, doc_prod, False
            
            # Fallback: verificar se uma string contém a outra (substring)
            if danfe_nome_upper in doc_nome_upper or doc_nome_upper in danfe_nome_upper:
                logger.info(f"  ✓ MATCH PARCIAL por substring no índice {i} (DANFE: '{danfe_nome}', DOC: '{doc_nome}')")
                return i, doc_prod, False
            
            # Verificar match parcial após normalização de códigos
            danfe_nome_normalized = normalize_code_separators(danfe_nome_upper)
            doc_nome_normalized = normalize_code_separators(doc_nome_upper)
            if danfe_nome_normalized in doc_nome_normalized or doc_nome_normalized in danfe_nome_normalized:
                logger.info(f"  ✓ MATCH PARCIAL por substring após normalização de códigos no índice {i}")
                logger.info(f"    DANFE: '{danfe_nome}' (normalizado: '{danfe_nome_normalized}')")
                logger.info(f"    DOC: '{doc_nome}' (normalizado: '{doc_nome_normalized}')")
                return i, doc_prod, False
    
    # PRIORIDADE 3: Tentar todos os produtos restantes e usar Bedrock para validar
    # Se chegou aqui, não encontrou match exato nem parcial
    # Vamos tentar com Bedrock para ver se são o mesmo produto
    logger.info(f"  ⚠ Nenhum match exato/parcial encontrado, tentando validar com Bedrock...")
    for i, doc_prod in enumerate(doc_produtos):
        if i in used_indices:
            continue
        
        doc_nome = (doc_prod.get('produto') or 
                   doc_prod.get('nomeProduto') or 
                   doc_prod.get('nome') or 
                   doc_prod.get('descricaoProduto') or 
                   doc_prod.get('descricao', '')).strip()
        
        # Usar Bedrock para validar se são o mesmo produto
        from .utils import compare_with_bedrock
        bedrock_result = compare_with_bedrock(danfe_nome, doc_nome, 'nome do produto')
        
        if bedrock_result == 'MATCH':
            logger.info(f"  ✓ MATCH via Bedrock no índice {i} (DANFE: '{danfe_nome}', DOC: '{doc_nome}')")
            return i, doc_prod, False
    
    logger.warning(f"  ✗ Nenhum match encontrado para produto (nome: '{danfe_nome}')")
    return None, None, False

def extract_quantity_and_unit(produto, is_danfe=True):
    """Extrai quantidade e unidade de medida do produto"""
    if is_danfe:
        quantidade = normalize_number(produto.get('quantidade', 0))
        unidade = produto.get('unidade', '').strip().upper()
    else:
        # Para documentos, buscar quantidade em diferentes campos
        qtd_raw = None
        for field in ['quantidade', 'quantidadeProduto', 'qtd', 'quantidadeItem', 'qCom', 'qTrib']:
            if field in produto and produto[field] is not None:
                qtd_raw = produto[field]
                break
        quantidade = normalize_number(qtd_raw or 0)
        unidade = (produto.get('unidadeMedida') or produto.get('unidade') or '').strip().upper()
    
    return quantidade, unidade

def quantities_match(danfe_qtd, danfe_unit, doc_qtd, doc_unit):
    """Verifica se as quantidades são equivalentes, considerando unidades de medida"""
    # Se ambas as quantidades são 0 ou muito pequenas, considerar match
    if danfe_qtd < 0.01 and doc_qtd < 0.01:
        return True
    
    # Normalizar unidades de medida comuns
    unit_normalization = {
        'KG': ['KG', 'KILO', 'KILOS', 'QUILO', 'QUILOS'],
        'G': ['G', 'GRAM', 'GRAMS', 'GRAMA', 'GRAMAS'],
        'L': ['L', 'LITRO', 'LITROS', 'LITER', 'LITERS'],
        'ML': ['ML', 'MILILITRO', 'MILILITROS', 'MILLILITER', 'MILLILITERS'],
        'UN': ['UN', 'UNID', 'UNIDADE', 'UNIDADES', 'UNIT', 'UNITS'],
        'SC': ['SC', 'SACO', 'SACOS', 'BAG', 'BAGS'],
        'PT': ['PT', 'POTE', 'POTES', 'POT'],
        'CX': ['CX', 'CAIXA', 'CAIXAS', 'BOX', 'BOXES']
    }
    
    # Normalizar unidades
    danfe_unit_norm = None
    doc_unit_norm = None
    
    for norm_unit, variants in unit_normalization.items():
        if danfe_unit in variants:
            danfe_unit_norm = norm_unit
        if doc_unit in variants:
            doc_unit_norm = norm_unit
    
    # Se não encontrou normalização, usar a unidade original
    if danfe_unit_norm is None:
        danfe_unit_norm = danfe_unit
    if doc_unit_norm is None:
        doc_unit_norm = doc_unit
    
    # Se unidades são diferentes, não são equivalentes
    if danfe_unit_norm != doc_unit_norm:
        return False
    
    # Se unidades são iguais, comparar quantidades (tolerância de 0.01 para arredondamento)
    diff = abs(danfe_qtd - doc_qtd)
    return diff < 0.01

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
        doc_idx, doc_prod, has_equivalent_code = find_matching_product(danfe_prod, doc_produtos, used_indices)
        
        if doc_prod is not None:
            used_indices.add(doc_idx)
            # Match encontrado - validar nome e quantidade
            fields = {}
            
            # Nome - sempre usar Bedrock para comparação semântica flexível
            danfe_nome = danfe_prod.get('produto', '').strip() or danfe_prod.get('nome', '').strip() or danfe_prod.get('descricao', '').strip()
            # Buscar nome do produto no documento (prioridade: produto > nomeProduto > nome > descricaoProduto > descricao)
            doc_nome = (doc_prod.get('produto') or 
                       doc_prod.get('nomeProduto') or 
                       doc_prod.get('nome') or 
                       doc_prod.get('descricaoProduto') or 
                       doc_prod.get('descricao', '')).strip()
            
            # Sempre usar Bedrock para comparação semântica (mais flexível)
            # Passar informação sobre código numérico equivalente se houver
            from .utils import compare_with_bedrock
            nome_status = compare_with_bedrock(danfe_nome, doc_nome, 'nome do produto', has_equivalent_code=has_equivalent_code)
            
            # Se Bedrock não conseguir determinar, tentar match parcial como fallback
            if nome_status not in ['MATCH', 'MISMATCH']:
                # Fallback: verificar se há palavras-chave em comum
                danfe_words = set(w.upper() for w in danfe_nome.split() if len(w) > 2)
                doc_words = set(w.upper() for w in doc_nome.split() if len(w) > 2)
                common_words = danfe_words & doc_words
                if len(common_words) >= 2:
                    nome_status = 'MATCH'
                elif danfe_nome.upper() in doc_nome.upper() or doc_nome.upper() in danfe_nome.upper():
                    nome_status = 'MATCH'
                else:
                    nome_status = 'MISMATCH'
            
            fields['nome'] = {
                'danfe': danfe_nome,
                'doc': doc_nome,
                'status': nome_status
            }
            
            # Item só é MISMATCH se o nome for totalmente divergente (Bedrock retornou MISMATCH)
            item_has_mismatch = (nome_status == 'MISMATCH')
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
        
        items_detail.append({
            'item': danfe_idx + 1,
            'danfe_position': danfe_idx + 1,
            'doc_position': None,
            'fields': {
                'nome': {
                    'danfe': danfe_nome,
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
        # Buscar nome do produto no documento (prioridade: produto > nomeProduto > nome > descricaoProduto > descricao)
        doc_nome = (doc_prod.get('produto') or 
                   doc_prod.get('nomeProduto') or 
                   doc_prod.get('nome') or 
                   doc_prod.get('descricaoProduto') or 
                   doc_prod.get('descricao', '')).strip()
        
        items_detail.append({
            'item': len(danfe_produtos) + doc_idx + 1,  # Continuar numeração após produtos do DANFE
            'danfe_position': None,  # Não está no DANFE
            'doc_position': doc_idx + 1,
            'fields': {
                'nome': {
                    'danfe': 'NÃO ENCONTRADO',
                    'doc': doc_nome,
                    'status': 'MISMATCH'
                }
            },
            'status': 'MISMATCH'
        })
    
    # Ordenar por posição DANFE primeiro, depois por posição DOC
    items_detail.sort(key=lambda x: (x['danfe_position'] if x['danfe_position'] is not None else 9999, x['doc_position'] if x['doc_position'] is not None else 9999))
    
    # Contar quantos produtos deram MATCH
    matched_items = [item for item in items_detail if item.get('status') == 'MATCH' and item.get('danfe_position') is not None]
    has_at_least_one_match = len(matched_items) > 0
    
    # Se tiver pelo menos 1 match, considerar como sucesso (mas manter all_match para indicar se todos deram match)
    # Armazenar quais produtos deram match (posição DANFE) para filtrar no envio ao Protheus
    matched_danfe_positions = [item['danfe_position'] for item in matched_items]
    
    return {
        'all_match': all_match,  # Indica se TODOS deram match
        'has_match': has_at_least_one_match,  # Indica se tem pelo menos 1 match
        'matched_danfe_positions': matched_danfe_positions,  # Posições dos produtos que deram match
        'comparison': {
            'doc_file': doc_file,
            'items': items_detail,
            'status': 'MATCH' if has_at_least_one_match else 'MISMATCH',  # Status baseado em ter pelo menos 1 match
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
    has_at_least_one_match = False
    all_matched_danfe_positions = []  # Acumular posições de todos os produtos que deram match
    
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
        
        # USAR APENAS DADOS DO JSON DO PEDIDO DE COMPRA (NÃO usar OCR)
        logger.info(f"[validar_produtos] Doc {doc_file} - Metadados: {len(doc_produtos_metadata)} produtos")
        logger.info(f"[validar_produtos] DANFE tem {len(danfe_produtos)} produtos")
        if danfe_produtos:
            first_danfe = danfe_produtos[0]
            logger.info(f"[validar_produtos] Primeiro produto DANFE: codigo={first_danfe.get('codigo')}, qtd={first_danfe.get('quantidade')}")
        
        # Usar apenas produtos dos metadados do JSON do pedido de compra
        if has_metadata and doc_produtos_metadata and len(doc_produtos_metadata) > 0:
            logger.info(f"[validar_produtos] Doc {doc_file} - ✅ Usando produtos dos metadados do JSON do pedido de compra ({len(doc_produtos_metadata)} produtos)")
            result = validate_products_comparison(danfe_produtos, doc_produtos_metadata, doc_file, "METADADOS JSON", doc)
            comparisons.append(result['comparison'])
            
            # Acumular informações sobre matches
            if result.get('has_match', False):
                has_at_least_one_match = True
                all_matched_danfe_positions.extend(result.get('matched_danfe_positions', []))
                logger.info(f"[validar_produtos] Doc {doc_file} - {len(result.get('matched_danfe_positions', []))} produto(s) deram MATCH (posições DANFE: {result.get('matched_danfe_positions', [])})")
            
            if not result['all_match']:
                all_match = False
            continue
        
        # Se chegou aqui, não tem produtos no JSON do pedido de compra
        logger.warning(f"[validar_produtos] Doc {doc_file} - ❌ Nenhum produto encontrado no JSON do pedido de compra")
        comparisons.append({
            'doc_file': doc_file,
            'doc_value': f'0 produtos encontrados (esperado {len(danfe_produtos)})',
            'status': 'MISMATCH'
        })
        all_match = False
    
    # Se tiver pelo menos 1 match, considerar como PASSED
    # Armazenar as posições dos produtos que deram match para uso no envio ao Protheus
    final_status = 'PASSED' if has_at_least_one_match else 'FAILED'
    final_message = f'Produtos validados ({len(all_matched_danfe_positions)} de {len(danfe_produtos)} produtos deram match)' if has_at_least_one_match else 'Nenhum produto validado'
    
    logger.info(f"[validar_produtos] RESULTADO FINAL: status={final_status}, matches={len(all_matched_danfe_positions)}, posições={all_matched_danfe_positions}")
    
    return {
        'rule': 'validar_produtos',
        'status': final_status,
        'danfe_value': f'{len(danfe_produtos)} produtos',
        'message': final_message,
        'comparisons': comparisons,
        'matched_danfe_positions': all_matched_danfe_positions  # Armazenar posições para filtrar no Protheus
    }
