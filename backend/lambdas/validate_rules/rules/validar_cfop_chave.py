import logging
import os
import boto3

from utils.cfop_table import (
    disambiguate_cfop_mappings,
    get_all_cfop_mappings_direct,
    normalize_cfop,
)

logger = logging.getLogger()


def validate(danfe_data, ocr_docs, context=None):
    """
    Valida se o CFOP do DANFE está mapeado na tabela Chave x CFOP
    e retorna a chave correspondente.

    context (opcional): process_type, uso_e_consumo, has_pedido_de_compra, natureza — para
    desambiguar quando existem vários mapeamentos ativos para o mesmo CFOP.
    """
    logger.info(f"[validar_cfop_chave.py] Starting validation with {len(ocr_docs)} docs")
    
    # Usar boto3 diretamente para acessar DynamoDB
    # Usar região da variável de ambiente (AWS_REGION é sempre definida nas Lambdas)
    aws_region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
    if not aws_region:
        # Fallback apenas para desenvolvimento local (não deve acontecer em Lambda)
        try:
            # Tentar obter região do contexto AWS
            session = boto3.Session()
            aws_region = session.region_name
            if not aws_region:
                logger.warning("[validar_cfop_chave] AWS_REGION não definida, usando fallback")
                aws_region = 'us-east-1'  # Fallback apenas para dev local
        except:
            aws_region = 'us-east-1'  # Fallback apenas para dev local
    logger.info(f"[validar_cfop_chave] Usando região AWS: {aws_region}")
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    table_name = os.environ.get('TABLE_NAME', 'DocumentProcessorTable')
    logger.info(f"[validar_cfop_chave] Nome da tabela DynamoDB: {table_name}, Região: {aws_region}")
    table = dynamodb.Table(table_name)
    
    # Extrair CFOP do DANFE
    danfe_cfop = None
    
    # Tentar extrair CFOP do DANFE (pode estar em diferentes locais)
    # O XML parseado pode ter 'produtos' ou 'itens'
    produtos = danfe_data.get('produtos', []) or danfe_data.get('itens', [])
    
    if produtos and len(produtos) > 0:
        # Pegar CFOP do primeiro produto/item (geralmente todos têm o mesmo CFOP)
        first_item = produtos[0]
        danfe_cfop = first_item.get('cfop') or first_item.get('CFOP') or first_item.get('codigoOperacao')
        logger.info(f"[validar_cfop_chave] CFOP encontrado no primeiro produto: {danfe_cfop}")
    
    # Se não encontrou no produto/item, tentar no nível do documento
    if not danfe_cfop:
        danfe_cfop = danfe_data.get('cfop') or danfe_data.get('CFOP')
        if danfe_cfop:
            logger.info(f"[validar_cfop_chave] CFOP encontrado no nível do documento: {danfe_cfop}")
    
    # Log da estrutura para debug
    logger.info(f"[validar_cfop_chave] Estrutura danfe_data keys: {list(danfe_data.keys())}")
    if produtos:
        logger.info(f"[validar_cfop_chave] Primeiro produto keys: {list(produtos[0].keys()) if produtos else 'N/A'}")
    
    if not danfe_cfop:
        logger.warning("[validar_cfop_chave] CFOP não encontrado no DANFE")
        return {
            'rule': 'validar_cfop_chave',
            'status': 'FAILED',
            'danfe_value': 'NÃO ENCONTRADO',
            'message': 'CFOP não encontrado no DANFE',
            'comparisons': [],
            'corrections': []
        }
    
    danfe_cfop_normalized = normalize_cfop(danfe_cfop)
    logger.info(f"[validar_cfop_chave] DANFE CFOP: {danfe_cfop_normalized}")
    
    # Buscar TODOS os mapeamentos correspondentes no DynamoDB
    cfop_mappings = get_all_cfop_mappings_direct(table, danfe_cfop_normalized)
    if len(cfop_mappings) > 1:
        before = len(cfop_mappings)
        cfop_mappings = disambiguate_cfop_mappings(cfop_mappings, context)
        logger.info(
            "[validar_cfop_chave] Mapeamentos após disambiguação: %d (antes: %d)",
            len(cfop_mappings),
            before,
        )

    comparisons = []
    all_match = True
    corrections = []
    
    # Validar quantidade de mapeamentos encontrados
    if len(cfop_mappings) == 0:
        # Nenhum mapeamento encontrado = ERRO
        logger.warning(f"[validar_cfop_chave] CFOP {danfe_cfop_normalized} NÃO encontrado na tabela Chave x CFOP")
        all_match = False
        
        comparisons.append({
            'doc_file': 'DANFE',
            'doc_value': f"CFOP: {danfe_cfop_normalized}",
            'chave': None,
            'descricao': None,
            'operacao': None,
            'status': 'MISMATCH',
            'source': 'DynamoDB',
            'message': 'CFOP não encontrado na tabela de mapeamento'
        })
        
        message = f"CFOP {danfe_cfop_normalized} não encontrado na tabela Chave x CFOP"
        danfe_value = f"{danfe_cfop_normalized} (NÃO MAPEADO)"
        cfop_data = None
        
    elif len(cfop_mappings) == 1:
        # Exatamente 1 mapeamento encontrado = SUCESSO
        cfop_mapping = cfop_mappings[0]
        chave_encontrada = cfop_mapping.get('chave', '')
        descricao = cfop_mapping.get('descricao', '')
        operacao = cfop_mapping.get('operacao', '')
        regra_text = cfop_mapping.get('regra', '')
        observacao = cfop_mapping.get('observacao', '')
        
        logger.info(f"[validar_cfop_chave] CFOP {danfe_cfop_normalized} encontrado! Chave: {chave_encontrada}, Descrição: {descricao}")
        
        comparisons.append({
            'doc_file': 'DANFE',
            'doc_value': f"CFOP: {danfe_cfop_normalized} → Chave: {chave_encontrada}",
            'chave': chave_encontrada,
            'descricao': descricao,
            'operacao': operacao,
            'regra': regra_text,
            'observacao': observacao,
            'cfop_encontrado': danfe_cfop_normalized,
            'status': 'MATCH',
            'source': 'DynamoDB'
        })
        
        message = f"CFOP {danfe_cfop_normalized} mapeado para chave {chave_encontrada}"
        danfe_value = f"{danfe_cfop_normalized} → {chave_encontrada}"
        
        cfop_data = {
            'cfop': danfe_cfop_normalized,
            'chave': chave_encontrada,
            'operacao': operacao,
            'descricao': descricao,
            'regra': regra_text,
            'observacao': observacao
        }
        
    else:
        # Mais de 1 mapeamento encontrado = ERRO (ambiguidade)
        logger.error(f"[validar_cfop_chave] CFOP {danfe_cfop_normalized} encontrado com MÚLTIPLOS mapeamentos ({len(cfop_mappings)})! Isso causa ambiguidade.")
        all_match = False
        
        # Adicionar todos os mapeamentos encontrados para exibição no frontend
        chaves_encontradas = []
        for idx, mapping in enumerate(cfop_mappings, 1):
            chave = mapping.get('chave', '')
            descricao = mapping.get('descricao', '')
            chaves_encontradas.append(f"{chave} ({descricao})")
            
            comparisons.append({
                'doc_file': 'DANFE',
                'doc_value': f"CFOP: {danfe_cfop_normalized} → Chave {idx}: {chave}",
                'chave': chave,
                'descricao': descricao,
                'operacao': mapping.get('operacao', ''),
                'regra': mapping.get('regra', ''),
                'observacao': mapping.get('observacao', ''),
                'cfop_encontrado': danfe_cfop_normalized,
                'status': 'MISMATCH',
                'source': 'DynamoDB',
                'message': f'Mapeamento {idx} de {len(cfop_mappings)} encontrados'
            })
        
        message = f"CFOP {danfe_cfop_normalized} encontrado com MÚLTIPLOS mapeamentos ({len(cfop_mappings)}): {', '.join(chaves_encontradas)}"
        danfe_value = f"{danfe_cfop_normalized} (MÚLTIPLOS MAPEAMENTOS: {len(cfop_mappings)})"
        
        # Salvar todos os mapeamentos encontrados para exibição no frontend
        cfop_data = {
            'cfop': danfe_cfop_normalized,
            'chave': None,  # Não há chave única
            'operacao': None,
            'descricao': None,
            'regra': None,
            'observacao': None,
            'multiple_mappings': True,
            'mappings_count': len(cfop_mappings),
            'mappings': [
                {
                    'chave': m.get('chave', ''),
                    'descricao': m.get('descricao', ''),
                    'operacao': m.get('operacao', ''),
                    'regra': m.get('regra', ''),
                    'observacao': m.get('observacao', '')
                }
                for m in cfop_mappings
            ]
        }
    
    result = {
        'rule': 'validar_cfop_chave',
        'status': 'PASSED' if all_match else 'FAILED',
        'danfe_value': danfe_value,
        'message': message,
        'comparisons': comparisons,
        'corrections': corrections
    }
    
    # Adicionar dados encontrados no nível raiz para facilitar acesso no Protheus
    if cfop_data:
        result['cfop_data'] = cfop_data
    
    return result

