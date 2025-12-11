import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

SCHEMA_TEMPLATE = {
    "numero_nota": "",
    "serie": "",
    "data_emissao": "",
    "natureza_operacao": "",
    "emitente": {"cnpj": "", "nome": "", "ie": ""},
    "destinatario": {"cnpj": "", "nome": "", "ie": ""},
    "produtos": [{
        "item": "",
        "codigo": "",
        "descricao": "",
        "ncm": "",
        "cfop": "",
        "unidade": "",
        "quantidade": "",
        "valor_unitario": "",
        "valor_total": "",
        "lote": "",
        "data_fabricacao": "",
        "data_validade": "",
        "icms": {"cst": "", "base_calculo": "", "aliquota": "", "valor": ""}
    }],
    "totais": {"valor_produtos": "", "valor_icms": "", "valor_nota": ""}
}

def handler(event, context):
    """Parse OCR para JSON estruturado usando Bedrock Nova"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    pk = f"PROCESS#{process_id}"
    
    # Buscar dados do Textract (apenas docs, não danfe)
    items = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={':pk': pk, ':sk': 'TEXTRACT='}
    )['Items']
    
    results = []
    
    for item in items:
        file_key = item.get('FILE_KEY', '')
        
        # Pular arquivos da pasta danfe
        if '/danfe/' in file_key:
            continue
            
        file_name = item.get('FILE_NAME')
        raw_text = item.get('RAW_TEXT', '')
        tables_data = item.get('TABLES_DATA')
        
        if tables_data:
            tables = json.loads(tables_data)
        else:
            tables = []
        
        logger.info(f"Processing OCR for: {file_name}")
        logger.info(f"RAW_TEXT length: {len(raw_text)}, TABLES count: {len(tables)}")
        
        # Usar tabelas estruturadas + raw_text
        logger.info(f"Processing {len(tables)} tables for {file_name}")
        tables_text = tables_to_structured_text(tables)
        logger.info(f"Tables text length: {len(tables_text)} chars")
        ocr_text = f"{tables_text}\n\n{raw_text}"
        
        logger.info(f"Combined OCR text length: {len(ocr_text)}")
        logger.info(f"OCR text preview: {ocr_text[:500]}")
        
        if not ocr_text.strip() or len(ocr_text.strip()) < 50:
            logger.warning(f"No sufficient OCR data for {file_name}. Text length: {len(ocr_text)}")
            logger.warning(f"Returning empty schema for {file_name}")
            
            # Salvar schema vazio
            sk = f"PARSED_OCR={file_name}"
            table.put_item(Item={
                'PK': pk,
                'SK': sk,
                'FILE_NAME': file_name,
                'PARSED_DATA': json.dumps(SCHEMA_TEMPLATE),
                'SOURCE': 'OCR',
                'ERROR': 'Insufficient OCR data'
            })
            
            results.append({
                'file_name': file_name,
                'parsed_data': SCHEMA_TEMPLATE
            })
            continue
        
        # Usar Bedrock para estruturar
        parsed_data = parse_with_bedrock(ocr_text, file_name)
        
        # Salvar no DynamoDB
        sk = f"PARSED_OCR={file_name}"
        table.put_item(Item={
            'PK': pk,
            'SK': sk,
            'FILE_NAME': file_name,
            'PARSED_DATA': json.dumps(parsed_data),
            'SOURCE': 'OCR'
        })
        
        results.append({
            'file_name': file_name,
            'parsed_data': parsed_data
        })
        
        logger.info(f"OCR parsed successfully for {file_name}")
    
    return {
        'process_id': process_id,
        'process_type': event.get('process_type'),
        'results': results,
        'source': 'OCR'
    }

def tables_to_structured_text(tables):
    """Converte tabelas do Textract para texto estruturado com colunas identificadas"""
    text_parts = []
    
    for idx, table in enumerate(tables):
        rows = table.get('rows', [])
        if not rows:
            logger.info(f"Table {idx + 1}: empty, skipping")
            continue
        
        logger.info(f"Table {idx + 1}: {len(rows)} rows")
        
        text_parts.append(f"\n=== TABELA {idx + 1} ===")
        
        # Primeira linha como cabeçalho
        if len(rows) > 0:
            header = rows[0]
            text_parts.append("COLUNAS: " + " | ".join(header))
            text_parts.append("-" * 80)
            logger.info(f"Table {idx + 1} header ({len(header)} columns): {header}")
        
        # Linhas de dados
        for row_idx, row in enumerate(rows[1:], start=1):
            row_text = " | ".join(row)
            text_parts.append(row_text)
            # Log primeiras 3 linhas de cada tabela
            if row_idx <= 3:
                logger.info(f"Table {idx + 1} row {row_idx}: {row}")
                logger.info(f"Table {idx + 1} row {row_idx} has {len(row)} cells (header has {len(header)} columns)")
                if len(row) != len(header):
                    logger.warning(f"MISMATCH: Row has {len(row)} cells but header has {len(header)}!")
    
    return '\n'.join(text_parts)

def parse_with_bedrock(ocr_text, file_name):
    """Usa Bedrock Nova para estruturar dados do OCR"""
    max_chunk = 15000
    
    if len(ocr_text) <= max_chunk:
        return parse_chunk(ocr_text)
    
    # Split em chunks
    chunks = [ocr_text[i:i+max_chunk] for i in range(0, len(ocr_text), max_chunk)]
    logger.info(f"Splitting into {len(chunks)} chunks")
    
    results = []
    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i+1}/{len(chunks)}")
        result = parse_chunk(chunk)
        results.append(result)
    
    # Merge results
    return merge_results(results)

def parse_chunk(text):
    """Parse um chunk de texto"""
    logger.info(f"=== PARSE_CHUNK START ===")
    logger.info(f"Text length: {len(text)} chars")
    logger.info(f"Text preview (first 1000 chars): {text[:1000]}")
    
    prompt = f'''Você é um sistema de extração fiscal inteligente.
Extraia dados de uma Nota Fiscal com tabelas estruturadas.

DOCUMENTO:
{text}

IMPORTANTE: O documento contém TABELAS com produtos E informações de cabeçalho.
Cada tabela tem colunas identificadas (COLUNAS: ...).
Extraia TODAS as linhas de TODAS as tabelas.
Extraia TODOS os campos do cabeçalho do documento (CNPJ, número pedido, data, fornecedor, etc).

REGRAS IMPORTANTES:

1. Retorne APENAS JSON válido.
2. Não adicione explicações, comentários ou texto fora do JSON.
3. Não invente valores. Se não existir no documento, retorne null.
4. Mantenha sempre o núcleo obrigatório para integração ERP:
- cnpjRemetente
- cnpjDestinatario
- documento (número da nota)
- serie
- dataEmissao
- itens[] (mínimo: item, codigoProduto, descricaoProduto, quantidade, valorTotal)

5. CRÍTICO - Extração de Tabelas:
   - Cada tabela começa com "=== TABELA X ==="
   - Depois vem "COLUNAS: ..." mostrando os nomes das colunas
   - Depois vem linhas separadas por " | "
   - Cada valor está na POSIÇÃO correspondente à coluna
   - Exemplo: se COLUNAS são "Item | Código | Descrição | Qtd | Valor Unit | Valor Total"
     E a linha é: "001 | ABC123 | PRODUTO X | 50 | 100.5 | 5025"
     Então: Item=001, Código=ABC123, Descrição=PRODUTO X, Qtd=50, Valor Unit=100.5, Valor Total=5025
   - Extraia TODAS as linhas de TODAS as tabelas
   - NÃO misture valores de colunas diferentes
   - Continue até processar TODAS as tabelas e TODAS as linhas

6. CRÍTICO - codigoProduto: 
   - Extraia o código COMPLETO do produto da coluna "Código" ou "COD. PROD"
   - SEMPRE remova TODOS os espaços do código
   - Exemplos de transformação:
     * "DU900003GL000 56" → "DU900003GL00056" (remove espaço)
     * "E9000001BD0020 1" → "E9000001BD00201" (remove espaço)
     * "FW100002G L00016" → "FW100002GL00016" (remove espaço)
     * "ELE00002BD0020 0" → "ELE00002BD00200" (remove espaço)
   - Códigos de produto são alfanuméricos e geralmente têm 10-15 caracteres
   - NÃO confunda números soltos na coluna de código com quantidade

EXEMPLO DE EXTRAÇÃO COM MAPEAMENTO CORRETO:

Se o documento tem:

=== TABELA 1 ===
COLUNAS: COD. PROD | DESCRIÇÃO DO PROD./SER. | NCM/SH | CST | CFOP | UN | QUANT. | V.UNITARIO | V.TOTAL
ELE00002BD0020 0 | EXCALIA MAX BD 20 LT | 38089299 | 040 | 5152 | BD | 50,0000 | 4.179,9100 | 208.995,50
E9000001BD0020 1 | 2,4 D TECNOMYL BD 20 LT | 38089322 | 140 | 5152 | BD | 25,0000 | 274,8000 | 6.870,00
DU900003GL000 56 | VIANCE TECNOMYL GL 5 LT | 38089329 | 140 | 5152 | GL | 80,0000 | 135,7200 | 10.857,60

=== TABELA 2 ===
COLUNAS: Item | Código | Descrição | Qtd | Valor Unit | Valor Total
003 | ABC123XYZ | PRODUTO A | 10 | 500 | 5000
004 | DEF456UVW | PRODUTO B | 5 | 500 | 2500
005 | GHI789RST | PRODUTO C | 8 | 400 | 3200

Mapeamento correto (REMOVA ESPAÇOS dos códigos):
{{
  "itens": [
    {{
      "codigoProduto": "ELE00002BD00200",
      "descricaoProduto": "EXCALIA MAX BD 20 LT",
      "unidadeMedida": "BD",
      "quantidade": 50,
      "valorUnitario": 4179.91,
      "valorTotal": 208995.50,
      "ncm": "38089299",
      "cfop": "5152"
    }},
    {{
      "codigoProduto": "E9000001BD00201",
      "descricaoProduto": "2,4 D TECNOMYL BD 20 LT",
      "unidadeMedida": "BD",
      "quantidade": 25,
      "valorUnitario": 274.80,
      "valorTotal": 6870.00,
      "ncm": "38089322",
      "cfop": "5152"
    }},
    {{
      "codigoProduto": "DU900003GL00056",
      "descricaoProduto": "VIANCE TECNOMYL GL 5 LT",
      "unidadeMedida": "GL",
      "quantidade": 80,
      "valorUnitario": 135.72,
      "valorTotal": 10857.60,
      "ncm": "38089329",
      "cfop": "5152"
    }}
  ]
}}

7. EXTRAIA PRIMEIRO OS CAMPOS DO CABEÇALHO:
   - Procure no início do documento por: CNPJ remetente/destinatário, número do pedido,
     data de emissão, fornecedor, filial, condição de pagamento, tipo de frete, etc.
   - Esses campos geralmente aparecem ANTES das tabelas de produtos
   - NÃO retorne apenas os itens, retorne TODOS os campos do cabeçalho também

8. Além dos obrigatórios, extraia TODOS os campos adicionais que encontrar no documento,
mantendo a estrutura abaixo. Se não existir, retorne null ou array vazio.

Estrutura completa desejada (preencha o que encontrar no documento):

{{
"cnpjRemetente": "...",
"cnpjDestinatario": "...",
"filial": "...",
"tipoDeDocumento": "...",
"documento": "...",
"numeroPedido": "...",
"serie": "...",
"dataEmissao": "...",
"dataDigitacao": "...",
"fornecedor": {{
    "codigo": "...",
    "loja": "..."
}},
"especie": "...",
"chaveAcesso": "...",
"condicaoPagamento": "...",
"tipoFrete": "...",
"moeda": "...",
"taxaCambio": "...",
"duplicata": {{
    "natureza": "...",
    "itens": [
    {{
        "parcela": "...",
        "dataVencimento": "...",
        "valor": "..."
    }}
    ]
}},
"itens": [
    {{
    "item": "...",
    "codigoProduto": "...",
    "descricaoProduto": "...",
    "unidadeMedida": "...",
    "armazem": "...",
    "quantidade": "...",
    "valorUnitario": "...",
    "valorTotal": "...",
    "lote": "...",
    "dataFabricacao": "...",
    "dataValidade": "...",
    "codigoOperacao": "...",
    "tes": "...",
    "pedidoDeCompra": {{
        "pedidoFornecedor": "...",
        "pedidoErp": "...",
        "itemPedidoErp": "..."
    }},
    "documentoOrigem": "...",
    "itemDocumentoOrigem": "...",
    "contaContabil": "...",
    "centroCusto": "...",
    "itemContaContabil": "..."
    }}
],
"rateioCentroCusto": [
    {{
    "centroCusto": "...",
    "percentual": "...",
    "valor": "...",
    "contaContabil": "...",
    "itemContaContabil": "...",
    "natureza": "..."
    }}
],
"impostos": {{
    "cabecalho": {{
    "desconto": "...",
    "despesas": "...",
    "frete": "...",
    "seguro": "..."
    }},
    "itens": [
    {{
        "item": "...",
        "baseICMS": "...",
        "aliquotaICMS": "...",
        "valorICMS": "...",
        "baseICMSST": "...",
        "aliquotaICMSST": "...",
        "valorICMSST": "...",
        "valorIPI": "...",
        "valorPIS": "...",
        "valorCOFINS": "...",
        "valorISSQN": "...",
        "valorOutrosImpostos": "..."
    }}
    ]
}}
}}

9. Caso encontre outros campos relevantes não previstos acima (ex: lote, ncm, cfop,
data_fabricacao, validade, frete por item, seguro por item, MAPA, peso),
inclua dentro do item correspondente, SEM excluir nenhum campo.

10. Formate CPF/CNPJ sem símbolos.
11. Datas no formato ISO: YYYY-MM-DD
12. Valores numéricos com ponto decimal.
13. Remova espaços extras de códigos de produto.

IMPORTANTE FINAL:
- Processe TODAS as tabelas do início ao fim
- Extraia TODAS as linhas de cada tabela
- Extraia TODOS os campos de cada linha (unidadeMedida, valorUnitario, valorTotal, ncm, cfop, etc)
- Respeite a POSIÇÃO de cada valor conforme as colunas
- Se houver coluna "Unid" ou "UM", extraia para unidadeMedida
- Se houver coluna "Valor Unit" ou "V.Unit", extraia para valorUnitario
- Se houver coluna "Valor Total" ou "V.Total", extraia para valorTotal
- NÃO omita campos que existem nas colunas
- Continue até não haver mais tabelas ou linhas

Retorne APENAS o JSON.
'''
    
    logger.info(f"Calling Bedrock Nova Pro...")
    
    request_body = {
        'messages': [{
            'role': 'user',
            'content': [{'text': prompt}]
        }],
        'inferenceConfig': {
            'maxTokens': 8000,
            'temperature': 0.1
        }
    }
    
    logger.info(f"Request body: {json.dumps(request_body)[:500]}")
    
    response = bedrock.invoke_model(
        modelId='us.amazon.nova-pro-v1:0',
        body=json.dumps(request_body)
    )
    
    logger.info(f"Bedrock response received")
    logger.info(f"Response keys: {response.keys()}")
    
    body_content = response['body'].read()
    logger.info(f"Body content (first 500): {body_content[:500]}")
    
    result = json.loads(body_content)
    logger.info(f"Response parsed: {json.dumps(result)[:500]}")
    
    content = result['output']['message']['content'][0]['text']
    logger.info(f"Nova Pro response (full): {content}")
    
    start = content.find('{')
    end = content.rfind('}') + 1
    
    logger.info(f"JSON boundaries: start={start}, end={end}")
    
    if start >= 0 and end > start:
        json_str = content[start:end]
        logger.info(f"Extracted JSON string: {json_str[:500]}")
        
        parsed = json.loads(json_str)
        logger.info(f"Successfully parsed JSON: {json.dumps(parsed, indent=2)}")
        return parsed
    
    raise Exception(f"No JSON found in Bedrock response: {content}")

def merge_results(results):
    """Merge múltiplos resultados JSON"""
    merged = SCHEMA_TEMPLATE.copy()
    all_produtos = []
    
    for result in results:
        # Pegar primeiro valor não vazio
        for key in ['numero_nota', 'serie', 'data_emissao', 'natureza_operacao']:
            if not merged[key] and result.get(key):
                merged[key] = result[key]
        
        # Merge emitente/destinatario
        for entity in ['emitente', 'destinatario']:
            if isinstance(result.get(entity), dict):
                for k, v in result[entity].items():
                    if v and not merged[entity].get(k):
                        merged[entity][k] = v
        
        # Acumular produtos
        if result.get('produtos'):
            all_produtos.extend(result['produtos'])
        
        # Merge totais
        if isinstance(result.get('totais'), dict):
            for k, v in result['totais'].items():
                if v and not merged['totais'].get(k):
                    merged['totais'][k] = v
    
    merged['produtos'] = all_produtos
    return merged
