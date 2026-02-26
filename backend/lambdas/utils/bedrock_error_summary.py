import json
import os
import boto3
import logging

logger = logging.getLogger()

def generate_error_summary_with_bedrock(error_data):
    """
    Gera uma mensagem amigável de erro usando Bedrock Nova Pro.
    
    Args:
        error_data: Dicionário ou JSON string com os dados completos do erro
        
    Returns:
        str: Mensagem amigável gerada pelo Bedrock, ou None em caso de erro
    """
    try:
        # Converter para string JSON se for dict
        if isinstance(error_data, dict):
            error_json_str = json.dumps(error_data, ensure_ascii=False, indent=2, default=str)
        else:
            error_json_str = str(error_data)
        
        # Bedrock Nova Pro está disponível apenas em us-east-1 por enquanto
        bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        prompt = """Você é um especialista em tradução de erros técnicos de sistemas ERP, APIs e integrações em mensagens claras, detalhadas e amigáveis para usuários finais.

Sua tarefa é analisar o erro técnico fornecido e transformá-lo em uma mensagem completa, detalhada e fácil de entender, incluindo APENAS os dados RELEVANTES para o problema específico.

REGRAS OBRIGATÓRIAS:

1. IDENTIFIQUE O PROBLEMA PRINCIPAL PRIMEIRO:
- Analise o erro e identifique qual é o problema REAL e específico
- Foque APENAS nas informações que são diretamente relacionadas a esse problema
- NÃO inclua informações que não sejam necessárias para entender ou resolver o problema

2. FORMATO DA MENSAGEM (3 parágrafos obrigatórios):

PARÁGRAFO 1 - Explicação detalhada do problema:
- Comece com "Não foi possível..." ou "Não foi possível processar..." ou similar
- Mencione APENAS os dados RELEVANTES para o problema específico:
  * Se o problema é duplicação de nota: mencione número da nota, série, data de emissão. NÃO mencione produtos, lotes, quantidades (não são relevantes)
  * Se o problema é valor divergente: mencione número da nota, série, valores divergentes, pedido e item específico. NÃO mencione todos os produtos se não forem relevantes
  * Se o problema é unidade de medida: mencione número da nota, produto específico com problema, unidade informada, pedido e item. NÃO mencione outros produtos sem problema
  * Se o problema é produto inválido: mencione número da nota, produto específico com problema, pedido e item. NÃO mencione produtos válidos
  * Se o problema é lote inválido: mencione número da nota, produto, lote específico com problema. NÃO mencione lotes válidos
  * Se o problema é fornecedor: mencione número da nota, código do fornecedor. NÃO mencione produtos se não forem relevantes
- Explique o que aconteceu de forma clara e específica
- Se houver valores divergentes, mencione ambos os valores e a diferença calculada
- Se o problema afeta um item específico, mencione apenas esse item. Se afeta a nota toda, mencione apenas dados da nota.

PARÁGRAFO 2 - Explicação do motivo e impacto:
- Explique claramente POR QUE o erro ocorreu
- Explique o impacto do problema (o que isso impede)
- Use linguagem técnica simples, sem jargões internos do sistema

PARÁGRAFO 3 - Orientação detalhada para resolução:
- Comece com "Para resolver" ou "Verifique" ou similar
- Forneça orientações específicas e acionáveis
- Mencione novamente APENAS os dados principais relevantes (número da nota, produto específico com problema, pedido, etc.) para contexto
- Indique o que deve ser verificado ou corrigido
- Sugira a ação específica a ser tomada

3. REGRA DE RELEVÂNCIA (MUITO IMPORTANTE):
- INCLUA informações quando elas são NECESSÁRIAS para entender ou resolver o problema
- NÃO INCLUA informações quando elas NÃO são relevantes para o problema específico
- Exemplos:
  * Problema de duplicação de nota: mencione número, série, data. NÃO mencione produtos, lotes, quantidades
  * Problema de valor divergente em um item: mencione nota, pedido, item específico, valores. NÃO mencione outros itens
  * Problema de unidade de medida: mencione nota, produto específico, unidade. NÃO mencione produtos sem problema
  * Problema de fornecedor inválido: mencione nota, código do fornecedor. NÃO mencione produtos

4. DETALHAMENTO SELETIVO:
- Inclua valores numéricos específicos APENAS quando relevantes (ex: valores divergentes, quantidades problemáticas)
- Mencione números de pedido, itens, produtos, lotes APENAS quando são parte do problema
- Mencione datas quando são relevantes para o problema
- Se houver comparação de valores, mostre ambos os valores e a diferença calculada

5. LINGUAGEM:
- Use linguagem simples e profissional
- Evite termos técnicos internos (tabelas, campos técnicos, stack trace, etc.)
- Use termos que o usuário final entende (nota fiscal, pedido, produto, lote, etc.)

6. NÃO FAÇA:
- NÃO mencione nomes de tabelas técnicas (SD1, SF1, etc.)
- NÃO mencione nomes de campos técnicos (D1_ITEMPC, F1_DOC, cTipo, cFormul, etc.)
- NÃO mencione stack trace, exception, lambda, payload, etc.
- NÃO invente informações que não estejam no erro
- NÃO use códigos de erro técnicos sem explicá-los em linguagem simples
- NÃO liste produtos, itens ou lotes quando não são relevantes para o problema

5. EXEMPLOS DE FORMATO:

Exemplo 1 - Erro de valor divergente:
"Não foi possível salvar a nota fiscal nº 187971, série 001, emitida em 25/02/2026, pois o valor total informado está diferente do valor presente no XML da nota. O valor total informado foi R$ 147.301,06, enquanto o valor correto no XML é R$ 147.300,00, gerando uma diferença de R$ 1,06.

Essa divergência impede o registro da nota, pois o sistema exige que o valor total da nota seja exatamente igual ao valor do XML oficial para garantir a consistência das informações fiscais.

Verifique o valor total da nota fiscal nº 187971 e confirme se ele corresponde exatamente ao valor do XML, especialmente considerando o item do pedido de compra AABRGU, item 0001, produto GFB00001TN10000, com quantidade 50,00 e valor unitário R$ 3.037,1196. Após corrigir o valor total para R$ 147.300,00, tente realizar o lançamento novamente."

Exemplo 2 - Erro de unidade de medida:
"Não foi possível processar o documento nº 348381, série 001, emitido em 04/02/2026, porque a unidade de medida informada no item 0001 do pedido de compra AABYA4 está incompatível com o cadastro do produto. O problema ocorreu no produto DCZ00001FR00506, informado com quantidade 970 KG e valor unitário 108,9398762887, vinculado ao lote 004/25.

Esse erro acontece porque a unidade de medida utilizada (KG) não corresponde à unidade padrão ou a uma unidade de conversão válida para esse produto, ou não está configurada corretamente para esse fornecedor. Por isso, o sistema não consegue validar as quantidades e valores do item.

Para resolver, verifique no cadastro do produto DCZ00001FR00506 qual é a unidade de medida correta e confirme se a unidade KG é válida para esse produto e fornecedor. Caso não seja, ajuste a unidade no documento ou atualize o cadastro do produto para permitir essa unidade antes de tentar novamente."

Exemplo 3 - Erro de nota duplicada (NÃO mencionar produtos):
"Não foi possível salvar a nota fiscal nº 256097, série 001, emitida em 29/09/2025, pois o número de nota fiscal já está cadastrado no sistema. A nota foi informada com chave de acesso 41250975263400000199550010002560971809684919, relacionada ao pedido de compra AABHFY.

Esse erro ocorre porque o sistema não permite duplicar notas fiscais que já foram registradas anteriormente. O código do fornecedor informado (000019) também está sendo marcado como inválido, o que pode indicar um problema adicional no cadastro do fornecedor.

Para resolver, verifique se a nota fiscal nº 256097, série 001, já foi cadastrada anteriormente no sistema. Se sim, não é necessário cadastrá-la novamente. Se não, verifique o cadastro do fornecedor (código 000019) e confirme se está ativo e válido. Após corrigir o cadastro do fornecedor, tente realizar o lançamento novamente."

Entrada (JSON completo do erro):

""" + error_json_str + """

Saída esperada:
Mensagem amigável, completa e detalhada no formato de 3 parágrafos conforme especificado acima, incluindo APENAS os dados relevantes presentes no erro.

IMPORTANTE: Retorne APENAS texto puro, sem formatação markdown, sem asteriscos (**), sem negrito, sem itálico, sem listas com marcadores. Apenas texto simples e direto."""
        
        # Obter modelo ID da variável de ambiente
        model_id = os.environ.get('BEDROCK_MODEL_ID', 'amazon.nova-pro-v1:0')
        response = bedrock_client.invoke_model(
            modelId=model_id,
            body=json.dumps({
                'messages': [{
                    'role': 'user',
                    'content': [{'text': prompt}]
                }],
                'inferenceConfig': {
                    'maxTokens': 2000,
                    'temperature': 0.3,
                    'topP': 0.9
                }
            })
        )
        
        body_bytes = response['body'].read()
        
        if not body_bytes:
            logger.error("Bedrock returned empty response")
            return None
        
        result = json.loads(body_bytes)
        
        # Extrair o texto da resposta
        if 'output' in result and 'message' in result['output']:
            content = result['output']['message'].get('content', [])
            if content and len(content) > 0:
                text_content = content[0].get('text', '')
                # Limpar apenas formatação markdown (asteriscos, negrito, etc.), mantendo quebras de linha
                text_content = text_content.replace('**', '').replace('*', '').replace('__', '').replace('_', '')
                # Retornar texto limpo, mantendo todas as quebras de linha
                return text_content.strip()
        
        logger.warning("Bedrock response format unexpected")
        logger.warning(f"Response structure: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        return None
        
    except Exception as e:
        logger.error(f"Erro ao gerar summary com Bedrock: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

