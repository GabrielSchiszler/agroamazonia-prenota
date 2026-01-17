import json
import logging
import boto3

logger = logging.getLogger()

def compare_with_bedrock(value1, value2, field):
    """Usa Bedrock Nova para comparação contextual (cria cliente internamente)"""
    bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
    return _compare_with_bedrock_client(bedrock_client, value1, value2, field)

def _compare_with_bedrock_client(bedrock_client, value1, value2, field):
    """Usa Bedrock Nova para comparação contextual"""
    
    # Prompt específico para nomes de produtos - focado em CONTEXTO
    if 'produto' in field.lower() or 'nome' in field.lower():
        prompt = f"""Compare os seguintes nomes de produtos verificando se são o MESMO CONTEXTO de produto:

Produto 1: {value1}
Produto 2: {value2}

REGRAS IMPORTANTES:
1. MESMO CONTEXTO (validado: true) = Produtos são o MESMO produto, mesmo que:
   - Tenham descrições diferentes (informações adicionais, lote, registro, etc.)
   - Tenham unidades de medida diferentes na descrição (KG, SC, PT, etc.)
   - Tenham formatação diferente (maiúsculas/minúsculas, espaços, etc.)
   - O nome principal/essencial seja o mesmo

2. CONTEXTO DIFERENTE (validado: false) = Produtos são DIFERENTES se:
   - O nome principal/essencial for diferente
   - Mesmo que o nome seja parecido, se o contexto (tipo de produto, categoria) for diferente
   - Exemplo: "SPHERIC PLUS" e "SPHERIC" podem ser parecidos, mas se um é fertilizante e outro é defensivo = DIFERENTE

EXEMPLOS:

EXEMPLO 1 (MESMO CONTEXTO - mesmo produto):
Produto1: SPHERIC PLUS NORTOX N2% Ca6,5% S13,5% B1,7% Cu0,85% Mn4% Zn2,1% 1X25
Produto2: SPHERIC PLUS SC 25 KG (03040012)
RESULTADO: {{"validado": true}}
EXPLICAÇÃO: Ambos são "SPHERIC PLUS" - mesmo contexto de produto, apenas descrições diferentes

EXEMPLO 2 (MESMO CONTEXTO - mesmo produto):
Produto1: PROTAC NORTOX AD 36X0,500
Produto2: PROTAC NORTOX AD PT 500 GR (03100013)
RESULTADO: {{"validado": true}}
EXPLICAÇÃO: Ambos são "PROTAC NORTOX AD" - mesmo contexto, apenas unidades diferentes

EXEMPLO 3 (CONTEXTO DIFERENTE - produtos diferentes):
Produto1: SPHERIC PLUS NORTOX
Produto2: GALIL SC 1X20
RESULTADO: {{"validado": false}}
EXPLICAÇÃO: Produtos completamente diferentes - contextos diferentes

EXEMPLO 4 (NOME PARECIDO MAS CONTEXTO DIFERENTE):
Produto1: SPHERIC PLUS FERTILIZANTE
Produto2: SPHERIC DEFENSIVO
RESULTADO: {{"validado": false}}
EXPLICAÇÃO: Nomes parecidos mas contextos diferentes (fertilizante vs defensivo)

Responda APENAS JSON: {{"validado": true}} ou {{"validado": false}}"""
    else:
        # Prompt genérico para outros campos
        prompt = f"""Compare os seguintes valores do campo '{field}':

Valor 1: {value1}
Valor 2: {value2}

REGRAS:
1. Valores idênticos ou muito próximos (diferença de 1 dígito) são válidos
2. Para CNPJ: "13563680000446" vs "1356368000446" (falta 1 zero) = VALIDO
3. Para códigos: "FWI00002GL00016" vs "FW100002GL00016" (I vs 1) = VALIDO
4. Para descrições: conteúdo principal igual = VALIDO

EXEMPLOS:

EXEMPLO 1 (CNPJ próximo):
Valor1: 13563680000446
Valor2: 1356368000446
RESULTADO: {{"validado": true}}

EXEMPLO 2 (Descrição similar):
Valor1: PRIMER BIO 33 GL 1 LT
Valor2: PRIMER BIO 33 GL LT Reg. do Estab (ET) MT 79796-1
RESULTADO: {{"validado": true}}

EXEMPLO 3 (Totalmente diferente):
Valor1: PRIMER BIO 33 GL 1 LT
Valor2: GALIL SC 1X20
RESULTADO: {{"validado": false}}

Responda APENAS JSON: {{"validado": true}} ou {{"validado": false}}"""
    
    try:
        response = bedrock_client.invoke_model(
            modelId='us.amazon.nova-pro-v1:0',
            body=json.dumps({
                'messages': [{
                    'role': 'user',
                    'content': [{'text': prompt}]
                }],
                'inferenceConfig': {
                    'max_new_tokens': 100,
                    'temperature': 0.1
                }
            })
        )
        
        body_bytes = response['body'].read()
        logger.info(f"Bedrock raw response: {body_bytes}")
        
        if not body_bytes:
            logger.error("Bedrock returned empty response")
            return False
        
        result = json.loads(body_bytes)
        content = result['output']['message']['content'][0]['text'].strip()
        logger.info(f"Bedrock content: {content}")
        
        # Extrair JSON do texto
        if '{' in content:
            json_start = content.index('{')
            json_end = content.rindex('}') + 1
            json_str = content[json_start:json_end]
            validation = json.loads(json_str)
            is_valid = validation.get('validado', False)
            return 'MATCH' if is_valid else 'MISMATCH'
        
        return 'MISMATCH'
    except Exception as e:
        logger.error(f"Bedrock error: {str(e)}")
        return 'MISMATCH'
