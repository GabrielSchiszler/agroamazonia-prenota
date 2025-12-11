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
