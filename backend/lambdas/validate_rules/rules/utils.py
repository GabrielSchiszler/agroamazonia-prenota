import json
import os
import logging
import boto3

logger = logging.getLogger()


def bedrock_compare_status(result) -> str:
    """Extrai MATCH/MISMATCH do retorno de compare_with_bedrock (dict) ou string legada."""
    if isinstance(result, dict):
        return result.get("status") or "MISMATCH"
    if result in ("MATCH", "MISMATCH"):
        return result
    return "MISMATCH"


def compare_with_bedrock(value1, value2, field, has_equivalent_code=False):
    """Usa Bedrock Nova para comparação contextual (cria cliente internamente).

    Retorno: dict ``{"status": "MATCH"|"MISMATCH", "bedrock": {...}}``.
    Para nomes de produto, ``bedrock`` inclui explicacao, nome_base, volumetria,
    categoria_agronomica, embalagem, detalhes. Use ``bedrock_compare_status()`` para obter só o status.
    """
    # Bedrock Nova Pro está disponível apenas em us-east-1 por enquanto
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
    return _compare_with_bedrock_client(
        bedrock_client, value1, value2, field, has_equivalent_code
    )


def _bedrock_result_from_validation(
    validation: dict, *, is_product: bool
) -> dict:
    """Monta retorno padronizado a partir do JSON do modelo."""
    ok = bool(validation.get("validado"))
    status = "MATCH" if ok else "MISMATCH"
    if is_product:
        detail = {
            "explicacao": validation.get("explicacao") or "",
            "nome_base": validation.get("nome_base") or "",
            "volumetria": validation.get("volumetria") or "",
            "categoria_agronomica": validation.get("categoria_agronomica")
            or validation.get("fungicida_ou_categoria")
            or "",
            "embalagem": validation.get("embalagem") or "",
            "detalhes": validation.get("detalhes") or "",
        }
    else:
        detail = {"explicacao": validation.get("explicacao") or ""}
    return {"status": status, "bedrock": detail}

def _compare_with_bedrock_client(bedrock_client, value1, value2, field, has_equivalent_code=False):
    """Usa Bedrock Nova para comparação contextual
    
    Args:
        bedrock_client: Cliente Bedrock
        value1: Primeiro valor a comparar
        value2: Segundo valor a comparar
        field: Nome do campo sendo comparado
        has_equivalent_code: Se True, indica que os códigos numéricos já foram validados como equivalentes
    """
    
    # Prompt específico para nomes de produtos
    if 'produto' in field.lower() or 'nome' in field.lower():
        # Se os códigos já foram validados como equivalentes, adicionar informação crítica no prompt
        equivalent_code_note = ""
        if has_equivalent_code:
            equivalent_code_note = """
⚠️ ATENÇÃO CRÍTICA - CÓDIGOS NUMÉRICOS EQUIVALENTES:
Os códigos numéricos dos produtos foram VALIDADOS como EQUIVALENTES (mesmo código, apenas separador diferente).
Exemplos: "20.00.20" = "20-00-20", "15.15.15" = "15-15-15"
Quando os códigos numéricos são equivalentes, os produtos são SEMPRE o MESMO produto, mesmo que outras partes do nome sejam diferentes.
Neste caso, você DEVE retornar "validado": true (e preencher os demais campos do JSON de saída) a menos que sejam produtos completamente diferentes (não relacionados).

"""
        
        prompt = f"""Compare os seguintes nomes de produtos:

Produto 1: {value1}
Produto 2: {value2}
{equivalent_code_note}REGRAS:
1. Produtos são o MESMO se tiverem o mesmo nome principal/essencial, mesmo que:
   - Tenham informações adicionais diferentes (código, lote, registro, etc.)
   - Tenham unidades de medida diferentes na descrição (KG, SC, PT, etc.)
   - Tenham tipo de embalagem diferente (galão/GL, bombona, frasco, bidão, etc.) — se o nome base e o volume forem equivalentes
   - Tenham categoria agronômica extra (fungicida, herbicida, inseticida, etc.) que descreve o uso — não use isso sozinho para separar SKU
   - Tenham formatação diferente (maiúsculas/minúsculas, espaços, etc.)
   - Tenham códigos numéricos com separadores diferentes (pontos vs traços)
     Exemplo: "15.15.15" é EQUIVALENTE a "15-15-15"
     Exemplo: "30.00.20" é EQUIVALENTE a "30-00-20"
     Exemplo: "20.00.20" é EQUIVALENTE a "20-00-20"

2. IMPORTANTE - Normalização de códigos numéricos:
   - Códigos numéricos com pontos (.) são EQUIVALENTES aos mesmos códigos com traços (-)
   - "15.15.15" = "15-15-15" = "15 15 15" (mesmo código, apenas separador diferente)
   - "30.00.20" = "30-00-20" = "30 00 20" (mesmo código, apenas separador diferente)
   - "20.00.20" = "20-00-20" = "20 00 20" (mesmo código, apenas separador diferente)
   - Se os códigos numéricos forem iguais (ignorando separadores), os produtos são o MESMO

3. Produtos são DIFERENTES apenas se:
   - O nome principal for completamente diferente
   - Não houver palavras-chave em comum que identifiquem o mesmo produto
   - Os códigos numéricos forem diferentes (mesmo após normalizar separadores)
   - O volume/capacidade for claramente diferente (ex.: 10 L vs 20 L)

4. NOME BASE / MARCA COMERCIAL (âncora forte):
   - Se AMBOS contiverem o mesmo nome distintivo de produto ou marca (ex.: VESSARYA, SPHERIC PLUS, PROTAC), isso é evidência forte de MESMO produto quando o volume for equivalente (regra 5), mesmo com redação diferente na DANFE x pedido.

5. VOLUME E CAPACIDADE (líquidos):
   - Trate como equivalentes o mesmo valor numérico de volume: "10L" = "10 LT" = "10 LITROS" = "10 L" (espaços e maiúsculas ignorados).

FORMATO OBRIGATÓRIO — responda um único objeto JSON (sem markdown, sem texto fora do JSON) com TODOS os campos:
{{
  "validado": true ou false,
  "explicacao": "frase curta com o resumo da decisão",
  "nome_base": "marca/nome principal identificado em ambos, ou divergência/ausente",
  "volumetria": "ex.: 10 L equivalente nos dois lados | 10 L vs 20 L | nao_identificado",
  "categoria_agronomica": "ex.: fungicida, herbicida, nao_se_aplica, so_em_um_lado, indeterminado",
  "embalagem": "resumo por lado (ex.: DANFE: galão/GL; DOC: bombona)",
  "detalhes": "fatores que pesaram: regras 1–5, código NPK, nome base, volume, embalagem"
}}

EXEMPLOS (todos com os 7 campos):

EXEMPLO 1 (MESMO PRODUTO - descrição diferente):
Produto1: SPHERIC PLUS NORTOX N2% Ca6,5% S13,5% B1,7% Cu0,85% Mn4% Zn2,1% 1X25
Produto2: SPHERIC PLUS SC 25 KG (03040012)
RESULTADO:
{{"validado":true,"explicacao":"Mesmo produto SPHERIC PLUS; apenas complementos e embalagem diferentes.","nome_base":"SPHERIC PLUS","volumetria":"nao_identificado","categoria_agronomica":"nao_se_aplica","embalagem":"Produto1: 1X25; Produto2: SC 25 KG","detalhes":"Regra 1: mesmo nome principal/essencial."}}

EXEMPLO 2 (MESMO PRODUTO - unidade diferente):
Produto1: PROTAC NORTOX AD 36X0,500
Produto2: PROTAC NORTOX AD PT 500 GR (03100013)
RESULTADO:
{{"validado":true,"explicacao":"Mesmo PROTAC NORTOX AD; só muda forma de venda/unidade.","nome_base":"PROTAC NORTOX AD","volumetria":"500 g equivalente (36x0,500 vs PT 500 GR)","categoria_agronomica":"nao_se_aplica","embalagem":"36X0,500 vs PT","detalhes":"Regra 1: unidades diferentes mas mesmo produto."}}

EXEMPLO 3 (MESMO PRODUTO - embalagem e categoria; mesmo nome base e volume):
Produto1: VESSARYA GL 10 LT
Produto2: VESSARYA BOMBONA 10L FUNGICIDA
RESULTADO:
{{"validado":true,"explicacao":"Mesmo VESSARYA, 10 L equivalente; GL vs bombona e fungicida não separam SKU.","nome_base":"VESSARYA","volumetria":"10 L equivalente (10 LT e 10L)","categoria_agronomica":"fungicida apenas no texto 2","embalagem":"Produto1: GL; Produto2: bombona","detalhes":"Regras 4–5: âncora VESSARYA + volume igual; regra 1 embalagem diferente."}}

EXEMPLO 4 (MESMO PRODUTO - código com ponto vs traço):
Produto1: 15.15.15 UNI BASE 180 AMIDICO
Produto2: 15-15-15 UNIFERTIL TN 1000 KG
RESULTADO:
{{"validado":true,"explicacao":"Código 15.15.15 equivalente a 15-15-15; mesmo produto.","nome_base":"15.15.15 / 15-15-15","volumetria":"nao_identificado","categoria_agronomica":"nao_se_aplica","embalagem":"textos diferentes após código","detalhes":"Regra 2: normalização de separadores no código NPK."}}

EXEMPLO 5 (MESMO PRODUTO - código com ponto vs traço):
Produto1: 30.00.20 UNI COBERTURA 180
Produto2: 30-00-20 UNIFERTIL TN 1000 KG
RESULTADO:
{{"validado":true,"explicacao":"30.00.20 = 30-00-20; mesmo código NPK.","nome_base":"30.00.20","volumetria":"nao_identificado","categoria_agronomica":"nao_se_aplica","embalagem":"UNI COBERTURA vs TN 1000 KG","detalhes":"Regra 2."}}

EXEMPLO 6 (MESMO PRODUTO - código equivalente validado):
Produto1: FERTILIZANTE 20.00.20
Produto2: 20-00-20 TOCANTINS TN 1000 KG
RESULTADO:
{{"validado":true,"explicacao":"20.00.20 equivalente a 20-00-20; mesmo produto apesar do restante do texto.","nome_base":"código 20-00-20","volumetria":"nao_identificado","categoria_agronomica":"nao_se_aplica","embalagem":"FERTILIZANTE genérico vs TOCANTINS TN","detalhes":"Regra 2 + nota de códigos equivalentes se aplicável."}}

EXEMPLO 7 (PRODUTOS DIFERENTES):
Produto1: SPHERIC PLUS NORTOX
Produto2: GALIL SC 1X20
RESULTADO:
{{"validado":false,"explicacao":"Marcas e linhas de produto distintas, sem nome base comum.","nome_base":"divergente (SPHERIC PLUS vs GALIL)","volumetria":"nao_identificado","categoria_agronomica":"indeterminado","embalagem":"sem equivalência","detalhes":"Regra 3: nomes principais diferentes."}}

EXEMPLO 8 (PRODUTOS DIFERENTES - códigos diferentes):
Produto1: 15.15.15 UNI BASE 180 AMIDICO
Produto2: 20.20.20 UNIFERTIL TN 1000 KG
RESULTADO:
{{"validado":false,"explicacao":"Códigos NPK 15.15.15 vs 20.20.20 são diferentes após normalizar separadores.","nome_base":"divergente por formulação","volumetria":"nao_identificado","categoria_agronomica":"nao_se_aplica","embalagem":"—","detalhes":"Regra 3: códigos numéricos distintos."}}

Responda APENAS o objeto JSON completo com os 7 campos, sem markdown."""
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
RESULTADO: {{"validado": true, "explicacao": "Diferença de um zero; mesmo CNPJ efetivo."}}

EXEMPLO 2 (Descrição similar):
Valor1: PRIMER BIO 33 GL 1 LT
Valor2: PRIMER BIO 33 GL LT Reg. do Estab (ET) MT 79796-1
RESULTADO: {{"validado": true, "explicacao": "Mesmo produto; segundo texto só adiciona registro."}}

EXEMPLO 3 (Totalmente diferente):
Valor1: PRIMER BIO 33 GL 1 LT
Valor2: GALIL SC 1X20
RESULTADO: {{"validado": false, "explicacao": "Produtos e linhas distintas."}}

Responda APENAS JSON com "validado" e "explicacao" (string obrigatória)."""
    
    is_product = "produto" in field.lower() or "nome" in field.lower()
    max_tokens = 600 if is_product else 200

    try:
        model_id = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")
        response = bedrock_client.invoke_model(
            modelId=model_id,
            body=json.dumps(
                {
                    "messages": [
                        {"role": "user", "content": [{"text": prompt}]}
                    ],
                    "inferenceConfig": {
                        "max_new_tokens": max_tokens,
                        "temperature": 0.1,
                    },
                }
            ),
        )

        body_bytes = response["body"].read()
        logger.info(f"Bedrock raw response: {body_bytes}")

        if not body_bytes:
            logger.error("Bedrock returned empty response")
            return {
                "status": "MISMATCH",
                "bedrock": {"explicacao": "Resposta vazia do Bedrock"},
            }

        result = json.loads(body_bytes)
        content = result["output"]["message"]["content"][0]["text"].strip()
        logger.info(f"Bedrock content: {content}")

        if "{" not in content:
            return {
                "status": "MISMATCH",
                "bedrock": {"explicacao": "Resposta sem JSON"},
            }

        json_start = content.index("{")
        json_end = content.rindex("}") + 1
        json_str = content[json_start:json_end]
        validation = json.loads(json_str)
        return _bedrock_result_from_validation(validation, is_product=is_product)

    except Exception as e:
        logger.error(f"Bedrock error: {str(e)}")
        return {
            "status": "MISMATCH",
            "bedrock": {"explicacao": f"Erro: {str(e)}"},
        }
