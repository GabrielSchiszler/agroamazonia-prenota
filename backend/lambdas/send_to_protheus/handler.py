import json
import os
import boto3
import requests
import base64
import random
import html
import urllib.request
import urllib.error
import socket
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
secrets_manager = boto3.client('secretsmanager')

def _env(name: str, default: str | None = None) -> str:
    """Helper para obter variáveis de ambiente com valor padrão opcional"""
    v = os.environ.get(name, default)
    if v is None or v == "":
        if default is not None:
            return default
        raise ValueError(f"Missing env var: {name}")
    return v

def _get_secret(secret_id: str) -> dict:
    """Obtém secret do AWS Secrets Manager"""
    resp = secrets_manager.get_secret_value(SecretId=secret_id)
    if resp.get("SecretString"):
        return json.loads(resp["SecretString"])
    return json.loads(resp["SecretBinary"].decode("utf-8"))

def get_ocr_failure_oauth2_token():
    """
    Obtém token de acesso OAuth2 para API de reporte de falhas OCR.
    Retorna o access_token ou None em caso de erro.
    """
    auth_url = os.environ.get('OCR_FAILURE_AUTH_URL')
    client_id = os.environ.get('OCR_FAILURE_CLIENT_ID')
    client_secret = os.environ.get('OCR_FAILURE_CLIENT_SECRET')
    username = os.environ.get('OCR_FAILURE_USERNAME')
    password = os.environ.get('OCR_FAILURE_PASSWORD')
    
    if not all([auth_url, client_id, client_secret, username, password]):
        print("WARNING: OCR_FAILURE OAuth2 credentials not fully configured")
        return None
    
    try:
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'grant_type': 'password',
            'username': username,
            'password': password
        }
        
        response = requests.post(auth_url, data=data, headers=headers, timeout=60)
        response.raise_for_status()
        
        token_response = response.json()
        access_token = token_response.get('access_token')
        
        return access_token
    except Exception as e:
        print(f"WARNING: Failed to obtain OCR_FAILURE OAuth2 token: {str(e)}")
        return None

def report_protheus_failure_to_sctask(process_id, error_details):
    """
    Reporta falha do Protheus para a API do SCTASK (mesma API usada pelo report_ocr_failure).
    
    Args:
        process_id: ID do processo
        error_details: Dicionário com detalhes do erro, incluindo 'cause' se disponível
    """
    try:
        api_url = os.environ.get('OCR_FAILURE_API_URL')
        if not api_url:
            print("WARNING: OCR_FAILURE_API_URL not configured, skipping SCTASK report")
            return None
        
        # Gerar ID único numérico de 6 dígitos
        id_unico = random.randint(100000, 999999)
        
        # Extrair causa do erro do Protheus
        protheus_cause = error_details.get('cause', [])
        if isinstance(protheus_cause, str):
            protheus_cause = [protheus_cause]
        elif not isinstance(protheus_cause, list):
            protheus_cause = []
        
        # Construir HTML simples com os detalhes do erro
        error_type = error_details.get('error_type', 'UNKNOWN')
        status_code = error_details.get('status_code', 'N/A')
        error_code = error_details.get('error_code', 'N/A')
        error_msg = error_details.get('error_message', error_details.get('error', ''))
        timeout_seconds = error_details.get('timeout_seconds')
        
        # Determinar motivo específico da falha (prioridade: causa do Protheus > error_message > error > tipo de erro)
        motivo_falha = None
        if protheus_cause:
            # Se houver causa do Protheus, usar como motivo principal
            if isinstance(protheus_cause, list) and len(protheus_cause) > 0:
                motivo_falha = str(protheus_cause[0]) if isinstance(protheus_cause[0], str) else str(protheus_cause[0])
            elif isinstance(protheus_cause, str):
                motivo_falha = protheus_cause
        elif error_msg and error_msg.strip() and error_msg != 'Sem mensagem':
            # Usar mensagem de erro específica
            motivo_falha = error_msg
        elif error_details.get('error'):
            # Usar erro genérico se disponível
            motivo_falha = str(error_details.get('error'))
        elif error_type and error_type != 'UNKNOWN':
            # Usar tipo de erro como fallback
            if error_type == 'ReadTimeout' or error_type == 'Timeout':
                motivo_falha = f'Timeout na requisição após {timeout_seconds or 60} segundos'
            elif error_type == 'ConnectionError' or error_type == 'ConnectTimeout':
                motivo_falha = 'Erro ao conectar com a API do Protheus'
            else:
                motivo_falha = f'Erro do tipo: {error_type}'
        else:
            # Último fallback
            motivo_falha = 'Erro desconhecido ao enviar para Protheus'
        
        # Construir descricaoFalha com motivo específico
        if status_code != 'N/A' and status_code >= 400:
            descricao_falha = f"Falha no envio para Protheus (HTTP {status_code}): {motivo_falha[:200]}"
        else:
            descricao_falha = f"Falha no envio para Protheus: {motivo_falha[:200]}"
        
        # Construir HTML simples
        html_parts = []
        html_parts.append('<div>')
        html_parts.append('<h2>Falha no Envio para Protheus</h2>')
        
        # Motivo da falha (sempre presente)
        html_parts.append('<h3>Motivo da Falha</h3>')
        html_parts.append(f'<p><strong>{html.escape(str(motivo_falha))}</strong></p>')
        
        # Informações gerais
        html_parts.append('<h3>Informações Gerais</h3>')
        html_parts.append('<ul>')
        
        if status_code != 'N/A':
            html_parts.append(f'<li><strong>Status HTTP:</strong> {status_code}</li>')
        
        html_parts.append(f'<li><strong>Tipo de Erro:</strong> {html.escape(str(error_type))}</li>')
        
        if error_code != 'N/A':
            html_parts.append(f'<li><strong>Código de Erro:</strong> {html.escape(str(error_code))}</li>')
        
        if timeout_seconds:
            html_parts.append(f'<li><strong>Timeout:</strong> {timeout_seconds} segundos</li>')
        
        html_parts.append('</ul>')
        
        # Causa do Protheus (se disponível e diferente do motivo principal)
        if protheus_cause and len(protheus_cause) > 1:
            html_parts.append('<h3>Causas Adicionais do Erro (Protheus)</h3>')
            html_parts.append('<ol>')
            # Mostrar apenas causas adicionais (pular a primeira que já está no motivo)
            causas_adicionais = protheus_cause[1:] if isinstance(protheus_cause, list) else []
            for causa_item in causas_adicionais:
                causa_text = str(causa_item) if isinstance(causa_item, str) else str(causa_item)
                html_parts.append(f'<li>{html.escape(causa_text)}</li>')
            html_parts.append('</ol>')
        elif protheus_cause and (not motivo_falha or motivo_falha not in str(protheus_cause)):
            # Se a causa não foi usada como motivo principal, mostrar aqui
            html_parts.append('<h3>Causa do Erro (Protheus)</h3>')
            html_parts.append('<ol>')
            for causa_item in (protheus_cause if isinstance(protheus_cause, list) else [protheus_cause]):
                causa_text = str(causa_item) if isinstance(causa_item, str) else str(causa_item)
                html_parts.append(f'<li>{html.escape(causa_text)}</li>')
            html_parts.append('</ol>')
        
        # Mensagem de erro adicional (se diferente do motivo)
        if error_msg and error_msg.strip() and error_msg != motivo_falha and error_msg != 'Sem mensagem':
            html_parts.append('<h3>Mensagem de Erro Adicional</h3>')
            html_parts.append(f'<p>{html.escape(str(error_msg))}</p>')
        
        # Detalhes técnicos (se disponíveis)
        if 'response_body' in error_details:
            html_parts.append('<h3>Detalhes Técnicos</h3>')
            response_body = error_details.get('response_body')
            if isinstance(response_body, dict):
                response_body_str = json.dumps(response_body, indent=2, ensure_ascii=False)
            else:
                response_body_str = str(response_body)
            html_parts.append(f'<pre>{html.escape(response_body_str[:2000])}</pre>')
        
        # Rodapé
        html_parts.append('<hr>')
        html_parts.append(f'<p><strong>Process ID:</strong> <code>{process_id}</code></p>')
        html_parts.append(f'<p><strong>Timestamp:</strong> {datetime.utcnow().isoformat()}</p>')
        
        html_parts.append('</div>')
        
        # detalhes: HTML simples
        detalhes_texto = "".join(html_parts)
        
        payload = {
            "idUnico": id_unico,
            "descricaoFalha": descricao_falha,
            "traceAWS": process_id,
            "detalhes": detalhes_texto  # Texto único, não array
        }
        
        # Obter token OAuth2
        access_token = get_ocr_failure_oauth2_token()
        
        # Preparar headers
        headers = {
            'Content-Type': 'application/json'
        }
        if access_token:
            headers['Authorization'] = f'Bearer {access_token}'
        
        print(f"Reporting Protheus failure to SCTASK API: {api_url}")
        print(f"Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        print(f"Headers: {json.dumps({k: v for k, v in headers.items() if k != 'Authorization'}, indent=2)}")
        print(f"Has Authorization token: {bool(access_token)}")
        
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
            
            # Log detalhado da resposta
            print(f"[SCTASK] Response Status Code: {response.status_code}")
            print(f"[SCTASK] Response Headers: {dict(response.headers)}")
            print(f"[SCTASK] Response Body (raw): {response.text}")
            
            # Tentar parsear JSON se possível
            try:
                api_response = response.json()
                print(f"[SCTASK] Response Body (parsed): {json.dumps(api_response, ensure_ascii=False, indent=2)}")
            except:
                print(f"[SCTASK] Response não é JSON válido")
            
            # Verificar se houve erro HTTP
            response.raise_for_status()
            
            # Se chegou aqui, a resposta foi bem-sucedida
            api_response = response.json() if response.text else {}
            
            # Extrair SCTASK ID da resposta
            # A API retorna: {"result": {"requisicao": "REQ1684015", ...}}
            # Ou pode retornar: {"tarefa": "..."}
            sctask_id = None
            if 'result' in api_response and 'requisicao' in api_response['result']:
                sctask_id = api_response['result']['requisicao']
                print(f"[SCTASK] SCTASK ID extraído de result.requisicao: {sctask_id}")
            elif 'tarefa' in api_response:
                sctask_id = api_response['tarefa']
                print(f"[SCTASK] SCTASK ID extraído de tarefa: {sctask_id}")
            else:
                print(f"[SCTASK] WARNING: Não foi possível extrair SCTASK ID da resposta")
                print(f"[SCTASK] Estrutura da resposta: {list(api_response.keys())}")
                if 'result' in api_response:
                    print(f"[SCTASK] Estrutura de result: {list(api_response['result'].keys()) if isinstance(api_response['result'], dict) else 'N/A'}")
            
        except requests.exceptions.HTTPError as http_err:
            print(f"[SCTASK] HTTP Error: {http_err}")
            print(f"[SCTASK] Status Code: {http_err.response.status_code if http_err.response else 'N/A'}")
            if http_err.response:
                print(f"[SCTASK] Response Headers: {dict(http_err.response.headers)}")
                print(f"[SCTASK] Response Body: {http_err.response.text}")
            raise
        except requests.exceptions.RequestException as req_err:
            print(f"[SCTASK] Request Exception: {req_err}")
            print(f"[SCTASK] Exception type: {type(req_err).__name__}")
            raise
        
        # Atualizar DynamoDB com sctask_id
        if sctask_id:
            try:
                table.update_item(
                    Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
                    UpdateExpression='SET sctask_id = :sctask, updated_at = :timestamp',
                    ExpressionAttributeValues={
                        ':sctask': sctask_id,
                        ':timestamp': datetime.utcnow().isoformat()
                    }
                )
                print(f"SCTASK ID {sctask_id} saved to DynamoDB")
            except Exception as e:
                print(f"WARNING: Failed to save SCTASK ID to DynamoDB: {str(e)}")
        
        return sctask_id
    except requests.exceptions.HTTPError as http_err:
        print(f"[SCTASK] HTTP Error ao reportar falha para SCTASK:")
        print(f"  - Status Code: {http_err.response.status_code if http_err.response else 'N/A'}")
        print(f"  - URL: {api_url}")
        if http_err.response:
            print(f"  - Response Headers: {dict(http_err.response.headers)}")
            print(f"  - Response Body: {http_err.response.text}")
            try:
                error_json = http_err.response.json()
                print(f"  - Response JSON: {json.dumps(error_json, ensure_ascii=False, indent=2)}")
            except:
                pass
        import traceback
        traceback.print_exc()
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"[SCTASK] Request Exception ao reportar falha para SCTASK:")
        print(f"  - Exception type: {type(req_err).__name__}")
        print(f"  - Exception message: {str(req_err)}")
        print(f"  - URL: {api_url}")
        import traceback
        traceback.print_exc()
        return None
    except Exception as e:
        print(f"[SCTASK] Unexpected error ao reportar falha para SCTASK:")
        print(f"  - Exception type: {type(e).__name__}")
        print(f"  - Exception message: {str(e)}")
        print(f"  - URL: {api_url}")
        import traceback
        traceback.print_exc()
        return None

def map_tipo_documento(modelo):
    """
    Mapeia tipo de documento conforme regra de negócio.
    NF-e (modelo 55) → "N"
    """
    if not modelo:
        return "N"
    
    modelo_str = str(modelo).strip()
    if modelo_str == "55":
        return "N"
    # Se for outro modelo, retornar "N" como padrão
    return "N"

def map_especie(modelo):
    """
    Mapeia espécie do documento conforme modelo.
    
    Regras de mapeamento:
    - modelo 55 → "SPED"
    - modelo 65 → "NFCe"
    - outros → "NF" (padrão)
    
    Para adicionar novas regras, adicione novos casos no dicionário especie_map abaixo.
    """
    if not modelo:
        return "NF"
    
    modelo_str = str(modelo).strip()
    
    # Dicionário de mapeamento: modelo -> espécie
    # Fácil de adicionar novas regras aqui
    especie_map = {
        "55": "SPED",  # NF-e → SPED
        "65": "NFCe"   # NFCe → NFCe
    }
    
    # Retornar mapeamento se existir, senão usar padrão
    return especie_map.get(modelo_str, "NF")

def map_serie(serie):
    """
    Mapeia série do documento.
    Converter para string e fazer left pad com zeros (3 dígitos).
    Ex.: 1 → "001"
    """
    if not serie:
        return "001"
    
    # Converter para string e remover espaços
    serie_str = str(serie).strip()
    
    # Se for numérico, fazer left pad com zeros
    if serie_str.isdigit():
        return serie_str.zfill(3)
    
    # Se já for string, tentar fazer pad se possível
    try:
        serie_int = int(serie_str)
        return str(serie_int).zfill(3)
    except (ValueError, TypeError):
        # Se não conseguir converter, retornar como está (limitado a 3 caracteres)
        return serie_str[:3].zfill(3) if len(serie_str) < 3 else serie_str[:3]

def map_data_emissao(data_emissao):
    """
    Mapeia data de emissão.
    Extrair apenas a data no formato YYYY-MM-DD.
    Ex.: 2025-10-24T16:39:00-03:00 → 2025-10-24
    """
    if not data_emissao:
        return ""
    
    data_str = str(data_emissao).strip()
    
    # Se já está no formato YYYY-MM-DD, retornar
    if len(data_str) >= 10 and data_str[4] == '-' and data_str[7] == '-':
        return data_str[:10]
    
    # Se tem 'T' (formato ISO), extrair parte antes do T
    if 'T' in data_str:
        return data_str.split('T')[0]
    
    # Se tem espaço, pegar primeira parte
    if ' ' in data_str:
        return data_str.split(' ')[0]
    
    # Tentar parsear outros formatos
    try:
        from datetime import datetime
        # Tentar formatos comuns
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                dt = datetime.strptime(data_str[:10], fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
    except Exception:
        pass
    
    # Se não conseguir parsear, retornar como está (limitado a 10 caracteres)
    return data_str[:10] if len(data_str) >= 10 else data_str

def map_chave_acesso(chave_acesso, inf_nfe_id=None):
    """
    Mapeia chave de acesso.
    Origem principal: protNFe/infProt/chNFe
    Fallback: infNFe/@Id (remover prefixo NFe)
    Retornar somente os 44 dígitos numéricos.
    """
    # Prioridade 1: chave_acesso direta
    if chave_acesso:
        # Extrair apenas dígitos
        chave_digits = ''.join(filter(str.isdigit, str(chave_acesso)))
        if len(chave_digits) == 44:
            return chave_digits
    
    # Fallback: infNFe/@Id (remover prefixo NFe)
    if inf_nfe_id:
        id_str = str(inf_nfe_id).strip()
        # Remover prefixo "NFe" se existir
        if id_str.startswith('NFe'):
            id_str = id_str[3:]
        # Extrair apenas dígitos
        chave_digits = ''.join(filter(str.isdigit, id_str))
        if len(chave_digits) == 44:
            return chave_digits
    
    # Se não encontrou chave válida, retornar vazio ou o que conseguiu extrair
    return chave_digits if 'chave_digits' in locals() and len(chave_digits) > 0 else ""

def map_tipo_frete(mod_frete):
    """
    Mapeia tipo de frete conforme transp/modFrete.
    0 → "CIF"
    1 → "FOB"
    2 → "TER"
    9 → "SEM"
    """
    if mod_frete is None:
        return "SEM"
    
    mod_frete_str = str(mod_frete).strip()
    tipo_frete_map = {
        '0': 'CIF',
        '1': 'FOB',
        '2': 'TER',
        '9': 'SEM'
    }
    
    return tipo_frete_map.get(mod_frete_str, 'SEM')

def map_moeda(modelo=None, moeda_informada=None):
    """
    Mapeia moeda conforme regra de negócio.
    NF-e nacional → "BRL"
    """
    # Se já foi informada uma moeda, usar ela
    if moeda_informada:
        moeda_str = str(moeda_informada).strip().upper()
        if moeda_str in ['BRL', 'USD', 'EUR', 'GBP']:
            return moeda_str
    
    # Regra de negócio: NF-e nacional → "BRL"
    # Se modelo é 55 ou 65 (NF-e ou NFCe nacional), usar BRL
    if modelo:
        modelo_str = str(modelo).strip()
        if modelo_str in ['55', '65']:
            return "BRL"
    
    # Padrão: BRL
    return "BRL"

def map_taxa_cambio(moeda, taxa_informada=None):
    """
    Mapeia taxa de câmbio conforme regra de negócio.
    moeda = BRL → 1
    Moeda estrangeira → taxa informada externamente
    """
    if not moeda:
        return 1
    
    moeda_str = str(moeda).strip().upper()
    
    # Se for BRL, taxa é sempre 1
    if moeda_str == "BRL":
        return 1
    
    # Se for moeda estrangeira e tem taxa informada, usar ela
    if taxa_informada is not None:
        try:
            taxa = float(taxa_informada)
            return taxa
        except (ValueError, TypeError):
            pass
    
    # Se for moeda estrangeira sem taxa informada, retornar 1 como padrão
    return 1

def extract_lotes_with_ai(info_adicional_text):
    """
    Extrai informações de lotes de um texto usando IA (Bedrock Nova).
    
    Args:
        info_adicional_text: Texto contendo informações adicionais (pode ser de produto ou da NF)
    
    Returns:
        Lista de dicionários com informações de lotes:
        [
            {
                "numero": "xpto",
                "quantidade": 20.0,
                "dataValidade": "2025-12-31",
                "dataFabricacao": "2025-01-15"
            },
            ...
        ]
        Retorna lista vazia se não encontrar lotes.
    """
    if not info_adicional_text or not info_adicional_text.strip():
        return []
    
    print(f"[EXTRACT_LOTES] Extraindo lotes do texto (tamanho: {len(info_adicional_text)} chars)")
    print(f"[EXTRACT_LOTES] Preview do texto: {info_adicional_text[:200]}...")
    
    prompt = f'''Você é um sistema de extração de informações de lotes de produtos a partir de texto livre.

TEXTO COM INFORMAÇÕES ADICIONAIS:
{info_adicional_text}

INSTRUÇÕES:
1. Extraia informações de lote SOMENTE quando houver evidência no texto de que um identificador é um lote/partida/batch E ele estiver associado às datas.
2. Um lote aceito DEVE conter obrigatoriamente:
   - numero (identificador do lote)
   - dataFabricacao
   - dataValidade
   A quantidade é opcional.
3. NÃO invente. NÃO chute. NÃO trate códigos/identificadores genéricos como lote se o texto não indicar isso.
   Exemplos do que NÃO é lote: número de pedido, série/nota, códigos internos, registros, FCI, CNPJ/IE, endereços, referências fiscais, etc.
4. Datas:
   - Retorne datas em YYYY-MM-DD.
   - Se o texto trouxer DD/MM/YYYY, converta.
   - Se a validade estiver em meses (ex: "18 MESES"), só retorne dataValidade se existir dataFabricacao para calcular; caso contrário, NÃO inclua o lote.
5. Se houver múltiplos lotes, retorne TODOS os lotes válidos.
6. Se não houver lote válido com (numero + dataFabricacao + dataValidade), retorne lista vazia.

3. FORMATOS COMUNS DE LOTE:
   - "LOTE:331/25" → numero: "331/25"
   - "LOTE:331/25 FABRIC:06/12/2025" → numero: "331/25", dataFabricacao: "2025-12-06"
   - "LOTE:331/25 FABRIC:06/12/2025 VALID:18 MESES" → numero: "331/25", dataFabricacao: "2025-12-06", dataValidade: calcular 18 meses a partir da fabricação
   - "LOTE:331/25 VALID:2026-12-06" → numero: "331/25", dataValidade: "2026-12-06"
   - "LOTE 12345 QTD: 20.0" → numero: "12345", quantidade: 20.0

4. Se houver múltiplos lotes no mesmo texto, extraia TODOS.
5. Se houver apenas um lote, extraia apenas esse lote.
6. Se não houver informações de lote, retorne uma lista vazia.
7. Se a validade estiver em meses (ex: "VALID:18 MESES"), calcule a data de validade somando os meses à data de fabricação.
8. Se a data estiver no formato DD/MM/YYYY, converta para YYYY-MM-DD.

FORMATO DE RESPOSTA (JSON):
{{
  "lotes": [
    {{
      "numero": "331/25",
      "quantidade": 40.0,
      "dataValidade": "2027-06-06",
      "dataFabricacao": "2025-12-06"
    }}
  ]
}}

REGRAS IMPORTANTES:
- Retorne APENAS JSON válido, sem explicações ou comentários.
- Se não encontrar lotes, retorne: {{"lotes": []}}
- Quantidade deve ser um número (float).
- Datas devem estar no formato YYYY-MM-DD.
- Se não conseguir determinar dataFabricacao OU dataValidade de um lote, NÃO inclua o lote na lista.
- Não inclua itens com numero vazio.

EXEMPLOS DE EXTRAÇÃO:
- "LOTE:331/25 FABRIC:06/12/2025 VALID:18 MESES" → {{"numero": "331/25", "dataFabricacao": "2025-12-06", "dataValidade": "2027-06-06", "quantidade": null}}
- "LOTE:123 QTD: 20.0" → {{"numero": "123", "quantidade": 20.0, "dataFabricacao": null, "dataValidade": null}}
- "LOTE:ABC/2025 FABRIC:15/01/2025 VALID:12 MESES" → {{"numero": "ABC/2025", "dataFabricacao": "2025-01-15", "dataValidade": "2026-01-15", "quantidade": null}}

Retorne APENAS o JSON.
'''
    
    try:
        request_body = {
            'messages': [{
                'role': 'user',
                'content': [{'text': prompt}]
            }],
            'inferenceConfig': {
                'maxTokens': 2000,
                'temperature': 0.1
            }
        }
        
        print(f"[EXTRACT_LOTES] Chamando Bedrock Nova Pro...")
        response = bedrock.invoke_model(
            modelId='us.amazon.nova-pro-v1:0',
            body=json.dumps(request_body)
        )
        
        body_content = response['body'].read()
        result = json.loads(body_content)
        content = result['output']['message']['content'][0]['text']
        
        print(f"[EXTRACT_LOTES] Resposta do Bedrock: {content[:500]}...")
        
        # Extrair JSON da resposta
        start = content.find('{')
        end = content.rfind('}') + 1
        
        if start == -1 or end == 0:
            print(f"[EXTRACT_LOTES] ERRO: Não foi possível encontrar JSON na resposta")
            return []
        
        json_str = content[start:end]
        parsed = json.loads(json_str)
        
        lotes = parsed.get('lotes', [])
        print(f"[EXTRACT_LOTES] {len(lotes)} lote(s) extraído(s)")
        
        # Validar e normalizar lotes (aceitar somente com numero + dataFabricacao + dataValidade)
        lotes_validos = []
        for lote in lotes:
            if not lote.get('numero'):
                print(f"[EXTRACT_LOTES] WARNING: Lote sem número, ignorando: {lote}")
                continue
            
            # Processar data de validade se for string com meses
            data_validade = lote.get('dataValidade')
            data_fabricacao = lote.get('dataFabricacao')
            
            # Normalizar data de fabricação se estiver em formato DD/MM/YYYY
            if isinstance(data_fabricacao, str) and '/' in data_fabricacao and len(data_fabricacao) == 10:
                try:
                    from datetime import datetime
                    dt = datetime.strptime(data_fabricacao, '%d/%m/%Y')
                    data_fabricacao = dt.strftime('%Y-%m-%d')
                    print(f"[EXTRACT_LOTES] Data de fabricação normalizada: {lote.get('dataFabricacao')} → {data_fabricacao}")
                except ValueError:
                    pass
            
            # Se dataValidade contém "MESES", tentar calcular baseado na data de fabricação
            if isinstance(data_validade, str) and 'MESES' in data_validade.upper():
                # Tentar extrair número de meses
                import re
                meses_match = re.search(r'(\d+)\s*MESES?', data_validade.upper())
                if meses_match and data_fabricacao:
                    try:
                        meses = int(meses_match.group(1))
                        # Parsear data de fabricação
                        from datetime import datetime, timedelta
                        if isinstance(data_fabricacao, str):
                            # Tentar diferentes formatos de data
                            dt_fabricacao = None
                            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                                try:
                                    dt_fabricacao = datetime.strptime(data_fabricacao, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if dt_fabricacao:
                                # Calcular data de validade adicionando meses
                                # Aproximação: adicionar dias (30 dias por mês)
                                # Melhor seria usar calendar, mas esta aproximação funciona bem
                                ano = dt_fabricacao.year
                                mes = dt_fabricacao.month
                                dia = dt_fabricacao.day
                                
                                # Adicionar meses
                                mes += meses
                                while mes > 12:
                                    mes -= 12
                                    ano += 1
                                
                                # Ajustar dia se necessário (ex: 31 de janeiro + 1 mês = 28/29 de fevereiro)
                                try:
                                    dt_validade = datetime(ano, mes, dia)
                                except ValueError:
                                    # Se o dia não existe no mês (ex: 31 de janeiro -> 28/29 de fevereiro)
                                    # Usar o último dia do mês
                                    from calendar import monthrange
                                    ultimo_dia = monthrange(ano, mes)[1]
                                    dt_validade = datetime(ano, mes, min(dia, ultimo_dia))
                                
                                data_validade = dt_validade.strftime('%Y-%m-%d')
                                print(f"[EXTRACT_LOTES] Data de validade calculada: {data_fabricacao} + {meses} meses = {data_validade}")
                    except (ValueError, AttributeError) as e:
                        print(f"[EXTRACT_LOTES] Erro ao calcular data de validade: {e}")
                        data_validade = None
            
            lote_valido = {
                'numero': str(lote.get('numero', '')).strip(),
                'quantidade': float(lote.get('quantidade', 0)) if lote.get('quantidade') else None,
                'dataValidade': data_validade if data_validade else None,
                'dataFabricacao': data_fabricacao if data_fabricacao else None
            }

            # Regra: só precisa ter o número do lote para ser considerado válido
            if not lote_valido['numero']:
                print(f"[EXTRACT_LOTES] Ignorando (sem número do lote): {lote_valido}")
                continue

            # Remover campos nulos para não enviar chaves vazias no payload
            lote_final = {'numero': lote_valido['numero']}
            if lote_valido['quantidade'] is not None:
                lote_final['quantidade'] = lote_valido['quantidade']
            if lote_valido['dataValidade'] is not None:
                lote_final['dataValidade'] = lote_valido['dataValidade']
            if lote_valido['dataFabricacao'] is not None:
                lote_final['dataFabricacao'] = lote_valido['dataFabricacao']

            lotes_validos.append(lote_final)
            print(f"[EXTRACT_LOTES] Lote aceito: numero={lote_final.get('numero')}, qtd={lote_final.get('quantidade', 'N/A')}, fab={lote_final.get('dataFabricacao', 'N/A')}, valid={lote_final.get('dataValidade', 'N/A')}")
        
        return lotes_validos
        
    except Exception as e:
        print(f"[EXTRACT_LOTES] ERRO ao extrair lotes com IA: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def convert_rastros_to_lotes(rastros):
    """
    Converte rastros do XML para o formato de lotes esperado.
    
    Args:
        rastros: Lista de rastros do XML parseado, cada um com:
            - lote: número do lote (nLote do XML)
            - data_fabricacao: data de fabricação (dFab do XML)
            - data_validade: data de validade (dVal do XML)
            - quantidade: quantidade do lote (qLote do XML)
    
    Returns:
        Lista de lotes no formato esperado:
        [
            {
                'numero': '...',
                'dataFabricacao': 'YYYY-MM-DD',
                'dataValidade': 'YYYY-MM-DD',
                'quantidade': float ou None
            },
            ...
        ]
    """
    if not rastros:
        return []
    
    lotes = []
    for rastro in rastros:
        if not rastro:
            continue
        
        # Extrair número do lote (campo 'lote' do JSON parseado)
        lote_numero = rastro.get('lote')
        if not lote_numero:
            print(f"[CONVERT_RASTROS] WARNING: Rastro sem número de lote, ignorando: {rastro}")
            continue
        
        # Normalizar data de fabricação (campo 'data_fabricacao' do JSON parseado)
        data_fabricacao = rastro.get('data_fabricacao')
        if data_fabricacao:
            # Tentar normalizar formato de data (pode vir como DD/MM/YYYY ou YYYY-MM-DD)
            try:
                from datetime import datetime
                if '/' in data_fabricacao and len(data_fabricacao) == 10:
                    dt = datetime.strptime(data_fabricacao, '%d/%m/%Y')
                    data_fabricacao = dt.strftime('%Y-%m-%d')
                elif len(data_fabricacao) == 10 and '-' in data_fabricacao:
                    # Já está no formato YYYY-MM-DD
                    pass
            except ValueError:
                print(f"[CONVERT_RASTROS] WARNING: Data de fabricação inválida: {data_fabricacao}")
                data_fabricacao = None
        
        # Normalizar data de validade (campo 'data_validade' do JSON parseado)
        data_validade = rastro.get('data_validade')
        if data_validade:
            # Tentar normalizar formato de data
            try:
                from datetime import datetime
                if '/' in data_validade and len(data_validade) == 10:
                    dt = datetime.strptime(data_validade, '%d/%m/%Y')
                    data_validade = dt.strftime('%Y-%m-%d')
                elif len(data_validade) == 10 and '-' in data_validade:
                    # Já está no formato YYYY-MM-DD
                    pass
            except ValueError:
                print(f"[CONVERT_RASTROS] WARNING: Data de validade inválida: {data_validade}")
                data_validade = None
        
        # Normalizar quantidade (campo 'quantidade' do JSON parseado)
        quantidade = rastro.get('quantidade')
        if quantidade:
            try:
                quantidade = float(quantidade)
            except (ValueError, TypeError):
                quantidade = None
        else:
            quantidade = None
        
        lote = {
            'numero': str(lote_numero).strip(),
            'dataFabricacao': data_fabricacao,
            'dataValidade': data_validade,
            'quantidade': quantidade
        }
        
        lotes.append(lote)
        print(f"[CONVERT_RASTROS] Rastro convertido: lote={lote['numero']}, qtd={lote['quantidade']}, fab={lote['dataFabricacao']}, valid={lote['dataValidade']}")
    
    return lotes

def process_produtos_with_lotes(produtos_filtrados, xml_data, request_body_data):
    """
    Processa produtos e faz split quando houver múltiplos lotes.
    
    Ordem de prioridade para buscar lotes:
    1. PRIORIDADE 1: Campo rastros do produto (XML estruturado)
    2. PRIORIDADE 2: info_adicional do produto (passar pela IA)
    3. PRIORIDADE 3: info_adicional da NF (passar pela IA)
    
    Args:
        produtos_filtrados: Lista de tuplas (idx_xml, produto_xml, pedido_de_compra, codigo_produto_rb)
        xml_data: Dados do XML parseado
        request_body_data: Dados do requestBody
    
    Returns:
        Lista de produtos processados, com split quando necessário:
        [
            {
                'produto_xml': {...},
                'pedido_de_compra': {...},
                'codigo_produto': '...',
                'quantidade': 20.0,
                'lote': {
                    'numero': 'xpto',
                    'dataValidade': '2025-12-31',
                    'dataFabricacao': '2025-01-15'
                }
            },
            ...
        ]
    """
    produtos_processados = []
    info_adicional_nf = xml_data.get('info_adicional', '') or ''
    
    print(f"\n[PROCESS_LOTES] Processando {len(produtos_filtrados)} produto(s) para extrair lotes...")
    
    for original_idx, produto_xml, pedido_de_compra, codigo_produto_rb in produtos_filtrados:
        print(f"\n[PROCESS_LOTES] Produto {original_idx + 1}: {produto_xml.get('descricao', 'N/A')[:50]}...")
        print(f"[PROCESS_LOTES.DEBUG] Dados recebidos:")
        print(f"  - original_idx: {original_idx}")
        print(f"  - codigo_produto_rb: {codigo_produto_rb}")
        print(f"  - pedido_de_compra (tipo): {type(pedido_de_compra)}")
        print(f"  - pedido_de_compra (valor): {pedido_de_compra}")
        if pedido_de_compra and isinstance(pedido_de_compra, dict):
            print(f"    - pedidoErp: {pedido_de_compra.get('pedidoErp', 'N/A')}")
            print(f"    - itemPedidoErp: {pedido_de_compra.get('itemPedidoErp', 'N/A')}")
        
        lotes = []
        
        # PRIORIDADE 1: Verificar rastros do produto (XML estruturado)
        rastros = produto_xml.get('rastro')
        if rastros:
            print(f"[PROCESS_LOTES] PRIORIDADE 1: Verificando rastros do produto (XML estruturado)")
            print(f"[PROCESS_LOTES] {len(rastros) if isinstance(rastros, list) else 1} rastro(s) encontrado(s)")
            lotes = convert_rastros_to_lotes(rastros if isinstance(rastros, list) else [rastros])
            print(f"[PROCESS_LOTES] {len(lotes)} lote(s) extraído(s) dos rastros")
        
        # PRIORIDADE 2: Se não encontrou nos rastros, verificar info_adicional do produto (IA)
        if not lotes:
            info_adicional_produto = produto_xml.get('info_adicional', '') or ''
            if info_adicional_produto and info_adicional_produto.strip():
                print(f"[PROCESS_LOTES] PRIORIDADE 2: Verificando info_adicional do produto com IA (tamanho: {len(info_adicional_produto)} chars)")
                lotes = extract_lotes_with_ai(info_adicional_produto)
                print(f"[PROCESS_LOTES] {len(lotes)} lote(s) encontrado(s) no produto via IA")
        
        # PRIORIDADE 3: Se não encontrou no produto, verificar info_adicional da NF (IA)
        if not lotes and info_adicional_nf and info_adicional_nf.strip():
            print(f"[PROCESS_LOTES] PRIORIDADE 3: Verificando info_adicional da NF com IA (tamanho: {len(info_adicional_nf)} chars)")
            lotes = extract_lotes_with_ai(info_adicional_nf)
            print(f"[PROCESS_LOTES] {len(lotes)} lote(s) encontrado(s) na NF via IA")
        
        quantidade_total = float(produto_xml.get('quantidade', 0))
        
        # Se não encontrou lotes, adicionar produto sem lote
        if not lotes:
            print(f"[PROCESS_LOTES] Nenhum lote encontrado, adicionando produto sem lote")
            print(f"[PROCESS_LOTES.DEBUG] Adicionando produto processado:")
            print(f"  - pedido_de_compra (tipo): {type(pedido_de_compra)}")
            print(f"  - pedido_de_compra (valor): {pedido_de_compra}")
            produtos_processados.append({
                'produto_xml': produto_xml,
                'pedido_de_compra': pedido_de_compra,
                'codigo_produto': codigo_produto_rb,
                'quantidade': quantidade_total,
                'lote': None
            })
            continue
        
        # Se encontrou lotes, fazer split do produto
        print(f"[PROCESS_LOTES] {len(lotes)} lote(s) encontrado(s), fazendo split do produto")
        
        # Se há apenas 1 lote e não tem quantidade específica, usar quantidade total
        if len(lotes) == 1:
            lote = lotes[0]
            if not lote.get('quantidade') or lote['quantidade'] == 0:
                lote['quantidade'] = quantidade_total
                print(f"[PROCESS_LOTES] Lote único sem quantidade, usando quantidade total: {quantidade_total}")
            
            produtos_processados.append({
                'produto_xml': produto_xml,
                'pedido_de_compra': pedido_de_compra,
                'codigo_produto': codigo_produto_rb,
                'quantidade': lote['quantidade'],
                'lote': {
                    'numero': lote['numero'],
                    'dataValidade': lote.get('dataValidade'),
                    'dataFabricacao': lote.get('dataFabricacao')
                }
            })
            print(f"[PROCESS_LOTES] Produto split em 1 item: qtd={lote['quantidade']}, lote={lote['numero']}")
        
        # Se há múltiplos lotes, criar um item para cada lote
        else:
            quantidade_distribuida = 0
            for i, lote in enumerate(lotes):
                qtd_lote = lote.get('quantidade', 0) or 0
                
                # Se o último lote não tem quantidade, usar o restante
                if i == len(lotes) - 1 and qtd_lote == 0:
                    qtd_lote = quantidade_total - quantidade_distribuida
                    print(f"[PROCESS_LOTES] Último lote sem quantidade, usando restante: {qtd_lote}")
                
                if qtd_lote > 0:
                    produtos_processados.append({
                        'produto_xml': produto_xml,
                        'pedido_de_compra': pedido_de_compra,
                        'codigo_produto': codigo_produto_rb,
                        'quantidade': qtd_lote,
                        'lote': {
                            'numero': lote['numero'],
                            'dataValidade': lote.get('dataValidade'),
                            'dataFabricacao': lote.get('dataFabricacao')
                        }
                    })
                    quantidade_distribuida += qtd_lote
                    print(f"[PROCESS_LOTES] Item {i+1}/{len(lotes)}: qtd={qtd_lote}, lote={lote['numero']}")
            
            # Validar se a soma das quantidades dos lotes bate com a quantidade total
            if abs(quantidade_distribuida - quantidade_total) > 0.01:
                print(f"[PROCESS_LOTES] WARNING: Soma das quantidades dos lotes ({quantidade_distribuida}) != quantidade total ({quantidade_total})")
    
    print(f"\n[PROCESS_LOTES] Total de produtos processados: {len(produtos_processados)} (incluindo splits)")
    return produtos_processados

def lambda_handler(event, context):
    print("="*80)
    print("SEND TO PROTHEUS - INICIO")
    print("="*80)
    print(f"Event recebido: {json.dumps(event, default=str)}")
    
    process_id = event['process_id']
    print(f"\n[1] Process ID: {process_id}")
    
    # Buscar dados do processo no DynamoDB
    print(f"\n[2] Consultando DynamoDB com PK=PROCESS#{process_id}")
    response = table.query(
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={':pk': f'PROCESS#{process_id}'}
    )
    
    print(f"[2.1] Total de items retornados: {len(response['Items'])}")
    print(f"[2.2] SKs encontrados: {[item['SK'] for item in response['Items']]}")
    
    items = {item['SK']: item for item in response['Items']}
    metadata = items.get('METADATA', {})
    
    print(f"\n[3] Metadata encontrado: {bool(metadata)}")
    if metadata:
        print(f"[3.1] Metadata keys: {list(metadata.keys())}")
        print(f"[3.2] Status: {metadata.get('STATUS')}")
        print(f"[3.3] Process Type: {metadata.get('PROCESS_TYPE')}")
    
    # Buscar JSON de entrada (novo formato com header e requestBody)
    input_json = None
    tenant_id = None
    pedido_compra_json = None
    
    # PRIORIDADE 1: Buscar metadados do pedido de compra (SK: PEDIDO_COMPRA_METADATA)
    print(f"\n[3.4] Buscando metadados do pedido de compra (PRIORIDADE 1)...")
    pedido_compra_item = items.get('PEDIDO_COMPRA_METADATA')
    if pedido_compra_item:
        file_metadata = pedido_compra_item.get('METADADOS')
        if file_metadata:
            try:
                if isinstance(file_metadata, str):
                    file_metadata = json.loads(file_metadata)
                
                # Verificar se é um pedido de compra (tem header e requestBody)
                if isinstance(file_metadata, dict) and ('header' in file_metadata or 'requestBody' in file_metadata):
                    pedido_compra_json = file_metadata
                    input_json = file_metadata  # Usar como input_json também
                    print(f"[3.4.1] Pedido de compra encontrado em PEDIDO_COMPRA_METADATA")
                    print(f"[3.4.2] Pedido de compra tem header: {bool(pedido_compra_json.get('header'))}")
                    print(f"[3.4.3] Pedido de compra tem requestBody: {bool(pedido_compra_json.get('requestBody'))}")
                    
                    # Extrair tenantId
                    if pedido_compra_json.get('header'):
                        tenant_id = pedido_compra_json['header'].get('tenantId') or pedido_compra_json['header'].get('tenant_id')
                        if tenant_id:
                            print(f"[3.4.4] tenantId encontrado no pedido de compra: {tenant_id}")
            except Exception as e:
                print(f"[3.4] ERRO ao processar metadados do pedido de compra: {e}")
    
    # PRIORIDADE 1.5: Buscar nos arquivos (METADADOS) como fallback
    if not pedido_compra_json:
        print(f"\n[3.4.5] Buscando pedido de compra nos arquivos do processo (fallback)...")
        for sk, item in items.items():
            if sk.startswith('FILE#'):
                file_metadata = item.get('METADADOS')
                if file_metadata:
                    try:
                        if isinstance(file_metadata, str):
                            file_metadata = json.loads(file_metadata)
                        
                        # Verificar se é um pedido de compra (tem header e requestBody)
                        if isinstance(file_metadata, dict) and ('header' in file_metadata or 'requestBody' in file_metadata):
                            pedido_compra_json = file_metadata
                            input_json = file_metadata  # Usar como input_json também
                            print(f"[3.4.6] Pedido de compra encontrado no arquivo: {item.get('FILE_NAME')} (fallback)")
                            print(f"[3.4.7] Pedido de compra tem header: {bool(pedido_compra_json.get('header'))}")
                            print(f"[3.4.8] Pedido de compra tem requestBody: {bool(pedido_compra_json.get('requestBody'))}")
                            
                            # Extrair tenantId
                            if pedido_compra_json.get('header'):
                                tenant_id = pedido_compra_json['header'].get('tenantId') or pedido_compra_json['header'].get('tenant_id')
                                if tenant_id:
                                    print(f"[3.4.9] tenantId encontrado no pedido de compra: {tenant_id}")
                            break
                    except Exception as e:
                        print(f"[3.4.5] ERRO ao processar metadados do arquivo {item.get('FILE_NAME')}: {e}")
    
    # PRIORIDADE 2: Buscar INPUT_JSON nos metadados do processo
    if not input_json:
        print(f"\n[3.5] Buscando INPUT_JSON nos metadados do processo (PRIORIDADE 2)...")
        input_json_str = None
        for key in ['INPUT_JSON', 'REQUEST_BODY', 'input_json', 'request_body']:
            if key in metadata:
                input_json_str = metadata.get(key)
                print(f"[3.5.1] INPUT_JSON encontrado na chave '{key}'")
                break
        
            try:
                if isinstance(input_json_str, str):
                    input_json = json.loads(input_json_str)
                else:
                    input_json = input_json_str
                
                print(f"[3.5.2] Input JSON parseado com sucesso")
                print(f"[3.5.3] Tipo do input_json: {type(input_json)}")
                print(f"[3.5.4] Keys do input_json: {list(input_json.keys()) if isinstance(input_json, dict) else 'N/A'}")
                
                # Extrair tenantId do header
                if input_json.get('header'):
                    header = input_json.get('header', {})
                    tenant_id = header.get('tenantId') or header.get('tenant_id') or header.get('TENANT_ID')
                    if tenant_id:
                        print(f"[3.5.5] tenantId encontrado no header: {tenant_id}")
            except Exception as e:
                print(f"[3.5] ERRO ao parsear INPUT_JSON: {e}")
                import traceback
                traceback.print_exc()
    
    # Se não encontrou pedido_compra_json mas tem input_json, usar input_json
    if not pedido_compra_json and input_json:
        pedido_compra_json = input_json
        print(f"[3.6] Usando input_json como pedido_compra_json")
    
    # Log final do que foi encontrado
    if input_json:
        print(f"\n[3.7] RESUMO - Dados encontrados:")
        print(f"[3.7.1] input_json disponível: {bool(input_json)}")
        print(f"[3.7.2] pedido_compra_json disponível: {bool(pedido_compra_json)}")
        print(f"[3.7.3] tenant_id: {tenant_id}")
        
        request_body = input_json.get('requestBody', {})
        print(f"[3.7.4] requestBody keys: {list(request_body.keys())}")
        print(f"[3.7.5] requestBody.cnpjEmitente: {request_body.get('cnpjEmitente')}")
        print(f"[3.7.6] requestBody.cnpjDestinatario: {request_body.get('cnpjDestinatario')}")
        print(f"[3.7.7] requestBody.itens: {len(request_body.get('itens', []))} itens")
    else:
        print(f"\n[3.7] AVISO: Nenhum input_json encontrado!")
    
    # Fallback: tentar buscar tenantId nos metadados do processo
    if not tenant_id:
        tenant_id = metadata.get('TENANT_ID') or metadata.get('tenantId') or metadata.get('tenant_id')
        if tenant_id:
            print(f"[3.6] tenantId encontrado nos metadados do processo: {tenant_id}")
        else:
            print(f"[3.6] WARNING: tenantId NÃO encontrado em nenhuma fonte!")
    
    # Se não encontrou pedido_compra_json mas tem input_json, usar input_json
    if not pedido_compra_json and input_json:
        pedido_compra_json = input_json
    
    # Buscar dados do CFOP e produtos validados da validação (buscar no último registro de validação)
    cfop_mapping = {}
    matched_danfe_positions = []  # Posições dos produtos que deram match na validação (fallback)
    product_matches = []  # Lista de matches: (danfe_position, doc_position) dos resultados de validação
    validation_items = [(sk, item) for sk, item in items.items() if sk.startswith('VALIDATION#')]
    if validation_items:
        # Ordenar por timestamp (mais recente primeiro)
        validation_items.sort(key=lambda x: x[1].get('TIMESTAMP', 0), reverse=True)
        # Pegar o mais recente
        latest_validation = validation_items[0][1]
        
        # Buscar CFOP_MAPPING
        cfop_mapping_str = latest_validation.get('CFOP_MAPPING', '')
        if cfop_mapping_str:
            try:
                cfop_mapping = json.loads(cfop_mapping_str)
                print(f"\n[3.5] CFOP Mapping encontrado: {json.dumps(cfop_mapping)}")
            except Exception as e:
                print(f"[3.5] ERRO ao parsear CFOP_MAPPING: {e}")
        else:
            print(f"[3.5] CFOP_MAPPING não encontrado no registro de validação")
        
        # Buscar VALIDATION_RESULTS para obter produtos que deram match
        validation_results_str = latest_validation.get('VALIDATION_RESULTS', '[]')
        try:
            validation_results = json.loads(validation_results_str) if isinstance(validation_results_str, str) else validation_results_str
            print(f"\n[3.6] Resultados de validação encontrados: {len(validation_results)} regras")
            
            # Buscar resultado da regra validar_produtos
            for rule_result in validation_results:
                if rule_result.get('rule') == 'validar_produtos':
                    # Extrair matches dos comparisons
                    comparisons = rule_result.get('comparisons', [])
                    for comparison in comparisons:
                        comparison_items = comparison.get('items', [])  # Renomeado para não sobrescrever 'items' do escopo superior
                        for item_detail in comparison_items:
                            if item_detail.get('status') == 'MATCH':
                                danfe_pos = item_detail.get('danfe_position')
                                doc_pos = item_detail.get('doc_position')
                                if danfe_pos is not None and doc_pos is not None:
                                    product_matches.append((danfe_pos, doc_pos))
                                    print(f"[3.6.1] Match encontrado: DANFE pos {danfe_pos} → DOC pos {doc_pos}")
                    
                    # Fallback: usar matched_danfe_positions se não encontrou nos items
                    if not product_matches:
                        matched_positions = rule_result.get('matched_danfe_positions', [])
                        if matched_positions:
                            matched_danfe_positions = matched_positions
                            print(f"[3.6.2] Usando matched_danfe_positions: {len(matched_danfe_positions)} produtos (posições DANFE: {matched_danfe_positions})")
                        else:
                            print(f"[3.6.2] Nenhum produto deu match na validação")
                    else:
                        print(f"[3.6.2] Total de matches encontrados: {len(product_matches)}")
                    break
        except Exception as e:
            print(f"[3.6] ERRO ao parsear VALIDATION_RESULTS: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"[3.5] Nenhum registro de validação encontrado")
    
    # Buscar PARSED_XML
    parsed_xml = None
    for sk, item in items.items():
        if sk.startswith('PARSED_XML'):
            parsed_xml = item
            print(f"\n[4] PARSED_XML encontrado com SK: {sk}")
            break
    
    if not parsed_xml:
        print("[4] AVISO: PARSED_XML não encontrado!")
        parsed_xml = {}
    
    # Buscar PARSED_OCR
    parsed_ocr = None
    for sk, item in items.items():
        if sk.startswith('PARSED_OCR'):
            parsed_ocr = item
            print(f"\n[5] PARSED_OCR encontrado com SK: {sk}")
            break
    
    if not parsed_ocr:
        print("[5] AVISO: PARSED_OCR não encontrado!")
        parsed_ocr = {}
    
    # Extrair dados parseados
    print(f"\n[6] Extraindo dados parseados...")
    
    xml_data = {}
    if parsed_xml and 'PARSED_DATA' in parsed_xml:
        try:
            xml_data = json.loads(parsed_xml['PARSED_DATA'])
            print(f"[6.1] XML data carregado com sucesso")
            print(f"[6.2] XML data keys: {list(xml_data.keys())}")
        except Exception as e:
            print(f"[6.1] ERRO ao parsear XML data: {e}")
    else:
        print(f"[6.1] XML data não disponível")
    
    ocr_data = {}
    if parsed_ocr and 'PARSED_DATA' in parsed_ocr:
        try:
            ocr_data = json.loads(parsed_ocr['PARSED_DATA'])
            print(f"[6.3] OCR data carregado com sucesso")
            print(f"[6.4] OCR data keys: {list(ocr_data.keys())}")
        except Exception as e:
            print(f"[6.3] ERRO ao parsear OCR data: {e}")
    else:
        print(f"[6.3] OCR data não disponível")
    
    # Montar payload para Protheus
    print(f"\n[7] Montando payload para Protheus...")
    
    # Extrair header do input JSON (novo formato)
    header = {}
    request_body_data = {}
    
    # Usar pedido_compra_json se disponível, senão usar input_json
    json_source = pedido_compra_json if pedido_compra_json else input_json
    
    if json_source:
        header = json_source.get('header', {})
        request_body_data = json_source.get('requestBody', {})
        source_name = "pedido_compra_json" if pedido_compra_json else "input_json"
        print(f"[7.0] Usando novo formato com header e requestBody (fonte: {source_name})")
        print(f"[7.0.1] json_source keys: {list(json_source.keys())}")
        print(f"[7.0.2] Header: {json.dumps(header, default=str, indent=2)}")
        print(f"[7.0.3] RequestBody type: {type(request_body_data)}")
        print(f"[7.0.4] RequestBody keys: {list(request_body_data.keys()) if isinstance(request_body_data, dict) else 'N/A'}")
        print(f"[7.0.5] RequestBody tem cnpjEmitente: {bool(request_body_data.get('cnpjEmitente'))} = {request_body_data.get('cnpjEmitente')}")
        print(f"[7.0.6] RequestBody tem cnpjDestinatario: {bool(request_body_data.get('cnpjDestinatario'))} = {request_body_data.get('cnpjDestinatario')}")
        print(f"[7.0.7] RequestBody tem itens: {len(request_body_data.get('itens', []))} itens")
        if request_body_data.get('itens'):
            print(f"[7.0.8] Primeiro item completo: {json.dumps(request_body_data['itens'][0], default=str, indent=2)}")
    else:
        print(f"[7.0] Usando formato antigo (sem header)")
        print(f"[7.0.1] input_json disponível: {bool(input_json)}")
        print(f"[7.0.2] pedido_compra_json disponível: {bool(pedido_compra_json)}")
        print(f"[7.0.3] json_source disponível: {bool(json_source)}")
    
    # Extrair dados do XML (fallback se não tiver requestBody)
    emitente = xml_data.get('emitente', {})
    destinatario = xml_data.get('destinatario', {})
    entrega = xml_data.get('entrega', {})
    totais = xml_data.get('totais', {})
    produtos_xml = xml_data.get('produtos', [])
    cobranca = xml_data.get('cobranca', {})
    transporte = xml_data.get('transporte', {})
    
    # Extrair dados do XML para aplicar regras de mapeamento
    modelo = xml_data.get('modelo', '')
    serie_xml = xml_data.get('serie', '')
    data_emissao_xml = xml_data.get('data_emissao', '')
    chave_acesso_xml = xml_data.get('chave_acesso', '')
    modalidade_frete = transporte.get('modalidade_frete', '')
    
    # Aplicar regras de mapeamento
    tipo_documento = map_tipo_documento(modelo)
    especie = map_especie(modelo)
    serie = map_serie(serie_xml)
    data_emissao = map_data_emissao(data_emissao_xml)
    chave_acesso = map_chave_acesso(chave_acesso_xml)
    tipo_frete = map_tipo_frete(modalidade_frete)
    
    # Moeda e taxa de câmbio
    moeda_informada = request_body_data.get('moeda') or ocr_data.get('moeda')
    moeda = map_moeda(modelo, moeda_informada)
    taxa_cambio = map_taxa_cambio(moeda)
    
    print(f"[7.2] Campos mapeados:")
    print(f"  tipoDeDocumento: {tipo_documento} (modelo: {modelo})")
    print(f"  especie: {especie} (modelo: {modelo})")
    print(f"  serie: {serie} (original: {serie_xml})")
    print(f"  dataEmissao: {data_emissao} (original: {data_emissao_xml})")
    print(f"  chaveAcesso: {chave_acesso[:20]}... (original: {chave_acesso_xml[:20] if chave_acesso_xml else 'N/A'}...)")
    print(f"  tipoFrete: {tipo_frete} (modFrete: {modalidade_frete})")
    print(f"  moeda: {moeda} (modelo: {modelo}, informada: {moeda_informada})")
    print(f"  taxaCambio: {taxa_cambio}")
    
    # Montar payload com APENAS os campos especificados
    # Extrair número do documento do campo codigo_nf do XML e garantir que tenha pelo menos 9 dígitos (com zeros à esquerda)
    numero_documento = xml_data.get('numero_nota', '').strip()
    if not numero_documento:
        # Fallback para numero_nota se codigo_nf não existir
        numero_documento = xml_data.get('numero_nota', '').strip()
 
   
    
    # Extrair CNPJ ou CPF do emitente - APENAS do XML
    print(f"\n[7.3] Extraindo CNPJ/CPF do emitente (APENAS do XML)...")
    cnpj_emitente = None
    cpf_emitente = None
    ie_emitente = None
    
    # Buscar CNPJ primeiro, depois CPF
    if emitente and emitente.get('cnpj'):
        cnpj_emitente = emitente.get('cnpj')
        # Normalizar CNPJ (apenas dígitos)
        cnpj_emitente = ''.join(filter(str.isdigit, str(cnpj_emitente)))
        print(f"  [7.3.1] CNPJ encontrado no XML emitente.cnpj: {cnpj_emitente} ({len(cnpj_emitente)} dígitos)")
    elif emitente and emitente.get('cpf'):
        cpf_emitente = emitente.get('cpf')
        # Normalizar CPF (apenas dígitos)
        cpf_emitente = ''.join(filter(str.isdigit, str(cpf_emitente)))
        print(f"  [7.3.1] CPF encontrado no XML emitente.cpf: {cpf_emitente} ({len(cpf_emitente)} dígitos)")
        
        # Quando for CPF, também buscar IE (Inscrição Estadual)
        ie_emitente_raw = emitente.get('ie') if emitente else None
        if ie_emitente_raw:
            ie_emitente = str(ie_emitente_raw).strip()
            if ie_emitente:
                print(f"  [7.3.2] IE encontrado no XML emitente.ie: {ie_emitente}")
            else:
                ie_emitente = None
                print(f"  [7.3.2] IE encontrado mas está vazio no XML emitente")
        else:
            ie_emitente = None
            print(f"  [7.3.2] IE não disponível no emitente")
    else:
        print(f"  [7.3.2] CNPJ/CPF do emitente não encontrado no XML - campo não será incluído no payload")
        if emitente:
            print(f"    - emitente.cnpj: {emitente.get('cnpj')}")
            print(f"    - emitente.cpf: {emitente.get('cpf')}")

    # # Extrair CNPJ destinatário - tentar múltiplas fontes
    # print(f"\n[7.4] Extraindo CNPJ do destinatário...")
    # cnpj_destinatario = None

    # # OBRIGATÓRIO: Pegar APENAS do XML destinatário.cnpj
    # if destinatario and destinatario.get('cnpj'):
    #     cnpj_destinatario = destinatario.get('cnpj')
    #     print(f"  [7.3.1] CNPJ encontrado no XML destinatario.cnpj: {cnpj_destinatario}")
    # else:
    #     print(f"  [7.3.2] ERRO: CNPJ do destinatário NÃO encontrado no XML!")
    #     print(f"    - destinatário disponível: {bool(destinatário)}")
    #     print(f"    - destinatário.cnpj: {destinatário.get('cnpj') if destinatário else 'N/A'}")
    #     print(f"    - XML data keys: {list(xml_data.keys()) if xml_data else 'N/A'}")
    #     # raise Exception(f"CNPJ do destinatário é obrigatório e deve estar no XML (destinatário.cnpj). CNPJ não encontrado no XML parseado.")
    
    # # Normalizar CNPJ destinatário (apenas dígitos)
    # if cnpj_destinatario:
    #     cnpj_destinatario = ''.join(filter(str.isdigit, str(cnpj_destinatario)))
    #     print(f"  [7.4.4] CNPJ destinatário normalizado (apenas dígitos): {cnpj_destinatario}")
    #     print(f"  [7.4.5] CNPJ destinatário length: {len(cnpj_destinatario)} dígitos")
        
    #     # Validar se tem 14 dígitos (CNPJ válido)
    #     if len(cnpj_destinatario) != 14:
    #         print(f"  [7.4.6] WARNING: CNPJ destinatário não tem 14 dígitos! Pode estar incompleto.")
    # else:
    #     print(f"  [7.4.7] WARNING: CNPJ do destinatário NÃO encontrado em nenhuma fonte!")
    #     print(f"    - XML destinatario.cnpj: {destinatario.get('cnpj') if destinatario else 'N/A'}")
    #     cnpj_destinatario = ""  # Manter como string vazia (destinatário pode ser opcional)
    
    payload = {
        "tipoDeDocumento": tipo_documento,
        "documento": numero_documento,
        "serie": serie,
        "dataEmissao": data_emissao,
        "especie": especie,
        "chaveAcesso": chave_acesso,
        "tipoFrete": tipo_frete,
        "moeda": moeda,
        "taxaCambio": taxa_cambio,
        "itens": []
    }
    
    # Incluir cnpjEmitente ou cpfEmitente apenas se existir
    if cnpj_emitente:
        payload["cnpjEmitente"] = cnpj_emitente
    elif cpf_emitente:
        payload["cpfEmitente"] = cpf_emitente
        # Quando for CPF, incluir IE se disponível
        if ie_emitente:
            payload["ieEmitente"] = ie_emitente
            print(f"[7.1.1.1] IE do emitente incluído: {ie_emitente}")
    
    print(f"\n[7.1] Payload base montado")
    if cnpj_emitente:
        print(f"[7.1.1] CNPJ do emitente incluído: {cnpj_emitente} ({len(cnpj_emitente)} dígitos)")
    elif cpf_emitente:
        print(f"[7.1.1] CPF do emitente incluído: {cpf_emitente} ({len(cpf_emitente)} dígitos)")
        if ie_emitente:
            print(f"[7.1.1.2] IE do emitente incluído: {ie_emitente}")
        else:
            print(f"[7.1.1.2] IE do emitente: não informado (campo não incluído no payload)")
    else:
        print(f"[7.1.1] CNPJ/CPF do emitente: não informado (campo não incluído no payload)")
    # if cnpj_destinatario:
    #     print(f"[7.1.2] CNPJ do destinatário incluído: {cnpj_destinatario} ({len(cnpj_destinatario)} dígitos)")
    # else:
    #     print(f"[7.1.2] CNPJ do destinatário: não informado (opcional)")
    
    # Adicionar produtos (APENAS os que deram match na validação)
    print(f"\n[8] Processando produtos...")
    
    # LOG DETALHADO: Mostrar conteúdo completo do requestBody['itens']
    if request_body_data and request_body_data.get('itens'):
        print(f"[8.0] DEBUG: Conteúdo completo do requestBody['itens']:")
        for idx, item_rb in enumerate(request_body_data['itens'], 1):
            print(f"[8.0.{idx}] Item {idx} completo:")
            print(f"  - codigoProduto: {item_rb.get('codigoProduto', 'N/A')}")
            print(f"  - produto: {item_rb.get('produto', 'N/A')}")
            print(f"  - pedidoDeCompra (tipo): {type(item_rb.get('pedidoDeCompra'))}")
            print(f"  - pedidoDeCompra (valor): {item_rb.get('pedidoDeCompra')}")
            if item_rb.get('pedidoDeCompra'):
                pedido = item_rb.get('pedidoDeCompra')
                if isinstance(pedido, dict):
                    print(f"    - pedidoErp: {pedido.get('pedidoErp', 'N/A')}")
                    print(f"    - itemPedidoErp: {pedido.get('itemPedidoErp', 'N/A')}")
                else:
                    print(f"    - pedidoDeCompra não é dict: {pedido}")
            print(f"  - Todas as chaves do item: {list(item_rb.keys())}")
    
    # LOG DETALHADO: Mostrar produtos XML disponíveis
    if produtos_xml:
        print(f"[8.0.XML] DEBUG: Produtos XML disponíveis ({len(produtos_xml)} produtos):")
        for idx, produto_xml in enumerate(produtos_xml, 1):
            print(f"[8.0.XML.{idx}] Produto XML {idx}:")
            print(f"  - descricao: {produto_xml.get('descricao', 'N/A')[:50]}")
            print(f"  - codigo: {produto_xml.get('codigo', 'N/A')}")
            print(f"  - quantidade: {produto_xml.get('quantidade', 'N/A')}")
    
    # LOG DETALHADO: Mostrar product_matches e matched_danfe_positions
    print(f"[8.0.MATCHES] DEBUG: Matches da validação:")
    print(f"  - product_matches (tipo): {type(product_matches)}")
    print(f"  - product_matches (valor): {product_matches}")
    print(f"  - matched_danfe_positions (tipo): {type(matched_danfe_positions)}")
    print(f"  - matched_danfe_positions (valor): {matched_danfe_positions}")
    
    # Se tiver requestBody com itens, usar eles; senão usar XML
    produtos_para_processar = []
    if request_body_data and request_body_data.get('itens'):
        produtos_para_processar = request_body_data['itens']
        print(f"[8.1] Usando produtos do requestBody: {len(produtos_para_processar)} produtos")
    elif produtos_xml:
        produtos_para_processar = produtos_xml
        print(f"[8.1] Usando produtos do XML: {len(produtos_para_processar)} produtos")
    else:
        print(f"[8.1] AVISO: Nenhum produto encontrado!")
    
    # Se houver produtos que deram match na validação, filtrar apenas esses
    # IMPORTANTE: Usar dados do XML (quantidade, valor unitário, código) + pedidoDeCompra do requestBody
    produtos_filtrados = []
    if product_matches:
        print(f"[8.2] Filtrando produtos usando matches da validação: {len(product_matches)} matches")
        
        # Para cada match, pegar produto do XML (danfe_position) e pedidoDeCompra do requestBody (doc_position)
        for danfe_pos, doc_pos in product_matches:
            print(f"[8.2.1] Processando match: DANFE pos {danfe_pos} → DOC pos {doc_pos}")
            
            # Buscar produto do XML pela posição DANFE
            produto_xml = None
            if produtos_xml:
                idx_xml = danfe_pos - 1  # Converter para 0-based
                if 0 <= idx_xml < len(produtos_xml):
                    produto_xml = produtos_xml[idx_xml]
                    print(f"[8.2.2] Produto XML encontrado na posição {danfe_pos}: {produto_xml.get('descricao', 'N/A')[:50]}...")
                else:
                    print(f"[8.2.2] ERRO: Posição DANFE {danfe_pos} (índice {idx_xml}) fora do range dos produtos XML ({len(produtos_xml)} produtos)")
            
            # Buscar item completo do requestBody pela posição DOC (para pegar código e pedidoDeCompra)
            item_request_body = None
            pedido_de_compra = None
            codigo_produto_rb = None
            if request_body_data and request_body_data.get('itens'):
                idx_doc = doc_pos - 1  # Converter para 0-based
                print(f"[8.2.3.DEBUG] Buscando item no requestBody:")
                print(f"  - doc_pos (1-based): {doc_pos}")
                print(f"  - idx_doc (0-based): {idx_doc}")
                print(f"  - Total de itens no requestBody: {len(request_body_data['itens'])}")
                if 0 <= idx_doc < len(request_body_data['itens']):
                    item_request_body = request_body_data['itens'][idx_doc]
                    print(f"[8.2.3.DEBUG] Item encontrado no índice {idx_doc}:")
                    print(f"  - Todas as chaves: {list(item_request_body.keys())}")
                    print(f"  - Item completo: {json.dumps(item_request_body, default=str, indent=2)}")
                    
                    pedido_de_compra_raw = item_request_body.get('pedidoDeCompra')
                    print(f"[8.2.3.DEBUG] pedidoDeCompra_raw (tipo): {type(pedido_de_compra_raw)}")
                    print(f"[8.2.3.DEBUG] pedidoDeCompra_raw (valor): {pedido_de_compra_raw}")
                    
                    if pedido_de_compra_raw:
                        if isinstance(pedido_de_compra_raw, dict):
                            pedido_de_compra = pedido_de_compra_raw
                        elif isinstance(pedido_de_compra_raw, str):
                            try:
                                pedido_de_compra = json.loads(pedido_de_compra_raw)
                                print(f"[8.2.3.DEBUG] pedidoDeCompra parseado de JSON string")
                            except:
                                pedido_de_compra = {}
                                print(f"[8.2.3.DEBUG] ERRO ao parsear pedidoDeCompra como JSON string")
                        else:
                            pedido_de_compra = {}
                            print(f"[8.2.3.DEBUG] pedidoDeCompra_raw não é dict nem string, usando dict vazio")
                    else:
                        pedido_de_compra = {}
                        print(f"[8.2.3.DEBUG] pedidoDeCompra_raw é None/False/vazio, usando dict vazio")
                    
                    codigo_produto_rb = item_request_body.get('codigoProduto', '').strip()
                    print(f"[8.2.3] Item do requestBody encontrado na posição DOC {doc_pos}:")
                    print(f"[8.2.3.1] Código do produto: {codigo_produto_rb}")
                    print(f"[8.2.3.2] pedidoDeCompra (final): {pedido_de_compra}")
                    if pedido_de_compra and isinstance(pedido_de_compra, dict):
                        print(f"[8.2.3.2.1] pedidoDeCompra.pedidoErp: {pedido_de_compra.get('pedidoErp', 'N/A')}")
                        print(f"[8.2.3.2.2] pedidoDeCompra.itemPedidoErp: {pedido_de_compra.get('itemPedidoErp', 'N/A')}")
                else:
                    print(f"[8.2.3] ERRO: Posição DOC {doc_pos} (índice {idx_doc}) fora do range dos itens do requestBody ({len(request_body_data['itens'])} itens)")
            
            # Se encontrou produto XML e item do requestBody, adicionar
            if produto_xml and item_request_body:
                idx_xml = danfe_pos - 1
                produtos_filtrados.append((idx_xml, produto_xml, pedido_de_compra, codigo_produto_rb))
                print(f"[8.2.4] ✅ Match adicionado: XML pos {danfe_pos} + requestBody pos {doc_pos} (código: {codigo_produto_rb})")
            else:
                print(f"[8.2.4] ❌ Match não adicionado: produto_xml={bool(produto_xml)}, item_request_body={bool(item_request_body)}")
        
        print(f"[8.2.5] Total de produtos filtrados (que deram match): {len(produtos_filtrados)}")
    elif matched_danfe_positions:
        # Fallback: usar matched_danfe_positions se product_matches estiver vazio
        print(f"[8.2] Usando matched_danfe_positions como fallback: {len(matched_danfe_positions)} produtos")
        print(f"[8.2.1] Posições DANFE que deram match: {matched_danfe_positions}")
        
        # Pegar produtos do XML nas posições que deram match
        if produtos_xml:
            for pos in matched_danfe_positions:
                idx = pos - 1  # Converter para 0-based
                if 0 <= idx < len(produtos_xml):
                    produto_xml = produtos_xml[idx]
                    # Tentar encontrar pedidoDeCompra pelo nome
                    nome_xml = produto_xml.get('descricao', '').strip() or produto_xml.get('nome', '').strip()
                    pedido_de_compra = None
                    codigo_produto_rb = None
                    
                    if request_body_data and request_body_data.get('itens'):
                        for item_rb in request_body_data['itens']:
                            nome_rb = item_rb.get('produto', '').strip()
                            if nome_xml and nome_rb:
                                nome_xml_norm = ' '.join(nome_xml.upper().split())
                                nome_rb_norm = ' '.join(nome_rb.upper().split())
                                palavras_xml = set(w for w in nome_xml_norm.split() if len(w) > 2)
                                palavras_rb = set(w for w in nome_rb_norm.split() if len(w) > 2)
                                palavras_comuns = palavras_xml.intersection(palavras_rb)
                                
                                if len(palavras_comuns) >= 2 or nome_xml_norm in nome_rb_norm or nome_rb_norm in nome_xml_norm:
                                    pedido_de_compra = item_rb.get('pedidoDeCompra', {})
                                    codigo_produto_rb = item_rb.get('codigoProduto', '').strip()
                                    print(f"[8.2.2.1] Código do produto encontrado no requestBody: {codigo_produto_rb}")
                                    break
                    
                    produtos_filtrados.append((idx, produto_xml, pedido_de_compra, codigo_produto_rb))
                    print(f"[8.2.2] Produto na posição DANFE {pos} adicionado (pedidoDeCompra: {bool(pedido_de_compra)}, código: {codigo_produto_rb})")
        
        print(f"[8.2.3] Total de produtos filtrados (fallback): {len(produtos_filtrados)}")
    else:
        # Se não houver produtos que deram match, usar todos (comportamento antigo)
        print(f"[8.2] AVISO: Nenhum produto deu match na validação, mas continuando com todos os produtos")
        produtos_filtrados = [(i, p, None, None) for i, p in enumerate(produtos_para_processar)]
    
    # LOG DETALHADO: Mostrar produtos_filtrados antes de processar lotes
    print(f"\n[8.2.6] DEBUG: produtos_filtrados antes de processar lotes ({len(produtos_filtrados)} produtos):")
    for idx, (idx_xml, produto_xml, pedido_de_compra, codigo_produto_rb) in enumerate(produtos_filtrados, 1):
        print(f"[8.2.6.{idx}] Produto filtrado {idx}:")
        print(f"  - idx_xml: {idx_xml}")
        print(f"  - produto_xml.descricao: {produto_xml.get('descricao', 'N/A')[:50]}")
        print(f"  - codigo_produto_rb: {codigo_produto_rb}")
        print(f"  - pedido_de_compra (tipo): {type(pedido_de_compra)}")
        print(f"  - pedido_de_compra (valor): {pedido_de_compra}")
        if pedido_de_compra and isinstance(pedido_de_compra, dict):
            print(f"    - pedidoErp: {pedido_de_compra.get('pedidoErp', 'N/A')}")
            print(f"    - itemPedidoErp: {pedido_de_compra.get('itemPedidoErp', 'N/A')}")
    
    # Processar produtos com lotes (fazer split se necessário)
    print(f"\n[8.3] Processando produtos com extração de lotes...")
    produtos_processados = process_produtos_with_lotes(produtos_filtrados, xml_data, request_body_data)
    
    # LOG DETALHADO: Mostrar produtos_processados após processar lotes
    print(f"[8.3.1] DEBUG: produtos_processados após processar lotes ({len(produtos_processados)} produtos):")
    for idx, produto_info in enumerate(produtos_processados, 1):
        print(f"[8.3.1.{idx}] Produto processado {idx}:")
        print(f"  - codigo_produto: {produto_info.get('codigo_produto', 'N/A')}")
        print(f"  - produto_xml.descricao: {produto_info.get('produto_xml', {}).get('descricao', 'N/A')[:50]}")
        print(f"  - pedido_de_compra (tipo): {type(produto_info.get('pedido_de_compra'))}")
        print(f"  - pedido_de_compra (valor): {produto_info.get('pedido_de_compra')}")
        if produto_info.get('pedido_de_compra') and isinstance(produto_info.get('pedido_de_compra'), dict):
            pedido = produto_info.get('pedido_de_compra')
            print(f"    - pedidoErp: {pedido.get('pedidoErp', 'N/A')}")
            print(f"    - itemPedidoErp: {pedido.get('itemPedidoErp', 'N/A')}")
    
    # Processar produtos processados (já com split de lotes se necessário)
    for produto_info in produtos_processados:
        idx = len(payload['itens']) + 1  # Índice sequencial no payload
        try:
            # Extrair dados do produto processado
            produto_xml = produto_info['produto_xml']
            pedido_de_compra = produto_info['pedido_de_compra']
            codigo_produto = produto_info['codigo_produto']
            quantidade = produto_info['quantidade']
            lote = produto_info.get('lote')
            
            # Usar dados do XML (valor unitário, unidade_trib)
            valor_unitario = float(produto_xml.get('valor_unitario', 0))
            unidade_trib = produto_xml.get('unidade_trib', '').strip()
            
            # Se não encontrou código do produto, tentar buscar novamente
            if not codigo_produto:
                if request_body_data and request_body_data.get('itens'):
                    # Buscar pelo pedidoDeCompra
                    for item_rb in request_body_data['itens']:
                        item_pedido = item_rb.get('pedidoDeCompra', {})
                        if item_pedido and pedido_de_compra:
                            if (item_pedido.get('pedidoErp') == pedido_de_compra.get('pedidoErp') and
                                item_pedido.get('itemPedidoErp') == pedido_de_compra.get('itemPedidoErp')):
                                codigo_produto = item_rb.get('codigoProduto', '').strip()
                                print(f"[8.{idx}.1] Código do produto encontrado no requestBody (por pedidoDeCompra): {codigo_produto}")
                                break
                
                # Se ainda não encontrou, usar código do XML como fallback (mas não é o ideal)
                if not codigo_produto:
                    codigo_xml = produto_xml.get('codigo', '').strip()
                    if codigo_xml.isdigit():
                        codigo_produto = codigo_xml.lstrip('0') or '0'
                    else:
                        codigo_produto = codigo_xml
                    print(f"[8.{idx}.2] AVISO: Usando código do XML como fallback: {codigo_produto}")
            
            print(f"[8.{idx}] Processando produto:")
            print(f"[8.{idx}.3] Código do produto: {codigo_produto}")
            print(f"[8.{idx}.4] Nome: {produto_xml.get('descricao', 'N/A')[:50]}...")
            print(f"[8.{idx}.5] Quantidade: {quantidade}")
            print(f"[8.{idx}.6] Valor unitário: {valor_unitario}")
            print(f"[8.{idx}.7] Lote: {lote}")
            print(f"[8.{idx}.8] pedidoDeCompra: {pedido_de_compra}")
            
            # Variável para armazenar codigoOperacao vindo do pedido de compra (prioridade sobre CFOP mapping)
            codigo_operacao_from_metadata = None
            
            # Tentar buscar codigoOperacao do item no requestBody pelo código do produto
            if request_body_data and request_body_data.get('itens'):
                for item_rb_check in request_body_data['itens']:
                    codigo_rb_check = item_rb_check.get('codigoProduto', '').strip()
                    if codigo_produto and codigo_rb_check:
                        codigo_rb_norm = codigo_rb_check.lstrip('0') or '0'
                        codigo_prod_norm = codigo_produto.lstrip('0') or '0'
                        if codigo_rb_norm == codigo_prod_norm and item_rb_check.get('codigoOperacao'):
                            codigo_operacao_from_metadata = item_rb_check['codigoOperacao']
                            print(f"[8.{idx}.8.1] codigoOperacao encontrado no requestBody por código: {codigo_operacao_from_metadata}")
                            break
            
            # Se não encontrou pedidoDeCompra, tentar buscar novamente
            if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                print(f"[8.{idx}.9] pedidoDeCompra não encontrado, buscando no requestBody...")
                print(f"[8.{idx}.9.DEBUG] Estado atual:")
                print(f"  - pedido_de_compra (tipo): {type(pedido_de_compra)}")
                print(f"  - pedido_de_compra (valor): {pedido_de_compra}")
                if pedido_de_compra:
                    print(f"  - pedido_de_compra.get('pedidoErp'): {pedido_de_compra.get('pedidoErp')}")
                
                nome_xml = produto_xml.get('descricao', '').strip() or produto_xml.get('nome', '').strip()
                print(f"[8.{idx}.9.DEBUG] Buscando por nome:")
                print(f"  - nome_xml: {nome_xml}")
                
                if request_body_data and request_body_data.get('itens'):
                    print(f"[8.{idx}.9.DEBUG] Itens disponíveis no requestBody para busca: {len(request_body_data['itens'])}")
                    # Tentar encontrar pelo nome
                    for item_idx, item_rb in enumerate(request_body_data['itens'], 1):
                        nome_rb = item_rb.get('produto', '').strip()
                        print(f"[8.{idx}.9.DEBUG.{item_idx}] Comparando com item {item_idx}:")
                        print(f"  - nome_rb: {nome_rb}")
                        print(f"  - codigoProduto: {item_rb.get('codigoProduto', 'N/A')}")
                        print(f"  - pedidoDeCompra (tipo): {type(item_rb.get('pedidoDeCompra'))}")
                        print(f"  - pedidoDeCompra (valor): {item_rb.get('pedidoDeCompra')}")
                        
                        if nome_xml and nome_rb:
                            nome_xml_norm = ' '.join(nome_xml.upper().split())
                            nome_rb_norm = ' '.join(nome_rb.upper().split())
                            palavras_xml = set(w for w in nome_xml_norm.split() if len(w) > 2)
                            palavras_rb = set(w for w in nome_rb_norm.split() if len(w) > 2)
                            palavras_comuns = palavras_xml.intersection(palavras_rb)
                            
                            print(f"  - nome_xml_norm: {nome_xml_norm}")
                            print(f"  - nome_rb_norm: {nome_rb_norm}")
                            print(f"  - palavras_comuns: {palavras_comuns} (count: {len(palavras_comuns)})")
                            print(f"  - nome_xml_norm in nome_rb_norm: {nome_xml_norm in nome_rb_norm}")
                            print(f"  - nome_rb_norm in nome_xml_norm: {nome_rb_norm in nome_xml_norm}")
                            
                            if len(palavras_comuns) >= 2 or nome_xml_norm in nome_rb_norm or nome_rb_norm in nome_xml_norm:
                                pedido_de_compra_raw = item_rb.get('pedidoDeCompra')
                                print(f"[8.{idx}.9.DEBUG.{item_idx}] ✅ Match por nome encontrado!")
                                print(f"  - pedidoDeCompra_raw (tipo): {type(pedido_de_compra_raw)}")
                                print(f"  - pedidoDeCompra_raw (valor): {pedido_de_compra_raw}")
                                
                                if pedido_de_compra_raw:
                                    if isinstance(pedido_de_compra_raw, dict):
                                        pedido_de_compra = pedido_de_compra_raw
                                    elif isinstance(pedido_de_compra_raw, str):
                                        try:
                                            pedido_de_compra = json.loads(pedido_de_compra_raw)
                                        except:
                                            pedido_de_compra = {}
                                    else:
                                        pedido_de_compra = {}
                                else:
                                    pedido_de_compra = {}
                                
                                print(f"[8.{idx}.10] pedidoDeCompra encontrado por nome: {pedido_de_compra}")
                                
                                # Capturar codigoOperacao do pedido de compra se existir
                                if item_rb.get('codigoOperacao'):
                                    codigo_operacao_from_metadata = item_rb['codigoOperacao']
                                    print(f"[8.{idx}.10.1] codigoOperacao encontrado no pedido de compra: {codigo_operacao_from_metadata}")
                                
                                break
                            else:
                                print(f"[8.{idx}.9.DEBUG.{item_idx}] ❌ Não deu match por nome")
                    
                    # Fallback: tentar por código
                    if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                        print(f"[8.{idx}.9.DEBUG] Buscando por código (fallback):")
                        print(f"  - codigo_produto procurado: {codigo_produto}")
                        for item_idx, item_rb in enumerate(request_body_data['itens'], 1):
                            codigo_rb = item_rb.get('codigoProduto', '').lstrip('0') or '0'
                            codigo_produto_normalized = codigo_produto.lstrip('0') or '0'
                            print(f"[8.{idx}.9.DEBUG.{item_idx}] Comparando códigos:")
                            print(f"  - codigo_rb: {codigo_rb}")
                            print(f"  - codigo_produto_normalized: {codigo_produto_normalized}")
                            print(f"  - São iguais? {codigo_rb == codigo_produto_normalized}")
                            
                            if codigo_rb == codigo_produto_normalized:
                                pedido_de_compra_raw = item_rb.get('pedidoDeCompra')
                                print(f"[8.{idx}.9.DEBUG.{item_idx}] ✅ Match por código encontrado!")
                                print(f"  - pedidoDeCompra_raw (tipo): {type(pedido_de_compra_raw)}")
                                print(f"  - pedidoDeCompra_raw (valor): {pedido_de_compra_raw}")
                                
                                if pedido_de_compra_raw:
                                    if isinstance(pedido_de_compra_raw, dict):
                                        pedido_de_compra = pedido_de_compra_raw
                                    elif isinstance(pedido_de_compra_raw, str):
                                        try:
                                            pedido_de_compra = json.loads(pedido_de_compra_raw)
                                        except:
                                            pedido_de_compra = {}
                                    else:
                                        pedido_de_compra = {}
                                else:
                                    pedido_de_compra = {}
                                
                                print(f"[8.{idx}.11] pedidoDeCompra encontrado por código: {pedido_de_compra}")
                                
                                # Capturar codigoOperacao do pedido de compra se existir
                                if item_rb.get('codigoOperacao'):
                                    codigo_operacao_from_metadata = item_rb['codigoOperacao']
                                    print(f"[8.{idx}.11.1] codigoOperacao encontrado no pedido de compra: {codigo_operacao_from_metadata}")
                                
                                break
                            else:
                                print(f"[8.{idx}.9.DEBUG.{item_idx}] ❌ Não deu match por código")
                else:
                    print(f"[8.{idx}.9.DEBUG] requestBody ou requestBody['itens'] não disponível")
            
            # Verificar se pedidoDeCompra foi encontrado (não é mais obrigatório)
            if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                print(f"[8.{idx}.12] AVISO: pedidoDeCompra não encontrado para produto {codigo_produto} - campo não será incluído no payload")
                print(f"  - Nome do produto XML: {produto_xml.get('descricao', 'N/A')}")
                print(f"  - Código do produto XML: {codigo_produto}")
                print(f"  - Continuando processamento sem pedidoDeCompra...")
            
            # Buscar codigoOperacao: prioridade 1 = pedido de compra, prioridade 2 = CFOP mapping
            codigo_operacao = ''
            if codigo_operacao_from_metadata:
                codigo_operacao = codigo_operacao_from_metadata
                print(f"[8.{idx}.12.op] Usando codigoOperacao do pedido de compra: {codigo_operacao}")
            elif cfop_mapping and cfop_mapping.get('chave'):
                codigo_operacao = cfop_mapping.get('chave', '')
                print(f"[8.{idx}.12.op] Usando codigoOperacao do CFOP mapping (chave): {codigo_operacao}")
            elif cfop_mapping and cfop_mapping.get('operacao'):
                codigo_operacao = cfop_mapping.get('operacao', '')
                print(f"[8.{idx}.12.op] Usando codigoOperacao do CFOP mapping (operacao): {codigo_operacao}")
            
            # Montar item do payload
            item = {
                "codigoProduto": codigo_produto,
                "quantidade": quantidade,
                "valorUnitario": valor_unitario,
                "codigoOperacao": codigo_operacao
            }
            
            # Adicionar pedidoDeCompra apenas se existir e tiver pedidoErp
            if pedido_de_compra and pedido_de_compra.get('pedidoErp'):
                item["pedidoDeCompra"] = pedido_de_compra
                print(f"[8.{idx}.12.1] pedidoDeCompra incluído no payload: pedidoErp={pedido_de_compra.get('pedidoErp')}")
            else:
                print(f"[8.{idx}.12.1] pedidoDeCompra não incluído no payload (não encontrado ou vazio)")
            
            # Adicionar unidade_trib se disponível no XML
            if unidade_trib:
                item["unidadeMedida"] = unidade_trib
            
            # Adicionar lote se disponível
            if lote:
                item["lote"] = {
                    "numero": lote['numero'],
                    "dataValidade": lote.get('dataValidade'),
                    "dataFabricacao": lote.get('dataFabricacao')
                }
                print(f"[8.{idx}.13] Lote adicionado: {lote['numero']}")
            
            payload['itens'].append(item)
            lote_info = f", lote={lote['numero']}" if lote else ""
            pedido_info = f", pedido={pedido_de_compra.get('pedidoErp')}" if (pedido_de_compra and pedido_de_compra.get('pedidoErp')) else ", pedido=N/A"
            print(f"[8.{idx}.14] ✅ Produto {idx} adicionado ao payload: código={codigo_produto}, qtd={quantidade}, valor={valor_unitario}, op={codigo_operacao}{pedido_info}{lote_info}")
            
        except Exception as e:
            print(f"[8.{idx}] ERRO ao processar produto: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continuar processamento mesmo se um produto falhar
            continue
    
    # Se não há produtos filtrados mas há produtos para processar (caso sem match)
    if not produtos_filtrados and produtos_para_processar:
        print(f"[8.3] AVISO: Nenhum produto deu match, mas há {len(produtos_para_processar)} produtos disponíveis")
        print(f"[8.3.1] Processando todos os produtos (comportamento antigo)")
        
        for idx, produto in enumerate(produtos_para_processar, 1):
            try:
                # Formato antigo: produtos do XML ou requestBody
                # Normalizar código produto (remover zeros à esquerda se numérico)
                codigo = produto.get('codigo', '')
                if codigo.isdigit():
                    codigo = codigo.lstrip('0') or '0'
                
                # Buscar codigoOperacao: prioridade 1 = pedido de compra, prioridade 2 = CFOP mapping, prioridade 3 = CFOP XML
                cfop_original = produto.get('cfop', '')
                codigo_operacao = ''
                
                # Prioridade 1: codigoOperacao do requestBody (pedido de compra)
                if request_body_data and request_body_data.get('itens'):
                    for item_rb_check in request_body_data['itens']:
                        codigo_rb_check = item_rb_check.get('codigoProduto', '').strip()
                        if codigo and codigo_rb_check:
                            codigo_rb_norm = codigo_rb_check.lstrip('0') or '0'
                            codigo_norm = codigo.lstrip('0') or '0'
                            if codigo_rb_norm == codigo_norm and item_rb_check.get('codigoOperacao'):
                                codigo_operacao = item_rb_check['codigoOperacao']
                                print(f"[8.{idx}] Produto {idx}: codigoOperacao do pedido de compra: {codigo_operacao}")
                                break
                
                if not codigo_operacao:
                    if cfop_mapping and cfop_mapping.get('chave'):
                        # Prioridade 2: Chave encontrada na validação
                        codigo_operacao = cfop_mapping.get('chave', '')
                        print(f"[8.{idx}] Produto {idx}: CFOP={cfop_original} → Chave encontrada na validação: {codigo_operacao}")
                    elif cfop_mapping and cfop_mapping.get('operacao'):
                        # Prioridade 3: Operação do mapping (mesmo valor da chave)
                        codigo_operacao = cfop_mapping.get('operacao', '')
                        print(f"[8.{idx}] Produto {idx}: CFOP={cfop_original} → Usando operação do mapping: {codigo_operacao}")
                    else:
                        # Fallback: Usar CFOP original do XML (caso não tenha passado pela validação)
                        codigo_operacao = cfop_original
                        print(f"[8.{idx}] Produto {idx}: CFOP={cfop_original} → Nenhuma chave encontrada, usando CFOP original como fallback")
                
                # Montar pedidoDeCompra
                pedido_de_compra = {
                    "pedidoErp": produto.get('pedido', ''),
                    "itemPedidoErp": produto.get('item_pedido', '')
                }
                
                item = {
                    "codigoProduto": codigo,
                    "quantidade": float(produto.get('quantidade', 0)),
                    "valorUnitario": float(produto.get('valor_unitario', 0)),
                    "codigoOperacao": codigo_operacao,
                    "pedidoDeCompra": pedido_de_compra
                }
                
                # Adicionar unidade_trib se disponível no XML
                unidade_trib = produto.get('unidade_trib', '').strip()
                if unidade_trib:
                    item["unidadeMedida"] = unidade_trib
                
                payload['itens'].append(item)
                print(f"[8.{idx}] Produto {idx} adicionado: {codigo}, qtd={item['quantidade']}, valor={item['valorUnitario']}, op={codigo_operacao}")
                
            except (ValueError, TypeError) as e:
                print(f"[8.{idx}] ERRO ao converter valores numéricos: {e}")
                continue
    
    # Verificar se existe campo "duplicatas" no JSON ou no XML e incluir no payload se houver
    print(f"\n[8.5] Verificando campo 'duplicatas'...")
    duplicatas = None
    duplicatas_source = None
    
    # Prioridade 1: Buscar em request_body_data.duplicatas
    if request_body_data and 'duplicatas' in request_body_data:
        duplicatas = request_body_data.get('duplicatas')
        duplicatas_source = "request_body_data"
        print(f"[8.5.1] Campo 'duplicatas' encontrado em request_body_data")
    
    # Prioridade 2: Buscar em xml_data.cobranca.duplicatas
    elif cobranca and 'duplicatas' in cobranca:
        duplicatas = cobranca.get('duplicatas')
        duplicatas_source = "xml_data.cobranca"
        print(f"[8.5.1] Campo 'duplicatas' encontrado em xml_data.cobranca")
    
    if duplicatas and isinstance(duplicatas, list) and len(duplicatas) > 0:
        print(f"[8.5.2] Processando {len(duplicatas)} duplicata(s) de {duplicatas_source}")
        # Validar formato das duplicatas
        duplicatas_validas = []
        for dup in duplicatas:
            if isinstance(dup, dict) and all(key in dup for key in ['vencimento', 'valor']):
                try:
                    # Converter valor para numérico (float)
                    valor_numerico = float(dup.get('valor', 0))
                    duplicata_valida = {
                        'vencimento': str(dup.get('vencimento', '')),
                        'valor': valor_numerico
                    }
                    # Sempre incluir numero se existir no JSON original
                    if 'numero' in dup and dup.get('numero') is not None:
                        duplicata_valida['numero'] = str(dup.get('numero', ''))
                    duplicatas_validas.append(duplicata_valida)
                except (ValueError, TypeError) as e:
                    print(f"[8.5.3] WARNING: Erro ao converter valor da duplicata para numérico: {e}, duplicata ignorada: {dup}")
            else:
                print(f"[8.5.3] WARNING: Duplicata inválida ignorada (campos obrigatórios: vencimento, valor): {dup}")
        
        if duplicatas_validas:
            payload['duplicatas'] = duplicatas_validas
            print(f"[8.5.4] {len(duplicatas_validas)} duplicata(s) adicionada(s) ao payload")
            for idx, dup in enumerate(duplicatas_validas, 1):
                numero_info = f", numero={dup.get('numero', 'N/A')}" if 'numero' in dup else ""
                print(f"[8.5.5] Duplicata {idx}: vencimento={dup['vencimento']}, valor={dup['valor']}{numero_info}")
        else:
            print(f"[8.5.6] WARNING: Nenhuma duplicata válida encontrada, campo não será incluído")
    elif duplicatas is not None:
        print(f"[8.5.7] Campo 'duplicatas' vazio ou inválido, não será incluído")
    else:
        print(f"[8.5.8] Campo 'duplicatas' não encontrado em request_body_data nem em xml_data.cobranca, não será incluído")
    
    # Enviar para Protheus via HTTP direto (autenticação Basic)
    protheus_secret_id = _env('PROTHEUS_SECRET_ID')
    protheus_endpoint = _env('PROTHEUS_API_URL')
    protheus_timeout = int(os.environ.get('PROTHEUS_TIMEOUT', '30'))
    
    print(f"\n{'='*80}")
    print(f"[9] PREPARANDO ENVIO PARA PROTHEUS (via HTTP direto)")
    print(f"{'='*80}")
    print(f"[9.1] Protheus URL: {protheus_endpoint}")
    print(f"[9.1.1] Protheus Secret ID: {protheus_secret_id}")
    print(f"[9.1.2] Timeout: {protheus_timeout}s")
    
    # Log do payload de forma fácil de visualizar
    print(f"\n[9.2] PAYLOAD COMPLETO:")
    print(f"{'-'*80}")
    payload_str = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    print(payload_str)
    print(f"{'-'*80}")
    
    # Validação final do payload antes de enviar
    print(f"\n[9.2.1] Validação do payload:")
    if payload.get('cnpjEmitente'):
        print(f"  ✓ cnpjEmitente: {payload.get('cnpjEmitente')} ({len(payload.get('cnpjEmitente'))} dígitos)")
    elif payload.get('cpfEmitente'):
        print(f"  ✓ cpfEmitente: {payload.get('cpfEmitente')} ({len(payload.get('cpfEmitente'))} dígitos)")
    else:
        print(f"  - cnpjEmitente/cpfEmitente: não informado (campo não incluído)")
    if payload.get('cnpjDestinatario'):
        print(f"  ✓ cnpjDestinatario: {payload.get('cnpjDestinatario')} ({len(payload.get('cnpjDestinatario'))} dígitos)")
    else:
        print(f"  ⚠ cnpjDestinatario: não informado (opcional)")
    print(f"  ✓ documento: {payload.get('documento')}")
    print(f"  ✓ itens: {len(payload.get('itens', []))} produto(s)")
    
    # Obter credenciais do Secrets Manager
    print(f"\n[9.2] Obtendo credenciais do Secrets Manager...")
    try:
        secret = _get_secret(protheus_secret_id)
        username = secret.get("username") or secret.get("user")
        password = secret.get("password") or secret.get("pass")
        
        if not username or not password:
            raise ValueError("Secret must contain username/password (or user/pass)")
        
        print(f"[9.2.1] Credenciais obtidas com sucesso (username: {username})")
    except Exception as secret_err:
        error_message = f"Erro ao obter credenciais do Secrets Manager: {str(secret_err)}"
        error_details = {
            'error': str(secret_err),
            'error_type': 'SecretsManagerError',
            'error_message': error_message,
            'secret_id': protheus_secret_id
        }
        print(f"[9.2.1] ERRO: {error_message}")
        error_with_details = Exception(error_message)
        error_with_details.error_details = error_details
        raise error_with_details
    
    # Criar Basic Auth header
    basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    
    # Montar headers para envio
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {basic_auth}',
        'Accept': 'application/json'
    }
    
    # Adicionar tenantId no header (obrigatório)
    if tenant_id:
        headers['tenantId'] = str(tenant_id)
        print(f"\n[9.4] tenantId:")
        print(f"  - Valor: {tenant_id}")
        print(f"  - Adicionado ao header como 'tenantId'")
    else:
        print(f"\n[9.4] WARNING: tenantId NÃO encontrado! A requisição pode falhar.")
        print(f"  - Verifique se o tenantId está no header do INPUT_JSON ou nos metadados do processo")
    
    # Log completo dos headers (sem senha)
    print(f"\n[9.6] HEADERS DA REQUISIÇÃO:")
    print(f"{'-'*80}")
    for key, value in headers.items():
        if key == 'Authorization':
            print(f"  {key}: Basic ***MASKED***")
        else:
            print(f"  {key}: {value}")
    print(f"{'-'*80}")
    
    # Montar URL completa para a rota /documento-entrada
    print(f"\n[9.7] Enviando requisição HTTP POST para: {protheus_endpoint}")
    
    # Salvar payload no DynamoDB antes de tentar enviar (para recuperar em caso de erro)
    try:
        payload_str_db = json.dumps(payload, default=str)
        table.update_item(
            Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
            UpdateExpression='SET protheus_request_payload = :payload, updated_at = :timestamp',
            ExpressionAttributeValues={
                ':payload': payload_str_db,
                ':timestamp': datetime.utcnow().isoformat()
            }
        )
        print(f"[9.7.1] Payload salvo no DynamoDB antes de enviar")
    except Exception as save_err:
        print(f"[9.7.1] WARNING: Erro ao salvar payload no DynamoDB: {str(save_err)}")
    
    try:
        # Preparar requisição HTTP POST
        body_json = json.dumps(payload, default=str)
        body_bytes = body_json.encode('utf-8')
        
        req = urllib.request.Request(
            url=protheus_endpoint,
            data=body_bytes,
            headers=headers,
            method='POST'
        )
        
        print(f"\n{'='*80}")
        print(f"[10] ENVIANDO REQUISIÇÃO PARA PROTHEUS")
        print(f"{'='*80}")
        
        # Fazer requisição HTTP
        try:
            with urllib.request.urlopen(req, timeout=protheus_timeout) as resp:
                response_status_code = resp.getcode()
                response_headers = dict(resp.headers)
                response_body_raw = resp.read().decode('utf-8', errors='replace')
                
                print(f"\n{'='*80}")
                print(f"[10] RESPOSTA DO PROTHEUS")
                print(f"{'='*80}")
                print(f"[10.1] HTTP Status Code: {response_status_code}")
                print(f"[10.2] Response Headers: {json.dumps(response_headers, indent=2, default=str)}")
                
                print(f"\n[10.3] Response Body:")
                print(f"{'-'*80}")
                try:
                    protheus_response = json.loads(response_body_raw)
                    print(json.dumps(protheus_response, indent=2, ensure_ascii=False, default=str))
                except:
                    protheus_response = {'raw_response': response_body_raw}
                    print(response_body_raw[:500] + ('...' if len(response_body_raw) > 500 else ''))
                print(f"{'-'*80}")
                
        except urllib.error.HTTPError as e:
            response_status_code = e.code
            response_headers = dict(e.headers) if e.headers else {}
            response_body_raw = e.read().decode('utf-8', errors='replace') if hasattr(e, 'read') else ''
            
            print(f"\n{'='*80}")
            print(f"[10] ERRO HTTP DO PROTHEUS")
            print(f"{'='*80}")
            print(f"[10.1] HTTP Status Code: {response_status_code}")
            print(f"[10.2] Response Headers: {json.dumps(response_headers, indent=2, default=str)}")
            print(f"[10.3] Response Body: {response_body_raw[:500]}")
            
            # Processar resposta de erro
            try:
                protheus_response = json.loads(response_body_raw) if response_body_raw else {}
            except:
                protheus_response = {'raw_response': response_body_raw}
        
        # Verificar se houve erro HTTP na resposta
        if response_status_code >= 400:
            error_details = {}
            protheus_cause = None
            
            error_code = 'N/A'
            error_msg = ''
            if isinstance(protheus_response, dict):
                error_code = protheus_response.get('errorCode', 'N/A')
                error_msg = protheus_response.get('message', '')
                
                if 'cause' in protheus_response:
                    protheus_cause = protheus_response.get('cause')
                    if isinstance(protheus_cause, list):
                        error_details['cause'] = protheus_cause
                    else:
                        error_details['cause'] = [protheus_cause] if protheus_cause else []
            
            if error_msg and error_msg.strip():
                error_message = f"HTTP {response_status_code} - {error_code}: {error_msg}"
            elif error_code != 'N/A':
                error_message = f"HTTP {response_status_code} - Código de erro: {error_code}"
            else:
                error_message = f"HTTP {response_status_code} - Resposta do Protheus: {json.dumps(protheus_response, default=str)[:200]}"
            
            error_details.update({
                'status_code': response_status_code,
                'response_body': protheus_response,
                'error_code': error_code,
                'error_message': error_message,
                'request_payload': payload,
                'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'},  # Não salvar senha
                'protheus_url': protheus_endpoint,
                'response_headers': response_headers if 'response_headers' in locals() else {}
            })
            
            # Salvar informações de erro no DynamoDB
            try:
                protheus_request_info = {
                    'protheus_url': protheus_endpoint,
                    'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'},  # Não salvar senha
                    'request_payload': payload,
                    'response_status_code': response_status_code,
                    'response_headers': response_headers if 'response_headers' in locals() else {},
                    'response_body': protheus_response,
                    'error_details': error_details
                }
                table.update_item(
                    Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
                    UpdateExpression='SET protheus_request_info = :info, updated_at = :timestamp',
                    ExpressionAttributeValues={
                        ':info': json.dumps(protheus_request_info, default=str),
                        ':timestamp': datetime.utcnow().isoformat()
                    }
                )
                print(f"[10.6.1] Informações de erro salvas no DynamoDB")
            except Exception as save_err:
                print(f"[10.6.1] WARNING: Erro ao salvar informações de erro: {str(save_err)}")
            
            print(f"\n[10] ERRO HTTP na resposta do Protheus:")
            print(f"[10.5] Status Code: {response_status_code}")
            print(f"[10.6] Detalhes: {json.dumps(error_details, indent=2, default=str)}")
            
            # Reportar falha para API do SCTASK
            print(f"\n[10.7] Reportando falha do Protheus para API do SCTASK...")
            try:
                sctask_id = report_protheus_failure_to_sctask(process_id, error_details)
                if sctask_id:
                    print(f"[10.8] Falha reportada com sucesso. SCTASK ID: {sctask_id}")
                else:
                    print(f"[10.8] Falha ao reportar para SCTASK (mas continuando com o erro)")
            except Exception as sctask_err:
                print(f"[10.8] Erro ao reportar para SCTASK: {str(sctask_err)}")
            
            error_with_details = Exception(error_message)
            error_with_details.error_details = error_details
            raise error_with_details
        
        # Sucesso
        print(f"[10.4] Resposta processada com sucesso")
        
        # Salvar informações da requisição no DynamoDB para feedback
        try:
            protheus_request_info = {
                'protheus_url': protheus_endpoint,
                'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'},  # Não salvar senha
                'request_payload': payload,
                'response_status_code': response_status_code,
                'response_headers': response_headers if 'response_headers' in locals() else {},
                'response_body': protheus_response
            }
            table.update_item(
                Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
                UpdateExpression='SET protheus_request_info = :info, updated_at = :timestamp',
                ExpressionAttributeValues={
                    ':info': json.dumps(protheus_request_info, default=str),
                    ':timestamp': datetime.utcnow().isoformat()
                }
            )
            print(f"[10.4.1] Informações da requisição Protheus salvas no DynamoDB")
        except Exception as save_err:
            print(f"[10.4.1] WARNING: Erro ao salvar informações da requisição: {str(save_err)}")
    except urllib.error.URLError as e:
        print(f"\n[10] ERRO de rede/DNS ao conectar ao Protheus: {e}")
        import traceback
        traceback.print_exc()
        
        error_type = type(e).__name__
        error_message = f"Erro de rede/DNS ao conectar ao Protheus: {str(e)}"
        error_details = {
            'error': str(e),
            'error_type': 'NetworkError',
            'error_message': error_message,
            'protheus_url': protheus_endpoint if 'protheus_endpoint' in locals() else protheus_url,
            'request_payload': payload if 'payload' in locals() else None,
            'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'} if 'headers' in locals() else None
        }
        
        # Reportar falha para API do SCTASK
        print(f"\n[10.7] Reportando falha de rede para API do SCTASK...")
        try:
            sctask_id = report_protheus_failure_to_sctask(process_id, error_details)
            if sctask_id:
                print(f"[10.8] Falha reportada com sucesso. SCTASK ID: {sctask_id}")
            else:
                print(f"[10.8] Falha ao reportar para SCTASK (mas continuando com o erro)")
        except Exception as sctask_err:
            print(f"[10.8] Erro ao reportar para SCTASK: {str(sctask_err)}")
        
        error_with_details = Exception(error_message)
        error_with_details.error_details = error_details
        raise error_with_details
    
    except socket.timeout:
        print(f"\n[10] TIMEOUT ao conectar ao Protheus (após {protheus_timeout}s)")
        error_message = f"Timeout após {protheus_timeout}s ao conectar ao Protheus (VPC/rota/SG/DNS?)"
        error_details = {
            'error': 'Timeout',
            'error_type': 'TimeoutError',
            'error_message': error_message,
            'timeout_seconds': protheus_timeout,
            'protheus_url': protheus_endpoint if 'protheus_endpoint' in locals() else protheus_url,
            'request_payload': payload if 'payload' in locals() else None,
            'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'} if 'headers' in locals() else None
        }
        
        # Reportar falha para API do SCTASK
        print(f"\n[10.7] Reportando falha de timeout para API do SCTASK...")
        try:
            sctask_id = report_protheus_failure_to_sctask(process_id, error_details)
            if sctask_id:
                print(f"[10.8] Falha reportada com sucesso. SCTASK ID: {sctask_id}")
            else:
                print(f"[10.8] Falha ao reportar para SCTASK (mas continuando com o erro)")
        except Exception as sctask_err:
            print(f"[10.8] Erro ao reportar para SCTASK: {str(sctask_err)}")
        
        error_with_details = Exception(error_message)
        error_with_details.error_details = error_details
        raise error_with_details
    
    except Exception as e:
        print(f"\n[10] ERRO ao enviar requisição para Protheus: {e}")
        import traceback
        traceback.print_exc()
        
        # Se a exceção já tem error_details, apenas re-raise
        if hasattr(e, 'error_details') and isinstance(e.error_details, dict):
            raise e
        
        # Caso contrário, criar exceção com detalhes
        error_type = type(e).__name__
        error_message = f"Falha ao enviar requisição para Protheus: {str(e)}"
        error_details = {
            'error': str(e),
            'error_type': error_type,
            'error_message': error_message,
            'protheus_url': protheus_endpoint if 'protheus_endpoint' in locals() else protheus_url,
            'request_payload': payload if 'payload' in locals() else None,
            'request_headers': {k: v for k, v in headers.items() if k != 'Authorization'} if 'headers' in locals() else None
        }
        
        # Reportar falha para API do SCTASK
        print(f"\n[10.7] Reportando falha da invocação Lambda para API do SCTASK...")
        try:
            sctask_id = report_protheus_failure_to_sctask(process_id, error_details)
            if sctask_id:
                print(f"[10.8] Falha reportada com sucesso. SCTASK ID: {sctask_id}")
            else:
                print(f"[10.8] Falha ao reportar para SCTASK (mas continuando com o erro)")
        except Exception as sctask_err:
            print(f"[10.8] Erro ao reportar para SCTASK: {str(sctask_err)}")
        
        error_with_details = Exception(error_message)
        error_with_details.error_details = error_details
        raise error_with_details
    
    # Se chegou aqui, API foi chamada com sucesso
    # Extrair id_unico do campo 'idUnico' da resposta
    id_unico = protheus_response.get('idUnico')
    print(f"\n[11] ID Único extraído: {id_unico}")
    
    # Atualizar status no DynamoDB com id_unico da API
    # Se falhar, re-lançar exceção para que Step Functions capture e dispare SNS
    try:
        print(f"\n[12] Atualizando DynamoDB...")
        update_expr = 'SET #status = :status, protheus_response = :response, updated_at = :timestamp'
        expr_values = {
            ':status': 'COMPLETED',
            ':response': json.dumps(protheus_response),
            ':timestamp': datetime.utcnow().isoformat()
        }
        if id_unico:
            update_expr += ', id_unico = :id_unico'
            expr_values[':id_unico'] = id_unico
            print(f"[12.1] Salvando id_unico: {id_unico}")
        
        table.update_item(
            Key={'PK': f'PROCESS#{process_id}', 'SK': 'METADATA'},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={'#status': 'STATUS'},
            ExpressionAttributeValues=expr_values
        )
        print(f"[12.2] DynamoDB atualizado com sucesso")
    except Exception as e:
        print(f"\n[12] ERRO ao atualizar DynamoDB: {e}")
        import traceback
        traceback.print_exc()
        # Re-lançar exceção para que Step Functions capture e dispare SNS
        raise Exception(f"Falha ao atualizar status no DynamoDB após envio para Protheus: {str(e)}")
    
    result = {
        'statusCode': 200,
        'process_id': process_id,
        'status': 'COMPLETED',
        'protheus_response': protheus_response
    }
    
    print(f"\n[13] Retornando resultado:")
    print(json.dumps(result, indent=2, default=str))
    print("="*80)
    print("SEND TO PROTHEUS - FIM")
    print("="*80)
    
    return result
