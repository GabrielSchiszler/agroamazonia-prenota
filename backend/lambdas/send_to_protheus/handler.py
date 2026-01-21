import json
import os
import boto3
import requests
import base64
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

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

def get_oauth2_token():
    """
    Obtém token de acesso OAuth2 usando client credentials grant.
    Retorna o access_token ou None em caso de erro.
    """
    auth_url = os.environ.get('PROTHEUS_AUTH_URL')
    client_id = os.environ.get('PROTHEUS_CLIENT_ID')
    client_secret = os.environ.get('PROTHEUS_CLIENT_SECRET')
    
    if not all([auth_url, client_id, client_secret]):
        missing = []
        if not auth_url: missing.append('PROTHEUS_AUTH_URL')
        if not client_id: missing.append('PROTHEUS_CLIENT_ID')
        if not client_secret: missing.append('PROTHEUS_CLIENT_SECRET')
        print(f"ERROR: Missing OAuth2 credentials in environment variables: {', '.join(missing)}")
        print(f"Available env vars: PROTHEUS_AUTH_URL={'SET' if auth_url else 'NOT SET'}, "
              f"PROTHEUS_CLIENT_ID={'SET' if client_id else 'NOT SET'}, "
              f"PROTHEUS_CLIENT_SECRET={'SET' if client_secret else 'NOT SET'}")
        return None
    
    try:
        # OAuth2 Client Credentials pode aceitar credenciais de duas formas:
        # 1. Basic Auth no header (Authorization: Basic base64(client_id:client_secret))
        # 2. client_id e client_secret no body
        # Vamos tentar ambas as abordagens
        
        # Abordagem 1: Basic Auth no header (padrão OAuth2)
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers_basic = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Abordagem 2: client_id e client_secret no body
        headers_body = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Tentar diferentes combinações
        # grant_type deve ser "client_credentials" para OAuth2 Client Credentials
        approaches = [
            {
                'name': 'Basic Auth + client_credentials',
                'headers': headers_basic,
                'data': {
                    'grant_type': 'client_credentials'
                }
            },
            {
                'name': 'Body Auth + client_credentials',
                'headers': headers_body,
                'data': {
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret
                }
            }
        ]
        
        response = None
        last_error = None
        
        for approach in approaches:
            try:
                print(f"Trying OAuth2 approach: {approach['name']}")
                print(f"URL: {auth_url}")
                
                response = requests.post(
                    auth_url, 
                    data=approach['data'], 
                    headers=approach['headers'], 
                    timeout=30
                )
                
                print(f"Response status: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"✓ Success with approach: {approach['name']}")
                    break
                else:
                    print(f"✗ Failed with approach: {approach['name']}")
                    print(f"Response body: {response.text[:300]}")
                    last_error = response
                    
            except Exception as e:
                print(f"✗ Exception with approach {approach['name']}: {str(e)}")
                last_error = e
                continue
        
        if not response or response.status_code != 200:
            if last_error:
                if hasattr(last_error, 'raise_for_status'):
                    last_error.raise_for_status()
                else:
                    raise Exception(f"All OAuth2 approaches failed. Last error: {str(last_error)}")
            else:
                raise Exception("All OAuth2 approaches failed")
        
        response.raise_for_status()
        
        # Protheus geralmente retorna JSON
        try:
            token_response = response.json()
            print(f"OAuth2 token response keys: {list(token_response.keys())}")
        except ValueError:
            # Se não for JSON, tentar parsear como form-urlencoded
            from urllib.parse import parse_qs
            token_response = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                            for k, v in parse_qs(response.text).items()}
            print(f"OAuth2 token response (form-urlencoded) keys: {list(token_response.keys())}")
        
        # Tentar diferentes campos possíveis para o access_token
        # Protheus pode retornar: access_token, accessToken, token, etc.
        access_token = (
            token_response.get('access_token') or 
            token_response.get('accessToken') or
            token_response.get('token')
        )
        
        if access_token:
            print("OAuth2 token obtained successfully")
            # Não logar o token completo por segurança, apenas os primeiros caracteres
            print(f"Token preview: {access_token[:20]}...")
            return access_token
        else:
            print(f"ERROR: No access_token in response.")
            print(f"Response keys: {list(token_response.keys()) if isinstance(token_response, dict) else 'N/A'}")
            print(f"Response preview: {str(token_response)[:500]}")
            # Não lançar exceção aqui, apenas retornar None
            # O código que chama deve tratar a ausência do token
            return None
            
    except Exception as e:
        print(f"ERROR: Failed to obtain OAuth2 token: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

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
 
   
    
    # Extrair CNPJ emitente - tentar múltiplas fontes
    print(f"\n[7.3] Extraindo CNPJ do emitente...")
    cnpj_emitente = None
    
    # Prioridade 1: requestBody.cnpjEmitente
    if request_body_data and request_body_data.get('cnpjEmitente'):
        cnpj_emitente = request_body_data.get('cnpjEmitente')
        print(f"  [7.3.1] CNPJ encontrado no requestBody.cnpjEmitente: {cnpj_emitente}")
    
    # Prioridade 2: XML emitente.cnpj
    if not cnpj_emitente and emitente and emitente.get('cnpj'):
        cnpj_emitente = emitente.get('cnpj')
        print(f"  [7.3.2] CNPJ encontrado no XML emitente.cnpj: {cnpj_emitente}")
    
    # Prioridade 3: OCR cnpjRemetente
    if not cnpj_emitente and ocr_data and ocr_data.get('cnpjRemetente'):
        cnpj_emitente = ocr_data.get('cnpjRemetente')
        print(f"  [7.3.3] CNPJ encontrado no OCR cnpjRemetente: {cnpj_emitente}")
    
    # Normalizar CNPJ emitente (apenas dígitos)
    if cnpj_emitente:
        cnpj_emitente = ''.join(filter(str.isdigit, str(cnpj_emitente)))
        print(f"  [7.3.4] CNPJ emitente normalizado (apenas dígitos): {cnpj_emitente}")
        print(f"  [7.3.5] CNPJ emitente length: {len(cnpj_emitente)} dígitos")
        
        # Validar se tem 14 dígitos (CNPJ válido)
        if len(cnpj_emitente) != 14:
            print(f"  [7.3.6] WARNING: CNPJ emitente não tem 14 dígitos! Pode estar incompleto.")
    else:
        print(f"  [7.3.7] ERRO: CNPJ do emitente NÃO encontrado em nenhuma fonte!")
        print(f"    - requestBody.cnpjEmitente: {request_body_data.get('cnpjEmitente') if request_body_data else 'N/A'}")
        print(f"    - XML emitente.cnpj: {emitente.get('cnpj') if emitente else 'N/A'}")
        print(f"    - OCR cnpjRemetente: {ocr_data.get('cnpjRemetente') if ocr_data else 'N/A'}")
        cnpj_emitente = ""  # Manter como string vazia para não quebrar o payload
    
    # Extrair CNPJ destinatário - tentar múltiplas fontes
    print(f"\n[7.4] Extraindo CNPJ do destinatário...")
    cnpj_destinatario = None
    
    # Prioridade 1: requestBody.cnpjDestinatario
    if request_body_data and request_body_data.get('cnpjDestinatario'):
        cnpj_destinatario = request_body_data.get('cnpjDestinatario')
        print(f"  [7.4.1] CNPJ encontrado no requestBody.cnpjDestinatario: {cnpj_destinatario}")
    
    # Prioridade 2: XML destinatario.cnpj
    if not cnpj_destinatario and destinatario and destinatario.get('cnpj'):
        cnpj_destinatario = destinatario.get('cnpj')
        print(f"  [7.4.2] CNPJ encontrado no XML destinatario.cnpj: {cnpj_destinatario}")
    
    # Prioridade 3: OCR cnpjDestinatario
    if not cnpj_destinatario and ocr_data and ocr_data.get('cnpjDestinatario'):
        cnpj_destinatario = ocr_data.get('cnpjDestinatario')
        print(f"  [7.4.3] CNPJ encontrado no OCR cnpjDestinatario: {cnpj_destinatario}")
    
    # Normalizar CNPJ destinatário (apenas dígitos)
    if cnpj_destinatario:
        cnpj_destinatario = ''.join(filter(str.isdigit, str(cnpj_destinatario)))
        print(f"  [7.4.4] CNPJ destinatário normalizado (apenas dígitos): {cnpj_destinatario}")
        print(f"  [7.4.5] CNPJ destinatário length: {len(cnpj_destinatario)} dígitos")
        
        # Validar se tem 14 dígitos (CNPJ válido)
        if len(cnpj_destinatario) != 14:
            print(f"  [7.4.6] WARNING: CNPJ destinatário não tem 14 dígitos! Pode estar incompleto.")
    else:
        print(f"  [7.4.7] WARNING: CNPJ do destinatário NÃO encontrado em nenhuma fonte!")
        print(f"    - requestBody.cnpjDestinatario: {request_body_data.get('cnpjDestinatario') if request_body_data else 'N/A'}")
        print(f"    - XML destinatario.cnpj: {destinatario.get('cnpj') if destinatario else 'N/A'}")
        print(f"    - OCR cnpjDestinatario: {ocr_data.get('cnpjDestinatario') if ocr_data else 'N/A'}")
        cnpj_destinatario = ""  # Manter como string vazia (destinatário pode ser opcional)
    
    payload = {
        "tipoDeDocumento": tipo_documento,
        "documento": numero_documento,
        "serie": serie,
        "dataEmissao": data_emissao,
        "cnpjEmitente": cnpj_emitente,
        # "cnpjDestinatario": cnpj_destinatario,  # Não enviar mais cnpjDestinatario no payload
        "especie": especie,
        "chaveAcesso": chave_acesso,
        "tipoFrete": tipo_frete,
        "moeda": moeda,
        "taxaCambio": taxa_cambio,
        "itens": []
    }
    
    # Validar se CNPJ emitente foi encontrado antes de montar payload
    if not cnpj_emitente or len(cnpj_emitente) < 14:
        print(f"\n[7.5] ERRO CRÍTICO: CNPJ do emitente inválido ou não encontrado!")
        print(f"  - CNPJ atual: '{cnpj_emitente}' (length: {len(cnpj_emitente) if cnpj_emitente else 0})")
        print(f"  - O campo cnpjEmitente é OBRIGATÓRIO para a API Protheus")
        raise Exception(f"CNPJ do emitente não encontrado ou inválido. CNPJ: '{cnpj_emitente}'")
    
    print(f"\n[7.1] Payload base montado")
    print(f"[7.1.1] CNPJ do emitente incluído: {cnpj_emitente} ({len(cnpj_emitente)} dígitos)")
    if cnpj_destinatario:
        print(f"[7.1.2] CNPJ do destinatário incluído: {cnpj_destinatario} ({len(cnpj_destinatario)} dígitos)")
    else:
        print(f"[7.1.2] CNPJ do destinatário: não informado (opcional)")
    
    # Adicionar produtos (APENAS os que deram match na validação)
    print(f"\n[8] Processando produtos...")
    
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
                if 0 <= idx_doc < len(request_body_data['itens']):
                    item_request_body = request_body_data['itens'][idx_doc]
                    pedido_de_compra = item_request_body.get('pedidoDeCompra', {})
                    codigo_produto_rb = item_request_body.get('codigoProduto', '').strip()
                    print(f"[8.2.3] Item do requestBody encontrado na posição DOC {doc_pos}:")
                    print(f"[8.2.3.1] Código do produto: {codigo_produto_rb}")
                    print(f"[8.2.3.2] pedidoDeCompra: {pedido_de_compra}")
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
    
    # Processar apenas produtos filtrados
    # produtos_filtrados agora contém: (idx_xml, produto_xml, pedido_de_compra, codigo_produto_rb)
    for original_idx, produto_xml, pedido_de_compra, codigo_produto_rb in produtos_filtrados:
        idx = len(payload['itens']) + 1  # Índice sequencial no payload
        try:
            # Usar dados do XML (quantidade, valor unitário)
            # E código do produto e pedidoDeCompra do requestBody (já encontrado na filtragem)
            
            # Extrair quantidade e valor unitário do XML
            quantidade = float(produto_xml.get('quantidade', 0))
            valor_unitario = float(produto_xml.get('valor_unitario', 0))
            
            # Usar código do produto do requestBody (já encontrado na filtragem)
            codigo_produto = codigo_produto_rb if codigo_produto_rb else None
            
            # Se não encontrou código do requestBody, tentar buscar novamente
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
            
            print(f"[8.{idx}] Processando produto (posição XML: {original_idx + 1}):")
            print(f"[8.{idx}.3] Código do produto (do requestBody): {codigo_produto}")
            print(f"[8.{idx}.4] Nome (do XML): {produto_xml.get('descricao', 'N/A')[:50]}...")
            print(f"[8.{idx}.5] Quantidade (do XML): {quantidade}")
            print(f"[8.{idx}.6] Valor unitário (do XML): {valor_unitario}")
            print(f"[8.{idx}.7] pedidoDeCompra: {pedido_de_compra}")
            
            # Se não encontrou pedidoDeCompra na filtragem, tentar buscar novamente
            if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                print(f"[8.{idx}.6] pedidoDeCompra não encontrado na filtragem, buscando no requestBody...")
                nome_xml = produto_xml.get('descricao', '').strip() or produto_xml.get('nome', '').strip()
                
                if request_body_data and request_body_data.get('itens'):
                    # Tentar encontrar pelo nome
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
                                print(f"[8.{idx}.7] pedidoDeCompra encontrado por nome: {pedido_de_compra}")
                                break
                    
                    # Fallback: tentar por código
                    if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                        for item_rb in request_body_data['itens']:
                            codigo_rb = item_rb.get('codigoProduto', '').lstrip('0') or '0'
                            if codigo_rb == codigo_produto:
                                pedido_de_compra = item_rb.get('pedidoDeCompra', {})
                                print(f"[8.{idx}.8] pedidoDeCompra encontrado por código: {pedido_de_compra}")
                                break
            
            # Validar se pedidoDeCompra foi encontrado
            if not pedido_de_compra or not pedido_de_compra.get('pedidoErp'):
                print(f"[8.{idx}.9] ERRO: pedidoDeCompra não encontrado para produto {codigo_produto}!")
                print(f"  - Nome do produto XML: {produto_xml.get('descricao', 'N/A')}")
                print(f"  - Código do produto XML: {codigo_produto}")
                raise Exception(f"pedidoDeCompra não encontrado para produto {codigo_produto} (nome: {produto_xml.get('descricao', 'N/A')[:50]}). É obrigatório para a API Protheus.")
            
            # Buscar codigoOperacao do CFOP mapping
            codigo_operacao = ''
            if cfop_mapping and cfop_mapping.get('chave'):
                codigo_operacao = cfop_mapping.get('chave', '')
            elif cfop_mapping and cfop_mapping.get('operacao'):
                codigo_operacao = cfop_mapping.get('operacao', '')
            
            item = {
                "codigoProduto": codigo_produto,
                "quantidade": quantidade,
                "valorUnitario": valor_unitario,
                "codigoOperacao": codigo_operacao,
                "pedidoDeCompra": pedido_de_compra
            }
            payload['itens'].append(item)
            print(f"[8.{idx}.10] ✅ Produto {idx} adicionado ao payload: código={codigo_produto}, qtd={quantidade}, valor={valor_unitario}, op={codigo_operacao}, pedido={pedido_de_compra.get('pedidoErp')}")
            
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
                
                # Usar chave encontrada na validação do CFOP (prioridade)
                # A chave é o código de operação que o Protheus precisa
                cfop_original = produto.get('cfop', '')
                codigo_operacao = ''
                
                if cfop_mapping and cfop_mapping.get('chave'):
                    # Prioridade 1: Chave encontrada na validação
                    codigo_operacao = cfop_mapping.get('chave', '')
                    print(f"[8.{idx}] Produto {idx}: CFOP={cfop_original} → Chave encontrada na validação: {codigo_operacao}")
                elif cfop_mapping and cfop_mapping.get('operacao'):
                    # Prioridade 2: Operação do mapping (mesmo valor da chave)
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
    
    # Enviar para Protheus
    api_url = os.environ.get('PROTHEUS_API_URL', 'https://api.agroamazonia.com/hom-ocr')
    
    # Se a URL não termina com /documento-entrada, adicionar (baseado no padrão do token URL)
    # O token é obtido de: /hom-ocr/documento-entrada/oauth2/token
    # A API provavelmente está em: /hom-ocr/documento-entrada
    if '/documento-entrada' not in api_url and not api_url.endswith('/'):
        # Tentar inferir a URL correta baseada na URL do token
        auth_url = os.environ.get('PROTHEUS_AUTH_URL', '')
        if auth_url and '/documento-entrada/oauth2/token' in auth_url:
            # Extrair a base URL e adicionar /documento-entrada
            base_url = auth_url.replace('/documento-entrada/oauth2/token', '')
            api_url = f"{base_url}/documento-entrada"
            print(f"[9.0] URL da API inferida da URL do token: {api_url}")
    
    print(f"\n{'='*80}")
    print(f"[9] PREPARANDO ENVIO PARA PROTHEUS")
    print(f"{'='*80}")
    print(f"[9.1] URL da API: {api_url}")
    
    # Log do payload de forma fácil de visualizar
    print(f"\n[9.2] PAYLOAD COMPLETO:")
    print(f"{'-'*80}")
    payload_str = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    print(payload_str)
    print(f"{'-'*80}")
    
    # Validação final dos CNPJs antes de enviar
    if not payload.get('cnpjEmitente') or len(payload.get('cnpjEmitente', '')) < 14:
        print(f"\n[9.2.1] ERRO CRÍTICO: cnpjEmitente inválido no payload final!")
        print(f"  - Valor no payload: '{payload.get('cnpjEmitente')}'")
        print(f"  - Length: {len(payload.get('cnpjEmitente', ''))}")
        raise Exception(f"CNPJ do emitente inválido no payload final. Valor: '{payload.get('cnpjEmitente')}'")
    else:
        print(f"\n[9.2.1] Validação do payload:")
        print(f"  ✓ cnpjEmitente: {payload.get('cnpjEmitente')} ({len(payload.get('cnpjEmitente'))} dígitos)")
        if payload.get('cnpjDestinatario'):
            print(f"  ✓ cnpjDestinatario: {payload.get('cnpjDestinatario')} ({len(payload.get('cnpjDestinatario'))} dígitos)")
        else:
            print(f"  ⚠ cnpjDestinatario: não informado (opcional)")
        print(f"  ✓ documento: {payload.get('documento')}")
        print(f"  ✓ itens: {len(payload.get('itens', []))} produto(s)")
    
    try:
        # Obter token OAuth2
        access_token = get_oauth2_token()
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Adicionar token OAuth2 se disponível
        if access_token:
            headers['Authorization'] = f'Bearer {access_token}'
            print(f"\n[9.3] OAuth2 Token:")
            print(f"  - Token length: {len(access_token)} caracteres")
            print(f"  - Token preview: {access_token[:30]}...{access_token[-10:]}")
        else:
            print(f"\n[9.3] WARNING: OAuth2 token não disponível, tentando sem autenticação")
        
        # Adicionar tenantId no header (obrigatório)
        if tenant_id:
            headers['tenantId'] = str(tenant_id)
            print(f"\n[9.4] tenantId:")
            print(f"  - Valor: {tenant_id}")
            print(f"  - Adicionado ao header como 'tenantId'")
        else:
            print(f"\n[9.4] WARNING: tenantId NÃO encontrado! A requisição pode falhar.")
            print(f"  - Verifique se o tenantId está no header do INPUT_JSON ou nos metadados do processo")
        
        # Adicionar API key se disponível (fallback)
        api_key = os.environ.get('PROTHEUS_API_KEY')
        if api_key:
            headers['x-api-key'] = api_key
            print(f"\n[9.5] API Key adicionada ao header (fallback)")
        
        # Log completo dos headers de forma fácil de visualizar
        print(f"\n[9.6] HEADERS DA REQUISIÇÃO:")
        print(f"{'-'*80}")
        for key, value in headers.items():
            if key == 'Authorization':
                token_part = value.split(' ')[1] if ' ' in value else value
                print(f"  {key}: Bearer {token_part[:20]}... (token completo: {len(token_part)} chars)")
            elif key == 'x-api-key':
                print(f"  {key}: {'*' * min(len(value), 20)}... (oculto por segurança)")
            else:
                print(f"  {key}: {value}")
        print(f"{'-'*80}")
        
        print(f"\n[9.7] Fazendo requisição POST para: {api_url}")
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        
        print(f"\n{'='*80}")
        print(f"[10] RESPOSTA DA API PROTHEUS")
        print(f"{'='*80}")
        print(f"[10.1] Status Code: {response.status_code}")
        
        print(f"\n[10.2] Response Headers:")
        print(f"{'-'*80}")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        print(f"{'-'*80}")
        
        print(f"\n[10.3] Response Body:")
        print(f"{'-'*80}")
        try:
            # Tentar formatar como JSON se possível
            response_json = response.json()
            print(json.dumps(response_json, indent=2, ensure_ascii=False, default=str))
        except:
            # Se não for JSON, mostrar como texto
            print(response.text)
        print(f"{'-'*80}")
        
        # Verificar status code ANTES de fazer raise_for_status
        if response.status_code >= 400:
            # Erro HTTP - capturar detalhes antes de lançar exceção
            status_code = response.status_code
            error_message = f"Falha ao enviar para Protheus"
            error_details = {}
            protheus_cause = None
            
            try:
                response_body = response.json()
                error_code = response_body.get('errorCode', 'N/A')
                error_msg = response_body.get('message', 'Sem mensagem de erro')
                
                # Extrair campo "cause" se existir (erro do Protheus)
                if 'cause' in response_body:
                    protheus_cause = response_body.get('cause')
                    # Se for uma lista, manter como lista; se for string, manter como string
                    if isinstance(protheus_cause, list):
                        error_details['cause'] = protheus_cause
                    else:
                        error_details['cause'] = [protheus_cause] if protheus_cause else []
                
                error_details.update({
                    'status_code': status_code,
                    'response_body': response_body,
                    'error_code': error_code,
                    'error_message': error_msg
                })
                error_message = f"Falha ao enviar para Protheus (HTTP {status_code}): {error_code} - {error_msg}"
            except:
                # Se não for JSON, usar texto
                response_text = response.text[:500] if response.text else 'Sem resposta'
                error_details = {
                    'status_code': status_code,
                    'response_body': response_text
                }
                error_message = f"Falha ao enviar para Protheus (HTTP {status_code}): {response_text}"
            
            print(f"\n[10] ERRO HTTP ao chamar API Protheus:")
            print(f"[10.5] Status Code: {status_code}")
            print(f"[10.6] Detalhes: {json.dumps(error_details, indent=2, default=str)}")
            import traceback
            traceback.print_exc()
            
            # Criar exceção com detalhes completos, incluindo cause se for erro do Protheus
            # O cause será serializado no Cause do Step Functions e enviado no SNS
            error_with_details = Exception(error_message)
            error_with_details.error_details = error_details
            # O cause já está em error_details, será serializado no Cause do Step Functions
            raise error_with_details
        
        # Se chegou aqui, status code é 2xx - sucesso
        protheus_response = response.json()
        print(f"[10.4] JSON parseado com sucesso")
    except requests.exceptions.HTTPError as http_err:
        # Fallback para capturar HTTPError se ainda ocorrer
        error_message = f"Falha ao enviar para Protheus"
        error_details = {}
        protheus_cause = None
        
        if hasattr(http_err, 'response') and http_err.response is not None:
            status_code = http_err.response.status_code
            try:
                response_body = http_err.response.json()
                error_code = response_body.get('errorCode', 'N/A')
                error_msg = response_body.get('message', http_err.response.text[:500])
                
                # Extrair campo "cause" se existir (erro do Protheus)
                if 'cause' in response_body:
                    protheus_cause = response_body.get('cause')
                    # Se for uma lista, manter como lista; se for string, manter como string
                    if isinstance(protheus_cause, list):
                        error_details['cause'] = protheus_cause
                    else:
                        error_details['cause'] = [protheus_cause] if protheus_cause else []
                
                error_details.update({
                    'status_code': status_code,
                    'response_body': response_body,
                    'error_code': error_code,
                    'error_message': error_msg
                })
                error_message = f"Falha ao enviar para Protheus (HTTP {status_code}): {error_code} - {error_msg}"
            except:
                response_text = http_err.response.text[:500] if http_err.response.text else 'Sem resposta'
                error_details = {
                    'status_code': status_code,
                    'response_body': response_text
                }
                error_message = f"Falha ao enviar para Protheus (HTTP {status_code}): {response_text}"
        else:
            error_details = {'error': str(http_err)}
            error_message = f"Falha ao enviar para Protheus: {str(http_err)}"
        
        print(f"\n[10] ERRO HTTP ao chamar API Protheus (HTTPError):")
        print(f"[10.5] Detalhes: {json.dumps(error_details, indent=2, default=str)}")
        import traceback
        traceback.print_exc()
        
        # Criar exceção com detalhes completos, incluindo cause se for erro do Protheus
        # O cause já está em error_details, será serializado no Cause do Step Functions
        error_with_details = Exception(error_message)
        error_with_details.error_details = error_details
        raise error_with_details
    except Exception as e:
        print(f"\n[10] ERRO ao chamar API Protheus: {e}")
        import traceback
        traceback.print_exc()
        
        # Se a exceção já tem error_details com cause, apenas re-raise
        if hasattr(e, 'error_details') and isinstance(e.error_details, dict):
            raise e
        
        # Caso contrário, criar exceção simples sem cause
        error_with_details = Exception(f"Falha ao enviar para Protheus: {str(e)}")
        error_with_details.error_details = {'error': str(e), 'error_type': type(e).__name__}
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
