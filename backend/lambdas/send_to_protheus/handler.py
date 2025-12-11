import json
import os
import boto3
import requests
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

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
    
    # Extrair dados do XML
    emitente = xml_data.get('emitente', {})
    destinatario = xml_data.get('destinatario', {})
    entrega = xml_data.get('entrega', {})
    totais = xml_data.get('totais', {})
    produtos_xml = xml_data.get('produtos', [])
    cobranca = xml_data.get('cobranca', {})
    transporte = xml_data.get('transporte', {})
    
    # Mapear modalidade frete: 0=CIF, 1=FOB, 9=Sem frete
    modalidade_frete = transporte.get('modalidade_frete', '')
    tipo_frete_map = {'0': 'CIF', '1': 'FOB', '9': 'SEM'}
    tipo_frete = ocr_data.get('tipoFrete') or tipo_frete_map.get(modalidade_frete, '')
    
    # Montar payload seguindo estrutura exata do exemplo
    payload = {
        "cnpjRemetente": emitente.get('cnpj') or "",
        "cnpjDestinatario": destinatario.get('cnpj') or "",
        "filial": "",
        "tipoDeDocumento": "N",
        "documento": xml_data.get('numero_nota', '').zfill(9),
        "serie": xml_data.get('serie') or "",
        "dataEmissao": xml_data.get('data_emissao', '').split('T')[0] if xml_data.get('data_emissao') else "",
        "dataDigitacao": xml_data.get('data_emissao', '').split('T')[0] if xml_data.get('data_emissao') else "",
        "fornecedor": {
            "codigo": "",
            "loja": ""
        },
        "especie": "NF",
        "chaveAcesso": xml_data.get('chave_acesso') or "",
        "condicaoPagamento": ocr_data.get('condicaoPagamento') or "",
        "tipoFrete": tipo_frete,
        "moeda": ocr_data.get('moeda') or "BRL",
        "taxaCambio": 1,
        "itens": []
    }
    
    # Adicionar duplicatas se disponíveis
    if cobranca and cobranca.get('duplicatas'):
        duplicatas = cobranca['duplicatas']
        payload['duplicata'] = {
            "natureza": "",
            "itens": [
                {
                    "parcela": dup.get('numero', '').zfill(3),
                    "dataVencimento": dup.get('vencimento', '').split('T')[0] if dup.get('vencimento') else "",
                    "valor": float(dup.get('valor', 0))
                }
                for dup in duplicatas
            ]
        }
    
    # Adicionar impostos do cabeçalho
    payload['impostos'] = {
        "cabecalho": {
            "desconto": float(totais.get('valor_desconto', 0)),
            "despesas": float(totais.get('valor_outros', 0)),
            "frete": float(totais.get('valor_frete', 0)),
            "seguro": float(totais.get('valor_seguro', 0))
        },
        "itens": []
    }
    
    print(f"[7.1] Payload base montado")
    
    # Adicionar produtos do XML
    print(f"\n[8] Processando produtos do XML...")
    print(f"[8.1] Total de produtos encontrados: {len(produtos_xml)}")
    
    for idx, produto in enumerate(produtos_xml, 1):
        try:
            # Normalizar código produto (remover zeros à esquerda se numérico)
            codigo = produto.get('codigo', '')
            if codigo.isdigit():
                codigo = codigo.lstrip('0') or '0'
            
            item = {
                "item": produto.get('item', '').zfill(4),
                "codigoProduto": codigo,
                "descricaoProduto": produto.get('descricao') or "",
                "unidadeMedida": produto.get('unidade') or "",
                "armazem": "",
                "quantidade": float(produto.get('quantidade', 0)),
                "valorUnitario": float(produto.get('valor_unitario', 0)),
                "valorTotal": float(produto.get('valor_total', 0)),
                "codigoOperacao": produto.get('cfop', ''),
                "tes": None,
                "pedidoDeCompra": {
                    "pedidoFornecedor": produto.get('pedido', ''),
                    "pedidoErp": produto.get('pedido', ''),
                    "itemPedidoErp": produto.get('item_pedido', '')
                },
                "documentoOrigem": "",
                "itemDocumentoOrigem": "",
                "contaContabil": "",
                "centroCusto": "",
                "itemContaContabil": ""
            }
            payload['itens'].append(item)
            
            # Adicionar impostos do item
            icms = produto.get('icms', {})
            ipi = produto.get('ipi', {})
            pis = produto.get('pis', {})
            cofins = produto.get('cofins', {})
            
            imposto_item = {
                "item": item['item'],
                "baseICMS": float(icms.get('base_calculo') or 0),
                "aliquotaICMS": float(icms.get('aliquota') or 0),
                "valorICMS": float(icms.get('valor') or 0),
                "baseICMSST": 0,
                "aliquotaICMSST": 0,
                "valorICMSST": 0,
                "valorIPI": float(ipi.get('valor') or 0),
                "valorPIS": float(pis.get('valor') or 0),
                "valorCOFINS": float(cofins.get('valor') or 0),
                "valorISSQN": 0,
                "valorOutrosImpostos": 0
            }
            payload['impostos']['itens'].append(imposto_item)
                
        except (ValueError, TypeError) as e:
            print(f"  - ERRO ao converter valores numéricos: {e}")
    
    # Enviar para Protheus
    api_url = os.environ.get('PROTHEUS_API_URL', 'https://virtserver.swaggerhub.com/agroamazonia/fast-ocr/1.0.0/documentos-entrada')
    
    print(f"\n[9] Enviando para Protheus...")
    print(f"[9.1] URL: {api_url}")
    print(f"[9.2] Payload completo (JSON string):")
    payload_str = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    print(payload_str)
    
    try:
        response = requests.post(api_url, json=payload, timeout=30)
        print(f"\n[10] Resposta da API Protheus:")
        print(f"[10.1] Status Code: {response.status_code}")
        print(f"[10.2] Headers: {dict(response.headers)}")
        print(f"[10.3] Body: {response.text}")
        
        response.raise_for_status()
        protheus_response = response.json()
        print(f"[10.4] JSON parseado com sucesso")
    except Exception as e:
        print(f"\n[10] ERRO ao chamar API Protheus: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    # Extrair id_unico do campo 'idUnico' da resposta
    id_unico = protheus_response.get('idUnico')
    print(f"\n[11] ID Único extraído: {id_unico}")
    
    # Atualizar status no DynamoDB com id_unico da API
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
