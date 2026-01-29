import os
import json
import boto3
import logging
import xml.etree.ElementTree as ET

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def handler(event, context):
    """Parse XML DANFE para JSON estruturado"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    bucket = os.environ['BUCKET_NAME']
    pk = f"PROCESS#{process_id}"
    
    try:
        # Buscar arquivo XML
        items = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': pk}
        )['Items']
        
        xml_file = None
        for item in items:
            if item.get('SK', '').startswith('FILE#') and item.get('FILE_NAME', '').lower().endswith('.xml'):
                xml_file = item
                break
        
        if not xml_file:
            error_msg = "XML não encontrado"
            logger.error(error_msg)
            update_process_status_to_failed(pk, process_id, error_msg, "XML_NOT_FOUND")
            raise Exception(error_msg)
        
        # Baixar XML do S3
        file_key = xml_file['FILE_KEY']
        logger.info(f"Downloading XML: {file_key}")
        
        response = s3.get_object(Bucket=bucket, Key=file_key)
        xml_content = response['Body'].read().decode('utf-8')
        
        # Parse XML
        parsed_data = parse_nfe_xml(xml_content)
        
        # Salvar no DynamoDB
        sk = f"PARSED_XML={xml_file['FILE_NAME']}"
        parsed_json = json.dumps(parsed_data)
        logger.info(f"Parsed data size: {len(parsed_json)} bytes")
        
        table.put_item(Item={
            'PK': pk,
            'SK': sk,
            'FILE_NAME': xml_file['FILE_NAME'],
            'PARSED_DATA': parsed_json,
            'SOURCE': 'XML'
        })
        
        logger.info(f"XML parsed and saved to DynamoDB successfully")
        
        # Retornar apenas process_id para próximo step
        return {
            'process_id': process_id
        }
    
    except Exception as e:
        error_msg = str(e)
        error_type = "PARSE_XML_ERROR"
        logger.error(f"Error parsing XML: {error_msg}")
        logger.exception("Full traceback:")
        
        # Atualizar status para FAILED e salvar erro
        update_process_status_to_failed(pk, process_id, error_msg, error_type)
        
        # Re-raise para que Step Functions capture o erro
        raise Exception(f"Parse XML failed: {error_msg}")

def update_process_status_to_failed(pk, process_id, error_message, error_type):
    """Atualiza o status do processo para FAILED e salva informações do erro"""
    try:
        from datetime import datetime
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Buscar item METADATA para atualizar
        metadata_sk = "METADATA"
        metadata_item = table.get_item(Key={'PK': pk, 'SK': metadata_sk})
        
        if 'Item' in metadata_item:
            # Atualizar status e adicionar erro
            table.update_item(
                Key={'PK': pk, 'SK': metadata_sk},
                UpdateExpression='SET #status = :status, error_info = :error, updated_at = :timestamp',
                ExpressionAttributeNames={
                    '#status': 'STATUS'
                },
                ExpressionAttributeValues={
                    ':status': 'FAILED',
                    ':error': {
                        'message': error_message,
                        'type': error_type,
                        'timestamp': timestamp,
                        'lambda': 'parse_xml'
                    },
                    ':timestamp': timestamp
                }
            )
            logger.info(f"Status atualizado para FAILED para processo {process_id}")
        else:
            # Se não existe METADATA, criar
            table.put_item(Item={
                'PK': pk,
                'SK': metadata_sk,
                'STATUS': 'FAILED',
                'PROCESS_ID': process_id,
                'error_info': {
                    'message': error_message,
                    'type': error_type,
                    'timestamp': timestamp,
                    'lambda': 'parse_xml'
                },
                'updated_at': timestamp
            })
            logger.info(f"Item METADATA criado com status FAILED para processo {process_id}")
            
    except Exception as update_error:
        logger.error(f"Erro ao atualizar status para FAILED: {update_error}")
        # Não re-raise para não mascarar o erro original

def parse_nfe_xml(xml_content):
    """Parse XML NFe para estrutura JSON"""
    root = ET.fromstring(xml_content)
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    
    inf_nfe = root.find('.//nfe:infNFe', ns)
    prot_nfe = root.find('.//nfe:protNFe/nfe:infProt', ns)
    
    # Dados principais
    ide = inf_nfe.find('nfe:ide', ns)
    emit = inf_nfe.find('nfe:emit', ns)
    dest = inf_nfe.find('nfe:dest', ns)
    total = inf_nfe.find('.//nfe:ICMSTot', ns)
    transp = inf_nfe.find('nfe:transp', ns)
    cobr = inf_nfe.find('nfe:cobr', ns)
    pag = inf_nfe.find('nfe:pag', ns)
    entrega = inf_nfe.find('nfe:entrega', ns)
    inf_resp_tec = inf_nfe.find('nfe:infRespTec', ns)
    
    # Extrair infAdic completo
    inf_adic = inf_nfe.find('nfe:infAdic', ns)
    inf_adic_text = get_text(inf_adic, 'nfe:infCpl', ns) if inf_adic is not None else None
    inf_ad_fisco = get_text(inf_adic, 'nfe:infAdFisco', ns) if inf_adic is not None else None
    
    # Produtos
    produtos = []
    for det in inf_nfe.findall('nfe:det', ns):
        prod = det.find('nfe:prod', ns)
        imposto = det.find('nfe:imposto', ns)
        
        # nItem é elemento, não atributo
        n_item = get_text(det, 'nfe:nItem', ns)
        
        # Coletar todos os rastros (pode haver múltiplos)
        rastros = []
        if prod:
            for r in prod.findall('nfe:rastro', ns):
                rastros.append({
                    'lote': get_text(r, 'nfe:nLote', ns),
                    'data_fabricacao': get_text(r, 'nfe:dFab', ns),
                    'data_validade': get_text(r, 'nfe:dVal', ns),
                    'quantidade': get_text(r, 'nfe:qLote', ns)
                })
        
        produto = {
            'item': n_item,
            'codigo': get_text(prod, 'nfe:cProd', ns),
            'descricao': get_text(prod, 'nfe:xProd', ns),
            'ncm': get_text(prod, 'nfe:NCM', ns),
            'cfop': get_text(prod, 'nfe:CFOP', ns),
            'unidade': get_text(prod, 'nfe:uCom', ns),
            'quantidade': get_text(prod, 'nfe:qCom', ns),
            'valor_unitario': get_text(prod, 'nfe:vUnCom', ns),
            'valor_total': get_text(prod, 'nfe:vProd', ns),
            'ean': get_text(prod, 'nfe:cEAN', ns),
            'ean_trib': get_text(prod, 'nfe:cEANTrib', ns),
            'unidade_trib': get_text(prod, 'nfe:uTrib', ns),
            'quantidade_trib': get_text(prod, 'nfe:qTrib', ns),
            'valor_unitario_trib': get_text(prod, 'nfe:vUnTrib', ns),
            'pedido': get_text(prod, 'nfe:xPed', ns),
            'item_pedido': get_text(prod, 'nfe:nItemPed', ns),
            'ind_tot': get_text(prod, 'nfe:indTot', ns),
            'info_adicional': get_text(det, 'nfe:infAdProd', ns),
            'rastro': rastros if rastros else None,
            'icms': extract_icms(imposto, ns),
            'ipi': extract_ipi(imposto, ns),
            'pis': extract_pis(imposto, ns),
            'cofins': extract_cofins(imposto, ns)
        }
        produtos.append(produto)
    
    # Duplicatas
    duplicatas = []
    if cobr:
        for dup in cobr.findall('.//nfe:dup', ns):
            duplicatas.append({
                'numero': get_text(dup, 'nfe:nDup', ns),
                'vencimento': get_text(dup, 'nfe:dVenc', ns),
                'valor': get_text(dup, 'nfe:vDup', ns)
            })
    
    # Endereço emitente
    ender_emit = emit.find('nfe:enderEmit', ns) if emit else None
    # Endereço destinatário
    ender_dest = dest.find('nfe:enderDest', ns) if dest else None
    # Transportadora
    transporta = transp.find('nfe:transporta', ns) if transp else None
    veiculo = transp.find('nfe:veicTransp', ns) if transp else None
    vol = transp.find('nfe:vol', ns) if transp else None
    
    # Pagamento
    det_pag = pag.find('nfe:detPag', ns) if pag else None
    
    # Fatura
    fat = cobr.find('nfe:fat', ns) if cobr else None
    
    return {
        'chave_acesso': inf_nfe.get('Id', '').replace('NFe', '') if inf_nfe else None,
        'numero_nota': get_text(ide, 'nfe:nNF', ns),
        'serie': get_text(ide, 'nfe:serie', ns),
        'modelo': get_text(ide, 'nfe:mod', ns),
        'data_emissao': get_text(ide, 'nfe:dhEmi', ns),
        'data_saida': get_text(ide, 'nfe:dhSaiEnt', ns),
        'tipo_nf': get_text(ide, 'nfe:tpNF', ns),
        'natureza_operacao': get_text(ide, 'nfe:natOp', ns),
        'finalidade': get_text(ide, 'nfe:finNFe', ns),
        'codigo_nf': get_text(ide, 'nfe:cNF', ns),
        'digito_verificador': get_text(ide, 'nfe:cDV', ns),
        'ambiente': get_text(ide, 'nfe:tpAmb', ns),
        'tipo_emissao': get_text(ide, 'nfe:tpEmis', ns),
        'tipo_impressao': get_text(ide, 'nfe:tpImp', ns),
        'destino_operacao': get_text(ide, 'nfe:idDest', ns),
        'consumidor_final': get_text(ide, 'nfe:indFinal', ns),
        'presenca': get_text(ide, 'nfe:indPres', ns),
        'intermediador': get_text(ide, 'nfe:indIntermed', ns),
        'versao_processo': get_text(ide, 'nfe:verProc', ns),
        'info_adicional': inf_adic_text,
        'info_fisco': inf_ad_fisco,
        'protocolo': {
            'numero': get_text(prot_nfe, 'nfe:nProt', ns),
            'data_recebimento': get_text(prot_nfe, 'nfe:dhRecbto', ns),
            'digest_value': get_text(prot_nfe, 'nfe:digVal', ns),
            'status': get_text(prot_nfe, 'nfe:cStat', ns),
            'motivo': get_text(prot_nfe, 'nfe:xMotivo', ns)
        } if prot_nfe is not None else None,
        'responsavel_tecnico': {
            'cnpj': get_text(inf_resp_tec, 'nfe:CNPJ', ns),
            'contato': get_text(inf_resp_tec, 'nfe:xContato', ns),
            'email': get_text(inf_resp_tec, 'nfe:email', ns),
            'fone': get_text(inf_resp_tec, 'nfe:fone', ns)
        } if inf_resp_tec is not None else None,
        'emitente': {
            'cnpj': get_text(emit, 'nfe:CNPJ', ns),
            'nome': get_text(emit, 'nfe:xNome', ns),
            'fantasia': get_text(emit, 'nfe:xFant', ns),
            'ie': get_text(emit, 'nfe:IE', ns),
            'crt': get_text(emit, 'nfe:CRT', ns),
            'endereco': {
                'logradouro': get_text(ender_emit, 'nfe:xLgr', ns),
                'numero': get_text(ender_emit, 'nfe:nro', ns),
                'complemento': get_text(ender_emit, 'nfe:xCpl', ns),
                'bairro': get_text(ender_emit, 'nfe:xBairro', ns),
                'municipio': get_text(ender_emit, 'nfe:xMun', ns),
                'codigo_municipio': get_text(ender_emit, 'nfe:cMun', ns),
                'uf': get_text(ender_emit, 'nfe:UF', ns),
                'cep': get_text(ender_emit, 'nfe:CEP', ns),
                'pais': get_text(ender_emit, 'nfe:xPais', ns),
                'codigo_pais': get_text(ender_emit, 'nfe:cPais', ns)
            } if ender_emit else None
        },
        'destinatario': {
            'cnpj': get_text(dest, 'nfe:CNPJ', ns),
            'nome': get_text(dest, 'nfe:xNome', ns),
            'ie': get_text(dest, 'nfe:IE', ns),
            'indicador_ie': get_text(dest, 'nfe:indIEDest', ns),
            'endereco': {
                'logradouro': get_text(ender_dest, 'nfe:xLgr', ns),
                'numero': get_text(ender_dest, 'nfe:nro', ns),
                'complemento': get_text(ender_dest, 'nfe:xCpl', ns),
                'bairro': get_text(ender_dest, 'nfe:xBairro', ns),
                'municipio': get_text(ender_dest, 'nfe:xMun', ns),
                'codigo_municipio': get_text(ender_dest, 'nfe:cMun', ns),
                'uf': get_text(ender_dest, 'nfe:UF', ns),
                'cep': get_text(ender_dest, 'nfe:CEP', ns),
                'pais': get_text(ender_dest, 'nfe:xPais', ns),
                'codigo_pais': get_text(ender_dest, 'nfe:cPais', ns),
                'fone': get_text(ender_dest, 'nfe:fone', ns)
            } if ender_dest else None
        },
        'entrega': {
            'cnpj': get_text(entrega, 'nfe:CNPJ', ns),
            'ie': get_text(entrega, 'nfe:IE', ns),
            'nome': get_text(entrega, 'nfe:xNome', ns),
            'logradouro': get_text(entrega, 'nfe:xLgr', ns),
            'numero': get_text(entrega, 'nfe:nro', ns),
            'complemento': get_text(entrega, 'nfe:xCpl', ns),
            'bairro': get_text(entrega, 'nfe:xBairro', ns),
            'municipio': get_text(entrega, 'nfe:xMun', ns),
            'codigo_municipio': get_text(entrega, 'nfe:cMun', ns),
            'uf': get_text(entrega, 'nfe:UF', ns),
            'cep': get_text(entrega, 'nfe:CEP', ns),
            'pais': get_text(entrega, 'nfe:xPais', ns),
            'codigo_pais': get_text(entrega, 'nfe:cPais', ns)
        } if entrega else None,
        'produtos': produtos,
        'totais': {
            'base_calculo_icms': get_text(total, 'nfe:vBC', ns),
            'valor_icms': get_text(total, 'nfe:vICMS', ns),
            'valor_icms_desonerado': get_text(total, 'nfe:vICMSDeson', ns),
            'base_calculo_icms_st': get_text(total, 'nfe:vBCST', ns),
            'valor_icms_st': get_text(total, 'nfe:vST', ns),
            'valor_produtos': get_text(total, 'nfe:vProd', ns),
            'valor_frete': get_text(total, 'nfe:vFrete', ns),
            'valor_seguro': get_text(total, 'nfe:vSeg', ns),
            'valor_desconto': get_text(total, 'nfe:vDesc', ns),
            'valor_ii': get_text(total, 'nfe:vII', ns),
            'valor_ipi': get_text(total, 'nfe:vIPI', ns),
            'valor_ipi_devolvido': get_text(total, 'nfe:vIPIDevol', ns),
            'valor_pis': get_text(total, 'nfe:vPIS', ns),
            'valor_cofins': get_text(total, 'nfe:vCOFINS', ns),
            'valor_outros': get_text(total, 'nfe:vOutro', ns),
            'valor_nota': get_text(total, 'nfe:vNF', ns),
            'valor_fcp': get_text(total, 'nfe:vFCP', ns),
            'valor_fcp_st': get_text(total, 'nfe:vFCPST', ns),
            'valor_fcp_st_ret': get_text(total, 'nfe:vFCPSTRet', ns)
        },
        'cobranca': {
            'fatura': {
                'numero': get_text(fat, 'nfe:nFat', ns),
                'valor_original': get_text(fat, 'nfe:vOrig', ns),
                'valor_desconto': get_text(fat, 'nfe:vDesc', ns),
                'valor_liquido': get_text(fat, 'nfe:vLiq', ns)
            } if fat else None,
            'duplicatas': duplicatas if duplicatas else None
        } if cobr else None,
        'pagamento': {
            'tipo': get_text(det_pag, 'nfe:tPag', ns),
            'valor': get_text(det_pag, 'nfe:vPag', ns),
            'indicador': get_text(det_pag, 'nfe:indPag', ns)
        } if det_pag else None,
        'transporte': {
            'modalidade_frete': get_text(transp, 'nfe:modFrete', ns),
            'transportadora': {
                'cnpj': get_text(transporta, 'nfe:CNPJ', ns),
                'cpf': get_text(transporta, 'nfe:CPF', ns),
                'nome': get_text(transporta, 'nfe:xNome', ns),
                'ie': get_text(transporta, 'nfe:IE', ns),
                'endereco': get_text(transporta, 'nfe:xEnder', ns),
                'municipio': get_text(transporta, 'nfe:xMun', ns),
                'uf': get_text(transporta, 'nfe:UF', ns)
            } if transporta else None,
            'veiculo': {
                'placa': get_text(veiculo, 'nfe:placa', ns),
                'uf': get_text(veiculo, 'nfe:UF', ns),
                'rntc': get_text(veiculo, 'nfe:RNTC', ns)
            } if veiculo else None,
            'volume': {
                'quantidade': get_text(vol, 'nfe:qVol', ns),
                'especie': get_text(vol, 'nfe:esp', ns),
                'marca': get_text(vol, 'nfe:marca', ns),
                'numeracao': get_text(vol, 'nfe:nVol', ns),
                'peso_liquido': get_text(vol, 'nfe:pesoL', ns),
                'peso_bruto': get_text(vol, 'nfe:pesoB', ns)
            } if vol else None
        } if transp else None
    }

def get_text(element, tag, ns):
    """Extrai texto de elemento XML"""
    if element is None:
        return None
    found = element.find(tag, ns)
    return found.text if found is not None else None

def extract_icms(imposto, ns):
    """Extrai dados de ICMS"""
    if imposto is None:
        return {}
    
    icms = imposto.find('nfe:ICMS', ns)
    if icms is None:
        return {}
    
    # Pode ser ICMS00, ICMS10, ICMS40, ICMS51, etc
    for child in icms:
        return {
            'cst': get_text(child, 'nfe:CST', ns),
            'origem': get_text(child, 'nfe:orig', ns),
            'base_calculo': get_text(child, 'nfe:vBC', ns),
            'aliquota': get_text(child, 'nfe:pICMS', ns),
            'valor': get_text(child, 'nfe:vICMS', ns)
        }
    
    return {}

def extract_ipi(imposto, ns):
    """Extrai dados de IPI"""
    if imposto is None:
        return {}
    
    ipi = imposto.find('nfe:IPI', ns)
    if ipi is None:
        return {}
    
    result = {'cenq': get_text(ipi, 'nfe:cEnq', ns)}
    
    for child in ipi:
        if child.tag.endswith('IPINT') or child.tag.endswith('IPITrib'):
            result['cst'] = get_text(child, 'nfe:CST', ns)
            result['base_calculo'] = get_text(child, 'nfe:vBC', ns)
            result['aliquota'] = get_text(child, 'nfe:pIPI', ns)
            result['valor'] = get_text(child, 'nfe:vIPI', ns)
    
    return result

def extract_pis(imposto, ns):
    """Extrai dados de PIS"""
    if imposto is None:
        return {}
    
    pis = imposto.find('nfe:PIS', ns)
    if pis is None:
        return {}
    
    for child in pis:
        return {
            'cst': get_text(child, 'nfe:CST', ns),
            'base_calculo': get_text(child, 'nfe:vBC', ns),
            'aliquota': get_text(child, 'nfe:pPIS', ns),
            'valor': get_text(child, 'nfe:vPIS', ns)
        }
    
    return {}

def extract_cofins(imposto, ns):
    """Extrai dados de COFINS"""
    if imposto is None:
        return {}
    
    cofins = imposto.find('nfe:COFINS', ns)
    if cofins is None:
        return {}
    
    for child in cofins:
        return {
            'cst': get_text(child, 'nfe:CST', ns),
            'base_calculo': get_text(child, 'nfe:vBC', ns),
            'aliquota': get_text(child, 'nfe:pCOFINS', ns),
            'valor': get_text(child, 'nfe:vCOFINS', ns)
        }
    
    return {}
