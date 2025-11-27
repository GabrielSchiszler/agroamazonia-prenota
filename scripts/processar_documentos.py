import boto3
import json
import os
import fitz  # PyMuPDF
from PIL import Image
import io

def pdf_para_imagem(pdf_bytes):
    """Converte primeira página do PDF para imagem"""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x zoom
    img_data = pix.tobytes("png")
    doc.close()
    return img_data

def processar_pasta(caminho_pasta, textract, bedrock):
    """Processa todos os documentos de uma pasta e salva resultado local"""
    resultados = []
    
    for item in os.listdir(caminho_pasta):
        item_path = os.path.join(caminho_pasta, item)
        
        if os.path.isdir(item_path):
            processar_pasta(item_path, textract, bedrock)
        
        elif item.lower().endswith(('.pdf', '.png', '.jpg', '.jpeg')):
            print(f"Processando: {item_path}")
            
            try:
                with open(item_path, 'rb') as f:
                    documento_bytes = f.read()
                
                # Converte PDF para imagem se necessário
                if item.lower().endswith('.pdf'):
                    try:
                        documento_bytes = pdf_para_imagem(documento_bytes)
                    except:
                        pass  # Usa PDF original se conversão falhar
                
                # Extrai texto com Textract
                response = textract.detect_document_text(Document={'Bytes': documento_bytes})
                texto_extraido = '\n'.join([block['Text'] for block in response['Blocks'] if block['BlockType'] == 'LINE'])
                
                # Determina tipo do documento
                tipo_doc = 'nota_fiscal' if any(x in item.lower() for x in ['nf', 'nota', 'fiscal']) else 'pedido_compra'
                
                # Prompt para estruturar dados
                if tipo_doc == 'nota_fiscal':
                    prompt = f"""Extraia TODAS as informações da nota fiscal em JSON completo:
                    
                    DADOS GERAIS:
                    - numero_nota, serie, data_emissao, data_vencimento, hora_emissao
                    - tipo_operacao, natureza_operacao, finalidade
                    
                    EMITENTE:
                    - cnpj_emitente, nome_emitente, razao_social_emitente
                    - endereco_emitente (logradouro, numero, bairro, cidade, uf, cep)
                    - inscricao_estadual_emitente, inscricao_municipal_emitente
                    - telefone_emitente, email_emitente
                    
                    DESTINATARIO:
                    - cnpj_destinatario, nome_destinatario, razao_social_destinatario
                    - endereco_destinatario (logradouro, numero, bairro, cidade, uf, cep)
                    - inscricao_estadual_destinatario, inscricao_municipal_destinatario
                    
                    VALORES:
                    - valor_produtos, valor_frete, valor_seguro, valor_desconto
                    - valor_outras_despesas, valor_ipi, valor_pis, valor_cofins
                    - valor_icms, valor_icms_st, valor_total_tributos, valor_total_nota
                    
                    ITENS (array):
                    - codigo, descricao, ncm, cfop, unidade
                    - quantidade, valor_unitario, valor_total_item
                    - aliquota_icms, valor_icms_item, aliquota_ipi, valor_ipi_item
                    
                    TRANSPORTE:
                    - modalidade_frete, transportadora, cnpj_transportadora
                    - placa_veiculo, uf_veiculo, peso_bruto, peso_liquido
                    
                    Texto: {texto_extraido}
                    
                    JSON:"""
                else:
                    prompt = f"""Extraia TODAS as informações do pedido de compra em JSON completo:
                    
                    DADOS GERAIS:
                    - numero_pedido, data_pedido, data_entrega, prazo_entrega
                    - condicoes_pagamento, forma_pagamento, observacoes
                    
                    FORNECEDOR:
                    - cnpj_fornecedor, nome_fornecedor, razao_social_fornecedor
                    - endereco_fornecedor (logradouro, numero, bairro, cidade, uf, cep)
                    - inscricao_estadual_fornecedor, telefone_fornecedor, email_fornecedor
                    
                    SOLICITANTE/COMPRADOR:
                    - cnpj_solicitante, nome_solicitante, endereco_solicitante
                    - contato_solicitante, departamento, centro_custo
                    
                    VALORES:
                    - subtotal_produtos, valor_frete, valor_desconto
                    - valor_acrescimos, valor_impostos, valor_total_pedido
                    
                    ITENS (array):
                    - codigo_item, descricao_completa, especificacao
                    - unidade_medida, quantidade_solicitada, valor_unitario
                    - valor_total_item, prazo_entrega_item, marca
                    
                    ENTREGA:
                    - local_entrega, endereco_entrega, responsavel_recebimento
                    - horario_entrega, instrucoes_especiais
                    
                    Texto: {texto_extraido}
                    
                    JSON:"""
                
                # Chama Bedrock Nova
                response = bedrock.converse(
                    modelId='amazon.nova-lite-v1:0',
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"maxTokens": 2000, "temperature": 0.1}
                )
                
                dados_estruturados = response['output']['message']['content'][0]['text']
                
                # Extrai JSON da resposta (remove markdown se houver)
                if '```json' in dados_estruturados:
                    dados_estruturados = dados_estruturados.split('```json')[1].split('```')[0].strip()
                elif '```' in dados_estruturados:
                    dados_estruturados = dados_estruturados.split('```')[1].split('```')[0].strip()
                
                try:
                    dados_json = json.loads(dados_estruturados)
                except:
                    dados_json = {"erro_parse": "Não foi possível converter para JSON", "resposta_raw": dados_estruturados, "texto_extraido": texto_extraido[:1000]}
                
                resultado = {
                    "arquivo": item,
                    "tipo": tipo_doc,
                    "dados": dados_json
                }
                
                resultados.append(resultado)
                print(f"✓ {item} processado")
                
            except Exception as e:
                print(f"✗ Erro em {item}: {e}")
                resultados.append({"arquivo": item, "erro": str(e)})
    
    # Salva resultados na pasta atual se houver documentos processados
    if resultados:
        arquivo_resultado = os.path.join(caminho_pasta, 'resultados.json')
        with open(arquivo_resultado, 'w', encoding='utf-8') as f:
            json.dump(resultados, f, indent=2, ensure_ascii=False)
        print(f"Resultados salvos em: {arquivo_resultado}")

def processar_documentos():
    # Configuração AWS
    textract = boto3.client('textract', region_name='us-east-1')
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    pasta_documentos = "documentos"
    
    if not os.path.exists(pasta_documentos):
        print(f"Pasta '{pasta_documentos}' não encontrada!")
        return
    
    processar_pasta(pasta_documentos, textract, bedrock)
    print("\nProcessamento concluído!")

if __name__ == "__main__":
    processar_documentos()