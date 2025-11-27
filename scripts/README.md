# Avaliador de Documentos - Textract + Bedrock

Sistema para extrair e estruturar informações de notas fiscais e pedidos de compra usando Amazon Textract e Amazon Bedrock com modelo Nova.

## Configuração

1. Instale as dependências:
```bash
pip install -r requirements.txt
```

2. Configure suas credenciais AWS:
```bash
aws configure
```

## Uso

1. Coloque seus PDFs na pasta `documentos/` (pode ter subpastas)
2. Execute o script:
```bash
python processar_documentos.py
```
3. Os resultados serão salvos em `resultados.json` em cada pasta que contém documentos

## Tipos de documento suportados

- **Nota Fiscal**: Extrai número, datas, CNPJ, valores, itens e impostos
- **Pedido de Compra**: Extrai número, fornecedor, itens e valores

## Formatos suportados

- PDF
- PNG, JPG, JPEG

## Estrutura de saída

### Nota Fiscal
```json
{
  "numero_nota": "123456",
  "data_emissao": "2024-01-15",
  "cnpj_emitente": "12.345.678/0001-90",
  "nome_emitente": "Empresa ABC",
  "valor_total": 1500.00,
  "itens": [
    {
      "descricao": "Produto X",
      "quantidade": 10,
      "valor_unitario": 150.00,
      "valor_total": 1500.00
    }
  ]
}
```

### Pedido de Compra
```json
{
  "numero_pedido": "PC-2024-001",
  "data_pedido": "2024-01-15",
  "fornecedor": "Fornecedor XYZ",
  "valor_total": 2500.00,
  "itens": [
    {
      "codigo": "PROD001",
      "descricao": "Material ABC",
      "quantidade": 5,
      "valor_unitario": 500.00
    }
  ]
}
```