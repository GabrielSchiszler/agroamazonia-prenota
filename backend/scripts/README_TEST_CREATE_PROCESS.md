# 📋 Script de Teste - Criar Processo com Documentos

Este script facilita a criação de um processo de teste com todos os documentos necessários.

## 🎯 Funcionalidades

- ✅ Cria um processo automaticamente
- ✅ Faz upload de um XML (DANFE) - pode usar arquivo local ou gerar automaticamente
- ✅ Faz upload de um documento adicional (PDF vazio) com metadados JSON
- ✅ Opcionalmente inicia o processamento

## 📦 Dependências

```bash
pip install requests
```

Opcional (para gerar PDF melhor):
```bash
pip install reportlab
```

Se `reportlab` não estiver instalado, o script criará um PDF mínimo válido sem bibliotecas externas.

## 🚀 Uso

### Opção 1: Usando valores padrão (produção)

O script usa as configurações de produção por padrão:

```bash
python3 test_create_process.py [--xml-file caminho/para/seu/arquivo.xml] [--start]
```

**Valores padrão:**
- API URL: `https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1`
- API Key: `agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx`

### Opção 2: Usando argumentos de linha de comando

```bash
python3 test_create_process.py \
  --api-url https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1 \
  --api-key agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx \
  [--xml-file caminho/para/seu/arquivo.xml] \
  [--start]
```

**Para desenvolvimento local:**
```bash
python3 test_create_process.py \
  --api-url http://localhost:8001 \
  --api-key dev \
  [--xml-file caminho/para/seu/arquivo.xml] \
  [--start]
```

### Opção 3: Usando arquivo .env

Crie um arquivo `.env` na pasta `backend/scripts/` (ou copie de `env.example`):

**Para desenvolvimento local:**
```env
API_URL=http://localhost:8001
API_KEY=dev
```

**Para produção:**
```env
API_URL=https://l7ergug2q0.execute-api.us-east-1.amazonaws.com
API_KEY=agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx
```

Depois execute:

```bash
python3 test_create_process.py [--start]
```

**Nota:** Você pode copiar o arquivo de exemplo:
```bash
cp env.example .env
# Edite o .env com suas configurações
```

## 📝 Parâmetros

| Parâmetro | Descrição | Obrigatório | Padrão |
|-----------|-----------|-------------|--------|
| `--api-url` | URL base da API | Não | `https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1` |
| `--api-key` | Chave de API | Não | `agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx` |
| `--xml-file` | Caminho para arquivo XML | Não | `test_nfe.xml` |
| `--start` | Iniciar processamento após criar | Não | `False` |
| `--env-file` | Arquivo .env para carregar | Não | `.env` |

**Nota:** Os valores padrão são de produção. Para desenvolvimento local, use `--api-url http://localhost:8001 --api-key dev`

## 📄 Arquivos Gerados

### XML (DANFE)

Se não especificar `--xml-file`, o script criará automaticamente um arquivo `test_nfe.xml` com um exemplo de NFe baseado no template fornecido.

Você pode:
- Usar seu próprio arquivo XML: `--xml-file meu_arquivo.xml`
- Deixar o script criar automaticamente: não use `--xml-file`

### PDF (Documento Adicional)

O script cria automaticamente um PDF vazio e faz upload junto com os metadados JSON fornecidos.

## 📊 Metadados JSON

O documento adicional é enviado com os seguintes metadados JSON (baseado no exemplo fornecido):

```json
{
  "header": {
    "tenantId": "00,010101"
  },
  "requestBody": {
    "moeda": "BRL",
    "itens": [...],
    "cnpjEmitente": "47180625006349",
    "cnpjDestinatario": "13563680000101"
  }
}
```

## 🔄 Fluxo do Script

1. **Gera Process ID** - Cria um UUID único para o processo
2. **Prepara XML** - Lê arquivo XML local ou cria um de exemplo
3. **Obtém URL para XML** - Solicita presigned URL para upload do DANFE
4. **Faz Upload do XML** - Envia o arquivo XML para S3
5. **Obtém URL para Documento Adicional** - Solicita presigned URL com metadados JSON
6. **Cria e Faz Upload do PDF** - Gera PDF vazio e envia para S3
7. **Verifica Processo** - Consulta o processo criado para confirmar
8. **Inicia Processamento** (opcional) - Se usar `--start`, inicia o workflow

## 📤 Exemplo de Saída

```
================================================================================
TESTE DE CRIAÇÃO DE PROCESSO COM DOCUMENTOS
================================================================================

ℹ️  Usando API URL padrão: https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1
ℹ️  Usando API Key padrão: agroamazonia_key_UPXsb8Hb8sjbxWBQq...
API URL: https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1
API Key: agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx

✓ Process ID gerado: 7d48cd96-c099-48dd-bbb6-d4fe8b2de318

📄 Lendo arquivo XML: test_nfe.xml
✓ XML carregado (12345 bytes)

================================================================================
1️⃣  OBTENDO URL PARA UPLOAD DO XML (DANFE)
================================================================================
✓ URL obtida: https://s3.amazonaws.com/...

================================================================================
2️⃣  FAZENDO UPLOAD DO XML
================================================================================
✓ XML enviado com sucesso!

================================================================================
3️⃣  OBTENDO URL PARA UPLOAD DO DOCUMENTO ADICIONAL
================================================================================
✓ URL obtida: https://s3.amazonaws.com/...
✓ Metadados JSON incluídos no documento adicional

================================================================================
4️⃣  CRIANDO E FAZENDO UPLOAD DO PDF VAZIO
================================================================================
✓ PDF vazio criado (1234 bytes)
✓ PDF enviado com sucesso!

================================================================================
5️⃣  VERIFICANDO PROCESSO CRIADO
================================================================================
✓ Processo encontrado:
   Status: CREATED
   Tipo: None
   DANFE: 1 arquivo(s)
   Adicionais: 1 arquivo(s)

================================================================================
✅ PROCESSO CRIADO COM SUCESSO!
================================================================================

Process ID: 7d48cd96-c099-48dd-bbb6-d4fe8b2de318
XML: test_nfe.xml
PDF: documento_adicional.pdf (com metadados JSON)
```

## 🔍 Verificar Processo Criado

Após executar o script, você pode verificar o processo:

```bash
curl -X GET "https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1/api/process/{process_id}" \
  -H "x-api-key: agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx"
```

## 🚀 Iniciar Processamento

Se não usou `--start`, você pode iniciar o processamento depois:

```bash
curl -X POST "https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1/api/process/start" \
  -H "Content-Type: application/json" \
  -H "x-api-key: agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx" \
  -d '{"process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318"}'
```

Ou simplesmente execute o script novamente com `--start`:

```bash
python3 test_create_process.py --start
```

## ⚠️ Notas Importantes

1. **Arquivo XML**: Se você fornecer um arquivo XML local, ele será usado. Caso contrário, o script criará um arquivo de exemplo automaticamente.

2. **PDF Vazio**: O PDF é criado automaticamente. Se `reportlab` estiver instalado, será um PDF válido com uma página vazia. Caso contrário, será um PDF mínimo válido.

3. **Metadados**: Os metadados JSON são enviados junto com o documento adicional através do campo `metadados` na requisição de presigned URL.

4. **API Key**: Por padrão, usa as configurações de produção. Para desenvolvimento local, use `--api-url http://localhost:8001 --api-key dev`.

5. **Configurações Padrão (Produção)**: 
   - API URL: `https://l7ergug2q0.execute-api.us-east-1.amazonaws.com/v1`
   - API Key: `agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx`
   - ⚠️ **Nota**: Os ajustes do envio dos dados para a API ainda estão sendo finalizados.

## 🐛 Troubleshooting

### Erro: "Erro ao obter URL para XML"
- Verifique se a API URL está correta
- Verifique se a API Key está válida
- Verifique se você tem permissão para criar processos

### Erro: "Erro ao fazer upload do XML"
- Verifique se o arquivo XML existe e é válido
- Verifique sua conexão com a internet
- Verifique se a presigned URL ainda é válida (expira em 1 hora)

### PDF não é criado corretamente
- Instale `reportlab` para melhor suporte: `pip install reportlab`
- O script funciona sem `reportlab`, mas o PDF será mínimo

## 📞 Suporte

Para obter sua API Key ou reportar problemas, entre em contato com o administrador do sistema.

