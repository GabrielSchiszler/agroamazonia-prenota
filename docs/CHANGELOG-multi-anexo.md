# Changelog: Multi-Anexo (v2)

> Migração da versão atual (fluxo único XML + pedido de compra) para a nova versão
> com suporte a múltiplos documentos, OCR via Textract e extração por IA (Bedrock).

---

## TL;DR — O que muda para quem consome a API?

| Aspecto | Antes (v1) | Agora (v2) | Breaking? |
|---------|-----------|------------|-----------|
| Rotas existentes | Todas permanecem iguais | Nenhuma rota removida ou alterada | **Não** |
| `POST /process/presigned-url/xml` | Obrigatório | Continua funcionando igual | **Não** |
| `POST /process/presigned-url/docs` | Upload 1 arquivo por vez | Continua funcionando igual | **Não** |
| `POST /process/presigned-url/batch` | Não existia | **Nova rota** — upload N arquivos de uma vez | **Não** (aditiva) |
| `POST /process/metadados/pedido` | Obrigatório antes do start | Opcional (se ausente, `process_type = DOCUMENTO_ENTRADA`) | **Sim** (relaxou) |
| `POST /process/start` | Exigia DANFE + pedido compra | Exige **pelo menos 1 arquivo** (qualquer tipo) | **Sim** (relaxou) |
| `GET /process/{id}` | `parsing_results` = XML + OCR | Agora inclui TEXTRACT, MERGED, BEDROCK_AI | **Não** (aditivo) |
| Step Functions | 4 etapas de processamento | 7 etapas (3 novas no meio) | Não (transparente) |

> **Nenhuma rota existente teve seu contrato de entrada alterado.**
> Tudo que funcionava antes continua funcionando. As mudanças são **aditivas**.

---

## 1. Rotas da API — Detalhamento

### 1.1 Rotas existentes (sem mudança de contrato)

#### `POST /process/presigned-url/xml`
Gera URL pré-assinada para upload de XML (DANFE).

```json
// Request — IGUAL
{
  "process_id": "uuid",
  "file_name": "nota.xml",
  "file_type": "application/xml",
  "metadados": { ... }          // opcional
}

// Response — IGUAL
{
  "upload_url": "https://...",
  "file_key": "processes/uuid/danfe/nota.xml",
  "file_name": "nota.xml",
  "content_type": "application/xml",
  "doc_type": "DANFE"
}
```

#### `POST /process/presigned-url/docs`
Gera URL pré-assinada para upload de 1 documento adicional.

```json
// Request — IGUAL
{
  "process_id": "uuid",
  "file_name": "boleto.pdf",
  "file_type": "application/pdf",
  "metadados": { ... }          // opcional
}

// Response — IGUAL
{
  "upload_url": "https://...",
  "file_key": "processes/uuid/docs/boleto.pdf",
  "file_name": "boleto.pdf",
  "content_type": "application/pdf",
  "doc_type": "ADDITIONAL"
}
```

#### `POST /process/metadados/pedido`
Vincula metadados do pedido de compra.

```json
// Request — IGUAL
{
  "process_id": "uuid",
  "metadados": { "header": { ... }, "requestBody": { ... } }
}
```

**Mudança de comportamento:** Antes era obrigatório para poder chamar `/start`. Agora é **opcional** — se não for enviado, o processo é classificado como `DOCUMENTO_ENTRADA` em vez de `AGROQUIMICOS`.

#### `POST /process/start`
Inicia o processamento via Step Functions.

```json
// Request — IGUAL
{ "process_id": "uuid" }

// Response — IGUAL (campo process_type pode ter novo valor)
{
  "execution_arn": "arn:aws:states:...",
  "process_id": "uuid",
  "process_type": "AGROQUIMICOS",   // ou "BARTER" ou "DOCUMENTO_ENTRADA" (novo)
  "status": "PROCESSING"
}
```

**Mudança de comportamento:**
- Antes: exigia DANFE **e** PEDIDO_COMPRA_METADATA.
- Agora: exige **pelo menos 1 arquivo** (qualquer tipo). Pedido compra é opcional.

---

### 1.2 Nova rota: `POST /process/presigned-url/batch`

Gera URLs pré-assinadas para **múltiplos arquivos** de uma vez (até 10).

```json
// Request
{
  "process_id": "uuid",
  "files": [
    {
      "file_name": "nota.xml",
      "file_type": "application/xml",
      "doc_type": "DANFE"           // ou "ADDITIONAL" (default)
    },
    {
      "file_name": "comprovante.pdf",
      "file_type": "application/pdf",
      "doc_type": "ADDITIONAL"
    },
    {
      "file_name": "foto_fatura.jpg",
      "file_type": "image/jpeg"
    }
  ]
}

// Response
{
  "process_id": "uuid",
  "files": [
    {
      "file_name": "nota.xml",
      "upload_url": "https://...",
      "file_key": "processes/uuid/danfe/nota.xml",
      "content_type": "application/xml",
      "doc_type": "DANFE"
    },
    {
      "file_name": "comprovante.pdf",
      "upload_url": "https://...",
      "file_key": "processes/uuid/docs/comprovante.pdf",
      "content_type": "application/pdf",
      "doc_type": "ADDITIONAL"
    },
    {
      "file_name": "foto_fatura.jpg",
      "upload_url": "https://...",
      "file_key": "processes/uuid/docs/foto_fatura.jpg",
      "content_type": "image/jpeg",
      "doc_type": "ADDITIONAL"
    }
  ]
}
```

**Limites:**
- Mínimo: 1 arquivo
- Máximo: 10 arquivos por processo
- Content-Types aceitos: `application/xml`, `text/xml`, `application/pdf`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`, `image/png`, `image/jpeg`, `image/tiff`

**Não é obrigatório usar esta rota.** As rotas individuais (`/presigned-url/xml` e `/presigned-url/docs`) continuam funcionando. A rota batch é uma **conveniência** para quem precisa enviar vários arquivos — o resultado final é o mesmo.

---

### 1.3 Mudança na resposta: `GET /process/{id}`

O campo `parsing_results` agora pode conter novos tipos de `source`:

| `source` | Quando aparece | Descrição |
|----------|----------------|-----------|
| `XML` | Sempre que houver NF-e XML | Parse estruturado do XML (igual ao v1) |
| `OCR` | Legado — mantido para compatibilidade | Resultado OCR genérico |
| `TEXTRACT` | **Novo** — para cada PDF/imagem processado | OCR por arquivo via Amazon Textract (`raw_text`, `tables`, `job_id`) |
| `MERGED` | **Novo** — após merge das extrações | JSON canônico unificando XML + Textract |
| `BEDROCK_AI` | **Novo** — após extração via IA | Campos estruturados extraídos por Bedrock para o Protheus |

Exemplo de `parsing_results` na v2:

```json
{
  "parsing_results": [
    {
      "source": "XML",
      "file_name": "nota.xml",
      "parsed_data": { "chave_acesso": "44...", "numero_nota": "123", ... }
    },
    {
      "source": "TEXTRACT",
      "file_name": "comprovante.pdf",
      "parsed_data": {
        "raw_text": "texto extraído do PDF...",
        "tables": [{ "rows": [["col1", "col2"], ["val1", "val2"]] }],
        "job_id": "sync"
      }
    },
    {
      "source": "MERGED",
      "file_name": "merged_extraction",
      "parsed_data": {
        "schema_version": 1,
        "nfe_xml": { ... },
        "nfe_file": "nota.xml",
        "textract_documents": [{ "file_name": "comprovante.pdf", "raw_text": "...", "tables": [...] }]
      }
    },
    {
      "source": "BEDROCK_AI",
      "file_name": "bedrock_extraction",
      "parsed_data": {
        "tipoDeDocumento": "NF",
        "documento": "000123",
        "serie": "001",
        "dataEmissao": "20260415",
        "chaveAcesso": "4425...",
        "cnpjEmitente": "12345678000199",
        "itens": [{ "codigoProduto": "ABC", "quantidade": 10, ... }]
      }
    }
  ]
}
```

**Para consumidores que só leem `source: "XML"`** — nada muda; os items XML continuam vindo como antes.

---

## 2. Step Functions — Novo fluxo de processamento

### Antes (v1)
```
NotifyReceipt → ParseXml → ValidateRules → [Protheus ou Falha]
```

### Agora (v2)
```
NotifyReceipt → ParseXml → ExtractDocuments → MergeExtractions → BedrockExtractFields → ValidateRules → [Protheus ou Falha]
```

| Etapa | Lambda | O que faz |
|-------|--------|-----------|
| 1. NotifyReceipt | `notify-receipt` | Inalterado |
| 2. ParseXml | `parse-xml` | Busca NF-e XML entre todos os anexos. Se não houver XML, **pula graciosamente** (não falha mais) |
| 3. **ExtractDocuments** | `extract-documents` | Roda Amazon Textract em PDFs e imagens. XMLs são ignorados (já tratados na etapa 2). DOCX é rejeitado (Textract não suporta nativamente) |
| 4. **MergeExtractions** | `merge-extractions` | Unifica PARSED_XML + resultados Textract em um JSON canônico (`MERGED_EXTRACTION`) |
| 5. **BedrockExtractFields** | `bedrock-extract-fields` | Usa IA (Amazon Bedrock Nova Pro) para extrair campos estruturados do Protheus a partir do merge |
| 6. ValidateRules | `validate-rules` | Inalterado |
| 7+ | Protheus/Feedback/Métricas | Inalterados (send_to_protheus consome BEDROCK_EXTRACTION como fallback) |

**Para o fluxo antigo** (1 XML + pedido compra):
- Etapa 3 processa 0 arquivos (não há PDFs) → retorna imediatamente
- Etapa 4 cria MERGED com apenas nfe_xml (sem textract_documents)
- Etapa 5 chama Bedrock só com o XML estruturado → extrai campos
- Resultado final: idêntico ao v1, mas com dados de enriquecimento adicionais

---

## 3. DynamoDB — Novos registros por processo

| SK | Criado por | Conteúdo |
|----|-----------|----------|
| `FILE#{nome}` | API (presigned-url) | **Inalterado** — agora pode haver mais FILE# por processo |
| `PARSED_XML={nome}` | parse_xml | **Inalterado** |
| `TEXTRACT#{nome}` | **extract_documents** | RAW_TEXT, TABLES_DATA, JOB_ID por arquivo |
| `MERGED_EXTRACTION` | **merge_extractions** | JSON canônico: `{ schema_version, nfe_xml, textract_documents }` |
| `PARSED_OCR=textract_merged` | **merge_extractions** | Backfill de compatibilidade para send_to_protheus |
| `BEDROCK_EXTRACTION` | **bedrock_extract_fields** | EXTRACTED_FIELDS: JSON com campos do Protheus extraídos por IA |

---

## 4. Infraestrutura (CDK) — O que deploiar

3 novas Lambdas adicionadas no `agroamazonia-stack.ts`:

| Lambda | Runtime | Timeout | Memória | Permissões extras |
|--------|---------|---------|---------|-------------------|
| `extract-documents` | Python 3.12 | 5 min | 512 MB | Textract (AnalyzeDocument, StartDocumentAnalysis, GetDocumentAnalysis), S3 read |
| `merge-extractions` | Python 3.12 | 2 min | 256 MB | DynamoDB read/write |
| `bedrock-extract-fields` | Python 3.12 | 3 min | 512 MB | Bedrock InvokeModel |

**Nenhuma Lambda existente foi removida ou teve sua configuração alterada.**

---

## 5. Frontend — Mudanças visuais

| Componente | Mudança |
|-----------|---------|
| Upload DANFE | **Inalterado** |
| Metadados pedido compra | **Inalterado** |
| Botão "Iniciar Processamento" | Agora aceita processos sem pedido compra (basta ter pelo menos 1 arquivo) |
| **Documentos Adicionais** | **Nova seção** — upload múltiplo de PDFs, imagens, XMLs complementares |
| **Dados Extraídos** | Agora renderiza 5 tipos de fonte: XML (azul), OCR (verde), Textract (roxo), Extração Unificada (laranja), Bedrock IA (vermelho) |

---

## 6. Cenários de uso — Ambos os fluxos coexistem

### Cenário A: Fluxo original (1 XML + pedido compra)

```
1. POST /process/presigned-url/xml       → upload nota.xml
2. PUT  nota.xml para S3
3. POST /process/metadados/pedido         → vincular metadados
4. POST /process/start                    → inicia Step Functions
   → ParseXml (extrai XML) → ExtractDocuments (0 PDFs) → Merge (só XML)
   → Bedrock (enriquece) → ValidateRules → SendToProtheus
```

**Resultado:** Idêntico ao v1. Bedrock é um bônus de enriquecimento.

### Cenário B: Multi-anexo (NF-e XML + PDFs complementares)

```
1. POST /process/presigned-url/xml        → upload nota.xml
2. POST /process/presigned-url/docs       → upload comprovante.pdf
3. POST /process/presigned-url/docs       → upload boleto.pdf
   (ou usar /presigned-url/batch para tudo de uma vez)
4. POST /process/metadados/pedido         → vincular metadados (opcional)
5. POST /process/start                    → inicia Step Functions
   → ParseXml (extrai NF-e) → ExtractDocuments (Textract nos PDFs)
   → Merge (XML + Textract) → Bedrock (extrai campos) → ValidateRules → SendToProtheus
```

### Cenário C: Somente documentos sem XML

```
1. POST /process/presigned-url/docs       → upload fatura.pdf
2. POST /process/presigned-url/docs       → upload contrato.pdf
3. POST /process/start                    → process_type = DOCUMENTO_ENTRADA
   → ParseXml (nenhum XML, pula) → ExtractDocuments (Textract nos PDFs)
   → Merge (só Textract) → Bedrock (extrai campos) → ValidateRules → SendToProtheus
```

---

## 7. Riscos e limitações conhecidas

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| DOCX não suportado pelo Textract | Arquivo marcado como REJECTED | Futuro: converter DOCX → PDF antes do Textract |
| Textract sync limitado a 10 MB | Arquivos maiores usam modo async (polling) | Implementado — async com polling a cada 3s |
| Bedrock pode retornar JSON inválido | BEDROCK_EXTRACTION salva com PARSE_ERROR | send_to_protheus usa dados anteriores como fallback |
| Processo sem XML nem PDF | Textract não processa, Bedrock recebe dados vazios | Bedrock retorna `fields_extracted: false`, fluxo continua |

---

## 8. Checklist de deploy

- [ ] Deploy CDK stack (3 novas Lambdas + Step Functions atualizado)
- [ ] Verificar que variável `BEDROCK_MODEL_ID` está configurada (default: `amazon.nova-pro-v1:0`)
- [ ] Verificar que a role da Lambda `extract-documents` tem permissão Textract
- [ ] Deploy do backend (FastAPI) com as alterações em `process_service.py`, `api.py`, `process_controller.py`
- [ ] Deploy das Lambdas: `parse_xml`, `extract_documents`, `merge_extractions`, `bedrock_extract_fields`, `send_to_protheus`
- [ ] Deploy do frontend (`app.js`, `processes.html`)
- [ ] Testar fluxo A (XML + pedido — regressão)
- [ ] Testar fluxo B (XML + PDFs — novo)
- [ ] Testar fluxo C (somente PDFs — novo)
