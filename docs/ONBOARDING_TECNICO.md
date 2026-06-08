# Onboarding Técnico — AgroAmazonia FAST OCR

Documento de referência para desenvolvedores que entram no projeto. Cobre arquitetura, tipos de processo, regras de negócio, pipeline, APIs, dados, ambientes e fluxo de trabalho.

> **Leitura complementar obrigatória:** [`CHANGELOG-multi-anexo.md`](./CHANGELOG-multi-anexo.md) — contrato atual da API v2 (multi-anexo, Textract, Bedrock).

---

## 1. O que é o projeto

O **FAST OCR** é uma solução serverless na AWS para:

1. **Receber documentos** de entrada fiscal/comercial (NF-e XML, PDFs, imagens, boletos).
2. **Extrair dados** via parse de XML, Amazon Textract e Amazon Bedrock (Nova Pro).
3. **Validar regras de negócio** comparando NF-e com o **Pedido de Compra** (JSON do ERP).
4. **Enviar ao Protheus** (ERP TOTVS) o documento de entrada quando tudo passa.
5. **Reportar falhas** ao ServiceNow e alimentar **métricas/dashboard** operacional.

O usuário final opera pelo **frontend SPA** (upload, acompanhamento, validações, dashboard). Integrações externas consomem a **API REST** (FastAPI atrás de API Gateway + Cognito OAuth2).

---

## 2. Arquitetura em alto nível

```
┌─────────────┐     OAuth2 Bearer      ┌──────────────────┐
│  Frontend   │ ─────────────────────► │  API Gateway     │
│  (CloudFront│                        │  + FastAPI λ     │
│   + S3)     │                        └────────┬─────────┘
└─────────────┘                                 │
                    presigned PUT               │ POST /process/start
                         ▼                      ▼
                  ┌─────────────┐        ┌──────────────────┐
                  │  S3 Bucket  │        │ Step Functions   │
                  │  processes/ │◄───────│  (workflow)      │
                  └─────────────┘        └────────┬─────────┘
                                                  │
         ┌────────────────────────────────────────┼────────────────────────┐
         ▼                    ▼                    ▼                        ▼
   parse-xml          extract-documents      merge-extractions      bedrock-extract-fields
         │                    │                    │                        │
         └────────────────────┴────────────────────┴────────────────────────┘
                                                  ▼
                                          validate-rules
                                    ┌─────────────┴─────────────┐
                                    ▼                           ▼
                            send-to-protheus            report-ocr-failure
                                    │                           │
                                    └───────────┬───────────────┘
                                                ▼
                                         update-metrics
                                                │
                                    notify-success / send-feedback
```

### Stack tecnológica

| Camada | Tecnologia | Pasta |
|--------|------------|-------|
| Infraestrutura | AWS CDK (TypeScript) | `infrastructure/` |
| API | FastAPI (Python), Clean Architecture | `backend/src/` |
| Pipeline | Lambdas Python 3.12 | `backend/lambdas/` |
| Frontend | JavaScript vanilla (SPA) | `frontend/` |
| Testes | pytest | `backend/tests/` |
| Scripts ops | Python / bash | `backend/scripts/` |

### Recursos AWS (padrão de nome)

| Recurso | Padrão | Exemplo (dev) |
|---------|--------|---------------|
| Lambda | `lambda-{nome}-{env}` | `lambda-validate-rules-dev` |
| DynamoDB | `tabela-document-processor-{env}` | `tabela-document-processor-dev` |
| S3 documentos | `bucket-agroamazonia-raw-documents-{env}-{account}` | — |
| Step Functions | `state-machine-document-processor-workflow-{env}` | — |
| Frontend | CloudFront + S3 | `fast-dash-dev.agroamazonia.com` |

**Região principal:** `sa-east-1`. Bedrock roda em `us-east-1` (modelo Nova Pro).

---

## 3. Estrutura do repositório

```
agroamazonia/
├── docs/
│   ├── ONBOARDING_TECNICO.md      ← este arquivo
│   └── CHANGELOG-multi-anexo.md   ← API v2 (referência de contrato)
├── infrastructure/
│   ├── bin/app.ts                 # Entry CDK, seleção de ENV
│   └── lib/
│       ├── agroamazonia-stack.ts  # Backend + Step Functions
│       └── frontend-stack.ts      # S3 + CloudFront
├── backend/
│   ├── src/
│   │   ├── main.py                # FastAPI + handler Lambda
│   │   ├── controllers/           # Rotas HTTP
│   │   ├── services/              # process_service, rules, dashboard, cfop
│   │   ├── repositories/          # DynamoDB (sem Scan!)
│   │   └── models/                # Pydantic (API)
│   ├── lambdas/                   # Uma pasta por etapa do pipeline
│   ├── tests/
│   └── scripts/                   # Testes E2E, sync regras, rebuild métricas
└── frontend/
    ├── index.html                 # Upload / processo individual
    ├── processes.html             # Lista de processos
    ├── dashboard.html             # Métricas
    ├── rules.html                 # CRUD de regras
    ├── app.js                     # Lógica principal
    ├── validation-renderer.js     # UI de validações
    └── server.py                  # Servidor local + gera config.js
```

> **Atenção:** o `README.md` na raiz descreve um modelo **legado** (`DOC_ID=`, rotas `/api/v1/document/*`). O sistema atual usa `PROCESS#{uuid}` e rotas `/process/*`. Use este onboarding e o CHANGELOG-multi-anexo como fonte da verdade.

---

## 4. Tipos de processo (`PROCESS_TYPE`)

O tipo é definido **no momento do `POST /process/start`**, lendo os metadados do pedido de compra (`PEDIDO_COMPRA_METADATA` no DynamoDB).

**Arquivo:** `backend/src/services/process_service.py` (método `start_process`).

### Tabela de tipos

| `PROCESS_TYPE` | Quando é definido | Prioridade |
|----------------|-------------------|------------|
| **USOCONSUMO** | `requestBody.usoEConsumo` (ou `header`) = `true` | 1ª (mais alta) |
| **BARTER** | `isCommodities: true` e **não** é uso e consumo | 2ª |
| **AGROQUIMICOS** | Existe pedido de compra, sem as flags acima | 3ª (padrão com pedido) |
| **DOCUMENTO_ENTRADA** | **Sem** `PEDIDO_COMPRA_METADATA` | — |

### Flags no JSON do pedido

```json
{
  "header": { "tenantId": "00,010101" },
  "requestBody": {
    "usoEConsumo": true,
    "isCommodities": false,
    "cnpjEmitente": "57600249000155",
    "cnpjDestinatario": "13563680002813",
    "itens": [
      {
        "codigoProduto": "AFU00001FR250M0",
        "codProdFornecedor": "10001",
        "produto": "NOME DO PRODUTO",
        "quantidade": 23.0,
        "valorUnitario": 2012.22,
        "unidadeMedida": "PC",
        "codigoOperacao": "1B",
        "pedidoDeCompra": {
          "pedidoErp": "1131195295",
          "itemPedidoErp": "0001"
        }
      }
    ]
  }
}
```

| Campo | Efeito |
|-------|--------|
| `usoEConsumo: true` | Tipo **USOCONSUMO**; merge especial no payload Protheus |
| `isCommodities: true` | Tipo **BARTER** (commodities, ex.: soja) |
| `codigoOperacao` no item | Se presente, **pula** `validar_cfop_chave` |
| `codProdFornecedor` | Comparação de produto por **código do fornecedor** (XML `cProd`) em vez de nome |
| `pedidoDeCompra.pedidoErp` | Usado em métricas (dedup) e `validar_numero_pedido` |

### Tipos legados na UI de regras

`SEMENTES` e `FERTILIZANTES` aparecem no CRUD de regras (`rules_controller.py`), mas **não** são derivados automaticamente no `/start`. Em produção os tipos ativos são **AGROQUIMICOS**, **BARTER**, **USOCONSUMO** e **DOCUMENTO_ENTRADA**.

---

## 5. Ciclo de vida e status do processo

### Status principais (`METADATA.STATUS`)

| Status | Significado |
|--------|-------------|
| `CREATED` | Processo criado, arquivos enviados (ou metadados vinculados), **ainda não iniciado** |
| `PROCESSING` | Step Functions em execução |
| `COMPLETED` / `VALIDATED` | Sucesso (validação OK + Protheus ou fluxo equivalente) |
| `FAILED` | Falha em lambda, validação ou Protheus |
| `VALIDATION_FAILED` | Falha específica de regras de negócio |

### Fluxo típico do desenvolvedor / QA

```
1. Gerar process_id (UUID)
2. Upload arquivo(s) via presigned URL (xml, docs ou batch)
3. (Opcional) POST /process/metadados/pedido
4. POST /process/start  →  PROCESSING
5. GET /process/{id}    →  acompanhar status e parsing_results
6. GET /process/{id}/validations  →  detalhe das regras
```

---

## 6. Pipeline Step Functions (detalhado)

**Definição:** `infrastructure/lib/agroamazonia-stack.ts`

### Fluxo feliz (v2 — multi-anexo)

```
NotificarRecebimento
  → ListarAnexos (Map, até 8 em paralelo)
       ├─ .xml        → parse-xml
       ├─ pdf/imagem  → extract-documents (Textract)
       └─ outros      → reject-attachment (sem OCR)
  → merge-extractions        → MERGED_EXTRACTION
  → bedrock-extract-fields   → BEDROCK_EXTRACTION
  → validate-rules           → VALIDATION#{timestamp}
  → HasValidationFailures?
       ├─ SIM → report-ocr-failure → update-metrics → send-feedback → ValidationFailed
       └─ NÃO → send-to-protheus → update-metrics → notify-success → ProcessSuccess
```

### Lambdas do pipeline

| Pasta | Função |
|-------|--------|
| `notify_receipt/` | Confirma recebimento, atualiza METADATA |
| `list_attachments/` | Lista anexos S3 e roteia xml/textract/skip |
| `parse_xml/` | NF-e XML → `PARSED_XML=*` |
| `extract_documents/` | PDF/imagem → Textract → `TEXTRACT#*` |
| `reject_attachment/` | Marca arquivo não suportado |
| `merge_extractions/` | Unifica XML + OCR → `MERGED_EXTRACTION` |
| `bedrock_extract_fields/` | IA extrai campos Protheus → `BEDROCK_EXTRACTION` |
| `validate_rules/` | Executa regras `validar_*` |
| `send_to_protheus/` | Monta payload e POST no ERP |
| `report_ocr_failure/` | Abre chamado ServiceNow em falha de validação |
| `update_metrics/` | Agrega contadores diários/mensais |
| `notify_success/` | Notificação de sucesso |
| `send_feedback/` | Feedback ServiceNow (falhas de lambda) |
| `update_process_status/` | Marca FAILED em erro de lambda |
| `s3_upload_handler/` | Hook de evento S3 (legado/auxiliar) |

### Entrada da Step Function

```json
{
  "process_id": "uuid",
  "process_type": "AGROQUIMICOS",
  "files": []
}
```

Os arquivos **não** vão no input — são descobertos via DynamoDB + S3.

---

## 7. API REST

**Entry:** `backend/src/main.py`  
**Prefixo em produção:** `/fast/v1` (API Gateway custom domain)

### Autenticação

- **OAuth2 client_credentials** (Cognito): header `Authorization: Bearer <token>`
- Variáveis: `OAUTH2_FRONTEND_TOKEN_URL`, `OAUTH2_FRONTEND_CLIENT_ID`, `OAUTH2_FRONTEND_CLIENT_SECRET`, `OAUTH2_FRONTEND_SCOPE`
- Alguns ambientes também exigem `x-api-key` (mesmo valor do `config.js` do frontend)
- Scripts de teste: use **`--dev`** ou `--env-file` — módulo `backend/scripts/api_auth.py` garante que o `.env` do ambiente vence variáveis do shell

### Rotas de processo (`/process/*`)

| Método | Rota | Descrição |
|--------|------|-----------|
| POST | `/process/presigned-url/xml` | URL para upload de NF-e (DANFE) |
| POST | `/process/presigned-url/docs` | URL para 1 documento adicional |
| POST | `/process/presigned-url/batch` | Até **10** arquivos de uma vez |
| POST | `/process/metadados/pedido` | Vincula JSON do pedido de compra |
| POST | `/process/start` | Inicia Step Functions |
| GET | `/process/{id}` | Detalhe do processo |
| GET | `/process/` | Lista processos |
| GET | `/process/{id}/validations` | Resultados das regras |
| POST | `/process/download` | URL de download |
| PUT | `/process/file/metadata` | Atualiza metadados de arquivo |

### Rotas administrativas

| Grupo | Rotas | Uso |
|-------|-------|-----|
| `/rules/*` | CRUD por `process_type` | Configurar quais `validar_*` rodam e em qual ordem |
| `/cfop-operation/*` | CRUD + lookup | Tabela Chave×CFOP → operação Protheus |
| `/dashboard/metrics` | GET por intervalo de datas | Cards do dashboard |
| `/auth/*` | Token Protheus, proxy | Integração ERP |
| `/health` | Health check | Monitoramento |

### Layout S3 dos uploads

```
processes/{process_id}/danfe/{upload_id}_{filename}   # XML / DANFE
processes/{process_id}/docs/{upload_id}_{filename}    # PDFs, imagens, boletos
```

---

## 8. Modelo de dados DynamoDB

**Tabela:** single-table design (`PK` + `SK`).  
**Repositório:** `backend/src/repositories/dynamodb_repository.py`  
**Política:** usar apenas `Query` / `GetItem` / `PutItem` / `UpdateItem` — **evitar Scan** em produção.

### Entidades por processo (`PK = PROCESS#{uuid}`)

| SK | Conteúdo |
|----|----------|
| `METADATA` | `STATUS`, `PROCESS_TYPE`, timestamps, resposta Protheus, `failure_summary`, chaves de métricas |
| `FILE#{upload_id}` | Arquivo enviado (`FILE_KEY`, `DOC_TYPE`, status upload) |
| `PEDIDO_COMPRA_METADATA` | JSON completo do pedido (sem arquivo físico) |
| `PARSED_XML={suffix}` | NF-e parseada |
| `TEXTRACT#{...}` | OCR por arquivo |
| `MERGED_EXTRACTION` | JSON canônico XML + Textract |
| `BEDROCK_EXTRACTION` | Campos estruturados para Protheus |
| `VALIDATION#{timestamp}` | `VALIDATION_RESULTS`, `VALIDATION_STATUS`, `CFOP_MAPPING` |

### Configuração global

| PK | SK | Conteúdo |
|----|-----|----------|
| `RULES#AGROQUIMICOS` | `RULE#validar_*` | Regra habilitada + `ORDER` |
| `RULES#BARTER` | `RULE#validar_*` | Idem para barter |
| `CFOP_OPERATION` | `CFOP#{code}` / `MAPPING#{chave}` | Mapeamento CFOP → operação |
| `METRICS#{YYYY-MM-DD}` | `SUMMARY` / `MONTHLY_SUMMARY` | Agregados do dashboard |
| `PROCESS` | `PROCESS#{uuid}` | Índice para listagem |

---

## 9. Sistema de regras de validação

### Onde vive

- **Implementação:** `backend/lambdas/validate_rules/rules/validar_*.py`
- **Orquestração:** `backend/lambdas/validate_rules/handler.py`
- **Configuração:** DynamoDB `RULES#{PROCESS_TYPE}`
- **UI:** `frontend/validation-renderer.js` + `regras_labels_catalog.json`

### Regras implementadas (Python)

| Regra | O que valida |
|-------|--------------|
| `validar_cnpj_fornecedor` | CNPJ emitente (raiz 8 dígitos) XML × pedido |
| `validar_cnpj_destinatario` | CNPJ destinatário XML × pedido |
| `validar_produtos` | Itens NF-e × `requestBody.itens` (código, qtd, preço, nome); suporta **multi-lote**; grava `matched_danfe_positions` para o Protheus |
| `validar_cfop_chave` | CFOP da NF × tabela `CFOP_OPERATION`; define `codigoOperacao` quando ausente no pedido |
| `validar_numero_pedido` | Número do pedido ERP no texto da NF |
| `validar_numero_nota` | Número da NF × pedido (com fallback Bedrock) |
| `validar_serie` | Série XML × pedido |
| `validar_data_emissao` | Data emissão XML × pedido |
| `validar_icms` | Total ICMS XML × pedido |

### Regras ativas em staging (exemplo de configuração)

| Process type | Regras (ordem) |
|--------------|----------------|
| **AGROQUIMICOS** | `validar_cnpj_fornecedor` → `validar_produtos` → `validar_cfop_chave` → `validar_cnpj_destinatario` |
| **BARTER** | `validar_produtos` → `validar_cfop_chave` → `validar_cnpj_destinatario` |

Outras regras (`validar_icms`, `validar_serie`, etc.) existem no código mas precisam ser **habilitadas no DynamoDB** para rodar.

### Fontes de dados na validação

- **DANFE (lado NF):** prioridade `PARSED_XML`; fallback sintético a partir de `MERGED_EXTRACTION`
- **Pedido de Compra (lado DOC):** sempre JSON de `PEDIDO_COMPRA_METADATA` — **não** usa OCR do PDF do pedido

### Comportamentos especiais

- `validar_cfop_chave` é **ignorada** se qualquer item do pedido já tiver `codigoOperacao`
- Com `codProdFornecedor` no pedido, `validar_produtos` compara **código XML** (`cProd`) vs fornecedor, não nome
- `validar_produtos` passa se **≥ 1** linha der match (multi-lote pode ter linhas extras)

---

## 10. Tabela CFOP (`CFOP_OPERATION`)

**Módulo:** `backend/lambdas/utils/cfop_table.py`  
**API CRUD:** `backend/src/controllers/cfop_operation_controller.py`

Dois tipos de registro:

```
PK: CFOP_OPERATION
SK: CFOP#5101          → índice CFOP → lista de MAPPING_IDs
SK: MAPPING#CHAVE_X    → OPERACAO, DESCRICAO, CFOP_LIST, PEDIDO_COMPRA, ATIVO, ...
```

A regra `validar_cfop_chave` usa `process_type`, `usoEConsumo`, `natureza` e presença de pedido para desambiguar quando um CFOP mapeia para várias chaves.

---

## 11. Integração Protheus

**Lambda:** `backend/lambdas/send_to_protheus/handler.py`

### Quando roda

Somente se `VALIDATION_STATUS != FAILED` (decisão na Step Function).

### O que monta o payload

- `BEDROCK_EXTRACTION` (campos principais)
- `MERGED_EXTRACTION` / `PARSED_XML` (fallback)
- Metadados do pedido (`itens`, quantidades, `pedidoDeCompra`)
- `CFOP_MAPPING` da validação → `codigoOperacao`
- `matched_danfe_positions` de `validar_produtos` → filtra linhas enviadas

### Credenciais

- URL: `PROTHEUS_API_URL` (por ambiente)
- Secret: `PROTHEUS_SECRET_ID` (AWS Secrets Manager)

### Erros Protheus

Códigos oficiais (17 regras) em `backend/lambdas/utils/protheus_regras_catalog.json`. Parser em `protheus_regras.py` classifica erro como **OCR** vs **Operacional** para métricas.

---

## 12. OCR, Textract e Bedrock

| Etapa | Tecnologia | Saída DynamoDB |
|-------|------------|----------------|
| XML | Parser próprio | `PARSED_XML=*` |
| PDF/imagem | Amazon Textract | `TEXTRACT#*` |
| Merge | Lambda | `MERGED_EXTRACTION` |
| Estruturação | Bedrock Nova Pro (`us.amazon.nova-pro-v1:0`) | `BEDROCK_EXTRACTION` |

**Tipos suportados no upload:** XML, PDF, PNG, JPEG, TIFF, TXT. DOCX é **rejeitado** (Textract não suporta nativamente).

Bedrock também é usado **dentro** de algumas regras (`compare_with_bedrock`) e no resumo de erro (`bedrock_error_summary.py`).

---

## 13. Métricas e deduplicação de falhas

**Lambda:** `backend/lambdas/update_metrics/handler.py`  
**Dedup:** `backend/lambdas/utils/failure_dedup.py`

### Chave de identidade de falha

```
NF | CNPJ | tipo_erro | pedido
```

Evita contar duas vezes a mesma NF reprocessada. Papéis: `primary` (conta) vs `duplicate` (vinculado).

### Contadores no dashboard

| Métrica | Significado |
|---------|-------------|
| `success_count` | Processos concluídos com sucesso |
| `failed_count` | Falhas que entram na taxa (exclui só-operacional Protheus) |
| `failed_rules` | Breakdown por código de regra |
| `success_prenota_count` | Sucesso com mensagem "pré-nota" no Protheus |
| Taxa de acerto | `sucessos / (sucessos + falhas)` — `metrics_rates.py` |

### Catálogos de labels

| Arquivo | Uso |
|---------|-----|
| `protheus_regras_catalog.json` | 17 regras Protheus |
| `api_regras_catalog.json` | Erros de schema/API |
| `regras_labels_catalog.json` | Labels PT para UI (gerado por `build_regras_labels_catalog.py`) |

### Rebuild de métricas

```bash
cd backend/scripts
python3 rebuild_all_metrics.py --output-dir ./out_regras        # preview
python3 rebuild_all_metrics.py --apply                          # grava METRICS#
```

---

## 14. Frontend

| Página | Arquivo | Função |
|--------|---------|--------|
| Upload / detalhe | `index.html` + `app.js` | Criar processo, upload batch, ver validações, iniciar |
| Lista | `processes.html` | Histórico de processos |
| Dashboard | `dashboard.html` | Métricas por período |
| Regras | `rules.html` | Admin de `RULES#*` |
| Config | `settings.html` | API URL, API key local |

**Renderização de validações:** `validation-renderer.js` — comparações campo a campo, multi-lote, labels "Pedido de Compra" (não "DOC").

### Rodar localmente

```bash
cd frontend
cp .env.example .env   # API_URL, OAUTH2_*, API_KEY
python3 server.py      # http://localhost:8080
```

O `server.py` gera `config.js` com variáveis de ambiente.

---

## 15. Ambientes

| CDK `ENV` | Uso comum | API (exemplo) | Dashboard |
|-----------|-----------|---------------|-----------|
| `dev` | Desenvolvimento | `https://api-dev.agroamazonia.com/fast/v1` | `fast-dash-dev.agroamazonia.com` |
| `stg` | Homologação ("hml") | `https://api-hml.agroamazonia.com/fast/v1` | `fast-dash-hml.agroamazonia.com` |
| `prd` | Produção | `https://api-prd.agroamazonia.com/fast/v1` | `fast-dash-prd.agroamazonia.com` |

> CDK usa `stg`; documentação e `.env.homolog` usam **hml** — é o mesmo ambiente de homologação.

### Arquivos de ambiente (não commitar segredos)

| Arquivo | Ambiente |
|---------|----------|
| `backend/.env.development` | dev |
| `backend/.env.homolog` | stg/hml |
| `backend/.env.prod` | prd |
| `backend/.env.example` | Template |

Variáveis essenciais: `TABLE_NAME`, `STATE_MACHINE_ARN`, `BUCKET_NAME`, `AWS_REGION`, `PROTHEUS_*`, `OAUTH2_FRONTEND_*`, URLs ServiceNow.

**Não use `source .env.development` diretamente** — o arquivo pode conter comandos `cdk deploy` no final. Use os scripts com `--dev` ou carregamento seguro via `api_auth.py`.

---

## 16. Deploy

```bash
export ENV=dev   # ou stg, prd
cd infrastructure && npm install && npm run build
cdk deploy AgroAmazoniaStack-${ENV}
cdk deploy FrontendStack-${ENV} --context apiUrl=https://api-dev.agroamazonia.com/fast
```

Deploy completo alternativo: `./deploy.sh` na raiz.

Após alterar lambdas Python, é necessário redeploy do stack backend. Frontend tem invalidação automática de CloudFront.

---

## 17. Desenvolvimento local (backend)

```bash
cd backend
pip install -r requirements.txt
export TABLE_NAME=... STATE_MACHINE_ARN=... BUCKET_NAME=...  # do .env.development
uvicorn src.main:app --reload --port 8000
```

Rotas locais: `http://localhost:8000/process/...` (sem prefixo `/fast/v1`).

---

## 18. Testes e scripts úteis

### Suíte E2E dev

```bash
cd backend/scripts
./run_dev_test_suite.sh              # todos os cenários
./run_dev_test_suite.sh --no-start   # só upload, sem Step Functions
./run_dev_test_suite.sh --quick      # unitários + 1 XML
```

Cenários cobertos: Bizify PDF (53, 61, 71, 78), uso e consumo, XML agro, barter, multilot OPTERADUO.

### Autenticação em scripts

```bash
python3 get_oauth2_bearer_token.py              # token dev
python3 upload_arquivos_cenarios.py --dev --scenario 53 --start
python3 check_process_dev.py --dev <process_id> --wait 300
```

### Sync de regras (stg → dev)

```bash
cd backend/scripts
./sync_rules_from_stg.sh    # RULES# + CFOP_OPERATION
```

### Testes unitários

```bash
cd backend
python3 -m pytest tests/ -q
python3 lambdas/validate_rules/test_validar_produtos_multilot.py
```

### Documentação de scripts

- `backend/scripts/README_TEST_CREATE_PROCESS.md`
- `backend/scripts/README_TEST_OAUTH2.md`
- `backend/scripts/REGRAS_VALIDACAO_PROTHEUS.md`

---

## 19. ServiceNow e notificações

| Integração | Lambda | Quando |
|------------|--------|--------|
| Reportar falha OCR | `report_ocr_failure` | Validação falhou |
| Feedback processo | `send_feedback` | Erro de lambda / falha geral |
| Sucesso | `notify_success` | Fluxo OK |

Credenciais OAuth2 ServiceNow em variáveis `OCR_FAILURE_*` e `SERVICENOW_FEEDBACK_API_URL`.

---

## 20. Armadilhas comuns (leia antes de debugar)

1. **OAuth do shell vs `.env`:** variáveis `OAUTH2_*` exportadas no terminal (ex.: prd) sobrescreviam o dev — use `--dev` nos scripts.
2. **README raiz desatualizado:** modelo `DOC_ID` é legado; use `PROCESS#{uuid}`.
3. **Dev sem regras no DynamoDB:** validação vazia → Protheus pode falhar; rode `sync_rules_from_stg.sh`.
4. **`/start` sem arquivos:** exige pelo menos 1 anexo; pedido é opcional (`DOCUMENTO_ENTRADA`).
5. **URL da API:** dev/hml usam `.../fast/v1/process`; alguns scripts antigos usam `.../fast/v1/api/process` — ver `--api-path-prefix`.
6. **WAF:** requisições sem User-Agent de navegador podem retornar 403; scripts usam UA padrão Chrome.
7. **`.env.development`:** não dar `source` direto (contém comandos `cdk deploy`).

---

## 21. Roteiro do primeiro dia (novo dev)

### Dia 1 — Entender o fluxo

1. Ler este documento e [`CHANGELOG-multi-anexo.md`](./CHANGELOG-multi-anexo.md).
2. Abrir `fast-dash-dev.agroamazonia.com` e criar um processo de teste (XML + pedido exemplo).
3. No AWS Console: Step Functions `state-machine-document-processor-workflow-dev` → ver execução.
4. No DynamoDB `tabela-document-processor-dev`: inspecionar `PROCESS#{id}` (METADATA, VALIDATION#, BEDROCK_EXTRACTION).

### Dia 2 — Código

1. Traçar `POST /process/start` em `process_service.py`.
2. Ler `validate_rules/handler.py` + uma regra (`validar_produtos.py`).
3. Ler `send_to_protheus/handler.py` — como o payload é montado.

### Dia 3 — Operar

1. Rodar `./run_dev_test_suite.sh --quick --no-start`.
2. Sincronizar regras: `./sync_rules_from_stg.sh`.
3. Rodar pytest das métricas: `tests/test_metrics_rates_and_process.py`.

### Trace end-to-end recomendado

```
process_controller.py
  → process_service.start_process()
  → agroamazonia-stack.ts (Step Functions)
  → validate_rules/handler.py
  → send_to_protheus/handler.py
  → update_metrics/handler.py
  → GET /process/{id} + validation-renderer.js
```

---

## 22. Glossário

| Termo | Significado |
|-------|-------------|
| **DANFE** | Documento auxiliar da NF-e; no sistema = arquivo XML da nota |
| **Pedido de Compra** | JSON do ERP (`header` + `requestBody`), não um PDF |
| **Pré-nota** | Documento criado no Protheus em estado intermediário |
| **Multi-lote** | Várias linhas `<det>` na NF com mesmo produto e lotes distintos (`rastro`) |
| **Chave CFOP** | Identificador na tabela `CFOP_OPERATION` para tipo de operação |
| **OCR** | Extração de texto de PDF/imagem (Textract) |
| **FAST** | Nome do produto/interface AgroAmazonia para este fluxo |

---

## 23. Referências rápidas de arquivos

| Tópico | Arquivo |
|--------|---------|
| Tipo de processo no start | `backend/src/services/process_service.py` |
| Step Functions | `infrastructure/lib/agroamazonia-stack.ts` |
| Regras Python | `backend/lambdas/validate_rules/rules/` |
| Config regras DynamoDB | `backend/scripts/copy_rules.py`, `sync_rules_from_stg.sh` |
| Protheus payload | `backend/lambdas/send_to_protheus/handler.py` |
| Métricas | `backend/lambdas/update_metrics/handler.py` |
| API models | `backend/src/models/api.py` |
| UI validação | `frontend/validation-renderer.js` |
| Auth scripts | `backend/scripts/api_auth.py` |
| Deploy infra | `infrastructure/README.md` |
| CORS API Gateway | `API_GATEWAY_CORS_SETUP.md` |

---

*Última atualização: junho/2026 — alinhado à API v2 (multi-anexo) e tipos USOCONSUMO, BARTER, AGROQUIMICOS, DOCUMENTO_ENTRADA.*
