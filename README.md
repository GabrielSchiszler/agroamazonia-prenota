# AgroAmazonia - Solução Serverless de Processamento de Documentos

## Arquitetura

Solução serverless na AWS para ingestão, processamento e extração de dados tabulares de documentos financeiros usando:

- **AWS CDK (TypeScript)**: Infraestrutura como código
- **FastAPI (Python)**: Backend API com Clean Architecture
- **AWS Textract**: Extração de tabelas
- **Step Functions**: Orquestração de workflows
- **DynamoDB**: Persistência com modelo otimizado
- **S3**: Armazenamento de documentos

## Estrutura do Projeto

```
agroamazonia/
├── infrastructure/          # CDK TypeScript
│   ├── bin/
│   │   └── app.ts          # Entry point CDK
│   └── lib/
│       └── agroamazonia-stack.ts  # Stack principal
├── backend/
│   ├── src/                # FastAPI Application
│   │   ├── controllers/    # Rotas HTTP
│   │   ├── services/       # Lógica de negócio
│   │   ├── repositories/   # Acesso DynamoDB
│   │   ├── models/         # Modelos Pydantic
│   │   └── main.py         # Entry point FastAPI
│   └── lambdas/            # Step Functions Lambdas
│       ├── notify_receipt/
│       ├── textract_processor/
│       └── processor/
└── scripts/                # Scripts auxiliares
```

## Modelo de Dados DynamoDB

### Estrutura da Tabela

- **Partition Key (PK)**: `DOC_ID=<ID_UNICO>`
- **Sort Key (SK)**: `METADATA=<timestamp>,TYPE=<tipo>`
- **DataPayload**: `CHAVE=VALOR,CHAVE=VALOR` (formato string)

### Exemplo de Registro

```json
{
  "PK": "DOC_ID=0101600000051601",
  "SK": "METADATA=1678886400,TYPE=PRE_NOTE",
  "DataPayload": "STATUS=PROCESSED,TIMESTAMP=1678886400,PROCESS_TYPE=SEMENTES,S3_PATH=s3://bucket/pre-notas/0101600000051601/doc.pdf"
}
```

### Restrições

- ✅ Permitido: `Query`, `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`
- ❌ Proibido: `Scan`

## Estrutura S3

```
s3://raw-documents/
├── pre-notas/
│   └── <ID_UNICO>/
│       └── documento.pdf
└── docs-xml/
    └── <ID_UNICO>/
        └── documento.xml
```

## Sistema de Regras (Chain of Responsibility)

Regras organizadas por **Tipo de Processo** (não por tipo de documento):

### SEMENTES
1. Validação de Imposto
2. Verificação de Documentação (Certificado Fitossanitário)

### AGROQUIMICOS
1. Validação de Licença IBAMA
2. Verificação de Valor

### FERTILIZANTES
1. Validação de Laudo de Composição

### Extensibilidade

Para adicionar novo tipo de processo:

1. Criar classes de regras em `services/rules_service.py`
2. Implementar métodos `check()` e `execute_action()`
3. Registrar workflow em `RulesService.get_workflow()`

## API Endpoints

### POST /api/v1/document/submit
Inicia processamento de documento

```json
{
  "document_id": "0101600000051601",
  "document_type": "PRE_NOTE",
  "process_type": "SEMENTES",
  "s3_path": "s3://bucket/pre-notas/0101600000051601/doc.pdf"
}
```

### GET /api/v1/document/{id}
Consulta todos os dados de um documento

### GET /api/v1/document/{id}/pre-note
Consulta apenas dados da pré-nota

## Deploy

### 1. Instalar Dependências

```bash
# Infraestrutura
cd infrastructure
npm install

# Backend
cd ../backend
pip install -r requirements.txt
```

### 2. Deploy CDK

```bash
cd infrastructure
npm run build
cdk bootstrap
cdk deploy
```

### 3. Configurar Variáveis de Ambiente

O CDK pode receber variáveis de ambiente para configuração do Lambda de reporte de falhas OCR. Configure as seguintes variáveis antes do deploy:

```bash
# OCR Failure API - ServiceNow
export OCR_FAILURE_API_URL="https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha"
export OCR_FAILURE_AUTH_URL="https://agroamazoniad.service-now.com/oauth_token.do"
export OCR_FAILURE_CLIENT_ID="seu_client_id"
export OCR_FAILURE_CLIENT_SECRET="seu_client_secret"
export OCR_FAILURE_USERNAME="seu_username"
export OCR_FAILURE_PASSWORD="sua_password"
```

**Nota**: As variáveis podem ser passadas via:
- Variáveis de ambiente do sistema (`export`)
- CDK Context (`cdk deploy -c ocrFailureApiUrl=...`)
- Se não fornecidas, valores padrão serão usados (apenas para desenvolvimento)

Após deploy, o CDK exibirá:
- API URL
- Bucket Name
- Table Name
- State Machine ARN

## Workflow Step Functions

1. **NotifyReceipt**: Registra recebimento no DynamoDB
2. **ExtractTables**: Chama Textract para extração de tabelas
3. **ProcessResults**: Persiste resultados e atualiza status

## Segurança (Least Privilege)

Todas as funções Lambda possuem permissões IAM restritas:

- **NotifyReceiptFunction**: `s3:GetObject` (bucket específico), `dynamodb:PutItem`
- **TextractFunction**: `textract:StartDocumentAnalysis`, `textract:GetDocumentAnalysis`, `s3:GetObject`
- **ProcessorFunction**: `dynamodb:Query`, `dynamodb:PutItem`, `dynamodb:UpdateItem`
- **ApiFunction**: `dynamodb:Query`, `dynamodb:GetItem`, `states:StartExecution`

## Desenvolvimento Local

```bash
# Instalar dependências
cd backend
pip install -r requirements.txt

# Executar FastAPI localmente
uvicorn src.main:app --reload
```

## Testes

```bash
cd backend
pytest tests/
```

## Monitoramento

- CloudWatch Logs para todas as Lambdas
- X-Ray tracing habilitado
- Métricas de Step Functions
- API Gateway logs e métricas
