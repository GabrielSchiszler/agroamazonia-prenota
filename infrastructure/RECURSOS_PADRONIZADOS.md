# Recursos Padronizados com Ambiente

Este documento lista TODOS os recursos que foram padronizados para incluir o ambiente no nome.

## Padrão de Nomenclatura

Todos os recursos seguem: `{tipo}-{nome}-{env}`

Onde:
- `{tipo}`: tipo do recurso (lambda, tabela, bucket, etc.)
- `{nome}`: nome do recurso em lowercase com hífens
- `{env}`: ambiente (dev, stg, prd)

## Recursos Padronizados

### Backend Stack (agroamazonia-stack.ts)

#### Lambdas (12 recursos)
✅ `lambda-notify-receipt-{env}` - NotifyReceiptFunction
✅ `lambda-processor-{env}` - ProcessorFunction
✅ `lambda-validate-rules-{env}` - ValidateRulesFunction
✅ `lambda-report-ocr-failure-{env}` - ReportOcrFailureFunction
✅ `lambda-send-to-protheus-{env}` - SendToProtheusFunction
✅ `lambda-update-metrics-{env}` - UpdateMetricsFunction
✅ `lambda-check-textract-{env}` - CheckTextractFunction
✅ `lambda-get-textract-{env}` - GetTextractFunction
✅ `lambda-parse-xml-{env}` - ParseXmlFunction
✅ `lambda-update-process-status-{env}` - UpdateProcessStatusFunction
✅ `lambda-parse-ocr-{env}` - ParseOcrFunction
✅ `lambda-s3-upload-handler-{env}` - S3UploadHandler
✅ `lambda-api-{env}` - ApiFunction

#### DynamoDB (1 recurso)
✅ `tabela-document-processor-{env}` - DocumentProcessorTable

#### S3 Buckets (1 recurso)
✅ `bucket-agroamazonia-raw-documents-{env}-{account}` - RawDocumentsBucket
   (inclui account ID para garantir unicidade global)

#### SNS Topics (1 recurso)
✅ `topic-agroamazonia-lambda-errors-{env}` - LambdaErrorTopic

#### Secrets Manager (1 recurso)
✅ `secret-agroamazonia-api-keys-{env}` - ApiKeysSecret

#### Step Functions (1 recurso)
✅ `state-machine-document-processor-workflow-{env}` - DocumentProcessorStateMachine

#### API Gateway (1 recurso)
✅ `api-agroamazonia-document-api-{env}` - DocumentApi

### Frontend Stack (frontend-stack.ts)

#### S3 Buckets (1 recurso)
✅ `bucket-agroamazonia-frontend-{env}-{account}` - FrontendBucket
   (inclui account ID para garantir unicidade global)

#### CloudFront (1 recurso)
✅ `distribution-agroamazonia-frontend-{env}` - FrontendDistribution

#### CloudFront Cache Policies (2 recursos)
✅ `cache-policy-html-no-cache-{env}` - HtmlCachePolicy
✅ `cache-policy-js-short-cache-{env}` - JsCachePolicy

#### CloudFront Response Headers Policy (1 recurso)
✅ `response-headers-policy-agroamazonia-frontend-{env}` - FrontendHeadersPolicy

## Total de Recursos Padronizados

- **Backend**: 18 recursos
- **Frontend**: 5 recursos
- **Total**: 23 recursos padronizados

## Recursos que NÃO Precisam de Nome Explícito

Os seguintes recursos são gerenciados pelo CDK e não precisam de nomes explícitos (são únicos por stack):

- IAM Roles (gerados automaticamente com nomes únicos baseados no construct ID)
- Step Functions Tasks (são estados internos, não recursos AWS)
- S3 Event Notifications (configurações, não recursos nomeados)
- CloudFront Origin Access Identity (OAI) - tem apenas comment, não nome
- S3 Deployment (BucketDeployment) - recurso temporário para deploy

## Validação

Para validar que todos os recursos estão padronizados, execute:

```bash
cd infrastructure
npx ts-node scripts/validate-names.ts
```

## Garantias

✅ Todos os recursos que podem ter conflito de nome entre ambientes estão padronizados
✅ Cada ambiente (dev, stg, prd) terá recursos com nomes únicos
✅ Não haverá conflito ao fazer deploy de múltiplos ambientes na mesma conta AWS
✅ Buckets S3 incluem account ID para garantir unicidade global

