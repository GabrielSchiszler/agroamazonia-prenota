# Infrastructure - AgroAmazonia

Infraestrutura AWS usando CDK para o projeto AgroAmazonia.

## Padrão de Nomenclatura

Todos os recursos seguem o padrão:
- **Lambdas**: `lambda-{nome}-{env}`
- **Tabelas DynamoDB**: `tabela-{nome}-{env}`
- **Buckets S3**: `bucket-{nome}-{env}-{account}`
- **API Gateway**: `api-{nome}-{env}`
- **State Machines**: `state-machine-{nome}-{env}`
- **Secrets**: `secret-{nome}-{env}`
- **Topics SNS**: `topic-{nome}-{env}`
- **CloudFront Distributions**: `distribution-{nome}-{env}`

Onde `{env}` pode ser: `dev`, `stg`, ou `prd`

## Ambientes

O projeto suporta múltiplos ambientes:
- **dev**: Desenvolvimento
- **stg**: Staging
- **prd**: Produção

## Configuração

### 1. Definir Ambiente

```bash
export ENV=dev  # ou stg, prd
```

### 2. Instalar Dependências

```bash
cd infrastructure
npm install
```

### 3. Compilar TypeScript

```bash
npm run build
```

### 4. Deploy

#### Deploy completo (backend + frontend):
```bash
cdk deploy --all
```

#### Deploy apenas backend:
```bash
cdk deploy AgroAmazoniaStack-${ENV}
```

#### Deploy apenas frontend:
```bash
cdk deploy FrontendStack-${ENV}
```

### 5. Visualizar Mudanças Antes do Deploy

```bash
cdk diff
```

## Stacks

### AgroAmazoniaStack (Backend)

Contém:
- Lambdas (notify-receipt, processor, validate-rules, etc.)
- DynamoDB Table
- S3 Bucket para documentos
- Step Functions State Machine
- API Gateway
- SNS Topic para erros
- Secrets Manager para API keys

### FrontendStack

Contém:
- S3 Bucket para frontend estático
- CloudFront Distribution
- Deploy automático do frontend
- Invalidação automática de cache após deploy

**Características do Frontend Stack:**
- ✅ Limpa o bucket antes de fazer upload (`prune: true`)
- ✅ Invalida cache do CloudFront após deploy
- ✅ Cache otimizado para assets (CSS, JS, imagens)
- ✅ Sem cache para HTML (facilita atualizações)
- ✅ SPA fallback (redireciona 404/403 para index.html)
- ✅ HTTPS obrigatório
- ✅ Headers de segurança configurados

## Exemplos de Uso

### Deploy em Desenvolvimento

```bash
export ENV=dev
cdk deploy --all
```

### Deploy em Staging

```bash
export ENV=stg
cdk deploy --all
```

### Deploy em Produção

```bash
export ENV=prd
cdk deploy --all --require-approval never  # ou 'any-change' para revisar
```

### Atualizar Apenas o Frontend

```bash
export ENV=dev
cdk deploy FrontendStack-dev
```

O deploy do frontend automaticamente:
1. Limpa o bucket S3 (remove arquivos antigos)
2. Faz upload dos novos arquivos
3. Invalida o cache do CloudFront

## Variáveis de Ambiente Necessárias

Para o backend funcionar corretamente, algumas variáveis podem ser necessárias:

```bash
# OAuth2 para reporte de falhas OCR (opcional, pode ser configurado via CDK context)
export OCR_FAILURE_API_URL='https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha'
export OCR_FAILURE_AUTH_URL='https://agroamazoniad.service-now.com/oauth_token.do'
export OCR_FAILURE_CLIENT_ID='seu_client_id'
export OCR_FAILURE_CLIENT_SECRET='seu_client_secret'
export OCR_FAILURE_USERNAME='fast.ocr'
export OCR_FAILURE_PASSWORD='sua_password'
```

Ou via CDK context:

```bash
cdk deploy --context ocrFailureApiUrl='...' --context ocrFailureClientId='...' ...
```

## Outputs

Após o deploy, os seguintes outputs estarão disponíveis:

### Backend:
- `ApiUrl`: URL da API Gateway
- `BucketName`: Nome do bucket S3 para documentos
- `TableName`: Nome da tabela DynamoDB
- `StateMachineArn`: ARN da Step Functions State Machine
- `ApiKeysSecretArn`: ARN do Secrets Manager para API keys
- `ErrorTopicArn`: ARN do SNS Topic para erros

### Frontend:
- `FrontendBucketName`: Nome do bucket S3 para frontend
- `CloudFrontDistributionId`: ID da distribuição CloudFront
- `CloudFrontDomainName`: Domínio do CloudFront
- `FrontendUrl`: URL completa do frontend (HTTPS)

## Limpeza

Para deletar todas as stacks:

```bash
export ENV=dev
cdk destroy --all
```

**Atenção**: Buckets e tabelas DynamoDB têm `RemovalPolicy.RETAIN` por padrão no backend, então não serão deletados automaticamente. O frontend tem `RemovalPolicy.DESTROY` e será deletado completamente.

## Troubleshooting

### Erro: "Bucket name already exists"
- Bucket names devem ser únicos globalmente
- O nome inclui o account ID para garantir unicidade
- Se ainda assim houver conflito, ajuste o nome no código

### Erro: "Invalid environment"
- Certifique-se de que `ENV` é exatamente: `dev`, `stg`, ou `prd`
- Case-sensitive

### Frontend não atualiza após deploy
- O CloudFront pode levar alguns minutos para invalidar o cache
- Verifique se a invalidação foi criada no console do CloudFront
- Os arquivos HTML e JS têm cache desabilitado por padrão

