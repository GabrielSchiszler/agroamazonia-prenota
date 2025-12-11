# Gerenciamento de API Keys

Sistema de API Keys customizado com **AWS Secrets Manager** para controle total de criação, listagem e revogação.

## Como Funciona

- **API Keys armazenadas no Secrets Manager** (JSON com chave:cliente)
- **Lambda Authorizer** valida requisições com cache em memória
- **Cache de 1 hora no API Gateway** + cache em memória do Lambda
- **Custo fixo**: $0.40/mês por secret (não por requisição)
- **Zero custo por requisição** após cache

## Comandos

### 1. Criar Nova API Key

```bash
python scripts/create_api_key.py "Cliente ABC"
```

**Output:**
```
✅ API Key criada com sucesso!
Cliente: Cliente ABC
API Key: xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA

Uso: curl -H 'x-api-key: xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA' https://your-api.com/endpoint
```

### 2. Listar Todas as API Keys

```bash
python scripts/list_api_keys.py
```

**Output:**
```
Cliente                        Status     API Key (parcial)
--------------------------------------------------------------------------------
Cliente ABC                    active     xK9mP2nQ7vR4sT8w...dF5gB0cA
Cliente XYZ                    revoked    aB3cD4eF5gH6iJ7k...mN8oP9qR

Total: 2 chaves
```

### 3. Revogar API Key (Desativar)

```bash
python scripts/revoke_api_key.py xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA
```

**Output:**
```
✅ API Key revogada com sucesso!
Cliente: Cliente ABC
Status: revoked
```

### 4. Deletar API Key (Permanente)

```bash
python scripts/delete_api_key.py xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA
```

**Output:**
```
⚠️  Tem certeza que deseja DELETAR permanentemente esta chave? (sim/não): sim
✅ API Key deletada permanentemente!
Cliente: Cliente ABC
```

## Estrutura Secrets Manager

```json
{
  "xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA": {
    "client_name": "Cliente ABC",
    "status": "active"
  },
  "aB3cD4eF5gH6iJ7k8lM9nN8oP9qR": {
    "client_name": "Cliente XYZ",
    "status": "revoked"
  }
}
```

## Status Possíveis

- **active**: Chave válida e funcionando
- **revoked**: Chave desativada (não funciona mais)

## Uso pelo Cliente

```bash
# Requisição com API Key
curl -H "x-api-key: xK9mP2nQ7vR4sT8wY3zL6hJ1dF5gB0cA" \
     https://your-api.execute-api.us-east-1.amazonaws.com/v1/api/v1/process/123
```

## Custo

**Secrets Manager**: $0.40/mês por secret (1 secret para todas as chaves)
**Lambda Authorizer**: Executa apenas quando cache expira (1 hora)
**API Gateway Cache**: 1 hora de cache por chave

**Exemplo**: 10.000 requisições/dia
- Com cache de 1h: ~417 execuções Lambda/dia
- Custo Lambda: ~$0.01/mês
- Custo total: ~$0.41/mês

**vs DynamoDB**: 10.000 req/dia = 300k GetItem/mês = ~$0.75/mês

## Segurança

✅ Chaves geradas com `secrets.token_urlsafe(32)` (256 bits)
✅ Cache em memória do Lambda (reutilização de container)
✅ Cache de 1 hora no API Gateway
✅ Revogação instantânea (cache expira em até 1h)
✅ Sem hardcoded keys no código
✅ Secrets Manager criptografado com KMS

## Deploy

Após fazer deploy do CDK:

```bash
cd infrastructure
cdk deploy
```

O Lambda Authorizer será criado automaticamente e vinculado ao API Gateway.

## Performance

- **1ª requisição**: Lambda busca Secrets Manager (~100ms)
- **Requisições seguintes**: Cache em memória (~1ms)
- **Após 1 hora**: API Gateway cache expira, Lambda executa novamente
- **Container Lambda**: Mantém cache enquanto container estiver quente
