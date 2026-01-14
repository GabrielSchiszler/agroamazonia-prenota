# Script de Teste OAuth2 - Protheus

Este script testa a autenticação OAuth2 do Protheus usando Client Credentials Grant.

## Uso

### Opção 1: Usando argumentos de linha de comando

```bash
cd backend/scripts
python3 test_protheus_oauth2.py \
  --token-url "https://api.agroamazonia.com/oauth/token" \
  --client-id "seu_client_id" \
  --client-secret "seu_client_secret" \
  --scope "read:org"  # opcional
```

### Opção 2: Usando arquivo .env

1. Copie o arquivo `env.example` para `.env`:
   ```bash
   cp env.example .env
   ```

2. Edite o arquivo `.env` e preencha as variáveis:
   ```bash
   export PROTHEUS_AUTH_URL=https://api.agroamazonia.com/oauth/token
   export PROTHEUS_CLIENT_ID=seu_client_id
   export PROTHEUS_CLIENT_SECRET=seu_client_secret
   ```

3. Execute o script:
   ```bash
   python3 test_protheus_oauth2.py
   ```

### Opção 3: Usando variáveis de ambiente do sistema

```bash
export PROTHEUS_AUTH_URL=https://api.agroamazonia.com/oauth/token
export PROTHEUS_CLIENT_ID=seu_client_id
export PROTHEUS_CLIENT_SECRET=seu_client_secret
python3 test_protheus_oauth2.py
```

## O que o script testa

O script testa **5 abordagens diferentes** de OAuth2 Client Credentials:

1. **Basic Auth + client_credentials**: Credenciais no header Authorization (Basic Auth)
2. **Body Auth + client_credentials**: Credenciais no body da requisição
3. **Basic Auth + Body Auth**: Ambos (alguns servidores exigem)
4. **Body Auth sem client_secret**: Apenas client_id no body
5. **JSON Body**: Envio em formato JSON ao invés de form-urlencoded

## Saída

O script mostra:
- Status code de cada requisição
- Headers enviados e recebidos
- Body da requisição e resposta
- Token obtido (se sucesso)
- Resumo final com abordagens que funcionaram

## Exemplo de saída

```
================================================================================
TESTE DE OAUTH2 CLIENT CREDENTIALS - PROTHEUS
================================================================================

Token URL: https://api.agroamazonia.com/oauth/token
Client ID: meu_client...
Client Secret: ********

================================================================================
ABORDAGEM 1: Basic Auth + client_credentials
================================================================================

Status Code: 200
Headers enviados:
  Authorization: Basic bX...
  Content-Type: application/x-www-form-urlencoded

Body enviado:
  grant_type=client_credentials

Response Headers:
  Content-Type: application/json
  ...

Response Body:
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 3600
}

✅ SUCESSO! Token obtido:
   Token (primeiros 30 chars): eyJhbGciOiJIUzI1NiIsInR5cCI6...
   Token completo: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
   Token Type: Bearer
   Expires In: 3600 segundos
```

## Troubleshooting

Se nenhuma abordagem funcionar:

1. **Verifique a URL do token**: Deve ser exatamente a mesma configurada no Postman
2. **Verifique Client ID e Secret**: Copie exatamente do Postman
3. **Verifique o Grant Type**: Deve ser `client_credentials`
4. **Verifique se há Scope obrigatório**: Alguns servidores exigem scope específico
5. **Verifique firewall/proxy**: Pode estar bloqueando requisições
6. **Verifique logs do servidor**: Pode ter mais detalhes sobre o erro

## Comparação com Postman

O script tenta replicar exatamente o que o Postman faz quando você configura:

- **Grant Type**: `client_credentials`
- **Access Token URL**: `{{tokenUrl}}`
- **Client ID**: `{{clientId}}`
- **Client Secret**: `{{clientSecret}}`
- **Client Authentication**: Basic Auth ou Body (testa ambos)

