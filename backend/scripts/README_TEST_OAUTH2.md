# Script de Teste - OAuth2 Token

Este script testa a criação do token OAuth2 para a API externa de reporte de falhas OCR.

## Como usar

### Opção 1: Via argumentos de linha de comando (Recomendado)

```bash
python3 backend/scripts/test_oauth2_token.py \
  --auth-url 'https://agroamazoniad.service-now.com/oauth_token.do' \
  --client-id 'seu_client_id' \
  --client-secret 'seu_client_secret' \
  --username 'fast.ocr' \
  --password 'sua_password'
```

### Opção 2: Via variáveis de ambiente

```bash
export OCR_FAILURE_AUTH_URL='https://agroamazoniad.service-now.com/oauth_token.do'
export OCR_FAILURE_CLIENT_ID='seu_client_id'
export OCR_FAILURE_CLIENT_SECRET='seu_client_secret'
export OCR_FAILURE_USERNAME='fast.ocr'
export OCR_FAILURE_PASSWORD='sua_password'
export OCR_FAILURE_API_URL='https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha'

python3 backend/scripts/test_oauth2_token.py
```

### Opção 3: Via arquivo .env

Crie um arquivo `.env` na raiz do projeto ou em `backend/scripts/`:

```bash
# .env
OCR_FAILURE_AUTH_URL=https://agroamazoniad.service-now.com/oauth_token.do
OCR_FAILURE_CLIENT_ID=seu_client_id
OCR_FAILURE_CLIENT_SECRET=seu_client_secret
OCR_FAILURE_USERNAME=fast.ocr
OCR_FAILURE_PASSWORD=sua_password
OCR_FAILURE_API_URL=https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha
```

Depois execute:

```bash
# Se tiver python-dotenv instalado
pip install python-dotenv
python3 backend/scripts/test_oauth2_token.py --env-file .env

# Ou o script tentará carregar manualmente
python3 backend/scripts/test_oauth2_token.py --env-file .env
```

### Instalar dependências (se necessário)

```bash
pip install requests
# Opcional (para suporte a .env)
pip install python-dotenv
```

## O que o script faz

1. **Verifica variáveis de ambiente**: Mostra quais variáveis estão configuradas e quais estão faltando
2. **Testa 4 abordagens diferentes de OAuth2**:
   - Basic Auth + grant_type=password
   - Basic Auth + grant_type=password_credentials
   - Body Auth + grant_type=password
   - Body Auth + grant_type=password_credentials
3. **Exibe o token obtido**: Mostra preview do token (sem expor completamente por segurança)
4. **Testa chamada à API**: Faz uma requisição de teste à API externa usando o token obtido

## Saída esperada

### Sucesso:
```
================================================================================
TESTE DE OAuth2 TOKEN
================================================================================

Variáveis de ambiente:
  OCR_FAILURE_AUTH_URL: SET
    Valor: https://agroamazoniad.service-now.com/oauth_token.do
  OCR_FAILURE_CLIENT_ID: SET
    Valor: abc123...
  ...

[1/4] Tentando: Basic Auth + password
  Status Code: 200
  ✅ SUCESSO com abordagem: Basic Auth + password

✅ TOKEN OBTIDO COM SUCESSO!
  Token preview: abc123xyz...xyz789
```

### Erro:
```
❌ ERRO: Variáveis de ambiente faltando: OCR_FAILURE_CLIENT_ID, OCR_FAILURE_CLIENT_SECRET
```

## Opções adicionais

```bash
# Pular teste de chamada à API (apenas obter token)
python3 test_oauth2_token.py --skip-api-test --client-id '...' --client-secret '...' --username '...' --password '...'

# Especificar URL da API diferente
python3 test_oauth2_token.py --api-url 'https://outra-url.com/api' --client-id '...' --client-secret '...' --username '...' --password '...'
```

## Troubleshooting

- **401 Unauthorized**: 
  - Verifique se as credenciais estão corretas (mesmas do Postman)
  - Verifique se o `username` está correto (ex: `fast.ocr`)
  - O script tenta 4 abordagens diferentes automaticamente

- **Variáveis não encontradas**: 
  - Use argumentos de linha de comando: `--client-id`, `--client-secret`, etc.
  - Ou exporte as variáveis antes de executar
  - Ou use arquivo `.env` com `--env-file`

- **Timeout**: 
  - Verifique a conectividade de rede com o ServiceNow
  - Verifique se a URL está correta: `https://agroamazoniad.service-now.com/oauth_token.do`

- **Token não encontrado na resposta**:
  - O script mostra a resposta completa para debug
  - Verifique se o ServiceNow está retornando o token no formato esperado

## Comparação com Postman

O script replica exatamente as 4 abordagens que o Postman pode usar:
1. Basic Auth header + `grant_type=password`
2. Basic Auth header + `grant_type=password_credentials`
3. Client credentials no body + `grant_type=password`
4. Client credentials no body + `grant_type=password_credentials`

O script mostra qual abordagem funcionou, facilitando o debug.

