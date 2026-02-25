# Configuração de CORS no API Gateway

## ⚠️ IMPORTANTE: Lambda Router

Se você está usando um **Lambda Router** que redireciona requisições para outras Lambdas, o Router **DEVE** adicionar headers CORS em todas as respostas, incluindo:
- Requisições OPTIONS (preflight)
- Respostas de sucesso
- Respostas de erro

O arquivo `backend/lambdas/router/handler.py` já foi corrigido para fazer isso automaticamente.

## Problema

Se o API Gateway retornar erros (401, 403, 500, etc.) **ANTES** de chegar à Lambda (por exemplo, autenticação Cognito falhando), a Lambda nunca será chamada e não poderá adicionar headers CORS. Nesse caso, é necessário configurar **GatewayResponse** no API Gateway.

## Solução: Configurar GatewayResponse

### Via Console AWS

1. Acesse o **API Gateway** no console AWS
2. Selecione sua API
3. No menu lateral, clique em **Gateway Responses**
4. Para cada tipo de erro que pode ocorrer antes da Lambda, configure:

#### Para erro 401 (Unauthorized):
- Clique em **Unauthorized** (ou crie um novo)
- Em **Response Headers**, adicione:
  - `Access-Control-Allow-Origin`: `*` (ou seu domínio específico)
  - `Access-Control-Allow-Credentials`: `true`
  - `Access-Control-Allow-Methods`: `GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD`
  - `Access-Control-Allow-Headers`: `Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With`

#### Para erro 403 (Forbidden):
- Clique em **Forbidden** (ou crie um novo)
- Adicione os mesmos headers acima

#### Para erro 500 (Internal Server Error):
- Clique em **Default 5XX** (ou crie um novo)
- Adicione os mesmos headers acima

#### Para erro 400 (Bad Request):
- Clique em **Bad Request Body** (ou crie um novo)
- Adicione os mesmos headers acima

### Via AWS CLI

```bash
# Para erro 401
aws apigateway put-gateway-response \
  --rest-api-id YOUR_API_ID \
  --response-type UNAUTHORIZED \
  --response-parameters '{
    "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
    "gatewayresponse.header.Access-Control-Allow-Credentials": "'true'",
    "gatewayresponse.header.Access-Control-Allow-Methods": "'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD'",
    "gatewayresponse.header.Access-Control-Allow-Headers": "'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With'"
  }' \
  --response-templates '{"application/json": "{\"message\":$context.error.messageString}"}'

# Para erro 403
aws apigateway put-gateway-response \
  --rest-api-id YOUR_API_ID \
  --response-type ACCESS_DENIED \
  --response-parameters '{
    "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
    "gatewayresponse.header.Access-Control-Allow-Credentials": "'true'",
    "gatewayresponse.header.Access-Control-Allow-Methods": "'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD'",
    "gatewayresponse.header.Access-Control-Allow-Headers": "'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With'"
  }' \
  --response-templates '{"application/json": "{\"message\":$context.error.messageString}"}'

# Para erro 500
aws apigateway put-gateway-response \
  --rest-api-id YOUR_API_ID \
  --response-type DEFAULT_5XX \
  --response-parameters '{
    "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
    "gatewayresponse.header.Access-Control-Allow-Credentials": "'true'",
    "gatewayresponse.header.Access-Control-Allow-Methods": "'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD'",
    "gatewayresponse.header.Access-Control-Allow-Headers": "'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With'"
  }' \
  --response-templates '{"application/json": "{\"message\":$context.error.messageString}"}'
```

### Via Terraform (se usar)

```hcl
resource "aws_api_gateway_gateway_response" "cors_401" {
  rest_api_id   = aws_api_gateway_rest_api.api.id
  response_type = "UNAUTHORIZED"

  response_parameters = {
    "gatewayresponse.header.Access-Control-Allow-Origin"  = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Credentials" = "'true'"
    "gatewayresponse.header.Access-Control-Allow-Methods" = "'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD'"
    "gatewayresponse.header.Access-Control-Allow-Headers" = "'Content-Type, Authorization, X-API-Key, x-api-key, Accept, Origin, X-Requested-With'"
  }

  response_templates = {
    "application/json" = "{\"message\":$context.error.messageString}"
  }
}
```

## Verificação

Após configurar, teste uma requisição que retorne erro 401/403 e verifique se os headers CORS estão presentes na resposta:

```bash
curl -X GET https://sua-api.com/v1/endpoint \
  -H "Origin: https://seu-frontend.com" \
  -v
```

Você deve ver os headers `Access-Control-Allow-Origin` na resposta, mesmo em caso de erro.

## Nota Importante

- Se você usar domínio específico em vez de `*`, certifique-se de que o `Access-Control-Allow-Credentials` seja `true`
- Não use `*` com `Access-Control-Allow-Credentials: true` - isso causará erro no browser
- Se usar `*`, defina `Access-Control-Allow-Credentials: false` ou remova esse header

