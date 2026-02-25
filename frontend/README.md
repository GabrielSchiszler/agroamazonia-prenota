# Frontend - AgroAmazonia

## Configuração

### Opção 1: Usando arquivo `.env` (Recomendado)

1. Criar arquivo `.env` na pasta `frontend/`:

```bash
cd frontend
cat > .env << 'EOF'
API_URL=http://localhost:8001
API_KEY=dev
OAUTH2_FRONTEND_TOKEN_URL=https://api-auth-hml.agroamazonia.io/oauth2/token
OAUTH2_FRONTEND_CLIENT_ID=seu-client-id
OAUTH2_FRONTEND_CLIENT_SECRET=seu-client-secret
OAUTH2_FRONTEND_SCOPE=App_Fast/HML
EOF
```

2. Iniciar o servidor (ele gerará `config.js` automaticamente):

```bash
python3 server.py
```

### Opção 2: Usando variáveis de ambiente do sistema

```bash
export API_URL='https://sua-api.com/v1'
export API_KEY='sua-api-key'
export OAUTH2_FRONTEND_TOKEN_URL='https://api-auth-hml.agroamazonia.io/oauth2/token'
export OAUTH2_FRONTEND_CLIENT_ID='seu-client-id'
export OAUTH2_FRONTEND_CLIENT_SECRET='seu-client-secret'
export OAUTH2_FRONTEND_SCOPE='App_Fast/HML'
python3 server.py
```

### Opção 3: Inline (temporário)

```bash
API_URL='https://sua-api.com/v1' \
API_KEY='sua-api-key' \
OAUTH2_FRONTEND_TOKEN_URL='https://api-auth-hml.agroamazonia.io/oauth2/token' \
OAUTH2_FRONTEND_CLIENT_ID='seu-client-id' \
OAUTH2_FRONTEND_CLIENT_SECRET='seu-client-secret' \
OAUTH2_FRONTEND_SCOPE='App_Fast/HML' \
python3 server.py
```

### Acessar

O servidor estará rodando em: http://localhost:8080

**Nota**: O servidor gera automaticamente o arquivo `config.js` a partir das variáveis de ambiente ou do arquivo `.env`. Não é necessário criar o `config.js` manualmente!

## Estrutura

```
frontend/
├── .env                 # Variáveis de ambiente (não commitar)
├── .env.example         # Template de configuração
├── config.js            # Configuração exportada (não commitar)
├── index.html           # HTML principal
├── app.js               # JavaScript principal
└── style.css            # Estilos
```

## Variáveis de Ambiente

### API
- `API_URL`: URL base da API (ex: `https://api-hml.agroamazonia.io`)
- `API_KEY`: Chave da API (opcional, para compatibilidade)

### OAuth2 Frontend (Autenticação)
- `OAUTH2_FRONTEND_TOKEN_URL`: URL do endpoint de token OAuth2 (ex: `https://api-auth-hml.agroamazonia.io/oauth2/token`)
- `OAUTH2_FRONTEND_CLIENT_ID`: Client ID do OAuth2 para o frontend
- `OAUTH2_FRONTEND_CLIENT_SECRET`: Client Secret do OAuth2 para o frontend (⚠️ **SENSÍVEL**)
- `OAUTH2_FRONTEND_SCOPE`: Escopo do OAuth2 (ex: `App_Fast/HML`)

## Segurança

⚠️ **IMPORTANTE**: Nunca commite os arquivos:
- `.env`
- `config.js`

Eles contêm credenciais sensíveis e estão no `.gitignore`.

**Especialmente sensível**: `OAUTH2_FRONTEND_CLIENT_SECRET` - nunca exponha em repositórios públicos ou logs!
