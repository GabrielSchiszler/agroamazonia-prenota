# Frontend - AgroAmazonia

## Configuração

### Opção 1: Usando arquivo `.env` (Recomendado)

1. Criar arquivo `.env` na pasta `frontend/`:

```bash
cd frontend
cat > .env << 'EOF'
API_URL=http://localhost:8001
API_KEY=dev
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
python3 server.py
```

### Opção 3: Inline (temporário)

```bash
API_URL='https://sua-api.com/v1' API_KEY='sua-api-key' python3 server.py
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

## Segurança

⚠️ **IMPORTANTE**: Nunca commite os arquivos:
- `.env`
- `config.js`

Eles contêm credenciais sensíveis e estão no `.gitignore`.
