# Frontend - AgroAmazonia

## Configuração

### 1. Copiar arquivo de configuração

```bash
cp .env.example .env
```

### 2. Editar `.env` com suas credenciais

```env
VITE_API_URL=https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1
VITE_API_KEY=agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx
```

### 3. Gerar `config.js`

```bash
cat > config.js << 'EOF'
window.ENV = {
    API_URL: 'https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1',
    API_KEY: 'agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx'
};
EOF
```

### 4. Abrir no navegador

```bash
# Servir com Python
python3 -m http.server 8080

# Ou com Node.js
npx serve .
```

Acesse: http://localhost:8080

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
