# 📮 Guia de Uso - Postman Collection

## 🚀 Importar Collection

1. Abra o Postman
2. Clique em **Import**
3. Selecione o arquivo `AgroAmazonia_API.postman_collection.json`
4. Clique em **Import**

## 🔑 Configurar API Key

Após importar, configure sua API Key:

1. Clique na collection **AgroAmazonia API**
2. Vá em **Variables**
3. Edite a variável `api_key` com sua chave:
   ```
   agroamazonia_key_<seu_codigo>
   ```
4. Clique em **Save**

## 📋 Estrutura da Collection

### 📚 Documentação (Público - sem API Key)
- **Swagger UI**: Acessa documentação interativa
- **Health Check**: Verifica status da API

### 🚀 Fluxo Completo (5 Passos)

#### 1️⃣ Gerar URL para Upload DANFE (XML)
- Gera automaticamente um `process_id` (UUID)
- Retorna URL assinada para upload
- URL salva automaticamente na variável `upload_url_xml`

#### 2️⃣ Upload DANFE (PUT no S3)
- **IMPORTANTE**: Selecione o arquivo XML em `Body > binary`
- Não precisa de API Key (URL já está assinada)
- Content-Type: `application/xml`

#### 3️⃣ Gerar URLs para Upload Documentos Adicionais
- Envia array com múltiplos arquivos
- Retorna array de URLs assinadas
- Primeira URL salva em `upload_url_doc`

#### 4️⃣ Upload Documento Adicional (PUT no S3)
- **IMPORTANTE**: Selecione o arquivo PDF em `Body > binary`
- Não precisa de API Key (URL já está assinada)
- Content-Type: `application/pdf`
- Repita para cada documento adicional

#### 5️⃣ Iniciar Processamento
- Escolha o tipo de processo:
  - `SEMENTES`
  - `AGROQUIMICOS`
  - `FERTILIZANTES`
- Inicia workflow de processamento

### 📋 Consultas
- **Listar Todos os Processos**: Lista todos os processos
- **Buscar Processo por ID**: Detalhes de um processo específico
- **Buscar Validações**: Resultados das validações

### ⚙️ Gerenciamento de Regras
- **Listar Regras**: Por tipo de processo
- **Ativar Regra**: Adiciona regra de validação
- **Desativar Regra**: Remove regra de validação

## 🎯 Exemplo de Uso Completo

### Passo a Passo:

1. **Execute**: `1️⃣ Gerar URL para Upload DANFE (XML)`
   - Process ID será gerado automaticamente
   - URL de upload será salva

2. **Execute**: `2️⃣ Upload DANFE (PUT no S3)`
   - Selecione seu arquivo XML em `Body > binary`
   - Clique em **Send**

3. **Execute**: `3️⃣ Gerar URLs para Upload Documentos Adicionais`
   - Edite o JSON para incluir seus arquivos
   - URLs serão geradas

4. **Execute**: `4️⃣ Upload Documento Adicional (PUT no S3)`
   - Selecione seu arquivo PDF em `Body > binary`
   - Repita para cada documento

5. **Execute**: `5️⃣ Iniciar Processamento`
   - Escolha o `process_type` adequado
   - Aguarde processamento (pode levar alguns minutos)

6. **Execute**: `Buscar Validações do Processo`
   - Veja os resultados das validações

## 🔧 Variáveis da Collection

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `base_url` | URL base da API | `https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1` |
| `api_key` | Sua chave de API | `agroamazonia_key_abc123...` |
| `process_id` | ID do processo (auto-gerado) | `7d48cd96-c099-48dd-bbb6-d4fe8b2de318` |
| `upload_url_xml` | URL de upload do XML (auto-salva) | `https://s3.amazonaws.com/...` |
| `upload_url_doc` | URL de upload do PDF (auto-salva) | `https://s3.amazonaws.com/...` |

## ⚠️ Dicas Importantes

### Upload de Arquivos (PUT)
- ✅ Use `Body > binary` e selecione o arquivo
- ✅ Certifique-se que o Content-Type está correto
- ✅ Não adicione API Key (URL já está assinada)
- ❌ Não use `Body > form-data`

### API Key
- ✅ Configurada automaticamente na collection
- ✅ Aplicada em todas as rotas protegidas
- ✅ Não é necessária para `/docs`, `/health` e uploads S3

### Process ID
- ✅ Gerado automaticamente no primeiro passo
- ✅ Salvo na variável `{{process_id}}`
- ✅ Usado automaticamente nas próximas requisições

## 📞 Suporte

Para obter sua API Key ou reportar problemas, entre em contato com o administrador do sistema.

## 🔗 Links Úteis

- **Swagger UI**: https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1/docs
- **Health Check**: https://ovyt3c2b2c.execute-api.us-east-1.amazonaws.com/v1/health
