# Frontend - AgroAmazonia

Interface web para gerenciamento e visualização do sistema de processamento de documentos.

## Funcionalidades

### 1. Configuração da API
- Campo para inserir a URL da API Gateway
- Configuração salva no localStorage do navegador

### 2. Upload de Documentos
- Formulário para envio de documentos
- Campos:
  - **ID do Documento**: Identificador único
  - **Tipo de Documento**: PRE_NOTE ou DOC_XML
  - **Tipo de Processo**: SEMENTES, AGROQUIMICOS ou FERTILIZANTES
  - **Arquivo**: Upload de PDF ou XML
- Preview das regras que serão aplicadas ao documento

### 3. Listagem de Documentos
- Visualização de todos os documentos processados
- Informações exibidas:
  - ID do documento
  - Status (Processando, Concluído, Falhou)
  - Tipo de documento e processo
  - Nome do arquivo
  - Data/hora de envio
- Botão de atualização manual

### 4. Visualização de Regras
- Três abas para cada tipo de processo:
  - 🌱 Sementes
  - 🧪 Agroquímicos
  - 🌾 Fertilizantes
- Para cada tipo, exibe:
  - Nome da regra
  - Descrição detalhada
  - Condição de validação
  - Ação executada em caso de falha
  - Ordem de execução (Chain of Responsibility)

## Estrutura de Regras

### SEMENTES
1. **Validação de Imposto**
   - Verifica se imposto está dentro do limite
   - Ação: REJECT (rejeita documento)

2. **Verificação de Documentação**
   - Valida presença de Certificado Fitossanitário
   - Ação: PENDING (aguarda documentação)

### AGROQUIMICOS
1. **Validação de Licença IBAMA**
   - Verifica presença de licença obrigatória
   - Ação: REJECT (rejeita imediatamente)

2. **Verificação de Valor**
   - Compara valor total com valor esperado
   - Ação: PENDING (análise de divergência)

### FERTILIZANTES
1. **Validação de Laudo de Composição**
   - Verifica presença de laudo químico
   - Ação: REJECT (rejeita sem laudo)

## Como Usar

### 1. Abrir o Frontend

```bash
cd frontend
# Abrir index.html em um navegador
# Ou usar um servidor local:
python -m http.server 8000
# Acessar: http://localhost:8000
```

### 2. Configurar API

1. Após o deploy do CDK, copie a URL da API dos outputs
2. Cole no campo "URL da API"
3. Clique em "Salvar"

### 3. Enviar Documento

1. Preencha o ID do documento
2. Selecione o tipo de documento
3. Selecione o tipo de processo (as regras serão exibidas)
4. Escolha o arquivo
5. Clique em "Enviar Documento"

### 4. Acompanhar Processamento

- A lista de documentos é atualizada automaticamente
- Status muda de "Processando" para "Concluído"
- Clique em "Atualizar Lista" para refresh manual

## Integração com API Real

O frontend está preparado para integração com a API real. Atualmente usa dados mockados para demonstração.

Para ativar a integração real, descomente as funções em `app.js`:

```javascript
// Em handleUpload()
const response = await fetch(`${API_URL}/api/v1/document/submit`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
        document_id: documentId,
        document_type: documentType,
        process_type: processType,
        s3_path: s3Path
    })
});

// Para listar documentos
await fetchDocuments();

// Para detalhes de um documento
await getDocumentDetails(documentId);
```

## Fluxo de Upload Real

1. **Upload para S3**:
   ```javascript
   // Obter URL pré-assinada da API
   const presignedUrl = await getPresignedUrl(fileName);
   
   // Upload direto para S3
   await fetch(presignedUrl, {
       method: 'PUT',
       body: file
   });
   ```

2. **Iniciar Processamento**:
   ```javascript
   // Chamar API para iniciar Step Functions
   await fetch(`${API_URL}/api/v1/document/submit`, {
       method: 'POST',
       body: JSON.stringify({...})
   });
   ```

3. **Monitorar Status**:
   ```javascript
   // Polling ou WebSocket para atualizações
   setInterval(async () => {
       const status = await getDocumentStatus(documentId);
       updateUI(status);
   }, 5000);
   ```

## Personalização

### Adicionar Novo Tipo de Processo

1. Em `app.js`, adicione ao objeto `PROCESS_RULES`:

```javascript
PROCESS_RULES.NOVO_TIPO = [
    {
        name: 'Nome da Regra',
        description: 'Descrição detalhada',
        condition: 'campo == valor',
        action: 'REJECT',
        actionDescription: 'O que acontece',
        order: 1
    }
];
```

2. Adicione opção no select do HTML:

```html
<option value="NOVO_TIPO">Novo Tipo</option>
```

3. Adicione botão na seção de regras:

```html
<div class="process-btn" onclick="showRules('NOVO_TIPO')">
    🆕 Novo Tipo
</div>
```

## Tecnologias

- HTML5
- CSS3 (Grid, Flexbox, Animations)
- JavaScript Vanilla (ES6+)
- LocalStorage para persistência
- Fetch API para requisições

## Responsividade

- Layout adaptativo para desktop e mobile
- Grid responsivo que vira coluna única em telas menores
- Componentes otimizados para touch

## Segurança

- Validação de inputs no frontend
- Sanitização de dados antes de envio
- CORS configurado no API Gateway
- Sem armazenamento de credenciais no frontend
