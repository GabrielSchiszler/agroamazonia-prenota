# Como Testar o Fluxo Completo - Step Functions

## 📊 Entendendo a Saída do `updateMetricsTaskSuccess`

Quando você testa o state `updateMetricsTaskSuccess` **isoladamente** no Step Functions Console, a saída mostra apenas o resultado da Lambda:

```json
{
  "statusCode": 200,
  "process_id": "30c52de7977676948cf434a6f053af9c",
  "metrics_updated": true,
  "deduplicated": true,
  "previous_status": "SUCCESS"
}
```

**Isso está correto!** ✅

## 🔄 Mas no Fluxo Completo...

Quando o fluxo completo roda, o Step Functions **preserva o estado anterior** porque usamos `resultPath: '$.metrics_result'` ao invés de `outputPath: '$.Payload'`.

### Estado ANTES do `updateMetricsTaskSuccess`:
```json
{
  "process_id": "30c52de7977676948cf434a6f053af9c",
  "protheus_result": { ... },
  "metrics_input": { ... }
}
```

### Estado DEPOIS do `updateMetricsTaskSuccess` (no fluxo completo):
```json
{
  "process_id": "30c52de7977676948cf434a6f053af9c",
  "protheus_result": { ... },        // ✅ PRESERVADO
  "metrics_input": { ... },          // ✅ PRESERVADO
  "metrics_result": {                // ✅ ADICIONADO
    "statusCode": 200,
    "process_id": "30c52de7977676948cf434a6f053af9c",
    "metrics_updated": true,
    "deduplicated": true,
    "previous_status": "SUCCESS"
  }
}
```

## 🧪 Como Testar o Fluxo Completo

### Opção 1: Testar a State Machine Completa

1. Vá para o AWS Step Functions Console
2. Selecione sua state machine
3. Clique em "Start execution"
4. Use este JSON como input:

```json
{
  "process_id": "30c52de7977676948cf434a6f053af9c"
}
```

5. Execute e acompanhe o fluxo completo
6. Quando chegar no `notifySuccessTask`, verifique que o estado contém `protheus_result`

### Opção 2: Testar o `notifySuccessTask` com Estado Simulado

1. Vá para o AWS Step Functions Console
2. Selecione sua state machine
3. Vá para a aba "Test" ou "Execution"
4. Selecione o state `NotifySuccessTask`
5. Use o JSON do arquivo `example_estado_apos_update_metrics.json` como input
6. Execute e verifique se consegue acessar `$.protheus_result`

### Opção 3: Verificar em uma Execução Real

1. Execute um processo completo (upload de documento)
2. Vá para a execução no Step Functions Console
3. Clique no state `updateMetricsTaskSuccess`
4. Veja o **Input** (deve ter `protheus_result`)
5. Veja o **Output** (deve ter `protheus_result` + `metrics_result`)
6. Clique no state `notifySuccessTask`
7. Veja o **Input** (deve ter `protheus_result`)

## 🔍 Diferença entre `outputPath` e `resultPath`

### `outputPath: '$.Payload'` (❌ ANTES - causava erro)
- **Substitui** todo o estado pelo resultado da Lambda
- Perde todos os dados anteriores (`protheus_result`, `metrics_input`, etc.)
- Estado final: apenas o Payload da Lambda

### `resultPath: '$.metrics_result'` (✅ AGORA - corrigido)
- **Preserva** o estado anterior
- **Adiciona** o resultado da Lambda em `$.metrics_result`
- Estado final: estado anterior + `metrics_result`

## 📋 Checklist de Validação

Quando testar o fluxo completo, verifique:

- [ ] `process_id` está presente em todos os states
- [ ] `protheus_result` está presente após `updateMetricsTaskSuccess`
- [ ] `metrics_result` está presente após `updateMetricsTaskSuccess`
- [ ] `notifySuccessTask` consegue acessar `$.protheus_result`
- [ ] O fluxo completa com sucesso até `successState`

## 🎯 Exemplo de Estado Completo

Veja o arquivo `example_estado_apos_update_metrics.json` para um exemplo completo do estado após o `updateMetricsTaskSuccess` no fluxo completo.

Este é o estado que o `notifySuccessTask` deve receber:

```json
{
  "process_id": "...",
  "protheus_result": { ... },      // ✅ Disponível
  "metrics_input": { ... },        // ✅ Disponível
  "metrics_result": { ... }        // ✅ Disponível
}
```

