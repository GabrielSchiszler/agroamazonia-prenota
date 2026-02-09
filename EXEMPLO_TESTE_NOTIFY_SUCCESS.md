# Exemplo de Teste - NotifySuccessTask

## 📥 INPUT (O que o NotifySuccessTask recebe do Step Functions)

Após o `updateMetricsTaskSuccess` usar `resultPath: '$.metrics_result'`, o estado preserva tanto o `protheus_result` quanto o resultado das métricas:

```json
{
  "process_id": "30c52de7977676948cf434a6f053af9c",
  "protheus_result": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "statusCode": 200,
      "process_id": "30c52de7977676948cf434a6f053af9c",
      "status": "COMPLETED",
      "protheus_response": {
        "message": "Documento de entrada criado com sucesso.",
        "idUnico": "123456789",
        "codigoStatus": "200",
        "documento": "46658499",
        "serie": "890"
      }
    },
    "SdkHttpMetadata": {
      "HttpStatusCode": 200
    },
    "StatusCode": 200
  },
  "metrics_result": {
    "statusCode": 200,
    "process_id": "30c52de7977676948cf434a6f053af9c",
    "metrics_updated": true,
    "deduplicated": false,
    "previous_status": null
  }
}
```

## 🔄 PROCESSAMENTO (O que o NotifySuccessTask faz)

O `NotifySuccessTask` recebe:

```typescript
{
  'process_id.$': '$.process_id',           // Extrai: "30c52de7977676948cf434a6f053af9c"
  'protheus_result.$': '$.protheus_result' // Extrai o objeto completo protheus_result
}
```

## ✅ Correção Aplicada

**Antes (com erro):**
```typescript
const updateMetricsTaskSuccess = new tasks.LambdaInvoke(this, 'UpdateMetricsSuccess', {
  lambdaFunction: updateMetricsLambda,
  payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
  outputPath: '$.Payload'  // ❌ Substitui TODO o estado, perdendo protheus_result
});
```

**Depois (corrigido):**
```typescript
const updateMetricsTaskSuccess = new tasks.LambdaInvoke(this, 'UpdateMetricsSuccess', {
  lambdaFunction: updateMetricsLambda,
  payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
  resultPath: '$.metrics_result'  // ✅ Preserva o estado anterior (incluindo protheus_result)
});
```

## 🧪 Como Testar

### 1. Testar no AWS Step Functions Console

1. Vá para o AWS Step Functions Console
2. Selecione sua state machine
3. Vá para a aba "Test" ou "Execution"
4. Use o JSON do INPUT acima como entrada
5. Execute o state `NotifySuccessTask` isoladamente
6. Verifique se consegue acessar `$.protheus_result`

### 2. Testar Localmente (simulação)

```python
import json

# INPUT (estado após updateMetricsTaskSuccess)
input_state = {
    "process_id": "30c52de7977676948cf434a6f053af9c",
    "protheus_result": {
        "ExecutedVersion": "$LATEST",
        "Payload": {
            "statusCode": 200,
            "process_id": "30c52de7977676948cf434a6f053af9c",
            "status": "COMPLETED",
            "protheus_response": {
                "message": "Documento de entrada criado com sucesso.",
                "idUnico": "123456789"
            }
        }
    },
    "metrics_result": {
        "statusCode": 200,
        "metrics_updated": True
    }
}

# Simular o que o NotifySuccessTask recebe
event = {
    "process_id": input_state["process_id"],
    "protheus_result": input_state["protheus_result"]
}

print("Event que será passado para notify_success Lambda:")
print(json.dumps(event, indent=2, ensure_ascii=False))
```

## 📋 Checklist de Validação

- [ ] `process_id` está presente e é um UUID válido
- [ ] `protheus_result` está presente e contém `Payload`
- [ ] `protheus_result.Payload.protheus_response` contém os dados da resposta Protheus
- [ ] O Lambda `notify_success` consegue extrair os dados corretamente

## 🔍 Fluxo Completo

```
sendToProtheusTask
  → resultPath: '$.protheus_result' (preserva protheus_result)
  ↓
prepareMetricsDataSuccess
  → resultPath: '$.metrics_input' (cria metrics_input, preserva protheus_result)
  ↓
updateMetricsTaskSuccess
  → resultPath: '$.metrics_result' (preserva protheus_result, adiciona metrics_result)
  ↓
notifySuccessTask
  → Recebe: process_id e protheus_result ✅
  → Envia feedback para API e SNS
  ↓
successState
```

