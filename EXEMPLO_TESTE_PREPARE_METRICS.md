# Exemplo de Teste - prepareMetricsDataSuccess

## 📥 INPUT (O que o prepareMetricsDataSuccess recebe do Step Functions)

Este é o estado do Step Functions **ANTES** do `prepareMetricsDataSuccess` ser executado:

```json
{
  "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
  "validation_result": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
      "validation_status": "PASSED",
      "failed_rules": []
    },
    "SdkHttpMetadata": { ... },
    "SdkResponseMetadata": { ... },
    "StatusCode": 200
  },
  "protheus_result": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "statusCode": 200,
      "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
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
      "AllHttpHeaders": { ... },
      "HttpHeaders": { ... },
      "HttpStatusCode": 200
    },
    "SdkResponseMetadata": {
      "RequestId": "5f68ac59-8e39-4add-94e3-eb22702c27e9"
    },
    "StatusCode": 200
  }
}
```

## 🔄 PROCESSAMENTO (O que o prepareMetricsDataSuccess faz)

O `prepareMetricsDataSuccess` é um `Pass` state que transforma o input usando JSONPath:

```typescript
{
  'process_id.$': '$.process_id',                    // Extrai: "fce1dc4b87be3e103cb2ece90cbb3509"
  'status': 'SUCCESS',                                // Valor fixo: "SUCCESS"
  'protheus_response.$': '$.protheus_result.Payload.protheus_response',  // Extrai o objeto protheus_response
  'protheus_result.$': '$.protheus_result',          // Mantém o objeto completo protheus_result
  'failure_result': {}                                // Valor fixo: objeto vazio
}
```

## 📤 OUTPUT (O que o prepareMetricsDataSuccess gera e passa para updateMetricsTaskSuccess)

Este é o estado do Step Functions **DEPOIS** do `prepareMetricsDataSuccess`, que será passado para `updateMetricsTaskSuccess`:

```json
{
  "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
  "status": "SUCCESS",
  "protheus_response": {
    "message": "Documento de entrada criado com sucesso.",
    "idUnico": "123456789",
    "codigoStatus": "200",
    "documento": "46658499",
    "serie": "890"
  },
  "protheus_result": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "statusCode": 200,
      "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
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
      "AllHttpHeaders": {
        "X-Amz-Executed-Version": ["$LATEST"],
        "x-amzn-Remapped-Content-Length": ["0"],
        "Connection": ["keep-alive"],
        "x-amzn-RequestId": ["5f68ac59-8e39-4add-94e3-eb22702c27e9"],
        "Content-Length": ["170"],
        "Date": ["Thu, 05 Feb 2026 20:02:22 GMT"],
        "X-Amzn-Trace-Id": ["Root=1-6984f730-32de97b51d2263946f77f1f5;Parent=12b663abd32481f1;Sampled=0;Lineage=2:eb76caae:0"],
        "Content-Type": ["application/json"]
      },
      "HttpHeaders": {
        "Connection": "keep-alive",
        "Content-Length": "170",
        "Content-Type": "application/json",
        "Date": "Thu, 05 Feb 2026 20:02:22 GMT",
        "X-Amz-Executed-Version": "$LATEST",
        "x-amzn-Remapped-Content-Length": "0",
        "x-amzn-RequestId": "5f68ac59-8e39-4add-94e3-eb22702c27e9",
        "X-Amzn-Trace-Id": "Root=1-6984f730-32de97b51d2263946f77f1f5;Parent=12b663abd32481f1;Sampled=0;Lineage=2:eb76caae:0"
      },
      "HttpStatusCode": 200
    },
    "SdkResponseMetadata": {
      "RequestId": "5f68ac59-8e39-4add-94e3-eb22702c27e9"
    },
    "StatusCode": 200
  },
  "failure_result": {}
}
```

## 🧪 Como Testar Manualmente

### 1. Testar o JSONPath no AWS Step Functions Console

1. Vá para o AWS Step Functions Console
2. Selecione sua state machine
3. Vá para a aba "Test" ou "Execution"
4. Use o JSON do INPUT acima como entrada
5. Execute o state `prepareMetricsDataSuccess` isoladamente
6. Verifique se o OUTPUT corresponde ao esperado

### 2. Testar Localmente (simulação)

Você pode simular o comportamento do `Pass` state usando este script Python:

```python
import json

# INPUT (estado antes do prepareMetricsDataSuccess)
input_state = {
    "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
    "protheus_result": {
        "ExecutedVersion": "$LATEST",
        "Payload": {
            "statusCode": 200,
            "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
            "status": "COMPLETED",
            "protheus_response": {
                "message": "Documento de entrada criado com sucesso.",
                "idUnico": "123456789",
                "codigoStatus": "200",
                "documento": "46658499",
                "serie": "890"
            }
        }
    }
}

# Simular o que o prepareMetricsDataSuccess faz
output = {
    "process_id": input_state["process_id"],
    "status": "SUCCESS",
    "protheus_response": input_state["protheus_result"]["Payload"]["protheus_response"],
    "protheus_result": input_state["protheus_result"],
    "failure_result": {}
}

print("OUTPUT gerado:")
print(json.dumps(output, indent=2, ensure_ascii=False))
```

### 3. Testar com AWS CLI

```bash
# Salvar o input em um arquivo
cat > test_input.json << 'EOF'
{
  "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
  "protheus_result": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "statusCode": 200,
      "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
      "status": "COMPLETED",
      "protheus_response": {
        "message": "Documento de entrada criado com sucesso.",
        "idUnico": "123456789"
      }
    }
  }
}
EOF

# Executar a state machine (substitua ARN pela sua)
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:DocumentProcessor" \
  --input file://test_input.json
```

## ⚠️ Observações Importantes

1. **O `protheus_response` pode variar**: Dependendo da resposta da API Protheus, o objeto `protheus_response` pode ter campos diferentes. O importante é que ele sempre estará em `$.protheus_result.Payload.protheus_response`.

2. **Campos opcionais**: Nem sempre o `protheus_response` terá `codigoStatus` ou `idUnico`. O código está preparado para lidar com isso.

3. **Estrutura do Step Functions**: O Step Functions sempre envolve o retorno do Lambda em uma estrutura com `Payload`, `SdkHttpMetadata`, etc. Por isso usamos `$.protheus_result.Payload.protheus_response` para extrair apenas o objeto de resposta.

## 📋 Checklist de Validação

- [ ] `process_id` está presente e é um UUID válido
- [ ] `status` é exatamente `"SUCCESS"`
- [ ] `protheus_response` é um objeto (não null/undefined)
- [ ] `protheus_result` contém toda a estrutura do Lambda (com Payload, SdkHttpMetadata, etc)
- [ ] `failure_result` é um objeto vazio `{}`

