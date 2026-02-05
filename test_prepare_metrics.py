#!/usr/bin/env python3
"""
Script para testar manualmente o comportamento do prepareMetricsDataSuccess
Simula o que o Step Functions Pass state faria com os JSONPaths
"""

import json

def simulate_prepare_metrics_data_success(input_state):
    """
    Simula o comportamento do prepareMetricsDataSuccess Pass state
    
    Args:
        input_state: Estado do Step Functions antes do prepareMetricsDataSuccess
        
    Returns:
        Estado transformado que será passado para updateMetricsTaskSuccess
    """
    
    # Extrair process_id
    process_id = input_state.get("process_id")
    
    # Extrair protheus_response do caminho: $.protheus_result.Payload.protheus_response
    protheus_result = input_state.get("protheus_result", {})
    protheus_payload = protheus_result.get("Payload", {})
    protheus_response = protheus_payload.get("protheus_response", {})
    
    # Construir output conforme a definição do Pass state
    output = {
        "process_id": process_id,
        "status": "SUCCESS",  # Valor fixo
        "protheus_response": protheus_response,  # Extraído via JSONPath
        "protheus_result": protheus_result,  # Objeto completo mantido
        "failure_result": {}  # Valor fixo (objeto vazio)
    }
    
    return output


# Exemplo de uso
if __name__ == "__main__":
    # Exemplo 1: Caso de sucesso completo
    print("=" * 80)
    print("TESTE 1: Caso de sucesso completo")
    print("=" * 80)
    
    input_example_1 = {
        "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
        "validation_result": {
            "ExecutedVersion": "$LATEST",
            "Payload": {
                "process_id": "fce1dc4b87be3e103cb2ece90cbb3509",
                "validation_status": "PASSED",
                "failed_rules": []
            },
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
                "HttpStatusCode": 200
            },
            "StatusCode": 200
        }
    }
    
    output_1 = simulate_prepare_metrics_data_success(input_example_1)
    print("\n📥 INPUT:")
    print(json.dumps(input_example_1, indent=2, ensure_ascii=False))
    print("\n📤 OUTPUT:")
    print(json.dumps(output_1, indent=2, ensure_ascii=False))
    
    # Exemplo 2: Caso sem codigoStatus (campo opcional)
    print("\n" + "=" * 80)
    print("TESTE 2: Caso sem codigoStatus (campo opcional)")
    print("=" * 80)
    
    input_example_2 = {
        "process_id": "abc123-def456-ghi789",
        "protheus_result": {
            "ExecutedVersion": "$LATEST",
            "Payload": {
                "statusCode": 200,
                "process_id": "abc123-def456-ghi789",
                "status": "COMPLETED",
                "protheus_response": {
                    "message": "Documento processado com sucesso.",
                    "idUnico": "987654321"
                    # Sem codigoStatus
                }
            },
            "StatusCode": 200
        }
    }
    
    output_2 = simulate_prepare_metrics_data_success(input_example_2)
    print("\n📥 INPUT:")
    print(json.dumps(input_example_2, indent=2, ensure_ascii=False))
    print("\n📤 OUTPUT:")
    print(json.dumps(output_2, indent=2, ensure_ascii=False))
    
    # Exemplo 3: Caso com protheus_response vazio (edge case)
    print("\n" + "=" * 80)
    print("TESTE 3: Caso com protheus_response vazio (edge case)")
    print("=" * 80)
    
    input_example_3 = {
        "process_id": "xyz789-abc123",
        "protheus_result": {
            "ExecutedVersion": "$LATEST",
            "Payload": {
                "statusCode": 200,
                "process_id": "xyz789-abc123",
                "status": "COMPLETED",
                "protheus_response": {}
            },
            "StatusCode": 200
        }
    }
    
    output_3 = simulate_prepare_metrics_data_success(input_example_3)
    print("\n📥 INPUT:")
    print(json.dumps(input_example_3, indent=2, ensure_ascii=False))
    print("\n📤 OUTPUT:")
    print(json.dumps(output_3, indent=2, ensure_ascii=False))
    
    print("\n" + "=" * 80)
    print("✅ Todos os testes concluídos!")
    print("=" * 80)
    
    # Validar que todos os outputs têm os campos obrigatórios
    required_fields = ["process_id", "status", "protheus_response", "protheus_result", "failure_result"]
    for i, output in enumerate([output_1, output_2, output_3], 1):
        missing = [field for field in required_fields if field not in output]
        if missing:
            print(f"❌ TESTE {i}: Campos faltando: {missing}")
        else:
            print(f"✅ TESTE {i}: Todos os campos obrigatórios presentes")
        
        if output["status"] != "SUCCESS":
            print(f"❌ TESTE {i}: status deve ser 'SUCCESS', mas é '{output['status']}'")
        else:
            print(f"✅ TESTE {i}: status correto")

