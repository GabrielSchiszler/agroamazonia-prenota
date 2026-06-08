#!/usr/bin/env bash
# Sincroniza regras de validação (RULES#*) e CFOP (CFOP_OPERATION) de stg → destino.
#
# Uso:
#   cd backend/scripts
#   source ../.env.development   # ou exportar credenciais/região manualmente
#   ./sync_rules_from_stg.sh
#   ./sync_rules_from_stg.sh tabela-document-processor-dev   # destino explícito
#
# Origem fixa: tabela-document-processor-stg (homolog/stg com regras completas).

set -euo pipefail

SOURCE_TABLE="${SOURCE_TABLE:-tabela-document-processor-stg}"
TARGET_TABLE="${1:-${TABLE_NAME:-tabela-document-processor-dev}}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-sa-east-1}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Origem:  ${SOURCE_TABLE} (${REGION})"
echo "Destino: ${TARGET_TABLE} (${REGION})"
echo ""

python3 "${SCRIPT_DIR}/copy_rules.py" \
  --source-table "${SOURCE_TABLE}" \
  --target-table "${TARGET_TABLE}" \
  --source-region "${REGION}" \
  --target-region "${REGION}" \
  --yes

echo ""
python3 "${SCRIPT_DIR}/copy_cfop_rules.py" \
  --source-table "${SOURCE_TABLE}" \
  --target-table "${TARGET_TABLE}" \
  --region "${REGION}" \
  --yes

echo ""
echo "Verificando regras no destino..."
python3 - <<PY
import os, boto3
from boto3.dynamodb.conditions import Key

region = "${REGION}"
target = "${TARGET_TABLE}"
ddb = boto3.resource("dynamodb", region_name=region)
t = ddb.Table(target)
for pt in ("AGROQUIMICOS", "BARTER"):
    n = len(t.query(
        KeyConditionExpression=Key("PK").eq(f"RULES#{pt}") & Key("SK").begins_with("RULE#")
    ).get("Items", []))
    print(f"  RULES#{pt}: {n} regra(s)")
cfop = t.query(KeyConditionExpression=Key("PK").eq("CFOP_OPERATION")).get("Items", [])
print(f"  CFOP_OPERATION: {len(cfop)} item(ns)")
PY
