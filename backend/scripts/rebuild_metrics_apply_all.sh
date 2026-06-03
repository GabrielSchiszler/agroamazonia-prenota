#!/usr/bin/env bash
# Recalcula e grava METRICS# de TODOS os dias (reclassificação operacional, etc.).
# Não use --date today se o dashboard mostra período ou últimos 7 dias.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=/dev/null
source <(grep '^export ' "$ROOT/backend/.env.prod")
cd "$ROOT/backend/scripts"
echo "Tabela: ${TABLE_NAME:?TABLE_NAME não definido}"
echo "Região: ${AWS_REGION:-sa-east-1}"
python3 rebuild_all_metrics.py --skip-regras --xlsx '' --apply "$@"
