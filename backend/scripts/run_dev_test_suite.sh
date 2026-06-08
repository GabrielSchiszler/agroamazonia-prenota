#!/usr/bin/env bash
# Suíte E2E no ambiente DEV (api-dev.agroamazonia.com).
#
# Pré-requisitos:
#   - Deploy recente de lambdas + frontend no dev
#   - backend/.env.development com OAuth2 dev
#   - pip install requests
#   - Regras no DynamoDB dev (rode ./sync_rules_from_stg.sh se vazio)
#
# Uso:
#   cd backend/scripts
#   ./run_dev_test_suite.sh              # todos os cenários
#   ./run_dev_test_suite.sh --quick      # só unitários + 1 XML
#   ./run_dev_test_suite.sh --only 53,xml-agro,multilot
#   ./run_dev_test_suite.sh --no-start   # só upload, sem Step Functions
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BACKEND_DIR}/.env.development"
API_URL="${API_URL:-https://api-dev.agroamazonia.com/fast/v1}"
LOG_DIR="${SCRIPT_DIR}/out_dev_tests"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/run_${TS}.log"
RESULTS_FILE="${LOG_DIR}/run_${TS}_results.txt"

QUICK=0
NO_START=0
ONLY=""
SKIP_RULES_SYNC=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) QUICK=1; shift ;;
    --no-start) NO_START=1; shift ;;
    --only) ONLY="$2"; shift 2 ;;
    --skip-rules-sync) SKIP_RULES_SYNC=1; shift ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    *) echo "Opção desconhecida: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "=== DEV test suite ${TS} ==="
echo "LOG: ${LOG_FILE}"

# Carrega só linhas KEY=VAL (evita executar comandos soltos no .env, ex. cdk deploy)
load_env_file() {
  local f="$1"
  [[ -f "${f}" ]] || return 1
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    line="${line#export }"
    [[ "${line}" == *"="* ]] || continue
    local key="${line%%=*}"
    local val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    val="${val%\"}"; val="${val#\"}"
    val="${val%\'}"; val="${val#\'}"
    export "${key}=${val}"
  done < "${f}"
}

if load_env_file "${ENV_FILE}"; then
  echo "Env: ${ENV_FILE}"
else
  echo "AVISO: ${ENV_FILE} não encontrado" >&2
fi

export API_URL
COMMON_UPLOAD=(
  python3 "${SCRIPT_DIR}/upload_arquivos_cenarios.py"
  --dev
  --api-url "${API_URL}"
)
COMMON_CREATE=(
  python3 "${SCRIPT_DIR}/test_create_process.py"
  --dev
  --api-url "${API_URL}"
)
# upload_arquivos_cenarios: inicia por padrão; use --no-start para só criar/upload
UPLOAD_START_FLAG=()
[[ "${NO_START}" -eq 1 ]] && UPLOAD_START_FLAG=(--no-start)
# test_create_process / barter / multilot: só iniciam com --start
CREATE_START_FLAG=()
[[ "${NO_START}" -eq 0 ]] && CREATE_START_FLAG=(--start)

should_run() {
  local id="$1"
  if [[ "${QUICK}" -eq 1 && -z "${ONLY}" ]]; then
    case "${id}" in
      unit-*|xml-agro) return 0 ;;
      *) return 1 ;;
    esac
  fi
  [[ -z "${ONLY}" ]] && return 0
  local IFS=,
  local part
  for part in ${ONLY}; do
    [[ "${part}" == "${id}" ]] && return 0
  done
  return 1
}

run_case() {
  local id="$1"
  shift
  if ! should_run "${id}"; then
    echo "[SKIP] ${id}"
    return 0
  fi
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "[RUN] ${id}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if "$@"; then
    echo "[OK] ${id}"
    echo "${id}: ok" >> "${RESULTS_FILE}"
    return 0
  else
    echo "[FAIL] ${id} (exit $?)" >&2
    echo "${id}: fail" >> "${RESULTS_FILE}"
    return 1
  fi
}

FAILURES=0
: > "${RESULTS_FILE}"

# ── 0. Pré-flight ─────────────────────────────────────────────
if [[ "${SKIP_RULES_SYNC}" -eq 0 ]]; then
  run_case "rules-sync" "${SCRIPT_DIR}/sync_rules_from_stg.sh" || FAILURES=$((FAILURES + 1))
fi

run_case "unit-validar-produtos" \
  bash -c "cd '${SCRIPT_DIR}/../lambdas/validate_rules' && python3 test_validar_produtos_multilot.py -q" \
  || FAILURES=$((FAILURES + 1))

run_case "unit-pytest-backend" \
  bash -c "cd '${BACKEND_DIR}' && python3 -m pytest \
    tests/test_protheus_regras_metrics.py \
    tests/test_regras_labels.py \
    tests/test_metrics_rates_and_process.py \
    tests/test_update_metrics_dedup.py \
    -q --tb=no" \
  || FAILURES=$((FAILURES + 1))

# ── 1. Cenários PDF Bizify (antigos — só PDF, sem XML) ───────
for SC in 53 61 71 78; do
  run_case "bizify-${SC}" \
    "${COMMON_UPLOAD[@]}" --scenario "${SC}" --pedido-json exemplo "${UPLOAD_START_FLAG[@]}" \
    || FAILURES=$((FAILURES + 1))
done

# ── 2. Uso e consumo (PDF + usoEConsumo) ──────────────────────
run_case "uso-consumo-53" \
  "${COMMON_UPLOAD[@]}" --scenario 53 --pedido-json exemplo --uso-e-consumo "${UPLOAD_START_FLAG[@]}" \
  || FAILURES=$((FAILURES + 1))

# ── 3. AGROQUIMICOS — XML padrão + pedido exemplo ────────────
run_case "xml-agro" \
  "${COMMON_CREATE[@]}" --xml-file "${SCRIPT_DIR}/test_nfe.xml" "${CREATE_START_FLAG[@]}" \
  || FAILURES=$((FAILURES + 1))

# ── 4. AGROQUIMICOS — codProdFornecedor (novo) ───────────────
# Coloque o XML da NF 15345 em arquivos/15345/ para E2E completo; senão usa test_nfe.xml
if [[ -d "${SCRIPT_DIR}/arquivos/15345" ]] && ls "${SCRIPT_DIR}/arquivos/15345"/*.xml &>/dev/null; then
  run_case "cod-fornecedor-15345" \
    "${COMMON_UPLOAD[@]}" --scenario 15345 \
    --pedido-json "${SCRIPT_DIR}/pedidos_teste/agroquimicos_cod_fornecedor.json" \
    "${UPLOAD_START_FLAG[@]}" \
    || FAILURES=$((FAILURES + 1))
else
  echo "[INFO] arquivos/15345/*.xml ausente — pulando E2E umbicura (adicione o XML da NF 15345)"
fi

# ── 5. BARTER ────────────────────────────────────────────────
if [[ -f "${SCRIPT_DIR}/test_nfe_barter_31decf12_1780506019.xml" ]]; then
  run_case "barter-xml" \
    python3 "${SCRIPT_DIR}/test_create_process_barter.py" \
      --dev \
      --api-url "${API_URL}" \
      --xml-file "${SCRIPT_DIR}/test_nfe_barter_31decf12_1780506019.xml" \
      "${CREATE_START_FLAG[@]}" \
    || FAILURES=$((FAILURES + 1))
fi

# ── 6. Multilot OPTERADUO (3 lotes XML + 1 linha pedido) ─────
run_case "multilot-opteraduo" \
  python3 "${SCRIPT_DIR}/test_process_multilot_opteraduo_aws.py" \
    --dev \
    --api-url "${API_URL}" \
    --api-path-prefix "" \
    "${CREATE_START_FLAG[@]}" \
  || FAILURES=$((FAILURES + 1))

# ── Resumo ───────────────────────────────────────────────────
echo ""
echo "=== Fim da suíte ==="
echo "Falhas: ${FAILURES}"
echo "Resultados: ${RESULTS_FILE}"
echo "Log completo: ${LOG_FILE}"
echo ""
echo "Para acompanhar um processo:"
echo "  python3 check_process_dev.py --dev <process_id> --wait 300"

exit "${FAILURES}"
