#!/bin/bash
# Inicia o frontend local apontando para dev, hml ou prd.
#
# Uso:
#   ./start.sh prd    # produção
#   ./start.sh hml    # homologação (stg)
#   ./start.sh dev    # desenvolvimento (padrão)
#
# OAuth: lê OAUTH2_* do backend/.env.{ambiente} (não commitar credenciais).

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/../backend" && pwd)"
TARGET="${1:-dev}"

case "$TARGET" in
  prd|prod|production)
    TARGET="prd"
    API_URL="https://api-prd.agroamazonia.com/fast"
    BACKEND_ENV_FILE="$BACKEND_DIR/.env.prod"
    ;;
  hml|homolog|stg|staging)
    TARGET="hml"
    API_URL="https://api-hml.agroamazonia.com/fast"
    BACKEND_ENV_FILE="$BACKEND_DIR/.env.homolog"
    ;;
  dev|development|local)
    TARGET="dev"
    API_URL="https://api-dev.agroamazonia.com/fast"
    BACKEND_ENV_FILE="$BACKEND_DIR/.env.development"
    ;;
  *)
    echo -e "${RED}Ambiente inválido: $TARGET${NC}"
    echo "Use: ./start.sh [prd|hml|dev]"
    exit 1
    ;;
esac

read_backend_var() {
  local key="$1"
  local file="$2"
  if [[ ! -f "$file" ]]; then
    return 1
  fi
  grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" | tail -1 | sed -E "s/^[[:space:]]*(export[[:space:]]+)?${key}=//" | sed -E "s/^['\"]//;s/['\"]$//"
}

if [[ ! -f "$BACKEND_ENV_FILE" ]]; then
  echo -e "${RED}Arquivo de ambiente não encontrado:${NC} $BACKEND_ENV_FILE"
  exit 1
fi

OAUTH2_FRONTEND_TOKEN_URL="$(read_backend_var OAUTH2_FRONTEND_TOKEN_URL "$BACKEND_ENV_FILE" || true)"
OAUTH2_FRONTEND_CLIENT_ID="$(read_backend_var OAUTH2_FRONTEND_CLIENT_ID "$BACKEND_ENV_FILE" || true)"
OAUTH2_FRONTEND_CLIENT_SECRET="$(read_backend_var OAUTH2_FRONTEND_CLIENT_SECRET "$BACKEND_ENV_FILE" || true)"
OAUTH2_FRONTEND_SCOPE="$(read_backend_var OAUTH2_FRONTEND_SCOPE "$BACKEND_ENV_FILE" || true)"

if [[ -z "$OAUTH2_FRONTEND_TOKEN_URL" || -z "$OAUTH2_FRONTEND_CLIENT_ID" || -z "$OAUTH2_FRONTEND_CLIENT_SECRET" ]]; then
  echo -e "${RED}OAuth2 incompleto em $BACKEND_ENV_FILE${NC}"
  echo "Defina: OAUTH2_FRONTEND_TOKEN_URL, OAUTH2_FRONTEND_CLIENT_ID, OAUTH2_FRONTEND_CLIENT_SECRET"
  exit 1
fi

cat > "$SCRIPT_DIR/.env" <<EOF
# Gerado por start.sh ($TARGET) — não commitar
ENV_NAME=$TARGET
API_URL=$API_URL
API_KEY=dev
OAUTH2_FRONTEND_TOKEN_URL=$OAUTH2_FRONTEND_TOKEN_URL
OAUTH2_FRONTEND_CLIENT_ID=$OAUTH2_FRONTEND_CLIENT_ID
OAUTH2_FRONTEND_CLIENT_SECRET=$OAUTH2_FRONTEND_CLIENT_SECRET
OAUTH2_FRONTEND_SCOPE=$OAUTH2_FRONTEND_SCOPE
EOF

echo -e "${GREEN}Frontend → ${TARGET^^}${NC}"
echo -e "  API_URL: ${API_URL}"
echo -e "  OAuth:   ${OAUTH2_FRONTEND_TOKEN_URL}"
echo -e "  Scope:   ${OAUTH2_FRONTEND_SCOPE}"
echo -e "${YELLOW}  Acesse: http://localhost:8080${NC}"
echo ""

cd "$SCRIPT_DIR"
exec python3 server.py
