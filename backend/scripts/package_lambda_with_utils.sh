#!/usr/bin/env bash
# Empacota uma Lambda (backend/lambdas/<nome>) com a pasta utils/ no zip.
set -euo pipefail

LAMBDA_NAME="${1:?usage: package_lambda_with_utils.sh <lambda_dir_name> [output.zip]}"
OUT_ZIP="${2:-/tmp/lambda-${LAMBDA_NAME}.zip}"
ROOT="$(cd "$(dirname "$0")/../lambdas" && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

if [[ ! -d "$ROOT/$LAMBDA_NAME" ]]; then
  echo "Diretório não encontrado: $ROOT/$LAMBDA_NAME" >&2
  exit 1
fi

if [[ -f "$ROOT/$LAMBDA_NAME/requirements.txt" ]]; then
  pip install -q -r "$ROOT/$LAMBDA_NAME/requirements.txt" -t "$WORKDIR"
fi

cp -a "$ROOT/$LAMBDA_NAME/." "$WORKDIR/"
cp -a "$ROOT/utils" "$WORKDIR/utils"

(
  cd "$WORKDIR"
  zip -qr "$OUT_ZIP" . -x '__pycache__/*' '*.pyc'
)

echo "$OUT_ZIP"
