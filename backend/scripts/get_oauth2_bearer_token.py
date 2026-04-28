#!/usr/bin/env python3
"""
Obtém access_token OAuth2 (client_credentials) para usar como Bearer nas chamadas à API.

Ordem de precedência (maior → menor):
  1. Variáveis já exportadas no shell ao rodar o script
  2. Arquivo .env.development (apenas chaves ainda não definidas)

Variáveis usadas:
  OAUTH2_FRONTEND_TOKEN_URL
  OAUTH2_FRONTEND_CLIENT_ID
  OAUTH2_FRONTEND_CLIENT_SECRET
  OAUTH2_FRONTEND_SCOPE

Exemplos:
  cd backend && python3 scripts/get_oauth2_bearer_token.py
  OAUTH2_FRONTEND_SCOPE='outro/scope' python3 scripts/get_oauth2_bearer_token.py
  python3 scripts/get_oauth2_bearer_token.py --env-file /caminho/.env.development
  python3 scripts/get_oauth2_bearer_token.py --json   # só imprime JSON da resposta do IdP
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests


def _backend_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_env_file(path: Path, override: bool = False) -> None:
    """Parse estilo export KEY=value / KEY=value; não sobrescreve os.environ se override=False."""
    if not path.is_file():
        return
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        print(f"[aviso] Não foi possível ler {path}: {e}", file=sys.stderr)
        return

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, rest = line.partition("=")
        key = key.strip()
        if not key:
            continue
        val = rest.strip()
        if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        if not override and key in os.environ:
            continue
        os.environ[key] = val


def _get_required(name: str) -> str:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        print(
            f"Erro: defina {name} (export no terminal ou em .env.development).",
            file=sys.stderr,
        )
        sys.exit(2)
    return str(v).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera Bearer token (OAuth2 client_credentials)")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Arquivo .env (default: backend/.env.development)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime só o JSON completo retornado pelo servidor de token",
    )
    args = parser.parse_args()

    env_path = args.env_file
    if env_path is None:
        env_path = _backend_root() / ".env.development"

    _load_env_file(env_path, override=False)

    token_url = _get_required("OAUTH2_FRONTEND_TOKEN_URL")
    client_id = _get_required("OAUTH2_FRONTEND_CLIENT_ID")
    client_secret = _get_required("OAUTH2_FRONTEND_CLIENT_SECRET")
    scope = _get_required("OAUTH2_FRONTEND_SCOPE")

    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }

    try:
        resp = requests.post(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
    except requests.RequestException as e:
        print(f"Erro na requisição ao token URL: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print(resp.text, file=sys.stderr)
        print(f"Resposta não é JSON (HTTP {resp.status_code})", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(data, indent=2, ensure_ascii=False))
        if resp.status_code >= 400:
            sys.exit(1)
        return

    if resp.status_code >= 400:
        print(json.dumps(data, indent=2, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)

    token = data.get("access_token") or data.get("accessToken") or data.get("token")
    if not token:
        print(json.dumps(data, indent=2, ensure_ascii=False), file=sys.stderr)
        print("Resposta sem access_token.", file=sys.stderr)
        sys.exit(1)

    tt = data.get("token_type") or data.get("tokenType") or "Bearer"
    exp = data.get("expires_in") or data.get("expiresIn") or "?"

    print("# Token obtido com sucesso")
    print(f"# token_type: {tt} | expires_in: {exp}")
    print(f"# arquivo .env (preenche só o que não estiver exportado): {env_path}")
    print()
    print(token)
    print()
    print('# Exemplo: curl -H "Authorization: Bearer <token_acima>" "$API_URL/process/..."')


if __name__ == "__main__":
    main()
