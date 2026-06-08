#!/usr/bin/env python3
"""
Fluxo via HTTP (API Gateway + OAuth / x-api-key). Alternativa sem API:
  test_process_multilot_opteraduo_direct.py (ProcessService + S3 + Step Functions).

Fluxo: novo process_id, upload do XML real da NFe OPTERADUO (3 <det> / 3 lotes),
vínculo dos metadados do pedido (1 linha AACBKV) e, opcionalmente, POST /process/start.

Autenticação (alinhado ao frontend):
  - OAuth2: mesmo POST que auth.js (grant_type=client_credentials, client_id, client_secret,
    scope, Content-Type: application/x-www-form-urlencoded).
  - Header nas chamadas à API: Authorization: Bearer <token> (como getAuthHeaders()).
  - O config.js do front também define API_KEY; muitos API Gateways exigem x-api-key no usage
    plan mesmo com Bearer. Se der 403, exporte API_KEY (mesmo valor do deploy / config.js).
  - User-Agent padrão imita navegador (WAF costuma bloquear "python-requests/..."); use
    --no-browser-ua para desligar ou AGRO_API_USER_AGENT para customizar.

Uso:
  cd backend/scripts
  # Só com export no shell (sem depender do .env do script):
  export API_URL='https://api-hml.agroamazonia.com/fast'
  export OAUTH2_FRONTEND_CLIENT_ID='...'
  export OAUTH2_FRONTEND_CLIENT_SECRET='...'
  export API_KEY='agroamazonia_key_...'   # mesmo do config.js / CDK, se a API exigir
  # opcional: OAUTH2_FRONTEND_TOKEN_URL, OAUTH2_FRONTEND_SCOPE
  python3 test_process_multilot_opteraduo_aws.py --start

  # Com OAuth e arquivo (export continua ganhando do arquivo):
  python3 test_process_multilot_opteraduo_aws.py \\
    --env-file ../.env.homolog \\
    --api-url 'https://api-hml.agroamazonia.com/fast' \\
    --start

  # Só API Key:
  python3 test_process_multilot_opteraduo_aws.py \\
    --api-url 'https://...' --api-key 'agroamazonia_key_...' --start

Variáveis úteis (export no terminal ou .env): API_URL, API_KEY,
  OAUTH2_FRONTEND_TOKEN_URL, OAUTH2_FRONTEND_CLIENT_ID,
  OAUTH2_FRONTEND_CLIENT_SECRET, OAUTH2_FRONTEND_SCOPE

Prioridade: argumentos CLI > variáveis exportadas no shell (os.environ) > arquivo --env-file > default.

Se a API local (FastAPI) não usar o prefixo /api, rode com:
  --api-path-prefix ""

Sobre Protheus: quando a validação grava 3 matches N:1 (mesma linha de pedido, 3 linhas
DANFE com lotes distintos em rastro), o lambda send_to_protheus monta 3 tuplas em
produtos_filtrados e process_produtos_with_lotes gera 3 itens no payload (um lote por linha XML).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import requests

from api_auth import (
    build_auth_headers,
    build_config_env,
    cfg_str as _cfg_str,
    resolve_env_file,
)

_SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_XML_NAME = "23260307467822000126551010000878991216037201.xml"

# Pedido de exemplo (1 linha) alinhado à NFe OPTERADUO multi-lote
PEDIDO_MULTILOT_OPTERADUO = {
    "header": {"tenantId": "00,010159"},
    "requestBody": {
        "moeda": "BRL",
        "itens": [
            {
                "codigoProduto": "I3000001GL00200",
                "produto": "OPTERADUO GL 20 LT",
                "valorUnitario": 2480,
                "codigoOperacao": "1B",
                "tipoDeProduto": {"chave": "ME", "descricao": "MERCADORIA"},
                "pedidoDeCompra": {
                    "pedidoErp": "AACBKV",
                    "itemPedidoErp": "0001",
                },
            }
        ],
        "cnpjEmitente": "07467822001289",
        "cnpjDestinatario": "13563680004603",
    },
}


def _upload_put(presigned_url: str, body: bytes, content_type: str) -> None:
    r = requests.put(
        presigned_url,
        data=body,
        headers={"Content-Type": content_type},
        timeout=120,
    )
    r.raise_for_status()


def _process_base_url(api_url: str, path_prefix: str) -> str:
    """Ex.: api_url=https://host/v1 + path_prefix=api → https://host/v1/api/process"""
    api_url = api_url.rstrip("/")
    p = (path_prefix or "").strip().strip("/")
    if p:
        return f"{api_url}/{p}/process"
    return f"{api_url}/process"


def run_flow(
    api_url: str,
    auth_headers: dict[str, str],
    xml_path: Path,
    start_processing: bool,
    path_prefix: str,
) -> str | None:
    process_id = str(uuid.uuid4())
    base = _process_base_url(api_url, path_prefix)

    print("=" * 80)
    print("Processo multi-lote OPTERADUO → AWS (XML real + pedido 1 linha)")
    print("=" * 80)
    print(f"API base: {base}")
    print(f"process_id: {process_id}")
    print(f"XML: {xml_path} ({xml_path.stat().st_size} bytes)")
    print()

    xml_bytes = xml_path.read_bytes()
    xml_name = xml_path.name

    # 1) Presigned XML
    r = requests.post(
        f"{base}/presigned-url/xml",
        headers=auth_headers,
        json={
            "process_id": process_id,
            "file_name": xml_name,
            "file_type": "application/xml",
        },
        timeout=60,
    )
    if not r.ok:
        print(f"ERRO presigned XML: {r.status_code}\n{r.text}")
        if r.status_code == 403:
            print(
                "Dica 403: exporte API_KEY (mesmo valor do config.js / deploy) junto com OAuth; "
                "muitos gateways exigem x-api-key no usage plan. "
                "Se já usa API_KEY, teste sem --no-browser-ua ou ajuste AGRO_API_USER_AGENT.",
                file=sys.stderr,
            )
        return None
    upload_url = r.json()["upload_url"]
    print("1) Upload XML (DANFE) …")
    _upload_put(upload_url, xml_bytes, "application/xml")
    print("   OK")

    # 2) Metadados pedido (sem PDF — mesmo fluxo que test_create_process.py)
    print("2) POST metadados/pedido …")
    r = requests.post(
        f"{base}/metadados/pedido",
        headers=auth_headers,
        json={"process_id": process_id, "metadados": PEDIDO_MULTILOT_OPTERADUO},
        timeout=60,
    )
    if not r.ok:
        print(f"ERRO metadados: {r.status_code}\n{r.text}")
        return None
    print("   OK", r.json().get("file_name", ""))

    # 3) Verificar
    print("3) GET processo …")
    r = requests.get(f"{base}/{process_id}", headers=auth_headers, timeout=60)
    if r.ok:
        data = r.json()
        print(f"   status={data.get('status')} type={data.get('process_type')}")
        danfe = data.get("files", {}).get("danfe", [])
        add = data.get("files", {}).get("additional", [])
        print(f"   DANFE: {len(danfe)} | adicionais/metadados: {len(add)}")
    else:
        print(f"   aviso: {r.status_code} {r.text[:200]}")

    if start_processing:
        print("4) POST process/start …")
        r = requests.post(
            f"{base}/start",
            headers=auth_headers,
            json={"process_id": process_id},
            timeout=60,
        )
        if r.ok:
            sd = r.json()
            print("   OK")
            print(f"   execution_arn: {sd.get('execution_arn')}")
            print(f"   status: {sd.get('status')}")
        else:
            print(f"ERRO start: {r.status_code}\n{r.text}")
            return process_id

    print()
    print("=" * 80)
    print("Resumo")
    print("=" * 80)
    print(f"process_id: {process_id}")
    print(f"Console: Step Functions / CloudWatch do send_to_protheus com este process_id.")
    print(f"GET: {base}/{process_id}")
    if not start_processing:
        print(f"Start manual: POST {base}/start  body: {json.dumps({'process_id': process_id})}")
    return process_id


def _looks_like_url(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    v = value.strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cria processo na AWS com NFe OPTERADUO (3 lotes) + pedido 1 linha AACBKV",
        epilog=(
            "Exemplo homolog (OAuth2 como no front, vars em ../.env.homolog):\n"
            "  python3 test_process_multilot_opteraduo_aws.py "
            "--env-file ../.env.homolog "
            "--api-url 'https://api-hml.agroamazonia.com/fast' --start\n\n"
            "Exemplo com x-api-key:\n"
            "  python3 test_process_multilot_opteraduo_aws.py "
            "--api-url 'https://api-hml.agroamazonia.com/fast' "
            "--api-key 'agroamazonia_key_...' --start"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--api-url",
        help="Base URL da API (ex: https://xxx.execute-api.us-east-1.amazonaws.com/v1)",
    )
    parser.add_argument("--api-key", help="Header x-api-key (se não usar OAuth2)")
    parser.add_argument(
        "--oauth-token-url",
        help="URL do token OAuth2 (default: OAUTH2_FRONTEND_TOKEN_URL ou auth hml)",
    )
    parser.add_argument("--oauth-client-id", help="OAUTH2_FRONTEND_CLIENT_ID")
    parser.add_argument("--oauth-client-secret", help="OAUTH2_FRONTEND_CLIENT_SECRET")
    parser.add_argument("--oauth-scope", help="OAUTH2_FRONTEND_SCOPE")
    parser.add_argument(
        "--user-agent",
        help="User-Agent nas requisições à API (default: navegador; env: AGRO_API_USER_AGENT)",
    )
    parser.add_argument(
        "--no-browser-ua",
        action="store_true",
        help="Não enviar User-Agent estilo navegador",
    )
    parser.add_argument(
        "--xml-file",
        help=f"Caminho do XML (padrão: {_SCRIPT_DIR / DEFAULT_XML_NAME})",
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Chamar POST /process/start após uploads",
    )
    parser.add_argument(
        "--api-path-prefix",
        default=os.environ.get("PROCESS_API_PATH_PREFIX", "api"),
        help=(
            "Segmento antes de /process na URL (padrão: api, como no API Gateway). "
            "Use \"\" para API local FastAPI em http://host:port sem /api."
        ),
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Arquivo .env (com --dev usa backend/.env.development com prioridade sobre o shell)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Carrega backend/.env.development (OAuth dev) com prioridade sobre variáveis do shell",
    )
    args = parser.parse_args()

    env_path, prefer_file = resolve_env_file(args.env_file, dev=args.dev, homolog=False)
    if env_path and env_path.is_file():
        print(f"Carregado: {env_path.resolve()}")
    config_env = build_config_env(env_path, prefer_file=prefer_file)
    api_url = _cfg_str(args.api_url, config_env, "API_URL")
    api_key_only = _cfg_str(args.api_key, config_env, "API_KEY")

    # Erro comum: passar a URL em --api-key (modo API key)
    if api_key_only and _looks_like_url(api_key_only):
        if not api_url:
            api_url = api_key_only.strip()
            api_key_only = _cfg_str(None, config_env, "API_KEY")
            print(
                "Aviso: a URL foi informada em --api-key; usando como API_URL. "
                "Para OAuth2 use --env-file com OAUTH2_*; para API key use --api-key com o valor x-api-key.",
                file=sys.stderr,
            )
        else:
            print(
                "ERRO: --api-key parece ser uma URL. "
                "Use --api-url para a base. Com OAuth2, não precisa de --api-key.",
                file=sys.stderr,
            )
            return 1

    if not api_url:
        print(
            "Defina --api-url ou API_URL (ex.: https://api-hml.agroamazonia.com/fast).",
            file=sys.stderr,
        )
        return 1

    try:
        auth_headers = build_auth_headers(config_env, args, env_file=env_path)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    xml_path = Path(args.xml_file).resolve() if args.xml_file else _SCRIPT_DIR / DEFAULT_XML_NAME
    if not xml_path.is_file():
        print(f"XML não encontrado: {xml_path}", file=sys.stderr)
        return 1

    try:
        pid = run_flow(api_url, auth_headers, xml_path, args.start, args.api_path_prefix)
    except RuntimeError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 1
    return 0 if pid else 1


if __name__ == "__main__":
    raise SystemExit(main())
