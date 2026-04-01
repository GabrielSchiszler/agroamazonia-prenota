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

_SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_XML_NAME = "23260307467822000126551010000878991216037201.xml"

# WAF / CloudFront costumam bloquear o User-Agent padrão do requests.
_DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

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


def _load_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        line = line.replace("export ", "", 1)
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        out[k] = v
    return out


def _cfg_str(
    arg_val: str | None,
    file_env: dict[str, str],
    env_key: str,
    default: str | None = None,
) -> str | None:
    """Prioridade: CLI > variáveis exportadas no shell > arquivo .env > default."""
    if arg_val is not None and str(arg_val).strip():
        return str(arg_val).strip()
    v = os.environ.get(env_key)
    if v is not None and str(v).strip():
        return v.strip()
    v = file_env.get(env_key)
    if v is not None and str(v).strip():
        return str(v).strip()
    return default


def fetch_oauth2_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> str:
    """Mesmo fluxo que frontend/auth.js (grant_type=client_credentials)."""
    r = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"OAuth2 token HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    token = data.get("access_token") or data.get("accessToken") or data.get("token")
    if not token:
        raise RuntimeError(f"Resposta OAuth2 sem access_token: {data}")
    return str(token)


def _browser_like_headers(
    file_env: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    if getattr(args, "no_browser_ua", False):
        return {}
    ua = _cfg_str(
        getattr(args, "user_agent", None),
        file_env,
        "AGRO_API_USER_AGENT",
        _DEFAULT_BROWSER_UA,
    )
    if not ua:
        return {}
    return {
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }


def build_auth_headers(
    file_env: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    """
    Headers para chamadas à API JSON (Bearer como o front; x-api-key se API_KEY existir,
    pois o config.js do front carrega a mesma chave e o API Gateway pode exigir nos dois).
    """
    api_key = _cfg_str(args.api_key, file_env, "API_KEY")
    base = {"Content-Type": "application/json", **_browser_like_headers(file_env, args)}

    cid = _cfg_str(args.oauth_client_id, file_env, "OAUTH2_FRONTEND_CLIENT_ID")
    csec = _cfg_str(args.oauth_client_secret, file_env, "OAUTH2_FRONTEND_CLIENT_SECRET")
    if cid and csec:
        token_url = _cfg_str(
            args.oauth_token_url,
            file_env,
            "OAUTH2_FRONTEND_TOKEN_URL",
            "https://api-auth-hml.agroamazonia.io/oauth2/token",
        )
        assert token_url
        scope = _cfg_str(
            args.oauth_scope,
            file_env,
            "OAUTH2_FRONTEND_SCOPE",
            "App_Fast/HML",
        )
        assert scope
        token = fetch_oauth2_token(token_url, cid, csec, scope)
        parts = [f"OAuth2 Bearer ({len(token)} chars)"]
        out = {**base, "Authorization": f"Bearer {token}"}
        if api_key:
            out["x-api-key"] = api_key
            parts.append("x-api-key")
        else:
            parts.append("(sem API_KEY — se a API retornar 403, exporte a mesma API_KEY do config.js)")
        print(f"Autenticação: {' + '.join(parts)}")
        return out

    if api_key:
        print("Autenticação: x-api-key")
        return {**base, "x-api-key": api_key}

    raise RuntimeError(
        "Defina OAuth2 (OAUTH2_FRONTEND_CLIENT_ID + OAUTH2_FRONTEND_CLIENT_SECRET) "
        "ou API_KEY / --api-key. Mesmas variáveis do frontend (config.js / .env.homolog)."
    )


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
        default=str(_SCRIPT_DIR / ".env"),
        help=(
            "Arquivo .env opcional; valores exportados no shell têm prioridade sobre este arquivo."
        ),
    )
    args = parser.parse_args()

    env = _load_env_file(Path(args.env_file))
    api_url = _cfg_str(args.api_url, env, "API_URL")
    api_key_only = _cfg_str(args.api_key, env, "API_KEY")

    # Erro comum: passar a URL em --api-key (modo API key)
    if api_key_only and _looks_like_url(api_key_only):
        if not api_url:
            api_url = api_key_only.strip()
            api_key_only = _cfg_str(None, env, "API_KEY")
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
        auth_headers = build_auth_headers(env, args)
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
