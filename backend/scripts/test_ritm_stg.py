#!/usr/bin/env python3
"""
Teste rápido STG/HML: envia metadados com requestBody.ritm e, por padrão, inicia o Step Functions (POST /process/start).

URL (igual ao frontend app.js: `${API_URL}/process/...`):
  - API_URL padrão: https://api-hml.agroamazonia.com/fast/v1 → rotas em .../fast/v1/process/...
  - O segmento extra `api` (…/fast/api/process) é usado em alguns gateways; aqui o HML costuma ser /v1.
  - Ajuste com --api-url, --api-path-prefix ou PROCESS_API_PATH_PREFIX (env).

Auth: OAuth2 + x-api-key opcional; User-Agent estilo navegador (WAF). Ver test_process_multilot_opteraduo_aws.py.

Validação do ritm no Protheus: CloudWatch do lambda send_to_protheus — payload com "ritm".
Validação no feedback: send_feedback / notify_success incluem details.ritm quando presente.

Uso:
  cd backend
  # Por padrão dispara Step Functions (POST /process/start) após upload + metadados:
  python3 scripts/test_ritm_stg.py --env-file .env.homolog --ritm "MEU-RITM-123"

  # Só upload + metadados, sem iniciar o fluxo:
  python3 scripts/test_ritm_stg.py --env-file .env.homolog --ritm "X" --no-start

  # Se 403 persistir, exporte a mesma API_KEY do config.js / deploy:
  export API_KEY='agroamazonia_key_...'

Requisitos: requests; XML em scripts/test_nfe.xml (ou --xml-file).
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
_BACKEND_DIR = _SCRIPT_DIR.parent

# WAF / CloudFront costumam bloquear o User-Agent padrão do requests.
_DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Mesmo padrão do front: window.ENV.API_URL termina em /v1; rotas são {API_URL}/process/...
DEFAULT_API_URL = "https://api-hml.agroamazonia.com/fast/v1"


def _load_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("cdk ") or line.startswith("cd "):
            continue
        line = line.replace("export ", "", 1)
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k:
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


def _process_base_url(api_url: str, path_prefix: str) -> str:
    """Ex.: api_url=.../fast/v1, path_prefix=\"\" → .../fast/v1/process.
    Com api_url=.../fast e path_prefix=api → .../fast/api/process (outro gateway)."""
    api_url = api_url.rstrip("/")
    p = (path_prefix or "").strip().strip("/")
    if p:
        return f"{api_url}/{p}/process"
    return f"{api_url}/process"


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


def fetch_oauth2_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> str:
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


def build_auth_headers(file_env: dict[str, str], args: argparse.Namespace) -> dict[str, str]:
    """
    Bearer (manual ou OAuth2) + x-api-key opcional + headers de navegador, como no multilot/front.
    """
    api_key = _cfg_str(args.api_key, file_env, "API_KEY")
    base = {"Content-Type": "application/json", **_browser_like_headers(file_env, args)}

    token_manual = _cfg_str(args.token, file_env, "AUTH_TOKEN") or _cfg_str(
        None, file_env, "BEARER_TOKEN"
    )
    if token_manual:
        out = {**base, "Authorization": f"Bearer {token_manual}"}
        if api_key:
            out["x-api-key"] = api_key
            print("Autenticação: Bearer (manual) + x-api-key")
        else:
            print(
                "Autenticação: Bearer (manual) "
                "(sem API_KEY — se a API retornar 403, exporte a mesma API_KEY do config.js)"
            )
        return out

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
        print("Obtendo Bearer via OAuth2 (client_credentials)...")
        token = fetch_oauth2_token(token_url, cid, csec, scope)
        parts = [f"OAuth2 Bearer ({len(token)} chars)"]
        out = {**base, "Authorization": f"Bearer {token}"}
        if api_key:
            out["x-api-key"] = api_key
            parts.append("x-api-key")
        else:
            parts.append(
                "(sem API_KEY — se a API retornar 403, exporte a mesma API_KEY do config.js)"
            )
        print(f"Autenticação: {' + '.join(parts)}")
        return out

    if api_key:
        print("Autenticação: x-api-key")
        return {**base, "x-api-key": api_key}

    raise RuntimeError(
        "Defina OAuth2 (OAUTH2_FRONTEND_CLIENT_ID + OAUTH2_FRONTEND_CLIENT_SECRET), "
        "AUTH_TOKEN/BEARER_TOKEN, ou API_KEY / --api-key. Mesmas variáveis do frontend (config.js / .env.homolog)."
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Teste requestBody.ritm em STG/HML (auth/URL como test_process_multilot_opteraduo_aws)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--api-url",
        help=f"Base da API sem /process (default: {DEFAULT_API_URL} ou API_URL)",
    )
    parser.add_argument("--token", help="Bearer manual (sobrescreve OAuth2; env: AUTH_TOKEN)")
    parser.add_argument("--api-key", help="Header x-api-key (env: API_KEY)")
    parser.add_argument("--oauth-token-url", help="OAUTH2_FRONTEND_TOKEN_URL")
    parser.add_argument("--oauth-client-id", help="OAUTH2_FRONTEND_CLIENT_ID")
    parser.add_argument("--oauth-client-secret", help="OAUTH2_FRONTEND_CLIENT_SECRET")
    parser.add_argument("--oauth-scope", help="OAUTH2_FRONTEND_SCOPE")
    parser.add_argument(
        "--user-agent",
        help="User-Agent nas requisições (env: AGRO_API_USER_AGENT)",
    )
    parser.add_argument(
        "--no-browser-ua",
        action="store_true",
        help="Não enviar User-Agent estilo navegador",
    )
    parser.add_argument("--xml-file", default=None, help="XML DANFE (default: scripts/test_nfe.xml)")
    parser.add_argument("--ritm", default="TEST-RITM-STG-001", help="Valor do campo requestBody.ritm")
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Não chamar POST /process/start (não inicia Step Functions)",
    )
    parser.add_argument(
        "--api-path-prefix",
        default=os.environ.get("PROCESS_API_PATH_PREFIX", ""),
        help=(
            'Segmento extra antes de /process (padrão vazio = mesmo que o front com API_URL …/v1). '
            'Use "api" para …/fast/api/process (ex.: test_process_multilot_opteraduo_aws).'
        ),
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Arquivo .env (ex.: .env.homolog no diretório backend)",
    )
    args = parser.parse_args()

    env_file: Path | None = None
    if args.env_file:
        env_file = Path(args.env_file).expanduser()
    else:
        for p in (Path(".env.homolog"), _BACKEND_DIR / ".env.homolog"):
            if p.is_file():
                env_file = p.resolve()
                break

    file_env: dict[str, str] = _load_env_file(env_file) if env_file else {}
    if env_file and env_file.is_file():
        print(f"Carregado: {env_file.resolve()}")

    api_url = _cfg_str(args.api_url, file_env, "API_URL", DEFAULT_API_URL)
    assert api_url
    path_prefix = (args.api_path_prefix or "").strip()

    try:
        headers = build_auth_headers(file_env, args)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    base = _process_base_url(api_url, path_prefix)

    xml_path = Path(args.xml_file).resolve() if args.xml_file else _SCRIPT_DIR / "test_nfe.xml"
    if not xml_path.is_file():
        print(f"XML não encontrado: {xml_path}", file=sys.stderr)
        return 1

    with open(xml_path, "rb") as f:
        xml_content = f.read()
    xml_name = xml_path.name

    process_id = str(uuid.uuid4())

    print(f"process_id: {process_id}")
    print(f"API base (process): {base}")
    print(f"ritm: {args.ritm!r}")

    # 1) Presigned XML
    r = requests.post(
        f"{base}/presigned-url/xml",
        headers=headers,
        json={
            "process_id": process_id,
            "file_name": xml_name,
            "file_type": "application/xml",
        },
        timeout=60,
    )
    if not r.ok:
        print(f"presigned-url/xml falhou: {r.status_code} {r.text}", file=sys.stderr)
        return 1
    up = r.json()
    put = requests.put(
        up["upload_url"],
        data=xml_content,
        headers={"Content-Type": "application/xml"},
        timeout=120,
    )
    if not put.ok:
        print(f"upload S3 falhou: {put.status_code} {put.text[:500]}", file=sys.stderr)
        return 1
    print("Upload XML OK")

    # 2) Metadados com ritm
    metadados = {
        "header": {"tenantId": "00,050101"},
        "requestBody": {
            "ritm": args.ritm,
            "isCommodities": True,
            "itens": [{"codigoProduto": "AAK00001KG00600", "produto": "SOJA"}],
            "cnpjDestinatario": "03856216000141",
        },
    }
    r = requests.post(
        f"{base}/metadados/pedido",
        headers=headers,
        json={"process_id": process_id, "metadados": metadados},
        timeout=60,
    )
    if not r.ok:
        print(f"metadados/pedido falhou: {r.status_code} {r.text}", file=sys.stderr)
        return 1
    print("Metadados (com ritm) OK:", json.dumps(r.json(), ensure_ascii=False))

    if not args.no_start:
        r = requests.post(
            f"{base}/start",
            headers=headers,
            json={"process_id": process_id},
            timeout=60,
        )
        if not r.ok:
            print(f"start (Step Functions) falhou: {r.status_code} {r.text}", file=sys.stderr)
            return 1
        sd = r.json()
        print("Step Functions iniciado:", json.dumps(sd, ensure_ascii=False))
        ex = sd.get("execution_arn")
        if ex:
            print(f"execution_arn: {ex}")
    else:
        print("Pulado POST /process/start (--no-start). Start manual:")
        print(f"  curl -X POST {base}/start -H 'Authorization: Bearer …' -d '{{\"process_id\":\"{process_id}\"}}'")

    print("\nPróximos passos:")
    print(
        f"  - CloudWatch send_to_protheus: buscar {process_id} e confirmar '\"ritm\"' no payload."
    )
    print(f"  - Dynamo: PK=PROCESS#{process_id}, SK=PEDIDO_COMPRA_METADATA → METADADOS com ritm no JSON.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
