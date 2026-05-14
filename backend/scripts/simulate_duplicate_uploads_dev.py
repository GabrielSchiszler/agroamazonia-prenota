#!/usr/bin/env python3
"""
Simula N uploads do mesmo arquivo com o mesmo nome de arquivo contra o ambiente de dev.

Fluxo (igual ao front):
  1. Gera um process_id novo
  2. Para cada tentativa: POST presigned → PUT no S3 com o mesmo file_name
  3. GET /process/{id} e confere quantos anexos existem com esse nome e file_keys distintos

Uso:
  Use as mesmas variáveis que o frontend (`frontend/.env` ou cópia no backend):

    VITE_API_URL=https://api-dev.agroamazonia.com/fast
    VITE_API_KEY=agroamazonia_key_...

  Ou o par clássico de scripts:

    API_URL=...
    API_KEY=...

  Também vale exportar no terminal (preenche o que CLI / .env não definiram):

    export VITE_API_URL=https://api-dev.agroamazonia.com/fast
    export VITE_API_KEY=agroamazonia_key_...
    cd backend && python3 scripts/simulate_duplicate_uploads_dev.py

  OAuth2 client_credentials (como frontend/auth.js): o script já tem fallback embutido para o app-client dev;
  export ou .env sobrescrevem se precisar.

    export OAUTH2_FRONTEND_TOKEN_URL=https://api-auth-dev.agroamazonia.com/oauth2/token
    export OAUTH2_FRONTEND_CLIENT_ID=...
    export OAUTH2_FRONTEND_CLIENT_SECRET=...
    export OAUTH2_FRONTEND_SCOPE=app-client-public/dev

  Se não houver --token / --token-file, o script tenta OAuth2 primeiro (token novo); só então usa
  BEARER_TOKEN do .env ou _TEMP_DEV_BEARER_JWT — JWT copiado do navegador expira (~1h) e o gateway responde 403.

  Para usar o mesmo token do navegador (DevTools → Network → cabeçalho Authorization):
    export BEARER_TOKEN='eyJ...'   # só o JWT, sem a palavra Bearer
    ou: python3 scripts/simulate_duplicate_uploads_dev.py --token 'eyJ...'
    ou: echo -n 'eyJ...' > .dev-bearer && python3 ... --token-file .dev-bearer
    (não commite o token; ele expira.)
  Um 403 com Bearer válido no dev costuma ser o WAF bloqueando o User-Agent `python-requests/*`;
  o script envia User-Agent de navegador nas chamadas à API.
  URLs que parecerem homolog (*hml*) são trocadas automaticamente pelo host de dev.

  Rodando a partir de backend/scripts/ também funciona: o script procura backend/.env se ./.env não existir.
  Carrega também backend/.env.development (OAuth / overrides). Depois frontend/.env.local e frontend/.env.

  Importante: use a mesma API_URL que o front dev (CloudFront), não um execute-api antigo —
  após `cdk deploy AgroAmazoniaStack-dev`, quem importa é típico https://api-dev.agroamazonia.com/fast

  Ou passe explicitamente:
    python3 scripts/simulate_duplicate_uploads_dev.py --api-url ... --api-key ...
    python3 scripts/simulate_duplicate_uploads_dev.py --env-file /caminho/.env

  O teste só passa se o backend já estiver deployado com upload único por arquivo (UUID no path S3).
  Se todas as file_key forem iguais, falta deploy da versão atual do process_service.generate_presigned_url.

Opções:
  --kind docs   → POST …/process/presigned-url/docs (padrão; igual ao frontend)
  --kind xml    → POST …/process/presigned-url/xml (mesmo nome de arquivo 4x)

Requer: pip install requests
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

# PDF mínimo válido (1 página em branco) — mesmo espírito do test_create_process.py
_MIN_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
    b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n"
    b"xref\n0 4\n0000000000 65535 f \n"
    b"trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n200\n%%EOF\n"
)

_MIN_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<root><note>stub-xml-for-dup-upload-test</note></root>
"""

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_ROOT = _SCRIPT_DIR.parent

# Fallback API dev (CloudFront) — não usar hosts *hml* (.env pode trazer homolog por engano).
_DEFAULT_API_URL = "https://api-dev.agroamazonia.com/fast"
_DEFAULT_API_KEY = "agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx"

# OAuth2 client_credentials — dev (espelha backend/.env.development; env/arquivo sobrescreve).
_DEFAULT_OAUTH2_TOKEN_URL = "https://api-auth-dev.agroamazonia.com/oauth2/token"
_DEFAULT_OAUTH2_SCOPE = "app-client-public/dev"
_DEFAULT_OAUTH2_CLIENT_ID = "2h24bn621omdn7tmkqir0bske9"
_DEFAULT_OAUTH2_CLIENT_SECRET = "1js03pf111gh60qd5i9tvjvu9umq27fh9vudgj0skrbil51cq7j"

# TEMPORÁRIO — só usado se OAuth falhar e não houver Bearer no .env/CLI. Apague antes de commit.
_TEMP_DEV_BEARER_JWT = "eyJraWQiOiJSU2lJOVdNVkpSbEZhdUJGODFWdG02TFIzck92TkVZT1pYanlxMXd1eCtRPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiIyaDI0Ym42MjFvbWRuN3Rta3FpcjBic2tlOSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXBwLWNsaWVudC1wdWJsaWNcL2RldiIsImF1dGhfdGltZSI6MTc3ODE4MDcwNSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnNhLWVhc3QtMS5hbWF6b25hd3MuY29tXC9zYS1lYXN0LTFfdXBhN1A2aGpvIiwiZXhwIjoxNzc4MTg0MzA1LCJpYXQiOjE3NzgxODA3MDUsInZlcnNpb24iOjIsImp0aSI6ImFiMDAwOWViLTExMGUtNGFmYS04YjJjLTY1ZDNiMTFiZTEzOCIsImNsaWVudF9pZCI6IjJoMjRibjYyMW9tZG43dG1rcWlyMGJza2U5In0.ovfkBj7JBFZ41Vx7BkwxrmhRDTd-amFZT4Mck6OaAV06h4qG-zowDQ2_n8fEXGOcxX6spQXUKb231WvnfufSBu1YWGavtbrZh4VzeRsu_GNXupGAKhy49xePsDnvuxdARNpfS8Hszhs5xO8SssnPKjTcKVI6fVXz3gN_T37_9ErhQMJwh-pAwF7eaUTiSk7jpdqGmZGR-aD4L9HEvYIFEsOu_Vyo7D6JTsIT3CdcXd3Dwz7aTJ6eUyueI3eA4zWPwz_nwfdCabG64rtlCY8Jrgpuz1ZMOM_Q1dfjsntLvYXh0GhJjZ3wNMU13fQdY1bss7o0xnuM-fa2MGu-J4OhvA"


_REPO_ROOT = _BACKEND_ROOT.parent

# Cabeçalhos como requisições do dashboard dev (alguns WAF/Gateway são mais permissivos).
_DASHBOARD_ORIGIN = "https://fast-dash-dev.agroamazonia.com"
# WAF em api-dev costuma bloquear o User-Agent padrão de python-requests → 403 genérico.
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _parse_env_line(line: str) -> tuple[str, str] | None:
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        return None
    s = s.replace("export ", "", 1).strip()
    key, _, rest = s.partition("=")
    key = key.strip()
    value = rest.strip().strip("\"'")
    if key:
        return key, value
    return None


def _resolve_env_file(raw: str) -> Path:
    """Primeiro arquivo que existir: (cwd)/raw ou backend/raw — assim funciona de scripts/ ou backend/."""
    p = Path(raw)
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(Path.cwd() / p)
        candidates.append(_BACKEND_ROOT / p)
    for cand in candidates:
        if cand.is_file():
            return cand
    return candidates[0]


def _merge_credentials_from_env_file(
    env_file: Path,
    api_url: str | None,
    api_key: str | None,
    bearer: str | None,
) -> tuple[str | None, str | None, str | None]:
    """URL/key como no front (VITE_*) e token OAuth opcional (AUTH_TOKEN / BEARER_TOKEN / …)."""
    if not env_file.is_file():
        return api_url, api_key, bearer
    print(f"Carregando variáveis do arquivo {env_file}...")
    with env_file.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = _parse_env_line(line)
            if not parsed:
                continue
            key, value = parsed
            if key == "API_URL" and not api_url:
                api_url = value
            elif key == "VITE_API_URL" and not api_url:
                api_url = value
            elif key == "API_KEY" and not api_key:
                api_key = value
            elif key == "VITE_API_KEY" and not api_key:
                api_key = value
            elif key in (
                "AUTH_TOKEN",
                "BEARER_TOKEN",
                "ACCESS_TOKEN",
                "OAUTH_ACCESS_TOKEN",
                "OAUTH2_ACCESS_TOKEN",
            ) and not bearer:
                bearer = value
    return api_url, api_key, bearer


def _first_nonempty_env(*names: str) -> str | None:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _merge_credentials_from_os_environ(
    api_url: str | None,
    api_key: str | None,
    bearer: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Preenche lacunas a partir do shell (`export VAR=...`)."""
    if not api_url:
        api_url = _first_nonempty_env("API_URL", "VITE_API_URL", "FAST_API_URL")
    if not api_key:
        api_key = _first_nonempty_env("API_KEY", "VITE_API_KEY")
    if not bearer:
        bearer = _first_nonempty_env(
            "AUTH_TOKEN",
            "BEARER_TOKEN",
            "ACCESS_TOKEN",
            "OAUTH_ACCESS_TOKEN",
            "OAUTH2_ACCESS_TOKEN",
        )
    return api_url, api_key, bearer


def _hostname_suggests_hml(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    return "hml" in host or "homolog" in host


def _coerce_dev_api_url(url: str | None) -> str | None:
    """Força host de dev; ignora homolog no .env acidental."""
    if not url or not str(url).strip():
        return None
    u = str(url).strip()
    if _hostname_suggests_hml(u):
        print(
            f"⚠️  API URL parece homolog ({u!r}) — usando dev: {_DEFAULT_API_URL}",
            file=sys.stderr,
        )
        return _DEFAULT_API_URL
    return u


def _coerce_dev_oauth_token_url(url: str | None) -> str:
    u = (url or "").strip()
    if not u or _hostname_suggests_hml(u):
        if u and _hostname_suggests_hml(u):
            print(
                f"⚠️  OAUTH2 token URL parece HML ({u!r}) — usando dev: {_DEFAULT_OAUTH2_TOKEN_URL}",
                file=sys.stderr,
            )
        return _DEFAULT_OAUTH2_TOKEN_URL
    return u


def _merge_oauth_from_file(path: Path, cfg: dict[str, str | None]) -> None:
    if not path.is_file():
        return
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = _parse_env_line(line)
            if not parsed:
                continue
            k, v = parsed
            if k == "OAUTH2_FRONTEND_TOKEN_URL" and not cfg.get("token_url"):
                cfg["token_url"] = v
            elif k == "OAUTH2_FRONTEND_CLIENT_ID" and not cfg.get("client_id"):
                cfg["client_id"] = v
            elif k == "OAUTH2_FRONTEND_CLIENT_SECRET" and not cfg.get("client_secret"):
                cfg["client_secret"] = v
            elif k == "OAUTH2_FRONTEND_SCOPE" and not cfg.get("scope"):
                cfg["scope"] = v


def _collect_oauth2_frontend_settings(env_file_arg: str) -> dict[str, str | None]:
    """OAUTH2_FRONTEND_* — shell export + .env + backend/.env.development + frontend/.env."""
    cfg: dict[str, str | None] = {
        "token_url": _first_nonempty_env("OAUTH2_FRONTEND_TOKEN_URL"),
        "client_id": _first_nonempty_env("OAUTH2_FRONTEND_CLIENT_ID"),
        "client_secret": _first_nonempty_env("OAUTH2_FRONTEND_CLIENT_SECRET"),
        "scope": _first_nonempty_env("OAUTH2_FRONTEND_SCOPE"),
    }
    paths = [
        _resolve_env_file(env_file_arg),
        _BACKEND_ROOT / ".env.development",
        _BACKEND_ROOT / ".env",
        _REPO_ROOT / "frontend" / ".env.local",
        _REPO_ROOT / "frontend" / ".env",
    ]
    seen: set[Path] = set()
    for p in paths:
        try:
            rp = p.resolve()
        except OSError:
            rp = p
        if rp in seen:
            continue
        seen.add(rp)
        _merge_oauth_from_file(p, cfg)

    if not (cfg.get("token_url") or "").strip():
        cfg["token_url"] = _DEFAULT_OAUTH2_TOKEN_URL
    if not (cfg.get("client_id") or "").strip():
        cfg["client_id"] = _DEFAULT_OAUTH2_CLIENT_ID
    if not (cfg.get("client_secret") or "").strip():
        cfg["client_secret"] = _DEFAULT_OAUTH2_CLIENT_SECRET
    if not (cfg.get("scope") or "").strip():
        cfg["scope"] = _DEFAULT_OAUTH2_SCOPE
    return cfg


def _fetch_oauth2_bearer_token(cfg: dict[str, str | None]) -> str | None:
    """client_credentials como frontend/auth.js getOAuth2Token."""
    client_id = (cfg.get("client_id") or "").strip()
    client_secret = (cfg.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        return None
    token_url = _coerce_dev_oauth_token_url(cfg.get("token_url"))
    scope = (cfg.get("scope") or "").strip() or _DEFAULT_OAUTH2_SCOPE
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    try:
        r = requests.post(
            token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=45,
        )
    except requests.RequestException as e:
        print(f"ERRO ao chamar token OAuth2: {e}", file=sys.stderr)
        return None
    if not r.ok:
        print(f"ERRO token OAuth2 HTTP {r.status_code}: {r.text[:800]}", file=sys.stderr)
        return None
    try:
        body = r.json()
    except Exception:
        print(f"ERRO token OAuth2: resposta não-JSON: {r.text[:400]}", file=sys.stderr)
        return None
    token = body.get("access_token") or body.get("accessToken") or body.get("token")
    if not token:
        print(f"ERRO token OAuth2: resposta sem access_token: {list(body.keys())}", file=sys.stderr)
        return None
    return str(token).strip()


def _build_json_headers(api_key: str | None, bearer: str | None) -> dict[str, str]:
    h: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": _BROWSER_USER_AGENT,
        "Origin": _DASHBOARD_ORIGIN,
        "Referer": f"{_DASHBOARD_ORIGIN}/",
    }
    if api_key:
        h["x-api-key"] = api_key
    if bearer:
        h["Authorization"] = f"Bearer {bearer.strip()}"
    return h


def _resolve_api_credentials(args: argparse.Namespace) -> tuple[str, str | None, str | None]:
    """Ordem: CLI → --env-file → frontend/.env* → variáveis exportadas no shell → defaults."""
    api_url = args.api_url
    api_key = args.api_key
    bearer = args.bearer_token

    env_path = _resolve_env_file(args.env_file)
    api_url, api_key, bearer = _merge_credentials_from_env_file(env_path, api_url, api_key, bearer)

    for extra in (
        _BACKEND_ROOT / ".env.development",
        _BACKEND_ROOT / ".env",
        _REPO_ROOT / "frontend" / ".env.local",
        _REPO_ROOT / "frontend" / ".env",
    ):
        api_url, api_key, bearer = _merge_credentials_from_env_file(extra, api_url, api_key, bearer)

    api_url, api_key, bearer = _merge_credentials_from_os_environ(api_url, api_key, bearer)

    if api_url:
        api_url = _coerce_dev_api_url(api_url)
    if not api_url:
        api_url = _DEFAULT_API_URL
        print(f"ℹ️  Usando API URL padrão (dev CloudFront): {api_url}")

    return api_url.strip(), (api_key.strip() if api_key else None), (bearer.strip() if bearer else None)


def _upload_to_s3(presigned_url: str, body: bytes, content_type: str) -> None:
    r = requests.put(presigned_url, data=body, headers={"Content-Type": content_type}, timeout=120)
    r.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser(description="Simula uploads repetidos com o mesmo file_name (dev).")
    parser.add_argument(
        "--api-url",
        "--apiUrl",
        "--api_url",
        dest="api_url",
        default=None,
        help="URL base da API (senão .env, export API_URL / VITE_API_URL ou padrão dev)",
    )
    parser.add_argument(
        "--api-key",
        "--apiKey",
        "--api_key",
        dest="api_key",
        default=None,
        help="Chave x-api-key (senão .env ou export API_KEY / VITE_API_KEY)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Arquivo com credenciais (padrão: .env no cwd ou backend/.env); aceita VITE_* como o frontend",
    )
    parser.add_argument(
        "--token",
        "--bearer",
        "--bearer-token",
        dest="bearer_token",
        default=None,
        help="JWT Bearer (mesmo do navegador); ou export BEARER_TOKEN / ACCESS_TOKEN",
    )
    parser.add_argument(
        "--token-file",
        "--token_file",
        dest="token_file",
        type=Path,
        default=None,
        help="Arquivo com o JWT (uma linha); usado se --token não for passado",
    )
    parser.add_argument("--count", type=int, default=4, help="Quantidade de uploads com o mesmo nome (padrão: 4).")
    parser.add_argument(
        "--kind",
        choices=("docs", "xml"),
        default="docs",
        help="presigned-url/docs (PDF/anexo) ou presigned-url/xml",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Arquivo local; se omitido, usa PDF ou XML mínimo embutido.",
    )
    parser.add_argument(
        "--file-name",
        default="duplicate_test_same_name.pdf",
        help="Nome lógico enviado em todos os presigned (deve ser igual em todas as rodadas).",
    )
    parser.add_argument(
        "--dry-presigned-only",
        action="store_true",
        help="Só pede as URLs e imprime file_keys; não faz PUT no S3.",
    )
    args = parser.parse_args()

    if not args.bearer_token and args.token_file:
        try:
            raw = Path(args.token_file).expanduser().read_text(encoding="utf-8", errors="replace").strip()
        except OSError as e:
            print(f"ERRO lendo --token-file: {e}", file=sys.stderr)
            return 2
        if raw.lower().startswith("bearer "):
            raw = raw[7:].strip()
        if not raw:
            print("ERRO: --token-file está vazio.", file=sys.stderr)
            return 2
        args.bearer_token = raw

    explicit_cli_bearer = bool(args.bearer_token)

    api_url, api_key, bearer = _resolve_api_credentials(args)
    # Chave vinda de CLI / .env / export — distinto do fallback embutido no script.
    api_key_from_sources = api_key

    # Sem --token/--token-file: OAuth primeiro (evita BEARER_TOKEN ou JWT embutido expirado → 403 no gateway).
    if not explicit_cli_bearer:
        oauth_cfg = _collect_oauth2_frontend_settings(args.env_file)
        tok = _fetch_oauth2_bearer_token(oauth_cfg)
        if tok:
            bearer = tok
            print("ℹ️  Bearer OAuth2 obtido (grant_type=client_credentials, como frontend/auth.js).")
        elif not bearer:
            temp = (_TEMP_DEV_BEARER_JWT or "").strip()
            if temp:
                bearer = temp
                print(
                    "ℹ️  OAuth falhou; usando JWT temporário embutido — apague _TEMP_DEV_BEARER_JWT depois.",
                    file=sys.stderr,
                )
        else:
            print(
                "ℹ️  OAuth falhou; usando Bearer do .env ou export (pode estar expirado).",
                file=sys.stderr,
            )
    elif bearer:
        print("ℹ️  Usando Bearer de --token / --token-file (sem pedir OAuth2).")

    if api_key_from_sources:
        api_key = api_key_from_sources
    elif bearer:
        # Frontend (app.js) só envia Bearer nas rotas /process/* — x-api-key inválida → 403 no gateway.
        api_key = None
        print(
            "ℹ️  Sem x-api-key nas credenciais carregadas — enviando só Bearer (igual ao frontend). "
            "Se precisar dos dois, defina VITE_API_KEY ou --api-key.",
        )
    else:
        api_key = _DEFAULT_API_KEY
        print(f"ℹ️  Usando API Key padrão (x-api-key / Usage Plan): {api_key[:30]}...")

    parts = [p for p in ("x-api-key" if api_key else None, "Bearer" if bearer else None) if p]
    print(f"ℹ️  Headers de auth: {' + '.join(parts)}")
    base = api_url.rstrip("/")
    headers = _build_json_headers(api_key, bearer)
    if "x-api-key" not in headers and "Authorization" not in headers:
        print(
            "Nenhuma credencial: defina VITE_API_KEY ou API_KEY e/ou token Bearer no .env.",
            file=sys.stderr,
        )
        return 2

    if args.file:
        body = args.file.read_bytes()
        logical_name = args.file.name  # mesmo basename em todas as rodadas se repetir --file
    else:
        logical_name = args.file_name
        if args.kind == "xml" and logical_name.lower().endswith(".pdf"):
            print(
                "Aviso: --kind xml com nome .pdf; usando duplicate_test_same_name.xml",
                file=sys.stderr,
            )
            logical_name = "duplicate_test_same_name.xml"
        if args.kind == "xml":
            body = _MIN_XML
            if not logical_name.lower().endswith(".xml"):
                logical_name = "duplicate_test_same_name.xml"
        else:
            body = _MIN_PDF
            if not logical_name.lower().endswith(".pdf"):
                logical_name = "duplicate_test_same_name.pdf"

    content_type = "application/xml" if args.kind == "xml" else "application/pdf"
    # Mesmo path que frontend/app.js: `${API_URL}/process/presigned-url/...` (sem segmento /api/).
    presigned_path = "/process/presigned-url/xml" if args.kind == "xml" else "/process/presigned-url/docs"

    process_id = str(uuid.uuid4())
    presigned_payload: dict = {
        "process_id": process_id,
        "file_name": logical_name,
        "file_type": content_type,
    }
    if args.kind == "docs":
        presigned_payload["metadados"] = {
            "tipo_documento": "documento_adicional",
            "tamanho_arquivo": len(body),
            "data_upload": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        }

    print(f"process_id: {process_id}")
    print(f"nome lógico (sempre igual): {logical_name}")
    print(f"presigned: POST {base}{presigned_path}")
    print(f"uploads: {args.count} × mesmo file_name, corpo {len(body)} bytes")
    print()

    file_keys: list[str] = []

    for i in range(1, args.count + 1):
        resp = requests.post(
            f"{base}{presigned_path}",
            headers=headers,
            json=presigned_payload,
            timeout=60,
        )
        if not resp.ok:
            print(f"[{i}] ERRO presigned {resp.status_code}: {resp.text}")
            if resp.status_code == 403:
                print(
                    "\n→ No API Gateway dev, 403 com Bearer válido costuma ser bloqueio de WAF ao "
                    "User-Agent `python-requests/*`. Este script envia User-Agent de navegador; "
                    "se ainda falhar, JWT expirado (~1h) ou falta de permissão na rota.",
                    file=sys.stderr,
                )
            return 1
        data = resp.json()
        fk = data.get("file_key", "")
        fn = data.get("file_name", "")
        file_keys.append(fk)
        print(f"[{i}] presigned OK  file_name={fn!r}  file_key={fk}")

        if not args.dry_presigned_only:
            try:
                _upload_to_s3(data["upload_url"], body, content_type)
                print(f"[{i}] PUT S3 OK")
            except requests.HTTPError as e:
                print(f"[{i}] ERRO PUT S3: {e}")
                return 1

    unique_keys = set(file_keys)
    print()
    print(f"file_keys distintos nas respostas presigned: {len(unique_keys)} (esperado: {args.count})")
    if len(unique_keys) != args.count:
        print("FALHA: esperava uma chave S3 diferente por upload.")
        if args.count > 1 and len(unique_keys) == 1:
            fk0 = file_keys[0] or ""
            # Novo backend: .../docs/<32 hex UUID>_<nome.ext>
            if fk0 and not re.search(r"/[a-f0-9]{32}_", fk0, re.I):
                print(
                    "\n→ A URL da API parece ser uma versão antiga: file_key sem prefixo UUID "
                    "(ex.: .../docs/arquivo.pdf em vez de .../docs/<uuid>_arquivo.pdf).\n"
                    "  Confirme que está chamando o mesmo host que o front dev após o deploy "
                    "(ex.: https://api-dev.agroamazonia.com/fast), não um execute-api antigo.\n"
                    "  O CDK publica lambda-api-dev; o Gateway precisa apontar para ela.",
                    file=sys.stderr,
                )
        return 1

    proc = requests.get(
        f"{base}/process/{process_id}",
        headers=_build_json_headers(api_key, bearer),
        timeout=60,
    )
    if not proc.ok:
        print(f"GET processo falhou {proc.status_code}: {proc.text}")
        return 1

    pdata = proc.json()
    bucket = pdata.get("files", {})
    if args.kind == "xml":
        rows = bucket.get("danfe") or []
    else:
        rows = bucket.get("additional") or []

    same_name = [r for r in rows if r.get("file_name") == logical_name and not r.get("metadata_only")]
    keys_on_process = {r.get("file_key") for r in same_name if r.get("file_key")}

    print(f"GET /process: anexos com file_name={logical_name!r}: {len(same_name)}")
    print(f"file_key distintos no processo: {len(keys_on_process)}")
    for r in same_name:
        print(f"  - status={r.get('status')} file_key={r.get('file_key')}")

    if len(keys_on_process) != args.count:
        print(
            "AVISO: contagem no GET não bate com o esperado "
            "(S3 event pode ainda não ter atualizado STATUS, ou filtro diferente)."
        )
    else:
        print("OK: mesmo nome lógico com N objetos distintos no processo.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
