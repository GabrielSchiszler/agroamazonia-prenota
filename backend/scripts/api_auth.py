"""
Autenticação unificada para scripts de teste (OAuth2 + x-api-key).

Quando --env-file ou --dev é usado, variáveis do arquivo têm prioridade sobre o shell
(evita que credenciais de outro ambiente exportadas no terminal sobrescrevam o .env).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import requests

_BACKEND_DIR = Path(__file__).resolve().parent.parent

DEV_API_URL = "https://api-dev.agroamazonia.com/fast/v1"
HOMOLOG_API_URL = "https://api-hml.agroamazonia.com/fast/v1"

_DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_OAUTH_KEYS = frozenset({
    "OAUTH2_FRONTEND_TOKEN_URL",
    "OAUTH2_FRONTEND_CLIENT_ID",
    "OAUTH2_FRONTEND_CLIENT_SECRET",
    "OAUTH2_FRONTEND_SCOPE",
    "API_URL",
    "API_KEY",
    "AUTH_TOKEN",
    "BEARER_TOKEN",
})


def load_env_file(path: Path | None) -> dict[str, str]:
    """Lê KEY=VAL; ignora comentários e linhas de comando (cdk deploy, cd)."""
    out: dict[str, str] = {}
    if path is None or not path.is_file():
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


def build_config_env(
    env_file: Path | None,
    *,
    prefer_file: bool = False,
) -> dict[str, str]:
    """
    Monta dict de configuração para lookup.

    prefer_file=True (--env-file / --dev): chaves do arquivo vencem o shell.
    Caso contrário: CLI/shell > arquivo (preenche só o que falta no shell).
    """
    file_env = load_env_file(env_file)
    merged = {k: str(v) for k, v in os.environ.items()}
    if prefer_file and file_env:
        merged.update(file_env)
        return merged
    for k, v in file_env.items():
        if k not in merged or not str(merged.get(k, "")).strip():
            merged[k] = v
    return merged


def resolve_env_file(
    env_file: str | Path | None,
    *,
    dev: bool = False,
    homolog: bool = False,
) -> tuple[Path | None, bool]:
    """Retorna (path, prefer_file). prefer_file só True se o arquivo existir."""
    if dev:
        p = _BACKEND_DIR / ".env.development"
        return (p if p.is_file() else None, p.is_file())
    if homolog:
        p = _BACKEND_DIR / ".env.homolog"
        return (p if p.is_file() else None, p.is_file())
    if env_file:
        p = Path(env_file).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        return (p if p.is_file() else None, p.is_file())
    for candidate in (
        Path(".env.homolog"),
        _BACKEND_DIR / ".env.homolog",
        _BACKEND_DIR / ".env.development",
    ):
        if candidate.is_file():
            return (candidate.resolve(), False)
    return (None, False)


def cfg_str(
    arg_val: str | None,
    config_env: dict[str, str],
    env_key: str,
    default: str | None = None,
) -> str | None:
    """Prioridade: argumento CLI > config_env (já mesclado conforme prefer_file)."""
    if arg_val is not None and str(arg_val).strip():
        return str(arg_val).strip()
    v = config_env.get(env_key)
    if v is not None and str(v).strip():
        return str(v).strip()
    return default


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


def _browser_like_headers(
    config_env: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    if getattr(args, "no_browser_ua", False):
        return {}
    ua = cfg_str(
        getattr(args, "user_agent", None),
        config_env,
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


def _oauth_source_hint(config_env: dict[str, str], env_file: Path | None) -> str:
    if env_file and env_file.is_file():
        return str(env_file.resolve())
    return "shell"


def build_auth_headers(
    config_env: dict[str, str],
    args: argparse.Namespace,
    *,
    env_file: Path | None = None,
    quiet: bool = False,
) -> dict[str, str]:
    """Bearer OAuth2 (client_credentials) + x-api-key opcional — igual ao frontend."""
    api_key = cfg_str(getattr(args, "api_key", None), config_env, "API_KEY")
    base = {
        "Content-Type": "application/json",
        **_browser_like_headers(config_env, args),
    }

    token_manual = cfg_str(getattr(args, "token", None), config_env, "AUTH_TOKEN") or cfg_str(
        None, config_env, "BEARER_TOKEN"
    )
    if token_manual:
        out = {**base, "Authorization": f"Bearer {token_manual}"}
        if api_key:
            out["x-api-key"] = api_key
        if not quiet:
            print(f"Autenticação: Bearer manual ({len(token_manual)} chars)")
        return out

    cid = cfg_str(getattr(args, "oauth_client_id", None), config_env, "OAUTH2_FRONTEND_CLIENT_ID")
    csec = cfg_str(
        getattr(args, "oauth_client_secret", None), config_env, "OAUTH2_FRONTEND_CLIENT_SECRET"
    )
    if cid and csec:
        token_url = cfg_str(
            getattr(args, "oauth_token_url", None),
            config_env,
            "OAUTH2_FRONTEND_TOKEN_URL",
            "https://api-auth-hml.agroamazonia.io/oauth2/token",
        )
        assert token_url
        scope = cfg_str(
            getattr(args, "oauth_scope", None),
            config_env,
            "OAUTH2_FRONTEND_SCOPE",
            "App_Fast/HML",
        )
        assert scope
        token = fetch_oauth2_token(token_url, cid, csec, scope)
        out = {**base, "Authorization": f"Bearer {token}"}
        parts = [f"OAuth2 Bearer ({len(token)} chars)"]
        if api_key:
            out["x-api-key"] = api_key
            parts.append("x-api-key")
        else:
            parts.append("(sem API_KEY)")
        if not quiet:
            src = _oauth_source_hint(config_env, env_file)
            print(f"Autenticação: {' + '.join(parts)} | env: {src}")
            print(f"  token_url={token_url} scope={scope}")
        return out

    if api_key:
        if not quiet:
            print("Autenticação: x-api-key")
        return {**base, "x-api-key": api_key}

    raise RuntimeError(
        "Defina OAuth2 (OAUTH2_FRONTEND_CLIENT_ID + OAUTH2_FRONTEND_CLIENT_SECRET) "
        "ou API_KEY / --api-key no --env-file ou no shell."
    )
