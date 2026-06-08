#!/usr/bin/env python3
"""
Consulta status, validações e payload Protheus de um processo na API dev/hml.

  cd backend/scripts
  python3 check_process_dev.py --dev f20bda2d87d583503cb2ece90cbb358a
  python3 check_process_dev.py --env-file ../.env.development <process_id>
  python3 check_process_dev.py --dev <id> --wait 300
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent

sys.path.insert(0, str(_SCRIPT_DIR))
from api_auth import (  # noqa: E402
    DEV_API_URL,
    build_auth_headers,
    build_config_env,
    cfg_str as _cfg_str,
    resolve_env_file,
)
from upload_arquivos_cenarios import _process_base_url  # noqa: E402


def _fetch(base: str, headers: dict, pid: str, timeout: int) -> dict:
    r = requests.get(f"{base}/{pid}", headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _fetch_validations(base: str, headers: dict, pid: str, timeout: int) -> list:
    r = requests.get(f"{base}/{pid}/validations", headers=headers, timeout=timeout)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    data = r.json()
    return data.get("validations") or []


def _summarize_validations(validations: list) -> str:
    if not validations:
        return "(sem VALIDATION_RESULTS na API)"
    lines = []
    for v in validations:
        rule = v.get("rule") or v.get("type") or "?"
        st = v.get("status") or "?"
        msg = (v.get("message") or "")[:120]
        lines.append(f"  - {rule}: {st}" + (f" — {msg}" if msg else ""))
        comps = v.get("comparisons") or v.get("docs") or []
        for c in comps[:1]:
            items = c.get("items") or []
            for it in items[:3]:
                pos = ""
                if it.get("danfe_position"):
                    pos = f" DANFE#{it['danfe_position']}"
                    if it.get("doc_position"):
                        pos += f" → Pedido#{it['doc_position']}"
                fields = it.get("fields") or {}
                for fk, fv in fields.items():
                    if isinstance(fv, dict):
                        lines.append(
                            f"      {fk}: {fv.get('danfe')} vs {fv.get('doc')} [{fv.get('status')}]"
                        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Status de processo na API")
    parser.add_argument("process_id", help="UUID ou id do processo")
    parser.add_argument("--api-url", default=None)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--dev", action="store_true", help=f"Carrega {_BACKEND_DIR / '.env.development'}")
    parser.add_argument("--wait", type=int, default=0, help="Poll até status terminal (segundos)")
    parser.add_argument("--interval", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    env_path, prefer_file = resolve_env_file(
        args.env_file, dev=args.dev, homolog=False
    )
    if env_path and env_path.is_file():
        print(f"Carregado: {env_path.resolve()}")
    config_env = build_config_env(env_path, prefer_file=prefer_file)

    api_url = _cfg_str(args.api_url, config_env, "API_URL", DEV_API_URL)
    base = _process_base_url(api_url, "")
    headers = build_auth_headers(
        config_env,
        argparse.Namespace(
            api_key=None,
            token=None,
            oauth_token_url=None,
            oauth_client_id=None,
            oauth_client_secret=None,
            oauth_scope=None,
            no_browser_ua=False,
        ),
        env_file=env_path,
    )

    pid = args.process_id.strip()
    terminal = {"COMPLETED", "FAILED", "VALIDATED", "VALIDATION_FAILED"}
    deadline = time.time() + args.wait if args.wait else 0

    while True:
        try:
            proc = _fetch(base, headers, pid, args.timeout)
        except requests.HTTPError as e:
            print(f"Erro GET processo: {e}", file=sys.stderr)
            return 1

        status = proc.get("status") or "?"
        ptype = proc.get("process_type") or "?"
        print(f"\nprocess_id: {pid}")
        print(f"status: {status}")
        print(f"process_type: {ptype}")

        fs = proc.get("failure_summary") or {}
        if fs:
            print(f"failure: {fs.get('reason_label') or fs.get('reason_code')}")

        prp = proc.get("protheus_request_payload")
        if prp and isinstance(prp, dict):
            itens = prp.get("itens") or []
            if itens:
                i0 = itens[0]
                print(
                    f"protheus item[0]: codigoProduto={i0.get('codigoProduto')!r} "
                    f"qtd={i0.get('quantidade')} op={i0.get('codigoOperacao')!r}"
                )

        vals = _fetch_validations(base, headers, pid, args.timeout)
        print("validações:")
        print(_summarize_validations(vals))

        if status in terminal or not args.wait or time.time() >= deadline:
            break
        print(f"… aguardando ({args.interval}s)")
        time.sleep(args.interval)

    return 0 if status in ("COMPLETED", "VALIDATED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
