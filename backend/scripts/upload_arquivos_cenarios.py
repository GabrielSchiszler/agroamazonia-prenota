#!/usr/bin/env python3
"""
Envia arquivos de teste em scripts/arquivos/<cenário>/ (ex.: 53, 61, 71, 78) para a API.

O POST `/presigned-url/batch` envia só ``file_name`` e ``file_type``; a API infere pasta/DOC_TYPE
(XML → ``danfe/``, demais → ``docs/``). Metadados do **pedido** (JSON ERP) são só o bloco
``header`` + ``requestBody``. Por defeito o script carrega ``scripts/pedido_metadata_exemplo.json``
se existir (POST ``/metadados/pedido`` após uploads). Use ``--no-pedido-metadata`` para não enviar.

**URL da API:** `test_create_process.py` usa `{API_URL}/api/process/...`. Este script, por defeito,
usa `{API_URL}/process/...` (igual ao front HML). Se obtiveres 404 nos POST, usa
`--legacy-api-process` ou `LEGACY_API_PROCESS=1` no `.env` para forçar `{API_URL}/api/process/...`.

O **tipo** USOCONSUMO no backend só existe se o JSON do pedido tiver `usoEConsumo: true` (requestBody
ou header). O script envia o ficheiro tal como está; use `--uso-e-consumo` para forçar esse campo.

  python3 scripts/upload_arquivos_cenarios.py --scenario 53 --legacy-api-process
  python3 scripts/upload_arquivos_cenarios.py --scenario 53 --uso-e-consumo
  python3 scripts/upload_arquivos_cenarios.py --scenario 53 --pedido-json exemplo --no-pedido-metadata

Auth: API_KEY, OAuth2, AUTH_TOKEN; API_URL (…/v1).
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import uuid
from pathlib import Path

import requests

from api_auth import (
    DEV_API_URL,
    build_auth_headers,
    build_config_env,
    cfg_str as _cfg_str,
    load_env_file as _load_env_file,
    resolve_env_file,
)

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent

DEFAULT_API_URL = DEV_API_URL
DEFAULT_SCENARIOS = ("53", "61", "71", "78")


def _process_base_url(api_url: str, path_prefix: str) -> str:
    api_url = api_url.rstrip("/")
    p = (path_prefix or "").strip().strip("/")
    if p:
        return f"{api_url}/{p}/process"
    return f"{api_url}/process"


def _content_type_for_path(path: Path) -> str:
    ext = path.suffix.lower()
    mapping = {
        ".pdf": "application/pdf",
        ".xml": "application/xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
    }
    if ext in mapping:
        return mapping[ext]
    guessed, _ = mimetypes.guess_type(path.name)
    if guessed:
        return guessed
    return "application/octet-stream"


def _collect_files(scenario_dir: Path) -> list[Path]:
    if not scenario_dir.is_dir():
        raise FileNotFoundError(f"Pasta não encontrada: {scenario_dir}")
    files = [p for p in scenario_dir.iterdir() if p.is_file() and not p.name.startswith(".")]
    files.sort(key=lambda p: p.name.lower())
    return files


def _batch_items(paths: list[Path]) -> list[dict[str, str]]:
    """Monta lista para BatchPresignedUrlRequest: file_name e file_type (doc_type inferido na API)."""
    items: list[dict[str, str]] = []
    for p in paths:
        ct = _content_type_for_path(p)
        if ct == "application/octet-stream":
            raise ValueError(
                f"Tipo não suportado para {p.name}. "
                f"Use pdf/xml/png/jpeg/tiff (ver ALLOWED_CONTENT_TYPES na API)."
            )
        items.append({"file_name": p.name, "file_type": ct})
    return items


def _warn_uso_e_consumo_no_metadados(metadados: dict) -> None:
    """Avisa se faltar usoEConsumo (o /start não definirá PROCESS_TYPE=USOCONSUMO)."""
    rb = metadados.get("requestBody")
    hd = metadados.get("header") if isinstance(metadados.get("header"), dict) else {}
    if not isinstance(rb, dict):
        return
    has = "usoEConsumo" in rb or "usoEConsumo" in hd
    print(f"  requestBody keys: {sorted(rb.keys())}")
    if not has:
        print(
            "  AVISO: metadados sem 'usoEConsumo' — ao POST /start o tipo não será USOCONSUMO. "
            'Inclua "usoEConsumo": true no JSON, atualize pedido_metadata_exemplo.json, ou use --uso-e-consumo.',
            file=sys.stderr,
        )


def _force_uso_e_consumo_true(metadados: dict) -> None:
    rb = metadados.setdefault("requestBody", {})
    if not isinstance(rb, dict):
        metadados["requestBody"] = {}
        rb = metadados["requestBody"]
    rb["usoEConsumo"] = True
    print("  Definido requestBody.usoEConsumo=true (--uso-e-consumo).")


def _post_pedido_metadata(
    base: str,
    headers: dict[str, str],
    process_id: str,
    metadados: dict,
    timeout: int,
) -> bool:
    url = f"{base}/metadados/pedido"
    print(f"  POST {url}")
    r = requests.post(
        url,
        headers=headers,
        json={"process_id": process_id, "metadados": metadados},
        timeout=timeout,
    )
    if not r.ok:
        print(f"metadados/pedido falhou: {r.status_code} {r.text}", file=sys.stderr)
        return False
    print("  OK metadados do pedido:", json.dumps(r.json(), ensure_ascii=False))
    return True


def run_scenario(
    base: str,
    headers: dict[str, str],
    scenario: str,
    arquivos_root: Path,
    start: bool,
    timeout: int,
    pedido_metadados: dict | None,
) -> int:
    scenario_dir = arquivos_root / scenario
    paths = _collect_files(scenario_dir)
    if not paths:
        print(f"[{scenario}] Nenhum arquivo em {scenario_dir}", file=sys.stderr)
        return 1

    process_id = str(uuid.uuid4())
    print(f"\n=== Cenário {scenario} | process_id={process_id} ===")
    print(f"Arquivos ({len(paths)}): {[p.name for p in paths]}")

    batch_items = _batch_items(paths)
    for it in batch_items:
        print(f"  → {it['file_name']}: {it['file_type']} (doc_type inferido na API)")

    r = requests.post(
        f"{base}/presigned-url/batch",
        headers=headers,
        json={"process_id": process_id, "files": batch_items},
        timeout=timeout,
    )
    if not r.ok:
        print(f"presigned-url/batch falhou: {r.status_code} {r.text}", file=sys.stderr)
        return 1

    pres = r.json()
    files_out = pres.get("files") or []
    if len(files_out) != len(paths):
        print("Resposta batch com quantidade inesperada de arquivos", file=sys.stderr)
        return 1

    for local_path, meta in zip(paths, files_out):
        body = local_path.read_bytes()
        put = requests.put(
            meta["upload_url"],
            data=body,
            headers={"Content-Type": meta.get("content_type") or _content_type_for_path(local_path)},
            timeout=timeout,
        )
        if not put.ok:
            print(
                f"Upload S3 falhou {local_path.name}: {put.status_code} {put.text[:400]}",
                file=sys.stderr,
            )
            return 1
        print(f"  OK upload: {local_path.name} → {meta.get('file_name')}")

    if pedido_metadados is not None:
        print("  Enviando metadados do pedido (JSON ERP)...")
        _warn_uso_e_consumo_no_metadados(pedido_metadados)
        if not _post_pedido_metadata(base, headers, process_id, pedido_metadados, timeout):
            return 1

    if start:
        r = requests.post(
            f"{base}/start",
            headers=headers,
            json={"process_id": process_id},
            timeout=timeout,
        )
        if not r.ok:
            print(f"start falhou: {r.status_code} {r.text}", file=sys.stderr)
            return 1
        sd = r.json()
        print("Step Functions:", json.dumps(sd, ensure_ascii=False))
        ex = sd.get("execution_arn")
        if ex:
            print(f"execution_arn: {ex}")
    else:
        print("Sem POST /start (--no-start). Inicie manualmente com process_id acima.")

    print(f"GET processo: {base}/{process_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload de scripts/arquivos/<cenário>/ via presigned-url/batch + start opcional.",
    )
    parser.add_argument(
        "--arquivos-dir",
        default=None,
        help=f"Raiz das pastas por número (default: {_SCRIPT_DIR / 'arquivos'})",
    )
    parser.add_argument("--scenario", help="Um cenário, ex.: 53")
    parser.add_argument(
        "--scenarios",
        help=f"Lista separada por vírgula (ex.: 53,61). Ignorado se --scenario for usado.",
    )
    parser.add_argument(
        "--all-default",
        action="store_true",
        help=f"Usa cenários padrão: {','.join(DEFAULT_SCENARIOS)}",
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Não chamar POST /process/start após os uploads",
    )
    parser.add_argument(
        "--pedido-json",
        metavar="FILE",
        default=None,
        help=(
            "JSON do pedido (header + requestBody). 'exemplo' → pedido_metadata_exemplo.json. "
            "Env PEDIDO_METADATA_JSON também. Se nada for passado, usa o ficheiro exemplo em scripts/ (se existir)."
        ),
    )
    parser.add_argument(
        "--no-pedido-metadata",
        action="store_true",
        help="Não chamar POST metadados/pedido (nem carregar o JSON de exemplo por defeito).",
    )
    parser.add_argument(
        "--uso-e-consumo",
        action="store_true",
        help=(
            "Depois de carregar o JSON do pedido, define requestBody.usoEConsumo=true "
            "(para o /start gravar PROCESS_TYPE=USOCONSUMO sem editar o ficheiro)."
        ),
    )
    parser.add_argument(
        "--legacy-api-process",
        action="store_true",
        help=(
            "Mesmo URL que test_create_process.py: .../api/process/... (não .../process/...). "
            "Use se metadados/batch/start derem 404 com o gateway antigo."
        ),
    )
    parser.add_argument("--api-url", help="API_URL (termina em .../v1)")
    parser.add_argument(
        "--api-path-prefix",
        default=None,
        help='Segmento antes de /process (ex.: "api"). Env: PROCESS_API_PATH_PREFIX.',
    )
    parser.add_argument("--api-key", help="x-api-key")
    parser.add_argument("--token", help="Bearer manual (AUTH_TOKEN)")
    parser.add_argument("--oauth-token-url", help="OAUTH2_FRONTEND_TOKEN_URL")
    parser.add_argument("--oauth-client-id", help="OAUTH2_FRONTEND_CLIENT_ID")
    parser.add_argument("--oauth-client-secret", help="OAUTH2_FRONTEND_CLIENT_SECRET")
    parser.add_argument("--oauth-scope", help="OAUTH2_FRONTEND_SCOPE")
    parser.add_argument("--no-browser-ua", action="store_true")
    parser.add_argument("--env-file", default=None, help="Ex.: .env.homolog ou ../.env.development")
    parser.add_argument(
        "--dev",
        action="store_true",
        help=f"Carrega {_BACKEND_DIR / '.env.development'} e API dev padrão",
    )
    parser.add_argument("--timeout", type=int, default=120, help="Timeout HTTP (s)")
    args = parser.parse_args()

    env_file, prefer_file = resolve_env_file(
        args.env_file, dev=args.dev, homolog=False
    )
    if env_file and env_file.is_file():
        print(f"Carregado: {env_file.resolve()}")

    config_env = build_config_env(env_file, prefer_file=prefer_file)
    api_url = _cfg_str(args.api_url, config_env, "API_URL", DEFAULT_API_URL)
    assert api_url
    path_prefix = (_cfg_str(args.api_path_prefix, config_env, "PROCESS_API_PATH_PREFIX", "") or "").strip().strip("/")
    _legacy = args.legacy_api_process or (
        (config_env.get("LEGACY_API_PROCESS") or "")
        .strip()
        .lower()
        in ("1", "true", "yes")
    )
    if _legacy:
        path_prefix = "api"

    try:
        headers = build_auth_headers(config_env, args, env_file=env_file)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    base = _process_base_url(api_url, path_prefix)
    print(f"API base (process): {base}")

    arquivos_root = Path(args.arquivos_dir).resolve() if args.arquivos_dir else _SCRIPT_DIR / "arquivos"

    if args.scenario:
        scenarios = [args.scenario.strip()]
    elif args.scenarios:
        scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    elif args.all_default:
        scenarios = list(DEFAULT_SCENARIOS)
    else:
        parser.error("Informe --scenario, --scenarios ou --all-default")

    start = not args.no_start

    pedido_metadados: dict | None = None
    if not args.no_pedido_metadata:
        pj = (args.pedido_json or os.environ.get("PEDIDO_METADATA_JSON") or "").strip()
        default_exemplo = _SCRIPT_DIR / "pedido_metadata_exemplo.json"
        if not pj and default_exemplo.is_file():
            pj_path = default_exemplo
            print(f"Metadados do pedido (padrão): {pj_path}")
        elif pj:
            if pj.lower() == "exemplo":
                pj_path = _SCRIPT_DIR / "pedido_metadata_exemplo.json"
            else:
                pj_path = Path(pj).expanduser()
            if not pj_path.is_file():
                print(f"Ficheiro de metadados do pedido não encontrado: {pj_path}", file=sys.stderr)
                return 1
            print(f"Metadados do pedido a enviar: {pj_path}")
        else:
            pj_path = None

        if pj_path is not None:
            try:
                pedido_metadados = json.loads(pj_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"Erro ao ler JSON do pedido: {e}", file=sys.stderr)
                return 1
        if args.uso_e_consumo and pedido_metadados is not None:
            _force_uso_e_consumo_true(pedido_metadados)
    else:
        print("Metadados do pedido: omitidos (--no-pedido-metadata).")
        if args.uso_e_consumo:
            print("--uso-e-consumo não tem efeito com --no-pedido-metadata.", file=sys.stderr)

    rc = 0
    for sc in scenarios:
        if run_scenario(
            base,
            headers,
            sc,
            arquivos_root,
            start=start,
            timeout=args.timeout,
            pedido_metadados=pedido_metadados,
        ):
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
