#!/usr/bin/env python3
"""
Teste de upload múltiplo via presigned URL (batch) — para homologação e entrega ao cliente.

================================================================================
ROTAS DE UPLOAD DISPONÍVEIS (HTTP POST, JSON, mesmo prefixo base da API)
================================================================================
Substitua {BASE} pela URL base que inclui o segmento até .../process
(ex.: https://api-dev.agroamazonia.com/fast/process ou .../fast/v1/process,
 conforme o ambiente — deve ser o mesmo prefixo que funciona no GET /process/).

**Dica:** se no navegador a lista de processos for `GET .../fast/process/`, use
`API_URL=https://api-dev.agroamazonia.com/fast` (sem `/v1`). Se for `.../fast/v1/process/`,
use `API_URL=.../fast/v1`.

  • Um arquivo NF/XML (DANFE):
      POST {BASE}/presigned-url/xml
      Body: process_id, file_name, file_type, opcional metadados

  • Um documento adicional (PDF, imagem, etc.):
      POST {BASE}/presigned-url/docs
      Body: process_id, file_name, file_type, opcional metadados

  • Vários arquivos de uma vez:
      POST {BASE}/presigned-url/batch
      Body: process_id, files: [ { "file_name", "file_type" }, ... ]
      (doc_type opcional; omitido → API infere por MIME/extensão: XML→DANFE, demais→ADDITIONAL.)
      Máximo de itens por requisição: 10 (limite da API).

Fluxo: a resposta traz upload_url por arquivo → HTTP PUT no upload_url com o binário
(Content-Type = content_type devolvido ou o mesmo file_type).

Rotas relacionadas (opcional): POST {BASE}/metadados/pedido, POST {BASE}/start

Autenticação: normalmente Authorization: Bearer <access_token> (OAuth2 client_credentials).
Alguns ambientes também aceitam/enviam x-api-key — use --api-key se necessário.

Variáveis úteis (ou arquivo .env ao lado do backend): API_URL, API_KEY,
OAUTH2_FRONTEND_TOKEN_URL, OAUTH2_FRONTEND_CLIENT_ID, OAUTH2_FRONTEND_CLIENT_SECRET,
OAUTH2_FRONTEND_SCOPE. Ver também scripts/get_oauth2_bearer_token.py

Relatório JSON (entrada + saída, Bearer/API key mascarados): por defeito
``scripts/presigned_batch_last_run.json``. Opções: ``--save-report /caminho``, ``--no-save-report``.

================================================================================
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent


def _load_upload_module():
    path = _SCRIPT_DIR / "upload_arquivos_cenarios.py"
    spec = importlib.util.spec_from_file_location("upload_arquivos_cenarios", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Não foi possível carregar {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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


def _safe_headers(h: dict[str, str]) -> dict[str, str]:
    """Mascara Authorization e x-api-key para relatório em disco."""
    out: dict[str, str] = {}
    for k, v in h.items():
        lk = str(k).lower()
        if lk == "authorization" and str(v).lower().startswith("bearer "):
            tok = str(v)[7:].strip()
            suf = tok[-8:] if len(tok) > 8 else "***"
            out[str(k)] = f"Bearer <redacted len={len(tok)} tail={suf}>"
        elif lk == "x-api-key":
            out[str(k)] = "<redacted>"
        else:
            out[str(k)] = str(v)
    return out


def _write_report(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def print_routes_reference() -> None:
    """Imprime o mesmo resumo do docstring (para copiar ao cliente)."""
    print(
        """
=== Rotas de upload (referência rápida) ===

POST {BASE}/presigned-url/xml       — 1 arquivo (NF/XML DANFE)
POST {BASE}/presigned-url/docs      — 1 documento adicional
POST {BASE}/presigned-url/batch     — N arquivos (máx. 10)

Headers típicos: Content-Type: application/json ; Authorization: Bearer <token>
Corpo batch (mínimo): {"process_id":"...","files":[{"file_name":"...","file_type":"application/pdf"}]}

Depois: PUT em cada upload_url retornado com o arquivo binário.
"""
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Testa POST /presigned-url/batch + uploads S3 (presigned PUT)."
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Arquivos locais para enviar (pdf, xml, png, ...).",
    )
    parser.add_argument(
        "--list-routes",
        action="store_true",
        help="Só imprime resumo das rotas de upload e sai.",
    )
    parser.add_argument(
        "--process-id",
        default=None,
        help="UUID do processo (default: gera um novo).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Só chama POST batch e imprime JSON; não faz PUT no S3.",
    )
    parser.add_argument(
        "--ping-process-list",
        action="store_true",
        help="Antes do batch, faz GET {base}/../ na lista de processos (útil para comparar com o browser).",
    )
    parser.add_argument("--api-url", help="API_URL (ex.: https://api-dev.agroamazonia.com/fast/v1)")
    parser.add_argument(
        "--legacy-api-process",
        action="store_true",
        help="Usa prefixo .../api/process/... (gateway legado).",
    )
    parser.add_argument(
        "--api-path-prefix",
        default=None,
        help='Segmento antes de /process (env PROCESS_API_PATH_PREFIX).',
    )
    parser.add_argument("--api-key", help="x-api-key")
    parser.add_argument("--token", help="Bearer manual (AUTH_TOKEN)")
    parser.add_argument("--oauth-token-url")
    parser.add_argument("--oauth-client-id")
    parser.add_argument("--oauth-client-secret")
    parser.add_argument("--oauth-scope")
    parser.add_argument("--env-file", default=None, help="Ex.: backend/.env.development")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument(
        "--save-report",
        type=Path,
        default=_SCRIPT_DIR / "presigned_batch_last_run.json",
        help=f"Grava entrada/saída da execução em JSON (headers mascarados). Default: {_SCRIPT_DIR / 'presigned_batch_last_run.json'}",
    )
    parser.add_argument(
        "--no-save-report",
        action="store_true",
        help="Não gravar ficheiro de relatório.",
    )
    args = parser.parse_args()

    if args.list_routes:
        print_routes_reference()
        print("(Ver docstring completa no topo de scripts/test_presigned_batch_client.py)")
        return 0

    uac = _load_upload_module()

    if not args.files:
        parser.error("Informe ao menos um arquivo ou use --list-routes.")

    env_file: Path | None = None
    if args.env_file:
        env_file = Path(args.env_file).expanduser().resolve()
    else:
        for p in (_BACKEND_DIR / ".env.development", _BACKEND_DIR / ".env.homolog", Path(".env.development")):
            if p.is_file():
                env_file = p.resolve()
                break

    file_env: dict[str, str] = _load_env_file(env_file) if env_file else {}
    if env_file and env_file.is_file():
        print(f"[env] {env_file}")

    api_url = uac._cfg_str(args.api_url, file_env, "API_URL", uac.DEFAULT_API_URL)
    assert api_url

    path_prefix = (
        uac._cfg_str(args.api_path_prefix, file_env, "PROCESS_API_PATH_PREFIX", "") or ""
    ).strip().strip("/")
    _legacy = args.legacy_api_process or (
        (file_env.get("LEGACY_API_PROCESS") or os.environ.get("LEGACY_API_PROCESS") or "")
        .strip()
        .lower()
        in ("1", "true", "yes")
    )
    if _legacy:
        path_prefix = "api"

    report: dict = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "input": {},
        "output": {},
    }

    try:
        headers = uac.build_auth_headers(file_env, args)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        report["output"]["auth_error"] = str(e)
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
        return 1

    base = uac._process_base_url(api_url, path_prefix)
    print(f"[API base process] {base}")

    paths = [Path(f).expanduser().resolve() for f in args.files]
    for p in paths:
        if not p.is_file():
            print(f"Arquivo não encontrado: {p}", file=sys.stderr)
            report["output"]["error"] = f"Arquivo não encontrado: {p}"
            if not args.no_save_report:
                _write_report(args.save_report, report)
                print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
            return 1

    process_id = (args.process_id or "").strip() or str(uuid.uuid4())

    input_common = {
        "api_url_resolved": api_url,
        "process_base": base,
        "batch_url": f"{base}/presigned-url/batch",
        "method": "POST",
        "headers": _safe_headers(headers),
        "local_files": [
            {"path": str(p), "size_bytes": p.stat().st_size} for p in paths
        ],
        "options": {
            "dry_run": args.dry_run,
            "ping_process_list": args.ping_process_list,
            "env_file": str(env_file) if env_file else None,
        },
        "process_id": process_id,
    }

    try:
        batch_items = uac._batch_items(paths)
    except ValueError as e:
        report["input"] = {**input_common, "body_build_error": str(e)}
        report["output"]["error"] = str(e)
        print(str(e), file=sys.stderr)
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
        return 1

    batch_url = input_common["batch_url"]
    payload = {"process_id": process_id, "files": batch_items}

    report["input"] = {**input_common, "body": payload}

    if args.ping_process_list:
        # Lista processos: mesmo recurso que o front usa (GET .../process/ ou .../process)
        list_url = base.rstrip("/") + "/"
        print(f"[ping] GET {list_url}")
        r0 = requests.get(list_url, headers=headers, timeout=args.timeout)
        print(f"[ping] HTTP {r0.status_code} len={len(r0.content)}")
        report["output"]["ping"] = {
            "url": list_url,
            "http_status": r0.status_code,
            "response_length": len(r0.content),
            "response_preview": r0.text[:2000] if r0.text else "",
        }
        if not r0.ok:
            print(r0.text[:800], file=sys.stderr)

    print(f"[batch] POST {batch_url}")
    print(f"[batch] process_id={process_id} files={len(batch_items)}")

    r = requests.post(
        batch_url,
        headers=headers,
        json=payload,
        timeout=args.timeout,
    )

    report["output"]["batch_post"] = {
        "http_status": r.status_code,
        "response_headers": dict(r.headers),
    }

    if not r.ok:
        report["output"]["batch_post"]["response_text"] = r.text[:16000]
        print(f"[batch] falhou HTTP {r.status_code}", file=sys.stderr)
        print(r.text[:4000], file=sys.stderr)
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
        return 1

    try:
        pres = r.json()
    except json.JSONDecodeError:
        report["output"]["batch_post"]["response_text"] = r.text[:16000]
        print(r.text[:2000], file=sys.stderr)
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
        return 1

    report["output"]["batch_post"]["response_body"] = pres

    print(json.dumps(pres, indent=2, ensure_ascii=False))

    if args.dry_run:
        print("[dry-run] Sem PUT S3.")
        report["output"]["note"] = "dry-run: PUT S3 não executado"
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}")
        return 0

    files_out = pres.get("files") or []
    if len(files_out) != len(paths):
        report["output"]["error"] = (
            f"Resposta com {len(files_out)} arquivos, enviados {len(paths)}"
        )
        print("Resposta com número de arquivos diferente do enviado.", file=sys.stderr)
        if not args.no_save_report:
            _write_report(args.save_report, report)
            print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
        return 1

    puts_log: list[dict] = []
    for local_path, meta in zip(paths, files_out):
        body = local_path.read_bytes()
        put_url = meta.get("upload_url")
        ct = meta.get("content_type") or uac._content_type_for_path(local_path)
        if not put_url:
            report["output"]["error"] = "Resposta sem upload_url"
            report["output"]["bad_meta"] = meta
            print("Resposta sem upload_url:", meta, file=sys.stderr)
            if not args.no_save_report:
                _write_report(args.save_report, report)
                print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
            return 1
        put = requests.put(
            put_url,
            data=body,
            headers={"Content-Type": ct},
            timeout=args.timeout,
        )
        entry = {
            "local_path": str(local_path),
            "target_file_name": meta.get("file_name"),
            "content_type": ct,
            "bytes_sent": len(body),
            "http_status": put.status_code,
            "response_preview": (put.text[:500] if put.text else ""),
        }
        puts_log.append(entry)
        if not put.ok:
            report["output"]["s3_puts"] = puts_log
            report["output"]["error"] = f"PUT falhou {local_path.name}: HTTP {put.status_code}"
            print(
                f"PUT falhou {local_path.name}: HTTP {put.status_code} {put.text[:400]}",
                file=sys.stderr,
            )
            if not args.no_save_report:
                _write_report(args.save_report, report)
                print(f"[relatório] salvo em {args.save_report.resolve()}", file=sys.stderr)
            return 1
        print(f"[ok] uploaded {local_path.name} -> {meta.get('file_name')}")

    report["output"]["s3_puts"] = puts_log
    report["output"]["exit"] = "success"

    print("\nConcluído. Opcional: POST {base}/start com {\"process_id\": ...}".replace("{base}", base))
    if not args.no_save_report:
        _write_report(args.save_report, report)
        print(f"[relatório] salvo em {args.save_report.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
