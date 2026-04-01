#!/usr/bin/env python3
"""
Cria o processo OPTERADUO multi-lote chamando ProcessService diretamente (sem API HTTP):
DynamoDB + S3 + link_pedido_compra_metadata + start_process (Step Functions).

Requer credenciais AWS e variáveis (export ou arquivo .env — export tem prioridade):

  TABLE_NAME, BUCKET_NAME, STATE_MACHINE_ARN
  AWS_REGION ou AWS_DEFAULT_REGION (opcional)

Os três devem ser do MESMO ambiente (ex.: tudo stg ou tudo prd). Se BUCKET_NAME for prd e
STATE_MACHINE_ARN for stg, o XML sobe num bucket e o parse_xml lê outro → NoSuchKey.

Uso:
  cd backend/scripts
  export TABLE_NAME=... BUCKET_NAME=... STATE_MACHINE_ARN=...
  # ou: --env-file ../.env.homolog
  python3 test_process_multilot_opteraduo_direct.py --start

  python3 test_process_multilot_opteraduo_direct.py --env-file ../.env.homolog --start
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_ROOT = _SCRIPT_DIR.parent
DEFAULT_XML_NAME = "23260307467822000126551010000878991216037201.xml"

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


def _parse_env_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None
    line = line.replace("export ", "", 1)
    k, _, v = line.partition("=")
    k, v = k.strip(), v.strip().strip('"').strip("'")
    return (k, v) if k else None


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        p = _parse_env_line(line)
        if p:
            out[p[0]] = p[1]
    return out


def merge_env_file(path: Path) -> None:
    """Preenche os.environ só onde a chave ainda não existe (export no shell ganha)."""
    for k, v in _load_env_file(path).items():
        if k not in os.environ and v:
            os.environ[k] = v


def _default_env_file() -> Path | None:
    for p in (_SCRIPT_DIR / ".env", _BACKEND_ROOT / ".env.homolog"):
        if p.is_file():
            return p
    return None


def _resource_tier(name: str) -> str | None:
    """
    Identifica stg vs prd pelos padrões usados nos recursos (bucket, tabela, state machine).
    Retorna None se não der para classificar.
    """
    n = name.lower()
    stg = bool(
        re.search(r"[-_]stg[-_]|[-_]stg$|workflow-stg|processor-stg|documents-stg", n)
        or "homolog" in n
        or re.search(r"\bhml\b", n)
    )
    prd = bool(
        re.search(r"[-_]prd[-_]|[-_]prd$|documents-prd|processor-prd", n)
        or re.search(r"[-_]prod[-_]", n)
    )
    if stg and prd:
        return None
    if stg:
        return "stg"
    if prd:
        return "prd"
    return None


def _assert_stack_alignment(*, skip: bool) -> None:
    bucket = os.environ.get("BUCKET_NAME", "")
    table = os.environ.get("TABLE_NAME", "")
    sm = os.environ.get("STATE_MACHINE_ARN", "")
    tagged = [
        ("BUCKET_NAME", bucket, _resource_tier(bucket)),
        ("TABLE_NAME", table, _resource_tier(table)),
        ("STATE_MACHINE_ARN", sm, _resource_tier(sm)),
    ]
    tiers = [t for _, _, t in tagged if t]
    if skip:
        if tiers:
            logging.warning(
                "Checagem stg/prd ignorada (--skip-env-check). Tiers detectados: %s",
                [(a, b) for a, _, b in tagged if b],
            )
        return
    if len(tiers) < 2:
        return
    unique = set(tiers)
    if len(unique) <= 1:
        return
    lines = "\n".join(
        f"  - {label}: tier={tier or '?'}  ({value[:70]}…)" if len(value) > 70 else f"  - {label}: tier={tier or '?'}  ({value})"
        for label, value, tier in tagged
    )
    raise ValueError(
        "Inconsistência de ambiente: BUCKET_NAME, TABLE_NAME e STATE_MACHINE_ARN "
        "parecem misturar stg e prd. O parse_xml usa o bucket da stack onde a Step "
        "Functions roda — o XML precisa estar nesse mesmo bucket.\n"
        f"{lines}\n"
        "Ajuste os exports para o mesmo ambiente (ex.: só .env.homolog stg, ou só prd). "
        "Em último caso: --skip-env-check"
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Processo OPTERADUO multi-lote via ProcessService (sem API HTTP)"
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Arquivo .env (export no shell continua com prioridade)",
    )
    parser.add_argument(
        "--xml-file",
        default="",
        help=f"XML DANFE (padrão: {_SCRIPT_DIR / DEFAULT_XML_NAME})",
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Chamar start_process (Step Functions) após upload e metadados",
    )
    parser.add_argument(
        "--process-id",
        default="",
        help="UUID fixo (opcional); senão gera um novo",
    )
    parser.add_argument(
        "--skip-env-check",
        action="store_true",
        help="Não validar se bucket/tabela/state machine são do mesmo ambiente (stg/prd)",
    )
    args = parser.parse_args()

    if args.env_file:
        merge_env_file(Path(args.env_file))
    else:
        auto = _default_env_file()
        if auto:
            merge_env_file(auto)
            logging.info("Variáveis do arquivo (sem sobrescrever export): %s", auto)

    required = ("TABLE_NAME", "BUCKET_NAME", "STATE_MACHINE_ARN")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(
            "Defina no shell ou no .env: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 1

    if not os.environ.get("AWS_DEFAULT_REGION") and os.environ.get("AWS_REGION"):
        os.environ["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION"]

    try:
        _assert_stack_alignment(skip=args.skip_env_check)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    sys.path.insert(0, str(_BACKEND_ROOT))
    from src.services.process_service import ProcessService

    xml_path = Path(args.xml_file).resolve() if args.xml_file else _SCRIPT_DIR / DEFAULT_XML_NAME
    if not xml_path.is_file():
        print(f"XML não encontrado: {xml_path}", file=sys.stderr)
        return 1

    process_id = args.process_id.strip() or str(uuid.uuid4())
    svc = ProcessService()

    logging.info("process_id=%s", process_id)
    logging.info("XML %s (%s bytes)", xml_path, xml_path.stat().st_size)

    pres = svc.generate_presigned_url(
        process_id,
        xml_path.name,
        "application/xml",
        doc_type="DANFE",
    )
    file_key = pres["file_key"]
    body = xml_path.read_bytes()

    svc.s3_client.put_object(
        Bucket=svc.bucket_name,
        Key=file_key,
        Body=body,
        ContentType="application/xml",
    )
    logging.info("S3 put_object OK: s3://%s/%s", svc.bucket_name, file_key)

    svc.link_pedido_compra_metadata(process_id, PEDIDO_MULTILOT_OPTERADUO)
    logging.info("Metadados pedido vinculados (PEDIDO_COMPRA_METADATA)")

    if not args.start:
        print(json.dumps({"process_id": process_id, "file_key": file_key, "started": False}, indent=2))
        print("Use --start para disparar a Step Functions.", file=sys.stderr)
        return 0

    out = svc.start_process(process_id)
    print(json.dumps({**out, "file_key": file_key}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
