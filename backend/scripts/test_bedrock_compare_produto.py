#!/usr/bin/env python3
"""
Chama o mesmo compare_with_bedrock() da regra validar_produtos e imprime o JSON
retornado pelo modelo (status + bedrock_analise: explicacao, nome_base, etc.).

Padrão: par EXPEDITION (pedido/DANFE — caso prd 26a269164744cb14019fa562036d4313).
Outros exemplos: VESSARYA, OPTERADUO.

Requisitos:
  - Credenciais AWS com bedrock:InvokeModel em us-east-1 (o cliente é fixo em us-east-1).
  - Opcional: BEDROCK_MODEL_ID (default amazon.nova-pro-v1:0)

Uso:
  cd backend/scripts
  export AWS_PROFILE=...   # ou AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
  python3 test_bedrock_compare_produto.py

  python3 test_bedrock_compare_produto.py \\
    --produto1 "OPTERADUO 1X20L" \\
    --produto2 "OPTERADUO GL 20 LT"

  python3 test_bedrock_compare_produto.py --equivalent-code
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_VALIDATE_RULES = _SCRIPT_DIR.parent / "lambdas" / "validate_rules"

# Nomes padrão (pedido x DANFE — GL vs bombona, mesmo produto)
DEFAULT_P1 = "EXPEDITION GL 10 LT"
DEFAULT_P2 = "EXPEDITION BOMBONA 10L INSETICIDA"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Testa comparação de nomes de produto via Bedrock (mesmo código da validação)"
    )
    parser.add_argument("--produto1", default=DEFAULT_P1, help="Texto produto lado 1 (ex.: pedido)")
    parser.add_argument("--produto2", default=DEFAULT_P2, help="Texto produto lado 2 (ex.: DANFE)")
    parser.add_argument(
        "--equivalent-code",
        action="store_true",
        help="Simula códigos já validados como equivalentes (has_equivalent_code=True)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log DEBUG (inclui resposta bruta do Bedrock no logger)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if not _VALIDATE_RULES.is_dir():
        print(f"Pasta validate_rules não encontrada: {_VALIDATE_RULES}", file=sys.stderr)
        return 1

    sys.path.insert(0, str(_VALIDATE_RULES))
    from rules.utils import bedrock_compare_status, compare_with_bedrock

    print("Produto 1:", args.produto1)
    print("Produto 2:", args.produto2)
    print("has_equivalent_code:", args.equivalent_code)
    print("BEDROCK_MODEL_ID:", os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0"))
    print("-" * 60)

    result = compare_with_bedrock(
        args.produto1,
        args.produto2,
        "nome do produto",
        has_equivalent_code=args.equivalent_code,
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("-" * 60)
    print("status (MATCH/MISMATCH):", bedrock_compare_status(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
