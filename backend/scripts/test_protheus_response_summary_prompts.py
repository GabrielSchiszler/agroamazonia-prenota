#!/usr/bin/env python3
"""
Testa o prompt de response_summary para SUCESSO (bedrock_success_summary).

Mesma entrada que o erro no send_feedback:
  {"process_id", "success": true, "details": organized_details}

Uso:
  cd backend && python3 scripts/test_protheus_response_summary_prompts.py --dry-run
  python3 scripts/test_protheus_response_summary_prompts.py --scenario prenota --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_LAMBDAS = os.path.normpath(os.path.join(_SCRIPTS_DIR, "..", "lambdas"))
if _LAMBDAS not in sys.path:
    sys.path.insert(0, _LAMBDAS)

from utils.bedrock_success_summary import (  # noqa: E402
    build_success_feedback_summary_prompt,
    generate_success_feedback_summary_with_bedrock,
)


def _organized_prenota() -> dict:
    """details como no feedback (payload_req, response_req, etc.)."""
    return {
        "process_type": "AGROQUIMICOS",
        "status": "SUCCESS",
        "payload_req": {
            "tipoDeDocumento": "N",
            "documento": "4714",
            "serie": "001",
            "dataEmissao": "2026-02-15",
            "chaveAcesso": "52260243267029000309550010000047141505337014",
            "itens": [
                {
                    "codigoProduto": "EXEMPLO",
                    "quantidade": 1,
                    "pedidoDeCompra": {"pedidoErp": "PED1", "itemPedidoErp": "0001"},
                }
            ],
        },
        "header_req": {"Content-Type": "application/json", "tenantId": "00,010132"},
        "response_req": {
            "status_code": 201,
            "body": {
                "message": (
                    "Documento de entrada criado como pré-nota devido a erros na classificação. "
                    "Verifique o log para mais detalhes."
                ),
                "details": [
                    "AJUDA:VALIDAÇÃO DE NOTA FISCAL VS XML\r\n"
                    "File not found : d:\\totvs\\ambiente_hml_03\\app_server\\protheus_data\\web\\nfe\\"
                    "00010149\\202602\\52260243267029000309550010000047141505337014.xml\r\n",
                    "Tabela SX3 25/03/2026 15:22:34",
                    "Inconsistencia nos Itens",
                    "--------------------------------------------------------------------",
                ],
            },
        },
    }


def _organized_sucesso() -> dict:
    return {
        "process_type": "AGROQUIMICOS",
        "status": "SUCCESS",
        "start_time": "2026-03-26T14:51:03.607516Z",
        "end_time": "2026-03-26T14:51:51.607632Z",
        "payload_req": {
            "tipoDeDocumento": "N",
            "documento": "18656",
            "serie": "005",
            "dataEmissao": "2026-03-25",
            "chaveAcesso": "52260347180625005881550050000186561698647138",
            "itens": [
                {
                    "codigoProduto": "9OF00004FR480G0",
                    "quantidade": 23.04,
                    "pedidoDeCompra": {"pedidoErp": "AACBKE", "itemPedidoErp": "0001"},
                }
            ],
        },
        "header_req": {"Content-Type": "application/json", "tenantId": "00,010132"},
        "response_req": {
            "status_code": 201,
            "body": {"message": "Documento de entrada criado com sucesso."},
        },
    }


def fixture_prenota() -> dict:
    return {
        "process_id": "exemplo-prenota",
        "success": True,
        "details": _organized_prenota(),
    }


def fixture_sucesso_pleno() -> dict:
    return {
        "process_id": "353073a647f37650b678ec40f26d4360",
        "success": True,
        "details": _organized_sucesso(),
    }


SCENARIOS = {
    "prenota": fixture_prenota,
    "sucesso": fixture_sucesso_pleno,
}


def run_one(name: str, data: dict, dry_run: bool) -> None:
    print(f"\n{'=' * 80}\nCenário: {name}\n{'=' * 80}")
    prompt = build_success_feedback_summary_prompt(data)
    if dry_run:
        print(prompt)
        print(f"\n--- Tamanho do prompt: {len(prompt)} caracteres ---\n")
        return
    summary = generate_success_feedback_summary_with_bedrock(data)
    if summary:
        print(summary)
    else:
        print("(Bedrock não retornou texto — verifique credenciais, modelo e região us-east-1.)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Testa prompt de sucesso (mesma entrada que o erro)")
    parser.add_argument("--dry-run", action="store_true", help="Só imprime o prompt")
    parser.add_argument(
        "--scenario",
        choices=["prenota", "sucesso", "all"],
        default="all",
    )
    args = parser.parse_args()

    if args.scenario == "all":
        for key in ("prenota", "sucesso"):
            run_one(key, SCENARIOS[key](), args.dry_run)
    else:
        run_one(args.scenario, SCENARIOS[args.scenario](), args.dry_run)

    if not args.dry_run:
        print("\nExemplo de payload de feedback (ServiceNow) após Bedrock:\n")
        sample = {**fixture_sucesso_pleno(), "response_summary": "<saída do modelo>"}
        print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
