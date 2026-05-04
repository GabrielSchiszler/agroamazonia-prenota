#!/usr/bin/env python3
"""
Varre processos no DynamoDB e classifica linhas de produto que tenham bloco `rastro` no XML parseado:

**Com qLote informado** (`rastro[].quantidade` preenchido) — compara com qCom (`produtos[].quantidade`):
  - divergencia — qCom ≠ qLote
  - igualdade_qcom_qlote — qCom = qLote (mesmo número; handler ainda pode ler do rastro)

**Sem quantidade no lote** — existe entrada de rastro mas sem valor em `quantidade` (sem qLote):
  - sem_quantidade_lote — não entra nas contagens de divergência/igualdade; bucket à parte

`processes_scanned` = quantidade de processos na tabela. As demais contagens são por **registro**
(linha de produto × entrada de rastro), não somam ao número de processos.

Requer credenciais AWS com leitura na tabela.

Uso:
    python3 find_processes_qtd_rastro_vs_qcom.py --table-name tabela-document-processor-stg --region sa-east-1
    python3 find_processes_qtd_rastro_vs_qcom.py --table-name ... --csv divergencias.csv
    python3 find_processes_qtd_rastro_vs_qcom.py --table-name ... --process-id <uuid>
    python3 find_processes_qtd_rastro_vs_qcom.py --table-name ... --json-file /tmp/out.json

Saída:
    - JSON Lines no stdout: **um objeto JSON por process_id** (inclui `numero_nota` / `serie` / `chave_acesso` do XML de referencia).
    - JSON `summary.divergencias_e_status_final_pipeline`:
      `lista_com_divergencia_e_completed_oid_numero_nota` — divergencia + COMPLETED, com flags `tem_mais_de_um_rastro_no_item` e `linhas_produto_com_multiplo_rastro`;
      `lista_com_divergencia_e_completed_so_com_multiplo_lote` — só os que tem linha de produto com 2+ rastros (multiplo lote no XML).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import boto3


TOL = 1e-4


def summarize_divergence_process_statuses(bundles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Entre processos com ao menos uma divergencia qCom≠qLote, agrega STATUS do METADATA.
    COMPLETED ~ fluxo concluido apos send_to_protheus (sucesso do pipeline).
    """
    empty_lists = {
        "process_ids_com_divergencia_status_completed": [],
        "process_ids_com_divergencia_status_nao_completed": [],
        "process_ids_com_divergencia_agrupados_por_status": {},
    }
    com_div = [b for b in bundles if (b.get("divergences") or [])]
    if not com_div:
        return {
            "processos_com_ao_menos_uma_divergencia": 0,
            "sucesso_pipeline_completed": 0,
            "nao_completed_total": 0,
            "detalhe_por_status": {},
            **empty_lists,
            "nota": (
                "STATUS lido do DynamoDB METADATA no momento do scan. "
                "COMPLETED: pipeline terminou com sucesso (inclui gravacao pos-envio ao Protheus). "
                "Demais valores: processo nao chegou a esse estado (validacao, erro na Lambda, timeout, etc.). "
                "process_ids_com_divergencia_status_nao_completed = IDs com divergencia e STATUS diferente de COMPLETED."
            ),
        }

    counter: Counter[str] = Counter()
    ids_completed: List[str] = []
    ids_nao_completed: List[str] = []
    ids_por_status: Dict[str, List[str]] = {}

    for b in com_div:
        pid = b.get("process_id")
        if not pid:
            continue
        st = b.get("status")
        key = str(st) if st is not None and str(st).strip() != "" else "(sem_status)"
        counter[key] += 1
        ids_por_status.setdefault(key, []).append(pid)
        if st == "COMPLETED":
            ids_completed.append(pid)
        else:
            ids_nao_completed.append(pid)

    completed = counter.get("COMPLETED", 0)
    total = len(com_div)

    agrupados = {k: sorted(set(v)) for k, v in sorted(ids_por_status.items())}

    return {
        "processos_com_ao_menos_uma_divergencia": total,
        "sucesso_pipeline_completed": completed,
        "nao_completed_total": total - completed,
        "detalhe_por_status": dict(sorted(counter.items())),
        "process_ids_com_divergencia_status_completed": sorted(set(ids_completed)),
        "process_ids_com_divergencia_status_nao_completed": sorted(set(ids_nao_completed)),
        "process_ids_com_divergencia_agrupados_por_status": agrupados,
        "nota": (
            "STATUS lido do DynamoDB METADATA no momento do scan. "
            "sucesso_pipeline_completed = processos com divergencia e STATUS COMPLETED (fluxo chegou ao fim apos send_to_protheus). "
            "process_ids_com_divergencia_status_nao_completed = divergencia qCom≠qLote e pipeline nao terminou como COMPLETED "
            "(FAILED, VALIDATION_FAILURE, etc. — ver etapa em error_info/logs). "
            "process_ids_com_divergencia_agrupados_por_status lista todos os IDs por valor de STATUS."
        ),
    }


def lista_divergencia_completed_oid_e_nota(
    bundles: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Processos com ao menos uma divergencia qCom≠qLote e STATUS COMPLETED.
    Retorna (lista completa, sublista apenas onde ha linha de produto com 2+ rastros/lotes no XML).
    """
    rows: List[Dict[str, Any]] = []
    for b in bundles:
        if not (b.get("divergences") or []):
            continue
        if b.get("status") != "COMPLETED":
            continue
        pid = b["process_id"]
        divs = b.get("divergences") or []
        # Uma "linha" = mesmo XML + indice do produto no det; 2+ rastros = multiplo lote naquele item
        chaves_multi = {
            (r.get("parsed_file"), r.get("produto_index_xml"))
            for r in divs
            if (r.get("num_rastros_na_linha") or 0) >= 2
        }
        tem_multiplo = len(chaves_multi) > 0
        rows.append(
            {
                "process_id": pid,
                "oid": pid,
                "numero_nota": b.get("numero_nota"),
                "serie": b.get("serie"),
                "chave_acesso": b.get("chave_acesso"),
                "tem_mais_de_um_rastro_no_item": tem_multiplo,
                "linhas_produto_com_multiplo_rastro": len(chaves_multi),
            }
        )
    rows.sort(key=lambda x: x["process_id"])
    so_multiplo = [r for r in rows if r["tem_mais_de_um_rastro_no_item"]]
    return rows, so_multiplo


def get_all_process_index_rows(table) -> List[Dict[str, Any]]:
    """Índice PK=PROCESS com SK começando em PROCESS# (mesmo padrão de fix_metrics.py)."""
    rows: List[Dict[str, Any]] = []
    kwargs = {
        "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk)",
        "ExpressionAttributeValues": {":pk": "PROCESS", ":sk": "PROCESS#"},
    }
    while True:
        resp = table.query(**kwargs)
        rows.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return rows


def query_process_items(table, process_id: str) -> List[Dict[str, Any]]:
    pk = f"PROCESS#{process_id}"
    items: List[Dict[str, Any]] = []
    kwargs = {
        "KeyConditionExpression": "PK = :pk",
        "ExpressionAttributeValues": {":pk": pk},
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize_rastros(rastro_field: Any) -> List[Dict[str, Any]]:
    if not rastro_field:
        return []
    if isinstance(rastro_field, dict):
        return [rastro_field]
    if isinstance(rastro_field, list):
        return [r for r in rastro_field if isinstance(r, dict)]
    return []


def infer_rule_hint(num_rastros_com_qtd: int, num_rastros: int) -> str:
    if num_rastros_com_qtd == 0:
        return "rastro_sem_qLote_usaria_qCom"
    if num_rastros == 1:
        return "um_lote_com_qLote_send_to_protheus_usa_qLote_no_item"
    return "multiplos_rastros_split_por_qLote"


def _row_base(
    idx: int,
    produto: Dict[str, Any],
    ri: int,
    r: Dict[str, Any],
    q_com: float,
    q_lote: float,
    rastros: List[Dict[str, Any]],
    chave: Any,
    hint: str,
    registro: str,
) -> Dict[str, Any]:
    delta = round(q_lote - q_com, 6)
    base = {
        "registro": registro,
        "produto_index_xml": idx + 1,
        "rastro_index": ri + 1,
        "descricao": (produto.get("descricao") or "")[:120],
        "codigo_xml": produto.get("codigo"),
        "q_com": q_com,
        "q_lote": q_lote,
        "delta": delta,
        "alteracao_numerica": abs(q_lote - q_com) > TOL,
        "lote_numero": r.get("lote"),
        "num_rastros_na_linha": len(rastros),
        "chave_acesso": chave,
        "regra_send_to_protheus_hint": hint,
    }
    if registro == "igualdade_qcom_qlote":
        base["observacao"] = (
            "qCom e qLote coincidem numericamente; mesmo assim process_produtos_with_lotes "
            "usa rastro[].quantidade no item quando qLote esta informado. "
            "Para sempre usar apenas qCom quando iguais, alterar send_to_protheus/handler.py."
        )
    return base


def _row_sem_quantidade_lote(
    idx: int,
    produto: Dict[str, Any],
    ri: int,
    r: Dict[str, Any],
    q_com: float,
    rastros: List[Dict[str, Any]],
    chave: Any,
    hint: str,
) -> Dict[str, Any]:
    return {
        "registro": "sem_quantidade_lote",
        "produto_index_xml": idx + 1,
        "rastro_index": ri + 1,
        "descricao": (produto.get("descricao") or "")[:120],
        "codigo_xml": produto.get("codigo"),
        "q_com": q_com,
        "q_lote": None,
        "delta": None,
        "alteracao_numerica": None,
        "lote_numero": r.get("lote"),
        "num_rastros_na_linha": len(rastros),
        "chave_acesso": chave,
        "regra_send_to_protheus_hint": hint,
        "observacao": (
            "Bloco rastro presente mas sem campo quantidade (qLote vazio/ausente). "
            "Nao ha comparacao lote x item; send_to_protheus usa quantidade comercial (qCom) "
            "nessa situacao (ex.: lote unico sem qtd preenchida)."
        ),
    }


def analyze_produtos_split(
    parsed: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Retorna (divergencias, igualdades com qLote, sem qLote no rastro) por entrada de rastro."""
    divergencias: List[Dict[str, Any]] = []
    igualdades: List[Dict[str, Any]] = []
    sem_qtd_lote: List[Dict[str, Any]] = []
    produtos = parsed.get("produtos") or []
    if not isinstance(produtos, list):
        return divergencias, igualdades, sem_qtd_lote

    chave = parsed.get("chave_acesso") or parsed.get("chaveAcesso")

    for idx, produto in enumerate(produtos):
        if not isinstance(produto, dict):
            continue
        q_com = safe_float(produto.get("quantidade"))
        if q_com is None:
            continue

        rastros = normalize_rastros(produto.get("rastro"))
        if not rastros:
            continue

        com_qtd = [r for r in rastros if safe_float(r.get("quantidade")) is not None]
        hint = infer_rule_hint(len(com_qtd), len(rastros))

        for ri, r in enumerate(rastros):
            q_lote = safe_float(r.get("quantidade"))
            if q_lote is None:
                sem_qtd_lote.append(_row_sem_quantidade_lote(idx, produto, ri, r, q_com, rastros, chave, hint))
                continue
            if abs(q_lote - q_com) <= TOL:
                igualdades.append(
                    _row_base(idx, produto, ri, r, q_com, q_lote, rastros, chave, hint, "igualdade_qcom_qlote")
                )
            else:
                divergencias.append(
                    _row_base(idx, produto, ri, r, q_com, q_lote, rastros, chave, hint, "divergencia")
                )

    return divergencias, igualdades, sem_qtd_lote


def load_parsed_data(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def scan_process(
    table,
    process_id: str,
    metadata_by_pid: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Agrega divergencias / igualdades / sem_quantidade_lote para um único process_id.
    Retorna None se não houver nenhum registro relevante (XML sem rastro+qCom etc.).
    """
    metadata_by_pid = metadata_by_pid or {}
    items = query_process_items(table, process_id)
    meta_item = next((i for i in items if i.get("SK") == "METADATA"), None)
    meta = dict(metadata_by_pid.get(process_id) or {})
    if meta_item:
        meta = {**meta, **meta_item}

    status = meta.get("STATUS")
    process_type = meta.get("PROCESS_TYPE")

    divergencias: List[Dict[str, Any]] = []
    igualdades: List[Dict[str, Any]] = []
    sem_qtd: List[Dict[str, Any]] = []
    nf_principal: Optional[Dict[str, Any]] = None

    for item in items:
        sk = item.get("SK") or ""
        if not sk.startswith("PARSED_XML"):
            continue
        parsed = load_parsed_data(item.get("PARSED_DATA"))
        if not parsed:
            continue
        file_name = item.get("FILE_NAME") or sk

        d, i, s = analyze_produtos_split(parsed)
        if nf_principal is None and (d or i or s):
            nf_principal = {
                "numero_nota": parsed.get("numero_nota"),
                "serie": parsed.get("serie"),
                "chave_acesso": parsed.get("chave_acesso"),
                "parsed_file_referencia": file_name,
            }
        for row in d:
            divergencias.append({"parsed_file": file_name, **row})
        for row in i:
            igualdades.append({"parsed_file": file_name, **row})
        for row in s:
            sem_qtd.append({"parsed_file": file_name, **row})

    total = len(divergencias) + len(igualdades) + len(sem_qtd)
    if total == 0:
        return None

    bundle: Dict[str, Any] = {
        "process_id": process_id,
        "status": status,
        "process_type": process_type,
        "counts": {
            "divergences": len(divergencias),
            "matches_qcom_equals_qlote": len(igualdades),
            "sem_quantidade_lote": len(sem_qtd),
            "total_registros": total,
        },
        "divergences": divergencias,
        "matches_qcom_equals_qlote": igualdades,
        "sem_quantidade_lote": sem_qtd,
    }
    if nf_principal:
        bundle["numero_nota"] = nf_principal.get("numero_nota")
        bundle["serie"] = nf_principal.get("serie")
        bundle["chave_acesso"] = nf_principal.get("chave_acesso")
        bundle["parsed_file_referencia_nf"] = nf_principal.get("parsed_file_referencia")
    else:
        bundle["numero_nota"] = None
        bundle["serie"] = None
        bundle["chave_acesso"] = None
        bundle["parsed_file_referencia_nf"] = None

    return bundle


def flatten_process_rows(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Rows planas para CSV (repete process_id / status / process_type em cada linha)."""
    pid = bundle["process_id"]
    status = bundle.get("status")
    ptype = bundle.get("process_type")
    nf = {
        "numero_nota": bundle.get("numero_nota"),
        "serie": bundle.get("serie"),
        "chave_acesso": bundle.get("chave_acesso"),
    }
    out: List[Dict[str, Any]] = []
    for key in ("divergences", "matches_qcom_equals_qlote", "sem_quantidade_lote"):
        for r in bundle.get(key) or []:
            out.append({"process_id": pid, "status": status, "process_type": ptype, **nf, **r})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Classifica rastro x quantidade: divergencias e igualdades quando ha qLote; "
            "registros sem_quantidade_lote quando ha rastro mas sem quantidade no lote."
        )
    )
    parser.add_argument("--table-name", required=True, help="Nome da tabela DynamoDB")
    parser.add_argument("--region", default="sa-east-1", help="Região AWS (default: sa-east-1)")
    parser.add_argument(
        "--process-id",
        action="append",
        dest="process_ids",
        metavar="UUID",
        help="Analisar apenas este process_id (pode repetir a flag)",
    )
    parser.add_argument(
        "--csv",
        metavar="FILE",
        help="Gravar também CSV neste caminho (utf-8)",
    )
    parser.add_argument(
        "--json-file",
        default="divergencias_qtd_rastro_vs_qcom.json",
        metavar="FILE",
        help="Relatorio JSON: summary + array processes (agrupado por process_id). Default: divergencias_qtd_rastro_vs_qcom.json",
    )
    parser.add_argument(
        "--no-json-file",
        action="store_true",
        help="Não gravar arquivo JSON (apenas stdout / --csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limitar quantidade de processos analisados (0 = sem limite; útil para teste)",
    )
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table_name)

    if args.process_ids:
        pid_list = args.process_ids
        meta_by_pid: Dict[str, Dict[str, Any]] = {}
    else:
        index_rows = get_all_process_index_rows(table)
        pid_list = []
        meta_by_pid = {}
        for row in index_rows:
            pid = row.get("PROCESS_ID")
            if pid:
                pid_list.append(pid)
                meta_by_pid[pid] = row
        pid_list.sort()

    if args.limit and args.limit > 0:
        pid_list = pid_list[: args.limit]

    bundles: List[Dict[str, Any]] = []
    count_processes = 0

    for pid in pid_list:
        count_processes += 1
        b = scan_process(table, pid, meta_by_pid)
        if b:
            bundles.append(b)

    bundles.sort(key=lambda x: x["process_id"])

    for b in bundles:
        print(json.dumps(b, ensure_ascii=False, default=str))

    tot_div = sum(b["counts"]["divergences"] for b in bundles)
    tot_igual = sum(b["counts"]["matches_qcom_equals_qlote"] for b in bundles)
    tot_sem = sum(b["counts"]["sem_quantidade_lote"] for b in bundles)
    comp_com_qtd = tot_div + tot_igual

    div_pipeline = summarize_divergence_process_statuses(bundles)
    lista_ok_full, lista_ok_so_multi = lista_divergencia_completed_oid_e_nota(bundles)
    div_pipeline["lista_com_divergencia_e_completed_oid_numero_nota"] = lista_ok_full
    div_pipeline["lista_com_divergencia_e_completed_so_com_multiplo_lote"] = lista_ok_so_multi

    fieldnames = [
        "registro",
        "process_id",
        "status",
        "process_type",
        "numero_nota",
        "serie",
        "chave_acesso",
        "parsed_file",
        "produto_index_xml",
        "rastro_index",
        "descricao",
        "codigo_xml",
        "q_com",
        "q_lote",
        "delta",
        "alteracao_numerica",
        "lote_numero",
        "num_rastros_na_linha",
        "chave_acesso",
        "regra_send_to_protheus_hint",
        "observacao",
    ]

    print(
        f"# RESUMO processos varridos (PK): {count_processes}",
        file=sys.stderr,
    )
    print(
        f"# Processos com ao menos 1 registro (rastro/qtd): {len(bundles)}",
        file=sys.stderr,
    )
    print(
        f"# Com qLote no rastro (comparacao qCom x qLote): total={comp_com_qtd} | "
        f"divergencias (qCom≠qLote)={tot_div} | igual (qCom=qLote)={tot_igual}",
        file=sys.stderr,
    )
    print(
        f"# Rastro sem quantidade (sem qLote): {tot_sem}",
        file=sys.stderr,
    )
    print(
        f"# Processos com divergencia qCom≠qLote — status METADATA: "
        f"COMPLETED={div_pipeline.get('sucesso_pipeline_completed', 0)} (pipeline ok pos-Protheus), "
        f"nao_COMPLETED={div_pipeline.get('nao_completed_total', 0)}, "
        f"total_processos_afetados={div_pipeline.get('processos_com_ao_menos_uma_divergencia', 0)}",
        file=sys.stderr,
    )
    nao_ids = div_pipeline.get("process_ids_com_divergencia_status_nao_completed") or []
    print(
        f"# Lista de process_ids com divergencia e status != COMPLETED: {len(nao_ids)} "
        f"(ver JSON summary.divergencias_e_status_final_pipeline.process_ids_com_divergencia_status_nao_completed)",
        file=sys.stderr,
    )
    ok_list = div_pipeline.get("lista_com_divergencia_e_completed_oid_numero_nota") or []
    print(
        f"# Divergencia qCom≠qLote + COMPLETED (oid + numero_nota): {len(ok_list)} "
        f"→ summary.divergencias_e_status_final_pipeline.lista_com_divergencia_e_completed_oid_numero_nota",
        file=sys.stderr,
    )
    ok_multi = div_pipeline.get("lista_com_divergencia_e_completed_so_com_multiplo_lote") or []
    print(
        f"# Destes, com mais de 1 rastro/lote na mesma linha de produto (XML): {len(ok_multi)} "
        f"→ lista_com_divergencia_e_completed_so_com_multiplo_lote",
        file=sys.stderr,
    )

    if not args.no_json_file:
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "table_name": args.table_name,
            "region": args.region,
            "process_ids_filter": args.process_ids,
            "limit_processes": args.limit or None,
            "summary": {
                "processes_scanned": count_processes,
                "processes_com_registros_rastro_qtd": len(bundles),
                "nota": (
                    "processes_scanned = processos percorridos na lista. "
                    "processes_com_registros = processos que geraram ao menos uma linha neste relatorio. "
                    "Contagens de divergencia/igual/sem lote = registros (item x rastro), nao processos."
                ),
                "com_quantidade_no_lote_para_comparar": comp_com_qtd,
                "divergence_count_qCom_diff_qLote": tot_div,
                "match_equal_count_qCom_eq_qLote": tot_igual,
                "sem_quantidade_no_rastro_count": tot_sem,
                "checagem": {
                    "com_qLote_soma": tot_div + tot_igual,
                    "formula": "divergence_count + match_equal_count == com_quantidade_no_lote_para_comparar",
                },
                "divergencias_e_status_final_pipeline": div_pipeline,
            },
            "processes": bundles,
        }
        json_path = args.json_file
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"# JSON escrito: {json_path}", file=sys.stderr)

    if args.csv:
        order = {"divergencia": 0, "igualdade_qcom_qlote": 1, "sem_quantidade_lote": 2}
        merged: List[Dict[str, Any]] = []
        for b in bundles:
            merged.extend(flatten_process_rows(b))
        merged.sort(
            key=lambda x: (
                x.get("process_id", ""),
                order.get(x.get("registro", ""), 9),
                x.get("parsed_file", ""),
                x.get("produto_index_xml", 0),
            )
        )
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", restval="")
            w.writeheader()
            for r in merged:
                w.writerow(r)
        print(f"# CSV escrito: {args.csv}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
