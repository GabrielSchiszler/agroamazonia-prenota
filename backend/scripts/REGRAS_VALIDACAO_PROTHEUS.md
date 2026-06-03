# Regras e métricas — um comando

Script único: **`rebuild_all_metrics.py`** — todos os processos, tabela de regras + preview das métricas do dashboard.

## Preview (não altera DynamoDB)

```bash
cd backend/scripts
export TABLE_NAME=tabela-document-processor-prd
export AWS_REGION=sa-east-1
# credenciais AWS...

python3 rebuild_all_metrics.py \
  --xlsx "/home/user/Downloads/Erros OCR.xlsx" \
  --output-dir ./out_regras
```

## Saídas em `out_regras/`

| Arquivo | Conteúdo |
|---------|----------|
| `REGRAS_CONSOLIDADAS.md` | 62 regras + contagens (Protheus, API, OCR validação, OCR pipeline) |
| `regras_contagens_snapshot.json` | JSON das regras |
| `METRICAS_PREVIEW.md` | **Como ficariam as métricas** (por dia, top regras, comparação com METRICS# atual) |
| `metrics_preview.json` | Mesmo conteúdo em JSON (`daily`, `monthly`, `failed_rules_global`) |

## Gravar métricas no DynamoDB

```bash
python3 rebuild_all_metrics.py --apply --xlsx "/home/user/Downloads/Erros OCR.xlsx"
```

Substitui registros `METRICS#YYYY-MM-DD` / `SUMMARY` com os valores recalculados.

## Só métricas (sem tabela de regras)

```bash
python3 rebuild_all_metrics.py --skip-regras
```

## O que entra em `failed_count` vs `failed_rules`

- **failed_count:** processos `FAILED` que contam na métrica (exclui só-Operacional Protheus).
- **failed_rules:** soma por REGRA_ID — validação `validar_*`, pipeline `OCR_LAMBDA_*`, Protheus OCR, API OCR do Excel.
- **Ignorados operacional:** processos com só erro operacional Protheus (não entram em falha nem sucesso).
- **success_prenota_count:** sucessos com mensagem Protheus *"documento de entrada criado como pré-nota"* (`protheus_response` no METADATA). O dashboard calcula classificados = `success_count − success_prenota_count`.
- **Taxa de acerto:** `sucessos / (sucessos + falhas) × 100` — módulo `lambdas/utils/metrics_rates.py` (API + front + processos novos via `update_metrics`).

## Testes antes do deploy

```bash
cd backend
python3 -m pytest tests/test_metrics_rates_and_process.py tests/test_protheus_regras_metrics.py tests/test_regras_labels.py -v

Após alterar catálogos Protheus/API (`lambdas/utils/*_catalog.json`):

```bash
cd backend/scripts
python3 build_regras_labels_catalog.py
```

Isso atualiza `src/utils/regras_labels_catalog.json` e `frontend/regras_labels_catalog.json` (labels do dashboard).
```

## Scripts auxiliares

- `sync_regras_metricas.py` — só tabela de regras (chamado automaticamente pelo rebuild).
- `fix_metrics.py` — legado, sem regras Protheus/OCR novas.
