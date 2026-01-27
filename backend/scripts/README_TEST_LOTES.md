# Teste de Geração de Payload com Lotes

Script de teste para validar a geração de payload com lotes no `send_to_protheus`.

## Como usar

```bash
cd backend/scripts
python3 test_send_protheus_lotes.py
```

## Cenários testados

### 1. Lote nos RASTROS (XML estruturado)
- **Fonte**: Campo `rastro` do produto no XML parseado
- **Sem IA**: Usa dados estruturados diretamente do XML
- **Exemplo**: Produto com 2 rastros, cada um com lote, quantidade, datas

### 2. Lote no TEXTO DO PRODUTO (info_adicional - IA)
- **Fonte**: Campo `info_adicional` do produto
- **Com IA**: Usa Bedrock Nova Pro para extrair lotes do texto
- **Exemplo**: Texto com "LOTE:331/25 FABRIC:06/12/2025 VALID:18 MESES"

### 3. Lote no TEXTO ADICIONAL DA NF (info_adicional da NF - IA)
- **Fonte**: Campo `info_adicional` da nota fiscal
- **Com IA**: Usa Bedrock Nova Pro para extrair lotes do texto
- **Exemplo**: Texto da NF com "LOTE:XYZ789 FABRIC:15/01/2025 VALID:12 MESES"

## Ordem de prioridade

1. **PRIORIDADE 1**: Rastros do produto (XML estruturado)
2. **PRIORIDADE 2**: Info adicional do produto (IA)
3. **PRIORIDADE 3**: Info adicional da NF (IA)

## O que o script testa

- ✅ Extração de lotes de cada fonte
- ✅ Conversão de rastros para formato de lotes
- ✅ Split de produtos quando há múltiplos lotes
- ✅ Distribuição de quantidades entre lotes
- ✅ Geração do payload final com lotes

## Saída

O script mostra:
- Dados de entrada de cada cenário
- Resultado do processamento
- Payload JSON gerado (formato Protheus)

## Notas

- O script usa mocks para não precisar de AWS Bedrock real
- Não precisa de credenciais AWS
- Pode ser executado localmente sem dependências externas

