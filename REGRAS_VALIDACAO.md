# Regras de Validação - Documentos Fiscais

## Regras Implementadas

### 1. Validar CNPJ Fornecedor
**Status**: ✅ Implementado

**Como funciona**:
- Normaliza CNPJs removendo formatação (pontos, traços, barras)
- Compara CNPJ do XML (DANFE) com CNPJ extraído do documento OCR
- Usa Amazon Bedrock para validar CNPJs com diferença de 1 dígito (erros de OCR)
- Correção automática: quando validado por IA, atualiza valor OCR para o valor correto do XML

**Exemplo**:
- XML: `13.563.680/0004-46` → normalizado: `13563680000446`
- OCR: `1356368000446` (faltou um zero)
- Resultado: ✓ VÁLIDO (IA detecta que são o mesmo CNPJ)
- Correção: OCR corrigido para `13563680000446`

**Tolerância**: Diferença de até 1 dígito

---

### 2. Validar Série
**Status**: ✅ Implementado

**Como funciona**:
- Remove zeros à esquerda para normalização
- Compara série do XML com série do OCR
- Aplica tolerância a erros comuns de OCR: `1↔I`, `0↔O`, `5↔S`, `8↔B`, `2↔Z`, `6↔G`
- Correção automática: quando detectado erro de OCR, corrige para valor do XML

**Exemplo**:
- XML: `1`
- OCR: `I` (letra I ao invés de número 1)
- Resultado: ✓ VÁLIDO (tolerância OCR)
- Correção: OCR corrigido para `1`

**Tolerância**: Erros de OCR com caracteres similares

---

### 3. Validar Número da Nota
**Status**: ✅ Implementado

**Como funciona**:
- Remove zeros à esquerda para normalização
- Compara número da nota do XML com número extraído do OCR
- Aceita diferentes formatações (com ou sem zeros à esquerda)

**Exemplo**:
- XML: `2655`
- OCR: `000002655`
- Resultado: ✓ VÁLIDO (mesmo número, formatação diferente)

**Tolerância**: Formatação com zeros à esquerda

---

### 4. Validar Data de Emissão
**Status**: ✅ Implementado

**Como funciona**:
- Extrai data em múltiplos formatos (DD/MM/YYYY, YYYY-MM-DD, etc)
- Normaliza para formato ISO (YYYY-MM-DD)
- Compara datas normalizadas

**Exemplo**:
- XML: `2025-10-28`
- OCR: `28/10/2025`
- Resultado: ✓ VÁLIDO (mesma data, formato diferente)

**Tolerância**: Diferentes formatos de data

---

### 5. Validar CNPJ Destinatário
**Status**: ✅ Implementado

**Como funciona**:
- Idêntico à validação de CNPJ Fornecedor
- Compara CNPJ do destinatário entre XML e OCR
- Tolerância a erros de OCR e correção automática

---

### 6. Validar Produtos (Item a Item)
**Status**: ✅ Implementado (Parcial)

**Como funciona**:

#### 6.1 Matching de Produtos
- Produtos podem estar em ordem diferente entre XML e OCR
- Usa código + quantidade como chave de identificação
- 3 estratégias de matching:
  1. Match exato (código e quantidade idênticos)
  2. Tolerância OCR (permite erros como `1↔I`, `0↔O`)
  3. Fallback por descrição (substring matching)

#### 6.2 Validação Campo a Campo
Para cada produto pareado, valida:

**✅ Código do Produto**:
- Remove espaços e caracteres especiais
- Aplica tolerância OCR
- Usa IA como fallback
- Correção automática quando validado

**✅ Descrição**:
- Verifica se descrição XML está contida na descrição OCR (ou vice-versa)
- Usa IA para validação semântica
- Correção automática quando validado

**✅ Unidade de Medida**:
- Comparação exata (case-insensitive)

**✅ Quantidade**:
- Comparação numérica com tolerância de 0.01

**✅ Valor Unitário**:
- Comparação numérica com tolerância de 0.01

**✅ Valor Total**:
- Comparação numérica com tolerância de 0.01

**Exemplo**:
```
XML:  DU900003GL00056 | VIANCE TECNOMYL GL 5 LT | GL | 80 | 135.72 | 10857.60
OCR:  DU900003GL000 56 | VIANCE TECNOMYL GL 5 LT | GL | 80 | 135.72 | 10857.60
                    ↑ espaço no meio do código
Resultado: ✓ VÁLIDO (tolerância OCR remove espaços)
Correção: Código OCR → DU900003GL00056
```

**Campos Validados**: Código, Descrição, Unidade, Quantidade, Valor Unitário, Valor Total

**Campos Extraídos mas NÃO Validados**: Lote, Data de Fabricação, Data de Validade, NCM, CFOP

---

## Regras Futuras (A Implementar)

### 7. Validar Lote
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair lote do XML e OCR
- Comparar lotes item a item
- Aplicar tolerância a erros de OCR
- Validar formato do lote (se aplicável)

**Campos**: `lote`

---

### 8. Validar Data de Validade
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair data de validade do XML e OCR
- Normalizar formatos de data
- Comparar datas item a item
- **Validação adicional**: Alertar se validade < 6 meses da data atual
- **Validação adicional**: Verificar se data de validade > data de fabricação

**Campos**: `dataValidade`

---

### 9. Validar Data de Fabricação
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair data de fabricação do XML e OCR
- Normalizar formatos de data
- Comparar datas item a item
- **Validação adicional**: Verificar se data de fabricação < data de emissão da nota
- **Validação adicional**: Verificar se data de fabricação < data de validade

**Campos**: `dataFabricacao`

---

### 10. Validar Tipo de Operação (CFOP)
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair CFOP do XML e OCR
- Comparar CFOPs item a item
- **Validação adicional**: Verificar se CFOP é válido (consultar tabela CFOP)
- **Validação adicional**: Verificar se CFOP corresponde à natureza da operação
- **Validação adicional**: Validar compatibilidade CFOP com tipo de produto

**Campos**: `cfop`, `codigoOperacao`

---

### 11. Validar ICMS
**Status**: ⏳ Pendente

**Como deve funcionar**:

#### 11.1 Operações Internas (mesmo estado)
- Verificar se ICMS está zerado
- Se ICMS > 0 em operação interna: gerar alerta

#### 11.2 Operações Interestaduais
- Identificar estado de origem e destino
- Consultar tabela de alíquotas por estado
- Calcular ICMS esperado: `base_calculo × aliquota`
- Comparar ICMS calculado com ICMS da nota
- Tolerância: R$ 0.10

#### 11.3 Base de Cálculo
- Validar se base de cálculo está correta
- Base pode variar por estado e tipo de produto
- Consultar tabela fiscal oficial

**Campos**: `baseICMS`, `aliquotaICMS`, `valorICMS`, `estado_origem`, `estado_destino`

**Tabela necessária**: Alíquotas de ICMS por estado

---

### 12. Validar Valor Total da Nota
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Somar `valorTotal` de todos os produtos
- Comparar com valor total da nota fiscal
- Tolerância: R$ 0.10
- Se divergência > tolerância: gerar erro

**Cálculo**: `Σ(valor_total_produtos) = valor_total_nota`

**Campos**: `valorTotal` (por item), `valor_nota` (total)

---

### 13. Conversão de Unidades
**Status**: ⏳ Pendente

**Como deve funcionar**:

#### 13.1 Detecção de Unidade
- Identificar se quantidade está em litros (L) ou unidade comercial (BD, GL, etc)
- Se vier em litros: converter para unidade comercial

#### 13.2 Tabela de Conversão
Exemplo:
- 1 BD (Bombona) = 20 litros
- 1 GL (Galão) = 5 litros
- 1 TB (Tambor) = 200 litros

#### 13.3 Validação
- Verificar se quantidade é múltiplo da embalagem
- Exemplo: 50 BD × 20L = 1000L total
- Se não for múltiplo: gerar alerta

**Tabela necessária**: Conversão de unidades por tipo de embalagem

---

### 14. Conversão de Moeda (PTAX)
**Status**: ⏳ Pendente

**Como deve funcionar**:

#### 14.1 Detecção de Moeda
- Identificar se pedido está em USD ou BRL
- Se USD: aplicar conversão

#### 14.2 Consulta PTAX
- Buscar taxa PTAX do Banco Central
- Usar taxa do dia anterior à data de emissão da nota
- API: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/

#### 14.3 Conversão
- Converter valores: `valor_BRL = valor_USD × PTAX`
- Aplicar a todos os valores (unitário, total)

#### 14.4 Validação PTAX
- Calcular PTAX implícito: `PTAX_calculado = valor_unitario ÷ quantidade`
- Comparar com PTAX oficial
- Se divergência > 2%: atualizar valores automaticamente

**Campos**: `moeda`, `taxaCambio`, `valorUnitario`, `valorTotal`

**API necessária**: Banco Central PTAX

---

### 15. Validar Data vs Chegada (BISIFY)
**Status**: ⏳ Pendente

**Como deve funcionar**:

#### 15.1 Integração BISIFY
- Consultar sistema BISIFY para obter data de chegada do produto
- Buscar por: CNPJ fornecedor, código do produto, ou número da nota

#### 15.2 Validação
- Comparar: `data_emissao_nota < data_chegada_produto`
- Se nota emitida APÓS chegada: gerar erro crítico
- Motivo: Nota fiscal deve ser emitida antes da chegada física

**Campos**: `dataEmissao`, `data_chegada_bisify`

**Integração necessária**: API BISIFY

---

### 16. Validar Origem do Produto (O-ST)
**Status**: ⏳ Pendente

**Como deve funcionar**:

#### 16.1 Integração O-ST
- Consultar sistema O-ST para obter dados do produto
- Buscar por: código do produto

#### 16.2 Validação de Nacionalidade
- Verificar se produto é nacional ou importado
- Comparar com informação da DANFE
- Se divergência: gerar alerta

#### 16.3 Validação de Grupo
- Verificar se grupo do produto no O-ST corresponde ao tipo declarado na DANFE
- Exemplo: Agroquímico deve estar no grupo correto
- Se divergência: gerar alerta

**Campos**: `nacionalidade`, `grupo_produto`

**Integração necessária**: API O-ST

---

### 17. Validar IPI
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair IPI do XML e OCR
- Consultar tabela TIPI por NCM
- Calcular IPI esperado: `valor_produto × aliquota_ipi`
- Verificar isenções por tipo de produto
- Comparar IPI calculado com IPI da nota
- Tolerância: R$ 0.10

**Campos**: `valorIPI`, `aliquotaIPI`, `ncm`

**Tabela necessária**: TIPI (Tabela de Incidência do IPI)

---

### 18. Validar NCM
**Status**: ⏳ Pendente

**Como deve funcionar**:
- Extrair NCM do XML e OCR
- Comparar NCMs item a item
- **Validação adicional**: Verificar se NCM é válido (8 dígitos)
- **Validação adicional**: Consultar tabela NCM para verificar se existe
- **Validação adicional**: Verificar se NCM corresponde ao tipo de produto

**Campos**: `ncm`

**Tabela necessária**: Tabela NCM completa

---

## Resumo de Status

### Implementadas (6 regras)
1. ✅ Validar CNPJ Fornecedor
2. ✅ Validar Série
3. ✅ Validar Número da Nota
4. ✅ Validar Data de Emissão
5. ✅ Validar CNPJ Destinatário
6. ✅ Validar Produtos (Código, Descrição, Unidade, Quantidade, Valores)

### Pendentes (12 regras)
7. ⏳ Validar Lote
8. ⏳ Validar Data de Validade
9. ⏳ Validar Data de Fabricação
10. ⏳ Validar Tipo de Operação (CFOP)
11. ⏳ Validar ICMS
12. ⏳ Validar Valor Total da Nota
13. ⏳ Conversão de Unidades
14. ⏳ Conversão de Moeda (PTAX)
15. ⏳ Validar Data vs Chegada (BISIFY)
16. ⏳ Validar Origem do Produto (O-ST)
17. ⏳ Validar IPI
18. ⏳ Validar NCM

---

## Integrações Necessárias

### APIs Externas
1. **Banco Central (PTAX)**: Cotação do dólar
2. **BISIFY**: Data de chegada de produtos
3. **O-ST**: Origem e grupo de produtos

### Tabelas de Referência
1. **Alíquotas ICMS**: Por estado (origem × destino)
2. **Conversão de Unidades**: Embalagens (BD, GL, TB, etc)
3. **TIPI**: Alíquotas de IPI por NCM
4. **NCM**: Tabela completa de códigos NCM
5. **CFOP**: Códigos fiscais de operações
