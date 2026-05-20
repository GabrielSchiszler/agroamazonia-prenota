# Textract: `UnsupportedDocumentException` em PDFs “normais”

O erro **não significa necessariamente** que o ficheiro “não é PDF”. Muitas vezes o binário é `%PDF` válido, abre em leitores e passa checagens simples (tamanho, páginas, sem password óbvia, sem XFA), e mesmo assim o **Textract recusa o documento**.

Mensagem típica da AWS:

`UnsupportedDocumentException: Request has unsupported document format`

Isto indica que o **parser interno do Textract** não consegue tratar algum detalhe do PDF — é uma limitação/comportamento do serviço, não um bug da aplicação quando `AnalyzeDocument` e `DetectDocumentText` falham **ambos** com o mesmo binário.

## Causas frequentes

1. **Compressão / imagens internas incompatíveis** (muito comum em NFSe municipal)  
   JPEG2000, JBIG2, CCITT “estranho”, streams duvidosos. O Textract é **mais rígido** que muitos visualizadores.

2. **PDF estruturalmente frágil**  
   `xref` inconsistente, objetos inválidos, streams truncados, metadados estranhos. Leitores toleram; Textract pode não.

3. **Página essencialmente imagem com encapsulamento ruim**  
   TIFF/JPEG embutido com encoding ou wrapper que o Textract não aceita.

4. **Produtor específico** (prefeitura, wkhtmltopdf/iText/Jasper antigos, engines municipais).

## Como confirmar no ambiente local

Com o PDF em disco:

```bash
pdfinfo arquivo.pdf
qpdf --check arquivo.pdf
pdfimages -list arquivo.pdf
```

Procurar avisos de estrutura, referências a **JP2** / **JBIG2**, ou falhas do `qpdf --check`.

Há também um script opcional no repositório:

```bash
./backend/scripts/check_pdf_textract_hints.sh arquivo.pdf
```

(Instala `poppler-utils` e `qpdf` no sistema se faltarem.)

## Comportamento na Lambda `extract-documents`

Depois de `AnalyzeDocument` e `DetectDocumentText` no **PDF original** falharem com `UnsupportedDocumentException`, a Lambda **rasteriza** cada página com **PyMuPDF** (PNG) e chama `DetectDocumentText` por página, agregando só linhas de texto (sem tabelas). Variáveis opcionais: `TEXTRACT_RASTER_DPI` (padrão 200), `TEXTRACT_RASTER_MAX_PAGES` (padrão 15). O resultado fica em `TEXTRACT_MODE=detect_document_text_raster_fallback`.

## Mitigações práticas

### A) Re-renderizar o PDF (“limpo”)

Rasterizar páginas e gerar um PDF novo só com imagens compatíveis costuma resolver a maioria dos casos. Exemplo em Python (requer `pdf2image` + Poppler no sistema, e `img2pdf` ou equivalente):

```python
from io import BytesIO
from pdf2image import convert_from_bytes
import img2pdf

def pdf_bytes_to_clean_pdf_bytes(body: bytes) -> bytes:
    pages = convert_from_bytes(body, dpi=300)
    imgs = []
    for p in pages:
        buf = BytesIO()
        p.save(buf, format="JPEG", quality=92)
        imgs.append(buf.getvalue())
    return img2pdf.convert(imgs)
```

**Lambda:** Poppler + `pdf2image` aumentam tamanho de deployment; opções comuns são **Lambda container image**, **layer** com binários, ou job num worker (ECS/Fargate) só para normalização.

### B) Enviar **PNG/JPEG por página** ao Textract

Em vez de reenviar o PDF problemático, chamar `DetectDocumentText` / análise síncrona com `Document={"Bytes": image_bytes}` por página. O Textract costuma ser **mais estável** com bitmaps simples do que com PDFs gerados por terceiros.

### C) Pré-validação na ingestão

Rejeitar ou encaminhar para fila manual PDFs que falhem `qpdf --check` ou que `pdfimages -list` mostre formatos exóticos, **antes** de gastar chamadas ao Textract.

## Leitura operacional

- Se **ambos** `AnalyzeDocument` e `DetectDocumentText` falham com `UnsupportedDocumentException`, o problema está no **binário vs. Textract**, não na lógica de negócio da app.
- A solução robusta é **normalizar o documento** (raster ou imagens por página), não insistir no PDF original.
