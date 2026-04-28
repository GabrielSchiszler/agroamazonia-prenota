from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Literal
from enum import Enum

class DocType(str, Enum):
    DANFE = "DANFE"
    ADDITIONAL = "ADDITIONAL"

class ProcessType(str, Enum):
    SEMENTES = "SEMENTES"
    AGROQUIMICOS = "AGROQUIMICOS"
    FERTILIZANTES = "FERTILIZANTES"

class PresignedUrlRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo")
    file_name: str = Field(..., description="Nome do arquivo")
    file_type: str = Field(..., description="Content-Type do arquivo", example="application/pdf")
    
class XmlPresignedUrlRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo (UUID gerado pelo frontend)")
    file_name: str = Field(..., description="Nome do arquivo XML")
    file_type: str = Field(default="application/xml", description="Content-Type do arquivo")
    metadados: Optional[Dict[str, Any]] = Field(default=None, description="Metadados adicionais do arquivo (JSON)")
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "file_name": "51251013563680006304550010000026551833379679.XML",
                "file_type": "application/xml",
                "metadados": {
                    "moeda": "BRL",
                    "pedidoFornecedor": "369763",
                    "pedidoErp": "023037"
                }
            }
        }

class DocsPresignedUrlRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo (UUID gerado pelo frontend)")
    file_name: str = Field(..., description="Nome do arquivo")
    file_type: str = Field(default="application/pdf", description="Content-Type do arquivo")
    metadados: Optional[Dict[str, Any]] = Field(default={}, description="Metadados adicionais do arquivo")
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "file_name": "pedido_compra.pdf",
                "file_type": "application/pdf",
                "metadados": {
                    "tipo_documento": "pedido_compra",
                    "fornecedor": "Empresa XYZ",
                    "valor_total": 15000.50
                }
            }
        }

class PresignedUrlResponse(BaseModel):
    upload_url: str
    file_key: str
    file_name: str
    content_type: str
    doc_type: str
    
    class Config:
        schema_extra = {
            "example": {
                "upload_url": "https://agroamazonia-raw-documents.s3.amazonaws.com/processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/danfe/nota_fiscal.xml?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
                "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/danfe/nota_fiscal.xml",
                "file_name": "nota_fiscal.xml",
                "content_type": "application/xml",
                "doc_type": "DANFE"
            }
        }

class DocsPresignedUrlResponse(BaseModel):
    upload_url: str
    file_key: str
    file_name: str
    content_type: str
    doc_type: str
    
    class Config:
        schema_extra = {
            "example": {
                "upload_url": "https://agroamazonia-raw-documents.s3.amazonaws.com/processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/pedido.pdf?X-Amz-Algorithm=...",
                "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/pedido.pdf",
                "file_name": "pedido.pdf",
                "content_type": "application/pdf",
                "doc_type": "ADDITIONAL"
            }
        }

class PedidoCompraMetadataRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo (UUID gerado pelo frontend)")
    metadados: Dict[str, Any] = Field(..., description="Metadados do pedido de compra (JSON)")
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "metadados": {
                    "header": {
                        "tenantId": "123"
                    },
                    "requestBody": {
                        "cnpjEmitente": "02290510001652",
                        "cnpjDestinatario": "13563680000101",
                        "itens": [
                            {
                                "codigoProduto": "41500001BD00205",
                                "quantidade": 1,
                                "valorUnitario": 880,
                                "codigoOperacao": "1B",
                                "pedidoDeCompra": {
                                    "pedidoErp": "AAAAYX",
                                    "itemPedidoErp": "0004"
                                }
                            }
                        ],
                        "duplicatas": [
                            {
                                "vencimento": "2026-01-30",
                                "valor": 440
                            }
                        ]
                    }
                }
            }
        }

class PedidoCompraMetadataResponse(BaseModel):
    success: bool
    message: str
    process_id: str
    metadados: Dict[str, Any]
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Metadados do pedido de compra vinculados com sucesso",
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "metadados": {
                    "header": {
                        "tenantId": "123"
                    },
                    "requestBody": {
                        "cnpjEmitente": "02290510001652"
                    }
                }
            }
        }

class ProcessStartRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo a ser iniciado")
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318"
            }
        }

class ProcessStartResponse(BaseModel):
    execution_arn: str
    process_id: str
    status: str
    
    class Config:
        schema_extra = {
            "example": {
                "execution_arn": "arn:aws:states:us-east-1:481665100875:execution:AgroAmazoniaStack-StateMachine:7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "status": "PROCESSING"
            }
        }

class ProcessResponse(BaseModel):
    process_id: str
    process_type: Optional[str] = None
    status: str
    sctask_id: Optional[str] = None
    files: Dict[str, List[Dict[str, Any]]]
    parsing_results: List[Dict[str, Any]] = []
    bedrock_by_file: List[Dict[str, Any]] = Field(default_factory=list)
    protheus_request_payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON do documento de entrada enviado (ou tentado) à API Protheus",
    )
    created_at: str
    error_info: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "process_type": "SEMENTES",
                "status": "COMPLETED",
                "files": {
                    "danfe": [
                        {
                            "file_name": "51251013563680006304550010000026551833379679.XML",
                            "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/danfe/51251013563680006304550010000026551833379679.XML",
                            "status": "UPLOADED"
                        }
                    ],
                    "additional": [
                        {
                            "file_name": "NF_000002655.PDF",
                            "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/NF_000002655.PDF",
                            "status": "UPLOADED"
                        },
                        {
                            "file_name": "FIL_013_NF_000002655.PDF",
                            "file_key": "processes/7d48cd96-c099-48dd-bbb6-d4fe8b2de318/docs/FIL_013_NF_000002655.PDF",
                            "status": "UPLOADED"
                        }
                    ]
                },
                "created_at": "1733068800"
            }
        }

MAX_FILES_PER_PROCESS = 10
ALLOWED_CONTENT_TYPES = {
    "application/xml", "text/xml",
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "image/png", "image/jpeg", "image/tiff",
}


def infer_doc_type_and_folder(file_name: str, file_type: str) -> tuple[str, str]:
    """Define DOC_TYPE e pasta no S3 só com nome + MIME (batch sem ``doc_type`` no cliente).

    Regra: XML (MIME ou extensão ``.xml``) → ``DANFE`` / prefixo ``danfe`` (legado).
    Qualquer outro tipo permitido pela API → ``ADDITIONAL`` / ``docs``.

    O pipeline (parse_xml, extract_documents, etc.) já decide pelo conteúdo/extensão;
    isto só posiciona o objeto no bucket e grava DOC_TYPE no Dynamo para listagens.
    """
    ft = (file_type or "").strip().lower().split(";")[0].strip()
    name_l = (file_name or "").strip().lower()
    if ft in ("application/xml", "text/xml") or name_l.endswith(".xml"):
        return "DANFE", "danfe"
    return "ADDITIONAL", "docs"


class BatchFileItem(BaseModel):
    file_name: str = Field(..., description="Nome do arquivo")
    file_type: str = Field(
        ...,
        description="Content-Type do arquivo (application/xml, application/pdf, ...)",
    )
    doc_type: Optional[str] = Field(
        default=None,
        description=(
            "Opcional. Omitido: inferido de file_type/extensão (XML→DANFE/danfe; "
            "demais→ADDITIONAL/docs). Envio explícito mantém compatibilidade com integrações antigas."
        ),
    )


class BatchPresignedUrlRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo (UUID)")
    files: List[BatchFileItem] = Field(
        ...,
        min_length=1,
        max_length=MAX_FILES_PER_PROCESS,
        description=f"Lista de arquivos (máx {MAX_FILES_PER_PROCESS})",
    )


class BatchPresignedUrlResponseItem(BaseModel):
    file_name: str
    upload_url: str
    file_key: str
    content_type: str
    doc_type: str


class BatchPresignedUrlResponse(BaseModel):
    process_id: str
    files: List[BatchPresignedUrlResponseItem]


class UpdateFileMetadataRequest(BaseModel):
    process_id: str = Field(..., description="ID do processo")
    file_name: str = Field(..., description="Nome do arquivo")
    metadados: Dict[str, Any] = Field(..., description="Novos metadados JSON para o arquivo")
    
    class Config:
        schema_extra = {
            "example": {
                "process_id": "7d48cd96-c099-48dd-bbb6-d4fe8b2de318",
                "file_name": "pedido_compra.pdf",
                "metadados": {
                    "moeda": "BRL",
                    "pedidoFornecedor": "369763",
                    "pedidoErp": "023037",
                    "itens": []
                }
            }
        }

class UpdateFileMetadataResponse(BaseModel):
    success: bool
    message: str
    file_name: str
    metadados: Dict[str, Any]
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Metadados atualizados com sucesso",
                "file_name": "pedido_compra.pdf",
                "metadados": {
                    "moeda": "BRL",
                    "pedidoFornecedor": "369763"
                }
            }
        }
