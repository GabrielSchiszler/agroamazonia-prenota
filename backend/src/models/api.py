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
    created_at: str
    
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
