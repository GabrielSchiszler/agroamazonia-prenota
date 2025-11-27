from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

class DocumentMetadata(BaseModel):
    document_id: str
    document_type: str  # PRE_NOTE, DOC_XML
    process_type: str  # SEMENTES, AGROQUIMICOS, FERTILIZANTES
    status: str
    timestamp: int
    s3_path: str
    
    def to_sk(self) -> str:
        """Converte metadados para formato SK: CHAVE=VALOR,CHAVE=VALOR"""
        return f"METADATA={self.timestamp},TYPE={self.document_type}"
    
    def to_data_payload(self) -> str:
        """Converte para formato de atributo DynamoDB"""
        return f"STATUS={self.status},TIMESTAMP={self.timestamp},PROCESS_TYPE={self.process_type},S3_PATH={self.s3_path}"

class TextractResult(BaseModel):
    job_id: str
    tables: list[Dict[str, Any]]
    raw_response: Optional[Dict[str, Any]] = None

class DocumentRecord(BaseModel):
    pk: str
    sk: str
    data_payload: str
    
    @staticmethod
    def from_metadata(metadata: DocumentMetadata) -> "DocumentRecord":
        return DocumentRecord(
            pk=f"DOC_ID={metadata.document_id}",
            sk=metadata.to_sk(),
            data_payload=metadata.to_data_payload()
        )
