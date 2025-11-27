from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

class ProcessCreateRequest(BaseModel):
    process_type: str = Field(..., description="SEMENTES, AGROQUIMICOS, FERTILIZANTES")

class ProcessCreateResponse(BaseModel):
    process_id: str
    process_type: str
    status: str

class PresignedUrlRequest(BaseModel):
    process_id: str
    file_name: str
    file_type: str

class PresignedUrlResponse(BaseModel):
    upload_url: str
    file_key: str

class ProcessStartRequest(BaseModel):
    process_id: str

class ProcessStartResponse(BaseModel):
    execution_arn: str
    process_id: str
    status: str

class ProcessResponse(BaseModel):
    process_id: str
    process_type: str
    status: str
    files: List[Dict[str, Any]]
    created_at: str
