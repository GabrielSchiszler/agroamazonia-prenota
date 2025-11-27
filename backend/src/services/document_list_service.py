from typing import List, Dict, Any
from src.repositories.dynamodb_repository import DynamoDBRepository

class DocumentListService:
    def __init__(self):
        self.repository = DynamoDBRepository()
    
    def list_all_documents(self) -> List[Dict[str, Any]]:
        """Lista todos os documentos (scan limitado para desenvolvimento)"""
        # Em produção, usar GSI ou manter índice separado
        # Por ora, retorna lista vazia para evitar scan
        return []
