import os
import boto3
from typing import Optional, Dict, Any
from boto3.dynamodb.conditions import Key

class DynamoDBRepository:
    def __init__(self):
        self.table_name = os.environ['TABLE_NAME']
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)
    
    def put_item(self, pk: str, sk: str, attributes: Dict[str, Any]) -> None:
        """Insere item no DynamoDB"""
        item = {'PK': pk, 'SK': sk, **attributes}
        self.table.put_item(Item=item)
    
    def get_item(self, pk: str, sk: str) -> Optional[Dict[str, Any]]:
        """Busca item específico usando GetItem"""
        response = self.table.get_item(Key={'PK': pk, 'SK': sk})
        return response.get('Item')
    
    def query_by_pk(self, pk: str) -> list[Dict[str, Any]]:
        """Query todos os itens de um PK"""
        response = self.table.query(KeyConditionExpression=Key('PK').eq(pk))
        return response.get('Items', [])
    
    def query_by_pk_and_sk_prefix(self, pk: str, sk_prefix: str) -> list[Dict[str, Any]]:
        """Query com filtro de SK usando begins_with"""
        response = self.table.query(
            KeyConditionExpression=Key('PK').eq(pk) & Key('SK').begins_with(sk_prefix)
        )
        return response.get('Items', [])
    
    def update_item(self, pk: str, sk: str, attributes: Dict[str, Any]) -> None:
        """Atualiza atributos de um item"""
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Updating item PK={pk}, SK={sk}, attributes={attributes}")
        
        reserved_keywords = {'STATUS', 'TIMESTAMP', 'TYPE', 'NAME', 'DATA'}
        expr_names = {}
        update_parts = []
        
        for k in attributes.keys():
            if k.upper() in reserved_keywords:
                expr_names[f'#{k}'] = k
                update_parts.append(f'#{k} = :{k}')
            else:
                update_parts.append(f'{k} = :{k}')
        
        update_expr = 'SET ' + ', '.join(update_parts)
        expr_values = {f':{k}': v for k, v in attributes.items()}
        
        try:
            params = {
                'Key': {'PK': pk, 'SK': sk},
                'UpdateExpression': update_expr,
                'ExpressionAttributeValues': expr_values
            }
            if expr_names:
                params['ExpressionAttributeNames'] = expr_names
            
            self.table.update_item(**params)
            logger.info("Update successful")
        except Exception as e:
            logger.error(f"Update failed: {str(e)}")
            raise
    
    def delete_item(self, pk: str, sk: str) -> None:
        """Remove item do DynamoDB"""
        self.table.delete_item(Key={'PK': pk, 'SK': sk})
