import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def is_textract_supported(file_name):
    """Verifica se o tipo de arquivo é suportado pelo Textract"""
    if not file_name:
        return False
    
    file_name_lower = file_name.lower()
    # Textract suporta: PDF, PNG, JPEG, TIFF
    # NÃO suporta: XML, DOCX, DOC, XLSX, etc.
    supported_extensions = ['.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.tif']
    return any(file_name_lower.endswith(ext) for ext in supported_extensions)

def handler(event, context):
    """Verifica se Textract já foi processado"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    process_id = event['process_id']
    files = event.get('files', [])
    
    # Verificar se Textract está habilitado (padrão: desativado)
    textract_enabled = os.environ.get('TEXTRACT_ENABLED', 'false').lower() == 'true'
    
    if not textract_enabled:
        logger.info("Textract está desativado. Retornando needs_textract=false")
        return {
            'process_id': process_id,
            'process_type': event.get('process_type'),
            'files': [],
            'already_processed': 0,
            'needs_textract': False
        }
    
    pk = f"PROCESS#{process_id}"
    
    # Buscar dados do Textract existentes
    existing = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={':pk': pk, ':sk': 'TEXTRACT='}
    )['Items']
    
    existing_files = {item['FILE_NAME'] for item in existing}
    
    # Filtrar apenas arquivos que:
    # 1. Ainda não foram processados
    # 2. São suportados pelo Textract (PDF, PNG, JPEG, TIFF)
    files_to_process = []
    skipped_files = []
    for file in files:
        file_name = file.get('FILE_NAME', '')
        
        # Pular arquivos não suportados pelo Textract
        if not is_textract_supported(file_name):
            logger.info(f"Arquivo não suportado pelo Textract (será pulado): {file_name}")
            skipped_files.append(file_name)
            continue
        
        # Pular arquivos já processados
        if file_name not in existing_files:
            files_to_process.append(file)
    
    logger.info(f"Total files: {len(files)}, Already processed: {len(existing_files)}, To process: {len(files_to_process)}, Skipped (not supported): {len(skipped_files)}")
    if skipped_files:
        logger.info(f"Arquivos pulados (não suportados pelo Textract): {skipped_files}")
    
    return {
        'process_id': process_id,
        'process_type': event.get('process_type'),
        'files': files_to_process,
        'already_processed': len(existing_files),
        'needs_textract': len(files_to_process) > 0
    }
