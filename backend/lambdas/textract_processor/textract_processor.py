import os
import json
import boto3
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

textract = boto3.client('textract')
s3 = boto3.client('s3')

def handler(event, context):
    """Extrai tabelas de UM arquivo usando Textract"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    file_info = event['file']
    bucket_name = os.environ['BUCKET_NAME']
    file_key = file_info['FILE_KEY']
    
    logger.info(f"Starting Textract for: {file_key}")
    
    try:
        response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': file_key
                }
            },
            FeatureTypes=['TABLES']
        )
        
        job_id = response['JobId']
        logger.info(f"Textract job started: {job_id}")
        
        # Aguarda conclusão
        while True:
            result = textract.get_document_analysis(JobId=job_id)
            status = result['JobStatus']
            
            if status in ['SUCCEEDED', 'FAILED']:
                break
            
            time.sleep(2)
        
        logger.info(f"Textract job {job_id} status: {status}")
        
        if status == 'FAILED':
            raise Exception(f"Textract job failed: {job_id}")
        
        tables = extract_tables(result)
        logger.info(f"Extracted {len(tables)} tables from {file_key}")
        
        output = {
            'file_key': file_key,
            'file_name': file_info['FILE_NAME'],
            'job_id': job_id,
            'tables': tables
        }
        
        logger.info(f"Returning: {json.dumps(output)}")
        return output
        
    except Exception as e:
        logger.error(f"Error processing {file_key}: {str(e)}")
        raise

def extract_tables(result):
    """Extrai estrutura de tabelas do resultado Textract"""
    blocks = result['Blocks']
    tables = []
    
    table_blocks = [b for b in blocks if b['BlockType'] == 'TABLE']
    
    for table_block in table_blocks:
        table_data = {
            'id': table_block['Id'],
            'rows': [],
            'confidence': table_block.get('Confidence', 0)
        }
        
        if 'Relationships' in table_block:
            for relationship in table_block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    cell_ids = relationship['Ids']
                    cells = [b for b in blocks if b['Id'] in cell_ids]
                    
                    # Organiza células por linha/coluna
                    cell_map = {}
                    for cell in cells:
                        if cell['BlockType'] == 'CELL':
                            row = cell.get('RowIndex', 0)
                            col = cell.get('ColumnIndex', 0)
                            text = get_cell_text(cell, blocks)
                            
                            if row not in cell_map:
                                cell_map[row] = {}
                            cell_map[row][col] = text
                    
                    # Converte para array de linhas
                    for row_idx in sorted(cell_map.keys()):
                        row_data = [cell_map[row_idx].get(col, '') 
                                   for col in sorted(cell_map[row_idx].keys())]
                        table_data['rows'].append(row_data)
        
        tables.append(table_data)
    
    return tables

def get_cell_text(cell, blocks):
    """Extrai texto de uma célula"""
    text = ''
    if 'Relationships' in cell:
        for relationship in cell['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = next((b for b in blocks if b['Id'] == child_id), None)
                    if word and word['BlockType'] == 'WORD':
                        text += word.get('Text', '') + ' '
    return text.strip()
