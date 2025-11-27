import os
import json
import boto3
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

textract = boto3.client('textract')

def handler(event, context):
    """Busca resultado do Textract e extrai tabelas"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    job_id = event['textract_job']['JobId']
    file_key = event['FILE_KEY']
    file_name = event['FILE_NAME']
    
    logger.info(f"Getting Textract results for job: {job_id}")
    
    # Aguarda e busca resultado
    max_attempts = 60
    for attempt in range(max_attempts):
        result = textract.get_document_analysis(JobId=job_id)
        status = result['JobStatus']
        
        logger.info(f"Attempt {attempt + 1}: Status = {status}")
        
        if status == 'SUCCEEDED':
            blocks = result['Blocks']
            tables = extract_tables(blocks)
            logger.info(f"Extracted {len(tables)} tables")
            
            return {
                'file_key': file_key,
                'file_name': file_name,
                'job_id': job_id,
                'tables': tables
            }
        elif status == 'FAILED':
            raise Exception(f"Textract job failed: {job_id}")
        
        time.sleep(5)
    
    raise Exception(f"Textract job timeout: {job_id}")

def extract_tables(blocks):
    """Extrai estrutura de tabelas"""
    tables = []
    table_blocks = [b for b in blocks if b.get('BlockType') == 'TABLE']
    
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
                    
                    cell_map = {}
                    for cell in cells:
                        if cell.get('BlockType') == 'CELL':
                            row = cell.get('RowIndex', 0)
                            col = cell.get('ColumnIndex', 0)
                            text = get_cell_text(cell, blocks)
                            
                            if row not in cell_map:
                                cell_map[row] = {}
                            cell_map[row][col] = text
                    
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
                    if word and word.get('BlockType') == 'WORD':
                        text += word.get('Text', '') + ' '
    return text.strip()
