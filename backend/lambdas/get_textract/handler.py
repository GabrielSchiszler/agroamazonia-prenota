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
    
    file_key = event['FILE_KEY']
    file_name = event['FILE_NAME']
    
    # Pular arquivos XML
    if file_name.lower().endswith('.xml'):
        logger.info(f"Skipping XML file: {file_name}")
        return {
            'file_key': file_key,
            'file_name': file_name,
            'job_id': 'N/A',
            'tables': [],
            'skipped': True
        }
    
    job_id = event['textract_job']['JobId']
    logger.info(f"Getting Textract results for job: {job_id}")
    
    # Aguarda e busca resultado
    max_attempts = 60
    for attempt in range(max_attempts):
        result = textract.get_document_analysis(JobId=job_id)
        status = result['JobStatus']
        
        logger.info(f"Attempt {attempt + 1}: Status = {status}")
        
        if status == 'SUCCEEDED':
            # Buscar todos os blocos (pode ter múltiplas páginas)
            all_blocks = result['Blocks']
            next_token = result.get('NextToken')
            
            while next_token:
                logger.info(f"Fetching next page of blocks with token: {next_token[:20]}...")
                result = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
                all_blocks.extend(result['Blocks'])
                next_token = result.get('NextToken')
            
            logger.info(f"Total blocks retrieved: {len(all_blocks)}")
            
            # Log tipos de blocos
            block_types = {}
            for block in all_blocks:
                bt = block.get('BlockType', 'UNKNOWN')
                block_types[bt] = block_types.get(bt, 0) + 1
            logger.info(f"Block types: {block_types}")
            
            tables = extract_tables(all_blocks)
            raw_text = extract_raw_text(all_blocks)
            logger.info(f"Extracted {len(tables)} tables and {len(raw_text)} chars")
            
            return {
                'file_key': file_key,
                'file_name': file_name,
                'job_id': job_id,
                'tables': tables,
                'raw_text': raw_text
            }
        elif status == 'FAILED':
            raise Exception(f"Textract job failed: {job_id}")
        
        time.sleep(5)
    
    raise Exception(f"Textract job timeout: {job_id}")

def extract_tables(blocks):
    """Extrai estrutura de tabelas"""
    tables = []
    table_blocks = [b for b in blocks if b.get('BlockType') == 'TABLE']
    logger.info(f"Found {len(table_blocks)} table blocks")
    
    # Log IDs de todas as tabelas
    for idx, tb in enumerate(table_blocks, start=1):
        conf = tb.get('Confidence', 0)
        page = tb.get('Page', 'unknown')
        logger.info(f"Table {idx}: ID={tb['Id']}, Page={page}, Confidence={conf:.2f}%")
    
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
        
        # Mesclar linhas quebradas
        table_data['rows'] = merge_broken_rows(table_data['rows'])
        
        logger.info(f"Table {table_block['Id']}: {len(table_data['rows'])} rows")
        tables.append(table_data)
    
    return tables

def merge_broken_rows(rows):
    """Mescla linhas onde a maioria das células está vazia (linhas quebradas)"""
    if not rows or len(rows) < 2:
        return rows
    
    merged = []
    i = 0
    
    while i < len(rows):
        current_row = rows[i]
        
        # Verificar se próxima linha existe e tem muitas células vazias
        if i + 1 < len(rows):
            next_row = rows[i + 1]
            
            # Contar células vazias na próxima linha (excluindo primeiras 2 colunas)
            empty_count = sum(1 for cell in next_row[2:] if not cell or cell.strip() == '')
            total_cells = len(next_row) - 2 if len(next_row) > 2 else len(next_row)
            
            # Verificar se primeira célula tem apenas 1 dígito (possível continuação)
            first_cell = next_row[0] if len(next_row) > 0 else ''
            is_single_digit = first_cell.strip().isdigit() and len(first_cell.strip()) <= 2
            
            # Se mais de 80% das células (exceto primeiras 2) estão vazias OU primeira célula é dígito isolado
            should_merge = (total_cells > 0 and (empty_count / total_cells) > 0.8) or is_single_digit
            
            if should_merge:
                logger.info(f"Merging broken row: {empty_count}/{total_cells} cells empty, first_cell='{first_cell}'")
                logger.info(f"Current row: {current_row[:3]}...")
                logger.info(f"Next row: {next_row[:3]}...")
                
                # Mesclar: concatenar valores não vazios SEM espaço para primeira coluna
                merged_row = []
                for j in range(max(len(current_row), len(next_row))):
                    curr_val = current_row[j] if j < len(current_row) else ''
                    next_val = next_row[j] if j < len(next_row) else ''
                    
                    # Se ambos têm valor
                    if curr_val and next_val:
                        # Primeira coluna: concatenar SEM espaço (códigos)
                        if j == 0:
                            merged_row.append(curr_val + next_val)
                        else:
                            merged_row.append(curr_val + ' ' + next_val)
                    else:
                        merged_row.append(curr_val or next_val)
                
                merged.append(merged_row)
                logger.info(f"Merged result: {merged_row[:3]}...")
                i += 2  # Pular próxima linha pois foi mesclada
                continue
        
        merged.append(current_row)
        i += 1
    
    return merged

def get_cell_text(cell, blocks):
    """Extrai texto de uma célula - TODOS os WORD children"""
    text_parts = []
    if 'Relationships' in cell:
        for relationship in cell['Relationships']:
            if relationship['Type'] == 'CHILD':
                child_ids = relationship['Ids']
                # Buscar todos os WORD blocks filhos
                for child_id in child_ids:
                    child = next((b for b in blocks if b['Id'] == child_id), None)
                    if child and child.get('BlockType') == 'WORD':
                        word_text = child.get('Text', '')
                        text_parts.append(word_text)
    
    full_text = ' '.join(text_parts)
    
    # Log células com códigos que parecem incompletos
    if full_text and len(full_text) > 8 and any(c.isalpha() for c in full_text) and any(c.isdigit() for c in full_text):
        if len(text_parts) > 1:
            logger.info(f"Cell text assembled from {len(text_parts)} parts: {text_parts} -> '{full_text}'")
    
    return full_text

def extract_raw_text(blocks):
    """Extrai todo o texto do documento"""
    lines = []
    for block in blocks:
        if block.get('BlockType') == 'LINE':
            lines.append(block.get('Text', ''))
    return '\n'.join(lines)
