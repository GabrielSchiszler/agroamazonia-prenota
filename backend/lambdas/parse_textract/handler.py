import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """Parse Textract blocks para extrair tabelas"""
    logger.info(f"Parsing Textract results")
    
    blocks = event.get('blocks', [])
    tables = extract_tables(blocks)
    
    return {
        'file_key': event['file_key'],
        'file_name': event['file_name'],
        'job_id': event['job_id'],
        'tables': tables
    }

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
