"""Utilitários para lidar com erros comuns de OCR"""

def are_similar_with_ocr_tolerance(val1, val2, max_diff=1):
    """
    Compara dois valores considerando erros comuns de OCR.
    Retorna True se forem iguais ou similares o suficiente.
    """
    str1 = str(val1).upper().strip()
    str2 = str(val2).upper().strip()
    
    if str1 == str2:
        return True
    
    # Erros comuns de OCR
    ocr_errors = {
        '1': 'I', 'I': '1',
        '0': 'O', 'O': '0',
        '5': 'S', 'S': '5',
        '8': 'B', 'B': '8',
        '2': 'Z', 'Z': '2',
        '6': 'G', 'G': '6',
        'A': '4', '4': 'A',
        'L': '1', '1': 'L'
    }
    
    if len(str1) != len(str2):
        return False
    
    diff_count = 0
    for c1, c2 in zip(str1, str2):
        if c1 != c2:
            # Verifica se é um erro comum de OCR
            if ocr_errors.get(c1) == c2 or ocr_errors.get(c2) == c1:
                diff_count += 0.5  # Erro comum conta menos
            else:
                diff_count += 1
    
    return diff_count <= max_diff
