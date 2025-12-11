// Renderiza validações com detalhes de produtos
function renderValidation(v) {
    const isPassed = v.status === 'PASSED';
    const bgColor = isPassed ? '#d4edda' : '#f8d7da';
    const borderColor = isPassed ? '#28a745' : '#dc3545';
    const textColor = isPassed ? '#155724' : '#721c24';
    const icon = isPassed ? '✓' : '✗';
    
    const ruleName = v.rule || v.type || 'Validação';
    
    // Se for validação com estrutura detalhada (produtos ou rastreabilidade)
    const docs = v.docs || v.comparisons || [];
    const isProductValidation = ruleName === 'validar_produtos';
    const isRastreabilidadeValidation = ruleName === 'validar_rastreabilidade';
    
    if ((isProductValidation || isRastreabilidadeValidation) && docs[0]?.items) {
        return renderDetailedValidation(v, bgColor, borderColor, textColor, icon, docs, isRastreabilidadeValidation, ruleName);
    }
    
    // Validação padrão
    return `
        <div style="background: ${bgColor}; padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid ${borderColor};">
            <h5 style="margin: 0 0 10px; color: ${textColor};">${icon} ${ruleName}</h5>
            <div style="margin-bottom: 10px;">
                <strong>DANFE:</strong> ${v.danfe_value}
            </div>
            ${v.message ? `<p style="color: ${textColor}; font-size: 0.9em; margin: 10px 0;">${v.message}</p>` : ''}
            ${renderComparisons(docs)}
        </div>
    `;
}

function renderComparisons(comparisons) {
    return comparisons.map(doc => {
        const isMatch = doc.status === 'MATCH';
        const docBg = isMatch ? 'white' : '#fff3cd';
        const statusBg = isMatch ? '#28a745' : (doc.status === 'MISMATCH' ? '#dc3545' : '#ffc107');
        
        return `
            <div style="background: ${docBg}; padding: 10px; border-radius: 4px; margin: 5px 0;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong>${doc.doc_file || doc.file_name}:</strong> ${doc.doc_value || doc.value || 'NÃO ENCONTRADO'}
                    </div>
                    <span style="padding: 4px 12px; background: ${statusBg}; color: white; border-radius: 12px; font-size: 0.8em; font-weight: bold;">${doc.status}</span>
                </div>
            </div>
        `;
    }).join('');
}

function renderDetailedValidation(v, bgColor, borderColor, textColor, icon, docs, isRastreabilidade = false, ruleName = '') {
    const comparison = docs[0];
    const docFileName = comparison.doc_file || comparison.file_name || 'Documento';
    
    return `
        <div style="background: ${bgColor}; padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid ${borderColor};">
            <h5 style="margin: 0 0 10px; color: ${textColor};">${icon} ${ruleName}</h5>
            <div style="margin-bottom: 10px;">
                <strong>DANFE:</strong> ${v.danfe_value}
            </div>
            ${v.message ? `<p style="color: ${textColor}; font-size: 0.9em; margin: 10px 0;">${v.message}</p>` : ''}
            
            <div style="background: white; padding: 15px; border-radius: 8px; margin-top: 10px;">
                <strong style="display: block; margin-bottom: 10px;">📄 ${docFileName}</strong>
                
                ${comparison.items.map(item => {
                    const itemMatch = item.status === 'MATCH';
                    const itemBg = itemMatch ? '#f8f9fa' : '#fff3cd';
                    const itemBorder = itemMatch ? '#28a745' : '#ffc107';
                    
                    const positionInfo = isRastreabilidade
                        ? `Item ${item.item}: ${item.codigo} - ${item.descricao}`
                        : (item.doc_position 
                            ? `Item ${item.danfe_position} (DANFE) → Item ${item.doc_position} (DOC)` 
                            : `Item ${item.danfe_position} (DANFE) → NÃO ENCONTRADO`);
                    
                    return `
                        <div style="background: ${itemBg}; border-left: 3px solid ${itemBorder}; padding: 12px; margin: 8px 0; border-radius: 4px;">
                            <strong style="display: block; margin-bottom: 8px;">${positionInfo}</strong>
                            
                            <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                                <thead>
                                    <tr style="background: #e9ecef;">
                                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">Campo</th>
                                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">DANFE</th>
                                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">Documento</th>
                                        <th style="padding: 8px; text-align: center; border: 1px solid #dee2e6; width: 80px; font-weight: 600;">Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${Object.entries(item.fields).map(([fieldName, fieldData]) => {
                                        const fieldMatch = fieldData.status === 'MATCH';
                                        const rowBg = fieldMatch ? 'white' : '#ffe5e5';
                                        const statusColor = fieldMatch ? '#28a745' : '#dc3545';
                                        const statusIcon = fieldMatch ? '✓' : '✗';
                                        
                                        return `
                                            <tr style="background: ${rowBg};">
                                                <td style="padding: 6px; border: 1px solid #dee2e6;"><strong>${fieldName}</strong></td>
                                                <td style="padding: 6px; border: 1px solid #dee2e6;">${fieldData.danfe || '-'}</td>
                                                <td style="padding: 6px; border: 1px solid #dee2e6;">${fieldData.doc || '-'}</td>
                                                <td style="padding: 6px; border: 1px solid #dee2e6; text-align: center;">
                                                    <span style="color: ${statusColor}; font-weight: bold;">${statusIcon}</span>
                                                </td>
                                            </tr>
                                        `;
                                    }).join('')}
                                </tbody>
                            </table>
                        </div>
                    `;
                }).join('')}
            </div>
        </div>
    `;
}
