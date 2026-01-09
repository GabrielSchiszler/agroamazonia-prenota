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
    // Se for validar_cfop_chave, exibir dados encontrados de forma mais detalhada
    const isCfopChave = ruleName === 'validar_cfop_chave';
    const cfopData = v.cfop_data || (docs[0] && docs[0].chave ? {
        chave: docs[0].chave,
        operacao: docs[0].operacao,
        descricao: docs[0].descricao,
        regra: docs[0].regra,
        observacao: docs[0].observacao,
        cfop: docs[0].cfop_encontrado
    } : null);
    
    return `
        <div style="background: ${bgColor}; padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid ${borderColor};">
            <h5 style="margin: 0 0 10px; color: ${textColor};">${icon} ${ruleName}</h5>
            <div style="margin-bottom: 10px;">
                <strong>DANFE:</strong> ${v.danfe_value}
            </div>
            ${v.message ? `<p style="color: ${textColor}; font-size: 0.9em; margin: 10px 0;">${v.message}</p>` : ''}
            ${isCfopChave && cfopData ? `
                <div style="background: white; padding: 12px; border-radius: 6px; margin-top: 10px; border: 1px solid ${borderColor};">
                    ${cfopData.multiple_mappings ? `
                        <h6 style="margin: 0 0 10px; color: #dc3545; font-size: 0.95em;">⚠️ ERRO: Múltiplos Mapeamentos Encontrados (${cfopData.mappings_count})</h6>
                        <p style="color: #721c24; font-size: 0.9em; margin-bottom: 15px;">
                            O CFOP <strong>${cfopData.cfop}</strong> foi encontrado com <strong>${cfopData.mappings_count} mapeamento(s)</strong> diferentes. 
                            Isso causa ambiguidade e impede a validação. Por favor, corrija a configuração para que cada CFOP tenha apenas um mapeamento.
                        </p>
                        <div style="margin-top: 15px;">
                            <strong style="display: block; margin-bottom: 10px; color: #333;">Mapeamentos Encontrados:</strong>
                            ${cfopData.mappings.map((mapping, idx) => `
                                <div style="background: #fff3cd; border-left: 3px solid #ffc107; padding: 10px; margin: 8px 0; border-radius: 4px;">
                                    <strong style="color: #856404;">Mapeamento ${idx + 1}:</strong>
                                    <table style="width: 100%; font-size: 0.9em; border-collapse: collapse; margin-top: 8px;">
                                        <tr>
                                            <td style="padding: 4px; font-weight: 600; color: #666; width: 100px;">Chave:</td>
                                            <td style="padding: 4px; color: #333; font-weight: 600;">${mapping.chave || '-'}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding: 4px; font-weight: 600; color: #666;">Operação:</td>
                                            <td style="padding: 4px; color: #333;">${mapping.operacao || '-'}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding: 4px; font-weight: 600; color: #666;">Descrição:</td>
                                            <td style="padding: 4px; color: #333;">${mapping.descricao || '-'}</td>
                                        </tr>
                                        ${mapping.regra ? `
                                        <tr>
                                            <td style="padding: 4px; font-weight: 600; color: #666;">Regra:</td>
                                            <td style="padding: 4px; color: #333;">${mapping.regra}</td>
                                        </tr>
                                        ` : ''}
                                        ${mapping.observacao ? `
                                        <tr>
                                            <td style="padding: 4px; font-weight: 600; color: #666;">Observação:</td>
                                            <td style="padding: 4px; color: #333;">${mapping.observacao}</td>
                                        </tr>
                                        ` : ''}
                                    </table>
                                </div>
                            `).join('')}
                        </div>
                    ` : `
                        <h6 style="margin: 0 0 10px; color: #333; font-size: 0.95em;">📋 Dados Encontrados:</h6>
                        <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666; width: 120px;">CFOP:</td>
                                <td style="padding: 6px; color: #333;">${cfopData.cfop || '-'}</td>
                            </tr>
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666;">Chave:</td>
                                <td style="padding: 6px; color: #333; font-weight: 600;">${cfopData.chave || '-'}</td>
                            </tr>
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666;">Operação:</td>
                                <td style="padding: 6px; color: #333;">${cfopData.operacao || '-'}</td>
                            </tr>
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666;">Descrição:</td>
                                <td style="padding: 6px; color: #333;">${cfopData.descricao || '-'}</td>
                            </tr>
                            ${cfopData.regra ? `
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666;">Regra:</td>
                                <td style="padding: 6px; color: #333;">${cfopData.regra}</td>
                            </tr>
                            ` : ''}
                            ${cfopData.observacao ? `
                            <tr>
                                <td style="padding: 6px; font-weight: 600; color: #666;">Observação:</td>
                                <td style="padding: 6px; color: #333;">${cfopData.observacao}</td>
                            </tr>
                            ` : ''}
                        </table>
                    `}
                </div>
            ` : ''}
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
