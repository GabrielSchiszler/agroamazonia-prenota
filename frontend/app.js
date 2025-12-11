let API_URL = localStorage.getItem('apiUrl') || window.ENV.API_URL;
let API_KEY = localStorage.getItem('apiKey') || window.ENV.API_KEY;
let selectedProcess = null;
let refreshInterval = null;

const PROCESS_RULES = {
    SEMENTES: [
        { name: 'Validação de Imposto', description: 'Verifica se o imposto total está dentro do limite permitido', action: 'REJECT', order: 1 },
        { name: 'Verificação de Documentação', description: 'Valida a presença do Certificado Fitossanitário obrigatório', action: 'PENDING', order: 2 }
    ],
    AGROQUIMICOS: [
        { name: 'Validação de Licença IBAMA', description: 'Verifica a presença da licença IBAMA obrigatória', action: 'REJECT', order: 1 },
        { name: 'Verificação de Valor', description: 'Compara o valor total do documento com o valor esperado', action: 'PENDING', order: 2 }
    ],
    FERTILIZANTES: [
        { name: 'Validação de Laudo de Composição', description: 'Verifica a presença do laudo de composição química obrigatório', action: 'REJECT', order: 1 }
    ]
};

document.addEventListener('DOMContentLoaded', function() {
    // Limpar localStorage e usar .env
    localStorage.removeItem('apiUrl');
    localStorage.removeItem('apiKey');
    API_URL = window.ENV.API_URL;
    API_KEY = window.ENV.API_KEY;
    
    document.getElementById('apiUrl').value = API_URL;
    document.getElementById('apiKey').value = API_KEY;
    showRules('SEMENTES');
    loadProcesses();
    updateDashboard();
});



function showPage(pageName) {
    document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
    document.querySelectorAll('.page').forEach(page => page.classList.remove('active'));
    
    event.target.classList.add('active');
    document.getElementById(pageName + 'Page').classList.add('active');
    
    const titles = {
        dashboard: 'Dashboard',
        processes: 'Processos',
        rules: 'Regras de Validação',
        settings: 'Configurações'
    };
    document.getElementById('pageTitle').textContent = titles[pageName];
}

async function updateDashboard() {
    try {
        const response = await fetch(`${API_URL}/api/process/`, {
            headers: { 'x-api-key': API_KEY }
        });
        if (!response.ok) return;

        const data = await response.json();
        const processes = data.processes || [];
        
        document.getElementById('totalProcesses').textContent = processes.length;
        document.getElementById('pendingProcesses').textContent = processes.filter(p => p.status === 'PROCESSING').length;
        document.getElementById('completedProcesses').textContent = processes.filter(p => p.status === 'COMPLETED').length;
        
        const recent = processes.slice(0, 5);
        const recentHtml = recent.length > 0 ? `
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Tipo</th>
                        <th>Status</th>
                        <th>Data</th>
                    </tr>
                </thead>
                <tbody>
                    ${recent.map(p => `
                        <tr onclick="showPage('processes'); selectProcess('${p.process_id}')" style="cursor: pointer;">
                            <td>${p.process_id.substring(0, 13)}...</td>
                            <td>${p.process_type}</td>
                            <td><span class="status-badge ${p.status.toLowerCase()}">${p.status}</span></td>
                            <td>${new Date(parseInt(p.created_at) * 1000).toLocaleString('pt-BR')}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        ` : '<p style="text-align: center; color: #999; padding: 40px;">Nenhum processo encontrado</p>';
        
        document.getElementById('recentProcesses').innerHTML = recentHtml;
    } catch (error) {
        console.error('Erro ao atualizar dashboard:', error);
    }
}

function saveApiConfig() {
    API_URL = document.getElementById('apiUrl').value.trim();
    API_KEY = document.getElementById('apiKey').value.trim();
    localStorage.setItem('apiUrl', API_URL);
    localStorage.setItem('apiKey', API_KEY);
    showToast('✓ Configuração salva!', 'success');
    loadProcesses();
}

function showCreateModal() {
    // Gerar UUID e criar processo diretamente
    const processId = crypto.randomUUID();
    
    selectedProcess = {
        process_id: processId,
        process_type: null,
        status: 'CREATED',
        files: { danfe: [], additional: [] }
    };
    
    showToast(`✓ Processo criado: ${processId.substring(0, 8)}...`, 'success');
    showPage('processes');
    
    document.getElementById('selectedProcessId').textContent = selectedProcess.process_id;
    document.getElementById('selectedProcessType').textContent = 'Aguardando tipo...';
    
    const statusBadge = document.getElementById('selectedProcessStatus');
    statusBadge.textContent = 'CREATED';
    statusBadge.className = 'status-badge created';
    
    document.getElementById('processDetails').style.display = 'block';
    document.getElementById('processesList').style.display = 'none';
    
    loadProcessFiles();
}

function closeCreateModal() {
    document.getElementById('createModal').style.display = 'none';
}

function showRulesPreview() {
    const processType = document.getElementById('processType').value;
    const preview = document.getElementById('rulesPreview');

    if (!processType) {
        preview.innerHTML = '';
        return;
    }

    const rules = PROCESS_RULES[processType];
    preview.innerHTML = '<h4 style="margin: 20px 0 10px;">Regras que serão aplicadas:</h4>' + rules.map(r => `
        <div class="rule-item">
            <h4>${r.order}. ${r.name}</h4>
            <p>${r.description}</p>
            <span class="rule-status ${r.action.toLowerCase()}">${r.action}</span>
        </div>
    `).join('');
}



async function loadProcesses(silent = false) {
    try {
        const response = await fetch(`${API_URL}/api/process/`, {
            headers: { 'x-api-key': API_KEY }
        });
        if (!response.ok) return;

        const data = await response.json();
        const list = document.getElementById('processesList');

        if (data.processes.length === 0) {
            list.innerHTML = `
                <div class="empty-state">
                    <div style="font-size: 4em;">📋</div>
                    <h3>Nenhum processo encontrado</h3>
                    <p>Clique em "Novo Processo" para começar</p>
                </div>
            `;
            return;
        }

        list.innerHTML = data.processes.map(p => `
            <div class="process-item ${p.status === 'FAILED' ? 'failed' : ''}" onclick="selectProcess('${p.process_id}')">
                <h3>${p.status === 'FAILED' ? '❌' : '📄'} ${p.process_id.substring(0, 13)}...</h3>
                <p><strong>Tipo:</strong> ${p.process_type || 'N/A'}</p>
                <p><strong>Criado:</strong> ${new Date(parseInt(p.created_at) * 1000).toLocaleString('pt-BR')}</p>
                <span class="status-badge ${p.status.toLowerCase()}">${p.status}</span>
            </div>
        `).join('');

    } catch (error) {
        console.error('Erro ao carregar processos:', error);
    }
}

async function selectProcess(processId, silent = false) {
    try {
        const response = await fetch(`${API_URL}/api/process/${processId}`, {
            headers: { 'x-api-key': API_KEY }
        });
        if (!response.ok) throw new Error('Falha ao carregar processo');

        selectedProcess = await response.json();
        
        document.getElementById('selectedProcessId').textContent = selectedProcess.process_id;
        document.getElementById('selectedProcessType').textContent = selectedProcess.process_type || 'N/A';
        
        const statusBadge = document.getElementById('selectedProcessStatus');
        statusBadge.textContent = selectedProcess.status;
        statusBadge.className = `status-badge ${selectedProcess.status.toLowerCase()}`;
        
        // Exibir SCTASK ID se existir
        console.log('SCTASK ID:', selectedProcess.sctask_id);
        const sctaskDiv = document.getElementById('sctaskInfo');
        if (selectedProcess.sctask_id) {
            sctaskDiv.innerHTML = `<span class="sctask-badge">🎫 SCTASK: ${selectedProcess.sctask_id}</span>`;
            console.log('SCTASK exibido:', selectedProcess.sctask_id);
        } else {
            sctaskDiv.innerHTML = '';
            console.log('SCTASK não encontrado no processo');
        }
        
        document.getElementById('processDetails').style.display = 'block';
        document.getElementById('processesList').style.display = 'none';
        document.getElementById('textractResults').innerHTML = '';
        document.getElementById('extractedData').innerHTML = '';
        
        loadProcessFiles();
        
        // Carregar validações se processo estiver completo, validado ou falhou
        if (selectedProcess.status === 'COMPLETED' || selectedProcess.status === 'VALIDATED' || selectedProcess.status === 'FAILED') {
            await loadValidationResults();
        }

        if (!silent) showToast('✓ Processo carregado', 'info');

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

function deselectProcess() {
    selectedProcess = null;
    document.getElementById('processDetails').style.display = 'none';
    document.getElementById('processesList').style.display = 'grid';
}

async function loadProcessFiles() {
    if (!selectedProcess) return;

    const danfeList = document.getElementById('danfeList');
    const docsList = document.getElementById('docsList');

    // DANFE files
    if (selectedProcess.files.danfe && selectedProcess.files.danfe.length > 0) {
        danfeList.innerHTML = selectedProcess.files.danfe.map(f => {
            const statusClass = f.status === 'UPLOADED' ? 'uploaded' : 'pending';
            const statusIcon = f.status === 'UPLOADED' ? '✅' : '⏳';
            const downloadBtn = f.status === 'UPLOADED' ? `<button onclick="downloadFile('${f.file_key}', '${f.file_name}')" style="padding: 4px 8px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">📥 Baixar</button>` : '';
            return `
                <div class="file-item file-${statusClass}" style="display: flex; justify-content: space-between; align-items: center;">
                    <span>${statusIcon} ${f.file_name}</span>
                    <div style="display: flex; gap: 8px; align-items: center;">
                        <span class="file-status">${f.status}</span>
                        ${downloadBtn}
                    </div>
                </div>
            `;
        }).join('');
    } else {
        danfeList.innerHTML = '<p style="color: #999; font-size: 0.9em;">⚠️ Nenhum DANFE enviado</p>';
    }

    // Additional docs
    if (selectedProcess.files.additional && selectedProcess.files.additional.length > 0) {
        docsList.innerHTML = selectedProcess.files.additional.map(f => {
            const statusClass = f.status === 'UPLOADED' ? 'uploaded' : 'pending';
            const statusIcon = f.status === 'UPLOADED' ? '✅' : '⏳';
            const downloadBtn = f.status === 'UPLOADED' ? `<button onclick="downloadFile('${f.file_key}', '${f.file_name}')" style="padding: 4px 8px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">📥 Baixar</button>` : '';
            return `
                <div class="file-item file-${statusClass}" style="display: flex; justify-content: space-between; align-items: center;">
                    <span>${statusIcon} ${f.file_name}</span>
                    <div style="display: flex; gap: 8px; align-items: center;">
                        <span class="file-status">${f.status}</span>
                        ${downloadBtn}
                    </div>
                </div>
            `;
        }).join('');
    } else {
        docsList.innerHTML = '<p style="color: #999; font-size: 0.9em;">Nenhum documento adicional</p>';
    }
    
    // Carregar dados extraídos e validações se processo estiver completo, validado ou falhou
    if (selectedProcess.status === 'COMPLETED' || selectedProcess.status === 'VALIDATED' || selectedProcess.status === 'FAILED') {
        loadExtractedData();
    }
}

async function loadExtractedData() {
    const extractedDiv = document.getElementById('extractedData');
    
    if (!selectedProcess.parsing_results || selectedProcess.parsing_results.length === 0) {
        extractedDiv.innerHTML = '<p style="color: #999;">Nenhum dado extraído disponível</p>';
        return;
    }
    
    let html = '';
    
    selectedProcess.parsing_results.forEach(result => {
        if (result.source === 'XML') {
            html += `
                <div style="margin-bottom: 20px;">
                    <h5 style="margin-bottom: 10px;">📝 ${result.file_name} (XML)</h5>
                    <details style="background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #dee2e6; max-width: 100%;">
                        <summary style="cursor: pointer; font-weight: bold; color: #007bff;">Ver JSON Extraído</summary>
                        <pre style="margin-top: 10px; background: white; padding: 15px; border-radius: 4px; overflow-x: auto; font-size: 0.85em; max-width: 100%; white-space: pre-wrap; word-wrap: break-word;">${JSON.stringify(result.parsed_data, null, 2)}</pre>
                    </details>
                </div>
            `;
        } else if (result.source === 'OCR') {
            html += `
                <div style="margin-bottom: 20px;">
                    <h5 style="margin-bottom: 10px;">📷 ${result.file_name} (OCR)</h5>
                    <details style="background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #dee2e6; max-width: 100%;">
                        <summary style="cursor: pointer; font-weight: bold; color: #28a745;">Ver JSON Extraído</summary>
                        <pre style="margin-top: 10px; background: white; padding: 15px; border-radius: 4px; overflow-x: auto; font-size: 0.85em; max-width: 100%; white-space: pre-wrap; word-wrap: break-word;">${JSON.stringify(result.parsed_data, null, 2)}</pre>
                    </details>
                </div>
            `;
        }
    });
    
    extractedDiv.innerHTML = html || '<p style="color: #999;">Nenhum dado extraído disponível</p>';
}

async function loadValidationResults() {
    try {
        const response = await fetch(`${API_URL}/api/process/${selectedProcess.process_id}/validations`, {
            headers: { 'x-api-key': API_KEY }
        });
        if (!response.ok) return;
        
        const data = await response.json();
        const resultsDiv = document.getElementById('textractResults');
        
        if (!data.validations || data.validations.length === 0) {
            resultsDiv.innerHTML = '<p style="color: #999;">Nenhuma validação disponível</p>';
            return;
        }
        
        resultsDiv.innerHTML = '<h4 style="margin: 20px 0 10px;">Resultados das Validações:</h4>' + 
            data.validations.map(v => renderValidation(v)).join('');
    } catch (error) {
        console.error('Erro ao carregar validações:', error);
    }
}

function handleDanfeSelect() {
    const fileInput = document.getElementById('danfeInput');
    const file = fileInput.files[0];
    if (file) uploadFile(file, 'DANFE', fileInput);
}

function handleDocsSelect() {
    const fileInput = document.getElementById('docsInput');
    const files = Array.from(fileInput.files);
    files.forEach(file => uploadFile(file, 'ADDITIONAL', fileInput));
}

async function uploadFile(file, docType, fileInput) {
    if (!file || !selectedProcess) return;

    try {
        const contentType = file.type || 'application/octet-stream';
        
        let urlResponse, upload_url;
        
        if (docType === 'DANFE') {
            urlResponse = await fetch(`${API_URL}/api/process/presigned-url/xml`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                },
                body: JSON.stringify({
                    process_id: selectedProcess.process_id,
                    file_name: file.name,
                    file_type: contentType
                })
            });
            if (!urlResponse.ok) throw new Error('Falha ao gerar URL');
            const data = await urlResponse.json();
            upload_url = data.upload_url;
        } else {
            urlResponse = await fetch(`${API_URL}/api/process/presigned-url/docs`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                },
                body: JSON.stringify({
                    process_id: selectedProcess.process_id,
                    files: [{
                        file_name: file.name,
                        file_type: contentType
                    }]
                })
            });
            if (!urlResponse.ok) throw new Error('Falha ao gerar URL');
            const data = await urlResponse.json();
            upload_url = data.urls[0].upload_url;
        }

        await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('PUT', upload_url, true);
            xhr.setRequestHeader('Content-Type', contentType);
            xhr.onload = () => xhr.status === 200 ? resolve() : reject(new Error('Falha no upload'));
            xhr.onerror = () => reject(new Error('Erro de rede'));
            xhr.send(file);
        });

        showToast(`✓ ${docType === 'DANFE' ? 'DANFE' : 'Documento'} enviado!`, 'success');
        fileInput.value = '';
        
        setTimeout(() => selectProcess(selectedProcess.process_id, true), 2000);

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

async function startProcess() {
    if (!selectedProcess) return;

    if (!selectedProcess.files.danfe || selectedProcess.files.danfe.length === 0) {
        showToast('❌ ⚠️ DANFE é obrigatório!', 'error');
        return;
    }

    if (!selectedProcess.files.additional || selectedProcess.files.additional.length === 0) {
        showToast('❌ Envie pelo menos um documento adicional', 'error');
        return;
    }

    const btn = document.getElementById('startBtn');
    btn.disabled = true;
    btn.textContent = 'Iniciando...';

    try {
        const response = await fetch(`${API_URL}/api/process/start`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'x-api-key': API_KEY
            },
            body: JSON.stringify({ 
                process_id: selectedProcess.process_id
            })
        });

        if (!response.ok) throw new Error('Falha ao iniciar processo');

        showToast(`✓ Processamento iniciado (AGROQUIMICOS)!`, 'success');
        
        // Manter na página e atualizar após 2 segundos
        setTimeout(() => refreshProcess(), 2000);

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = '🚀 Iniciar Processamento';
    }
}

async function refreshProcess() {
    if (!selectedProcess) return;
    
    // Mostrar loading
    const refreshBtn = event.target;
    const originalContent = refreshBtn.innerHTML;
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = '⏳';
    
    try {
        // Recarregar processo
        await selectProcess(selectedProcess.process_id, true);
        
        // Recarregar validações
        if (selectedProcess.status === 'COMPLETED' || selectedProcess.status === 'VALIDATED' || selectedProcess.status === 'FAILED') {
            await loadValidationResults();
        }
        
        showToast('✓ Processo atualizado', 'success');
    } catch (error) {
        showToast('❌ Erro ao atualizar', 'error');
    } finally {
        refreshBtn.disabled = false;
        refreshBtn.innerHTML = originalContent;
    }
}

let currentProcessType = 'SEMENTES';

const AVAILABLE_RULES = [
    { name: 'validar_numero_nota', description: 'Valida número da nota fiscal' },
    { name: 'validar_serie', description: 'Valida série da nota fiscal' },
    { name: 'validar_data_emissao', description: 'Valida data de emissão' },
    { name: 'validar_cnpj_fornecedor', description: 'Valida CNPJ do fornecedor' },
    { name: 'validar_produtos', description: 'Valida produtos (descrição, quantidade, valores)' },
    { name: 'validar_rastreabilidade', description: 'Valida rastreabilidade (lote, validade, fabricação)' },
    { name: 'validar_icms', description: 'Valida ICMS (interno zerado, interestadual)' }
];

async function showRules(processType) {
    currentProcessType = processType;
    document.querySelectorAll('.rule-tab').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');

    const display = document.getElementById('rulesDisplay');
    display.innerHTML = '<p style="text-align: center; padding: 40px;">Carregando...</p>';

    try {
        const response = await fetch(`${API_URL}/api/rules/${processType}`, {
            headers: { 'x-api-key': API_KEY }
        });
        const data = response.ok ? await response.json() : { rules: [] };
        const activeRules = data.rules || [];
        const activeRuleNames = activeRules.map(r => r.rule_name || r.RULE_NAME);

        display.innerHTML = `
            <h3>Regras de Validação - ${processType}</h3>
            <p style="background: #f0f4ff; padding: 15px; border-radius: 8px; margin: 15px 0; color: #666;">
                <strong>✓</strong> Marque as regras que deseja ativar para este tipo de processo.
            </p>
            <div style="display: grid; gap: 15px;">
                ${AVAILABLE_RULES.map((rule, index) => {
                    const isActive = activeRuleNames.includes(rule.name);
                    return `
                        <div style="background: white; padding: 20px; border-radius: 8px; border: 2px solid ${isActive ? '#28a745' : '#e0e0e0'}; display: flex; align-items: center; gap: 15px;">
                            <input type="checkbox" 
                                id="rule_${rule.name}" 
                                ${isActive ? 'checked' : ''}
                                onchange="toggleRule('${processType}', '${rule.name}', this.checked, ${index + 1})"
                                style="width: 20px; height: 20px; cursor: pointer;">
                            <label for="rule_${rule.name}" style="flex: 1; cursor: pointer; margin: 0;">
                                <strong style="display: block; font-size: 1.1em; margin-bottom: 5px;">${rule.description}</strong>
                                <span style="color: #666; font-size: 0.9em;">Função: ${rule.name}</span>
                            </label>
                            ${isActive ? '<span style="color: #28a745; font-weight: bold;">✓ ATIVA</span>' : '<span style="color: #999;">○ Inativa</span>'}
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    } catch (error) {
        display.innerHTML = `<p style="color: red; text-align: center; padding: 40px;">❌ Erro: ${error.message}</p>`;
    }
}

async function toggleRule(processType, ruleName, isEnabled, order) {
    try {
        if (isEnabled) {
            // Adicionar regra
            const response = await fetch(`${API_URL}/api/rules/`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                },
                body: JSON.stringify({
                    process_type: processType,
                    rule_name: ruleName,
                    order: order,
                    enabled: true
                })
            });
            if (!response.ok) throw new Error('Falha ao ativar regra');
            showToast('✓ Regra ativada!', 'success');
        } else {
            // Remover regra
            const response = await fetch(`${API_URL}/api/rules/${processType}/${ruleName}`, {
                method: 'DELETE',
                headers: { 'x-api-key': API_KEY }
            });
            if (!response.ok) throw new Error('Falha ao desativar regra');
            showToast('✓ Regra desativada!', 'success');
        }
        
        // Recarregar regras
        setTimeout(() => showRules(processType), 500);
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
        // Reverter checkbox
        document.getElementById(`rule_${ruleName}`).checked = !isEnabled;
    }
}

function showAlert(elementId, type, message) {
    const alertDiv = document.getElementById(elementId);
    alertDiv.innerHTML = `<div class="alert ${type}">${message}</div>`;
    setTimeout(() => alertDiv.innerHTML = '', 5000);
}

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    
    setTimeout(() => toast.classList.add('show'), 100);
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

async function downloadFile(fileKey, fileName) {
    try {
        const response = await fetch(`${API_URL}/api/process/download`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': API_KEY
            },
            body: JSON.stringify({ file_key: fileKey })
        });
        
        if (!response.ok) throw new Error('Falha ao gerar URL de download');
        
        const data = await response.json();
        window.open(data.download_url, '_blank');
        showToast('✓ Abrindo arquivo...', 'success');
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}
