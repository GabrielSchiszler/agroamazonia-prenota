let API_URL = localStorage.getItem('apiUrl') || 'http://localhost:8001';
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
    document.getElementById('apiUrl').value = API_URL;
    showRules('SEMENTES');
    loadProcesses();
    updateDashboard();
    startAutoRefresh();
});

function startAutoRefresh() {
    refreshInterval = setInterval(() => {
        if (selectedProcess) {
            selectProcess(selectedProcess.process_id, true);
        } else {
            loadProcesses(true);
            updateDashboard();
        }
    }, 5000);
}

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
    
    if (pageName === 'processes') loadProcesses();
    if (pageName === 'dashboard') updateDashboard();
}

async function updateDashboard() {
    try {
        const response = await fetch(`${API_URL}/api/v1/process/`);
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
    localStorage.setItem('apiUrl', API_URL);
    showToast('✓ Configuração salva!', 'success');
    loadProcesses();
}

function showCreateModal() {
    document.getElementById('createModal').style.display = 'block';
    document.getElementById('processType').value = '';
    document.getElementById('rulesPreview').innerHTML = '';
    document.getElementById('createAlert').innerHTML = '';
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

async function createProcess() {
    const processType = document.getElementById('processType').value;
    if (!processType) {
        showAlert('createAlert', 'error', '❌ Selecione o tipo de processo');
        return;
    }

    const btn = document.getElementById('createBtn');
    btn.disabled = true;
    btn.textContent = 'Criando...';

    try {
        const response = await fetch(`${API_URL}/api/v1/process/create`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ process_type: processType })
        });

        if (!response.ok) throw new Error('Falha ao criar processo');

        const data = await response.json();
        showToast(`✓ Processo criado com sucesso!`, 'success');
        
        closeCreateModal();
        showPage('processes');
        await loadProcesses();
        selectProcess(data.process_id);

    } catch (error) {
        showAlert('createAlert', 'error', `❌ Erro: ${error.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Criar Processo';
    }
}

async function loadProcesses(silent = false) {
    try {
        const response = await fetch(`${API_URL}/api/v1/process/`);
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
            <div class="process-item" onclick="selectProcess('${p.process_id}')">
                <h3>📄 ${p.process_id.substring(0, 13)}...</h3>
                <p><strong>Tipo:</strong> ${p.process_type}</p>
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
        const response = await fetch(`${API_URL}/api/v1/process/${processId}`);
        if (!response.ok) throw new Error('Falha ao carregar processo');

        selectedProcess = await response.json();
        
        document.getElementById('selectedProcessId').textContent = selectedProcess.process_id;
        document.getElementById('selectedProcessType').textContent = selectedProcess.process_type;
        
        const statusBadge = document.getElementById('selectedProcessStatus');
        statusBadge.textContent = selectedProcess.status;
        statusBadge.className = `status-badge ${selectedProcess.status.toLowerCase()}`;
        
        document.getElementById('processDetails').style.display = 'block';
        document.getElementById('processesList').style.display = 'none';
        document.getElementById('textractResults').innerHTML = '';
        
        loadProcessFiles();

        if (!silent) showToast('✓ Processo carregado', 'info');

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

function deselectProcess() {
    selectedProcess = null;
    document.getElementById('processDetails').style.display = 'none';
    document.getElementById('processesList').style.display = 'grid';
    loadProcesses(true);
}

async function loadProcessFiles() {
    if (!selectedProcess) return;

    const filesList = document.getElementById('filesList');

    if (selectedProcess.files.length === 0) {
        filesList.innerHTML = '<p style="text-align: center; color: #999; padding: 20px;">Nenhum arquivo enviado</p>';
        return;
    }

    filesList.innerHTML = '<h4 style="margin: 20px 0 10px;">Arquivos:</h4>' + selectedProcess.files.map(f => {
        const statusClass = f.status === 'UPLOADED' ? 'uploaded' : 'pending';
        const statusIcon = f.status === 'UPLOADED' ? '✅' : '⏳';
        return `
            <div class="file-item file-${statusClass}">
                <span>${statusIcon} ${f.file_name}</span>
                <span class="file-status">${f.status}</span>
            </div>
        `;
    }).join('');
    
    // Carregar resultados se processo estiver completo
    if (selectedProcess.status === 'COMPLETED') {
        loadTextractResults();
    }
}

async function loadTextractResults() {
    try {
        const response = await fetch(`${API_URL}/api/v1/process/${selectedProcess.process_id}/results`);
        if (!response.ok) return;
        
        const data = await response.json();
        const resultsDiv = document.getElementById('textractResults');
        
        if (!data.results || data.results.length === 0) {
            resultsDiv.innerHTML = '<p style="color: #999;">Nenhum resultado disponível</p>';
            return;
        }
        
        resultsDiv.innerHTML = '<h4 style="margin: 20px 0 10px;">Tabelas Extraídas:</h4>' + 
            data.results.map(r => `
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                    <h5 style="margin: 0 0 10px;">📄 ${r.file_name}</h5>
                    <p style="color: #666; font-size: 0.9em; margin: 5px 0;">Tabelas encontradas: ${r.table_count}</p>
                    ${r.tables.map((table, idx) => `
                        <div style="margin: 15px 0;">
                            <strong>Tabela ${idx + 1}</strong> (Confiança: ${table.confidence.toFixed(1)}%)
                            <div style="overflow-x: auto; margin-top: 10px;">
                                <table style="width: 100%; border-collapse: collapse; background: white;">
                                    ${table.rows.map((row, rowIdx) => `
                                        <tr style="${rowIdx === 0 ? 'background: #e9ecef; font-weight: bold;' : ''}">
                                            ${row.map(cell => `
                                                <td style="border: 1px solid #dee2e6; padding: 8px; font-size: 0.85em;">${cell || '-'}</td>
                                            `).join('')}
                                        </tr>
                                    `).join('')}
                                </table>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `).join('');
    } catch (error) {
        console.error('Erro ao carregar resultados:', error);
    }
}

function handleFileSelect() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];
    if (file) uploadFile();
}

async function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];

    if (!file || !selectedProcess) return;

    try {
        const contentType = file.type || 'application/octet-stream';
        
        const urlResponse = await fetch(`${API_URL}/api/v1/process/presigned-url`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                process_id: selectedProcess.process_id,
                file_name: file.name,
                file_type: contentType
            })
        });

        if (!urlResponse.ok) throw new Error('Falha ao gerar URL');

        const { upload_url } = await urlResponse.json();

        await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('PUT', upload_url, true);
            xhr.setRequestHeader('Content-Type', contentType);
            xhr.onload = () => xhr.status === 200 ? resolve() : reject(new Error('Falha no upload'));
            xhr.onerror = () => reject(new Error('Erro de rede'));
            xhr.send(file);
        });

        showToast(`✓ Arquivo enviado!`, 'success');
        fileInput.value = '';
        
        setTimeout(() => selectProcess(selectedProcess.process_id, true), 2000);

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

async function startProcess() {
    if (!selectedProcess) return;

    if (selectedProcess.files.length === 0) {
        showToast('❌ Envie pelo menos um arquivo', 'error');
        return;
    }

    const btn = document.getElementById('startBtn');
    btn.disabled = true;
    btn.textContent = 'Iniciando...';

    try {
        const response = await fetch(`${API_URL}/api/v1/process/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ process_id: selectedProcess.process_id })
        });

        if (!response.ok) throw new Error('Falha ao iniciar processo');

        showToast('✓ Processamento iniciado!', 'success');
        
        setTimeout(() => deselectProcess(), 1500);

    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = '🚀 Iniciar Processamento';
    }
}

function showRules(processType) {
    document.querySelectorAll('.rule-tab').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');

    const rules = PROCESS_RULES[processType];
    const display = document.getElementById('rulesDisplay');

    display.innerHTML = `
        <h3>Workflow de Validação - ${processType}</h3>
        <p style="background: #f0f4ff; padding: 15px; border-radius: 8px; margin: 15px 0; color: #666;">
            <strong>Como funciona:</strong> As regras são executadas em sequência (Chain of Responsibility). 
            Se uma regra falhar, a ação correspondente é executada e o fluxo para.
        </p>
        ${rules.map(r => `
            <div class="rule-item">
                <h4>Regra ${r.order}: ${r.name}</h4>
                <p>${r.description}</p>
                <span class="rule-status ${r.action.toLowerCase()}">${r.action}</span>
            </div>
        `).join('')}
    `;
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
