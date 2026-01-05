let API_URL = localStorage.getItem('apiUrl') || window.ENV.API_URL;
let API_KEY = localStorage.getItem('apiKey') || window.ENV.API_KEY;
let selectedProcess = null;
let refreshInterval = null;
let currentPage = 'dashboard';
let dailyProcessesChart, successErrorRateChart, hourlyChart, errorChart, typeChart, failedRulesChart, failedRulesByDayChart;

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
    loadDashboardMetrics();
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
    
    // Atualizar página atual
    currentPage = pageName;
    
    // Mostrar/ocultar botão de refresh
    const refreshBtn = document.getElementById('refreshBtn');
    if (refreshBtn) {
        refreshBtn.style.display = pageName === 'dashboard' ? 'inline-block' : 'none';
    }
    
    // Gerenciar auto-refresh do dashboard
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
    
    if (pageName === 'dashboard') {
        loadDashboardMetrics();
        // Auto-refresh a cada 60 segundos apenas no dashboard
        refreshInterval = setInterval(() => {
            if (currentPage === 'dashboard') {
                loadDashboardMetrics();
            }
        }, 60000);
    }
}

let currentStartDate = null;
let currentEndDate = null;

async function loadDashboardMetrics() {
    try {
        let url = `${API_URL}/api/v1/dashboard/metrics`;
        if (currentStartDate && currentEndDate) {
            url += `?start_date=${currentStartDate}&end_date=${currentEndDate}`;
        }
        
        const response = await fetch(url, {
            headers: { 'x-api-key': API_KEY }
        });
        if (!response.ok) return;

        const data = await response.json();
        updateMetricCards(data);
        createDailyProcessesChart(data);
        createSuccessErrorRateChart(data);
        createHourlyChart(data);
        createErrorChart(data);
        createTypeChart(data);
        createFailedRulesChart(data);
        createFailedRulesByDayChart(data);
        
    } catch (error) {
        console.error('Erro ao carregar métricas:', error);
        showToast('❌ Erro ao carregar métricas', 'error');
    }
}

function applyDateFilter() {
    const startDate = document.getElementById('startDateFilter').value;
    const endDate = document.getElementById('endDateFilter').value;
    
    if (!startDate || !endDate) {
        showToast('⚠️ Selecione data inicial e final', 'error');
        return;
    }
    
    if (new Date(startDate) > new Date(endDate)) {
        showToast('⚠️ Data inicial deve ser anterior à data final', 'error');
        return;
    }
    
    currentStartDate = startDate;
    currentEndDate = endDate;
    loadDashboardMetrics();
    showToast('✓ Filtro aplicado', 'success');
}

function resetDateFilter() {
    currentStartDate = null;
    currentEndDate = null;
    document.getElementById('startDateFilter').value = '';
    document.getElementById('endDateFilter').value = '';
    loadDashboardMetrics();
    showToast('✓ Filtro resetado', 'success');
}

function setFilterPeriod(period) {
    const today = new Date();
    let startDate, endDate;
    
    switch(period) {
        case 'today':
            startDate = endDate = today.toISOString().split('T')[0];
            break;
        case 'week':
            startDate = new Date(today.getTime() - 6 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
            endDate = today.toISOString().split('T')[0];
            break;
        case 'month':
            startDate = new Date(today.getFullYear(), today.getMonth(), 1).toISOString().split('T')[0];
            endDate = today.toISOString().split('T')[0];
            break;
        case 'lastMonth':
            const lastMonth = new Date(today.getFullYear(), today.getMonth() - 1, 1);
            startDate = lastMonth.toISOString().split('T')[0];
            endDate = new Date(today.getFullYear(), today.getMonth(), 0).toISOString().split('T')[0];
            break;
    }
    
    document.getElementById('startDateFilter').value = startDate;
    document.getElementById('endDateFilter').value = endDate;
    applyDateFilter();
}

function refreshDashboard() {
    loadDashboardMetrics();
    showToast('✓ Dashboard atualizado', 'success');
}

function updateMetricCards(data) {
    // Suportar tanto formato antigo (today) quanto novo (period)
    const summary = data.summary || {};
    const today = data.today || {};
    
    // Se há período filtrado, usar summary; senão usar today
    const total = summary.total || today.total_count || 0;
    const success = summary.success || today.success_count || 0;
    const failed = summary.failed || today.failed_count || 0;
    const successRate = summary.success_rate || summary.success_rate_week || today.success_rate || 0;
    
    document.getElementById('totalToday').textContent = total;
    document.getElementById('successToday').textContent = success;
    document.getElementById('failedToday').textContent = failed;
    document.getElementById('successRate').textContent = successRate.toFixed(1) + '%';
    
    // Formatar tempo médio de processamento (usar summary quando disponível, senão today)
    const avgTime = summary.avg_processing_time || today.avg_processing_time || 0;
    let timeDisplay;
    if (avgTime >= 60) {
        const minutes = Math.floor(avgTime / 60);
        const seconds = Math.round(avgTime % 60);
        timeDisplay = `${minutes}m ${seconds}s`;
    } else {
        timeDisplay = `${Math.round(avgTime)}s`;
    }
    document.getElementById('avgTime').textContent = timeDisplay;
}

function createDailyProcessesChart(data) {
    const ctx = document.getElementById('dailyProcessesChart').getContext('2d');
    const periodData = data.period || data.last_7_days || [];
    
    if (dailyProcessesChart) dailyProcessesChart.destroy();
    
    // Formatar datas para exibição
    const labels = periodData.map(d => {
        const date = new Date(d.date);
        return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    }).reverse();
    
    dailyProcessesChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Sucessos',
                data: periodData.map(d => d.success).reverse(),
                backgroundColor: 'rgba(16, 185, 129, 0.8)',
                borderColor: '#10b981',
                borderWidth: 2,
                borderRadius: 6
            }, {
                label: 'Falhas',
                data: periodData.map(d => d.failed).reverse(),
                backgroundColor: 'rgba(239, 68, 68, 0.8)',
                borderColor: '#ef4444',
                borderWidth: 2,
                borderRadius: 6
            }, {
                label: 'Total',
                data: periodData.map(d => d.total).reverse(),
                type: 'line',
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                borderWidth: 3,
                tension: 0.4,
                fill: false,
                pointRadius: 5,
                pointHoverRadius: 7
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 15,
                        font: { size: 12, weight: '600' }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: { size: 14, weight: 'bold' },
                    bodyFont: { size: 13 }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        font: { size: 11 }
                    }
                },
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        font: { size: 11 }
                    }
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            }
        }
    });
}

function createSuccessErrorRateChart(data) {
    const ctx = document.getElementById('successErrorRateChart').getContext('2d');
    const summary = data.summary || {};
    const today = data.today || {};
    
    const total = summary.total || today.total_count || 0;
    const success = summary.success || today.success_count || 0;
    const failed = summary.failed || today.failed_count || 0;
    
    if (successErrorRateChart) successErrorRateChart.destroy();
    
    successErrorRateChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Sucessos', 'Falhas'],
            datasets: [{
                data: [success, failed],
                backgroundColor: [
                    'rgba(16, 185, 129, 0.9)',
                    'rgba(239, 68, 68, 0.9)'
                ],
                borderColor: [
                    '#10b981',
                    '#ef4444'
                ],
                borderWidth: 3,
                hoverOffset: 10
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 15,
                        usePointStyle: true,
                        font: { size: 12, weight: '600' }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    },
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12
                }
            },
            cutout: '60%'
        }
    });
}

function createHourlyChart(data) {
    const ctx = document.getElementById('hourlyChart').getContext('2d');
    const hourly = data.today?.processes_by_hour || {};
    
    if (hourlyChart) hourlyChart.destroy();
    
    const hours = Array.from({length: 24}, (_, i) => i.toString().padStart(2, '0'));
    const counts = hours.map(h => hourly[h] || 0);
    
    hourlyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: hours.map(h => h + ':00'),
            datasets: [{
                label: 'Processos',
                data: counts,
                backgroundColor: '#667eea',
                borderColor: '#5568d3',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true }
            }
        }
    });
}

function createErrorChart(data) {
    const ctx = document.getElementById('errorChart').getContext('2d');
    const errors = data.today?.failure_reasons || {};
    
    if (errorChart) errorChart.destroy();
    
    const labels = Object.keys(errors);
    const values = Object.values(errors);
    
    // Se não há erros, criar gráfico vazio
    if (labels.length === 0) {
        errorChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Sem erros'],
                datasets: [{
                    data: [1],
                    backgroundColor: ['#10b981']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false }
                }
            }
        });
        return;
    }
    
    const colorPalette = [
        'rgba(239, 68, 68, 0.9)',   // Vermelho
        'rgba(245, 158, 11, 0.9)',  // Laranja
        'rgba(139, 92, 246, 0.9)',  // Roxo
        'rgba(236, 72, 153, 0.9)',  // Rosa
        'rgba(6, 182, 212, 0.9)',   // Ciano
        'rgba(34, 197, 94, 0.9)'    // Verde
    ];
    
    errorChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels.map(label => {
                const translations = {
                    'VALIDATION_FAILED': 'Validação Falhou',
                    'TEXTRACT_ERROR': 'Erro OCR',
                    'PROCESSING_ERROR': 'Erro Processamento',
                    'API_ERROR': 'Erro API',
                    'TIMEOUT_ERROR': 'Timeout'
                };
                return translations[label] || label;
            }),
            datasets: [{
                data: values,
                backgroundColor: colorPalette.slice(0, labels.length),
                borderColor: colorPalette.slice(0, labels.length).map(c => c.replace('0.9', '1')),
                borderWidth: 3,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 12,
                        usePointStyle: true,
                        font: { size: 12, weight: '600' }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    },
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12
                }
            },
            cutout: '60%'
        }
    });
}

function createTypeChart(data) {
    const ctx = document.getElementById('typeChart').getContext('2d');
    const types = data.processes_by_type || data.processes_by_type_week || data.today?.processes_by_type || {};
    
    if (typeChart) typeChart.destroy();
    
    const labels = ['Sementes', 'Agroquímicos', 'Fertilizantes'];
    const values = [
        types.SEMENTES || 0,
        types.AGROQUIMICOS || 0,
        types.FERTILIZANTES || 0
    ];
    
    typeChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: [
                    'rgba(16, 185, 129, 0.9)',
                    'rgba(59, 130, 246, 0.9)',
                    'rgba(245, 158, 11, 0.9)'
                ],
                borderColor: [
                    '#10b981',
                    '#3b82f6',
                    '#f59e0b'
                ],
                borderWidth: 3,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: {
                        padding: 12,
                        usePointStyle: true,
                        font: { size: 12, weight: '600' }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    },
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12
                }
            },
            cutout: '60%'
        }
    });
}

function createFailedRulesChart(data) {
    const ctx = document.getElementById('failedRulesChart').getContext('2d');
    const failedRules = data.failed_rules || data.failed_rules_week || {};
    
    if (failedRulesChart) failedRulesChart.destroy();
    
    const labels = Object.keys(failedRules);
    const values = Object.values(failedRules);
    
    // Se não há regras que falharam, criar gráfico vazio
    if (labels.length === 0) {
        failedRulesChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Nenhuma regra falhou'],
                datasets: [{
                    data: [0],
                    backgroundColor: ['rgba(16, 185, 129, 0.8)']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
        return;
    }
    
    // Ordenar por valor (maior para menor) e pegar top 10
    const sorted = labels.map((label, i) => ({ label, value: values[i] }))
        .sort((a, b) => b.value - a.value)
        .slice(0, 10);
    
    // Traduzir nomes de regras
    const ruleTranslations = {
        'validar_produtos': 'Validar Produtos',
        'validar_cnpj_fornecedor': 'Validar CNPJ Fornecedor',
        'validar_numero_pedido': 'Validar Número Pedido',
        'validar_valor_total': 'Validar Valor Total',
        'validar_data_emissao': 'Validar Data Emissão'
    };
    
    failedRulesChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: sorted.map(item => ruleTranslations[item.label] || item.label),
            datasets: [{
                label: 'Falhas',
                data: sorted.map(item => item.value),
                backgroundColor: 'rgba(239, 68, 68, 0.8)',
                borderColor: '#ef4444',
                borderWidth: 2,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        padding: 10,
                    callbacks: {
                        label: function(context) {
                            return `Falhas: ${context.parsed.x}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    grid: { color: 'rgba(0, 0, 0, 0.05)' },
                    ticks: { font: { size: 11 }, stepSize: 1 }
                },
                y: {
                    grid: { display: false },
                    ticks: { font: { size: 11 } }
                }
            }
        }
    });
}

function createFailedRulesByDayChart(data) {
    const ctx = document.getElementById('failedRulesByDayChart').getContext('2d');
    const periodData = data.period || data.last_7_days || [];
    
    if (failedRulesByDayChart) failedRulesByDayChart.destroy();
    
    // Coletar todas as regras que falharam no período
    const allRules = new Set();
    periodData.forEach(day => {
        Object.keys(day.failed_rules || {}).forEach(rule => allRules.add(rule));
    });
    
    if (allRules.size === 0) {
        failedRulesByDayChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Nenhuma regra falhou no período'],
                datasets: [{
                    data: [0],
                    backgroundColor: ['rgba(16, 185, 129, 0.8)']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
        return;
    }
    
    // Traduzir nomes de regras
    const ruleTranslations = {
        'validar_produtos': 'Validar Produtos',
        'validar_cnpj_fornecedor': 'Validar CNPJ Fornecedor',
        'validar_numero_pedido': 'Validar Número Pedido',
        'validar_valor_total': 'Validar Valor Total',
        'validar_data_emissao': 'Validar Data Emissão'
    };
    
    const rulesArray = Array.from(allRules);
    const colorPalette = [
        'rgba(239, 68, 68, 0.8)',
        'rgba(245, 158, 11, 0.8)',
        'rgba(139, 92, 246, 0.8)',
        'rgba(236, 72, 153, 0.8)',
        'rgba(6, 182, 212, 0.8)',
        'rgba(34, 197, 94, 0.8)',
        'rgba(251, 146, 60, 0.8)',
        'rgba(99, 102, 241, 0.8)'
    ];
    
    // Formatar datas
    const dates = periodData.map(d => {
        const date = new Date(d.date);
        return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    }).reverse();
    
    // Criar datasets para cada regra
    const datasets = rulesArray.slice(0, 8).map((rule, index) => ({
        label: ruleTranslations[rule] || rule,
        data: periodData.map(d => (d.failed_rules || {})[rule] || 0).reverse(),
        backgroundColor: colorPalette[index % colorPalette.length],
        borderColor: colorPalette[index % colorPalette.length].replace('0.8', '1'),
        borderWidth: 2,
        borderRadius: 4
    }));
    
    failedRulesByDayChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: dates,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 12,
                        font: { size: 11, weight: '600' }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 10
                }
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { font: { size: 11 } }
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    grid: { color: 'rgba(0, 0, 0, 0.05)' },
                    ticks: { font: { size: 11 }, stepSize: 1 }
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            }
        }
    });
}

function createFailedRulesByDayChart(data) {
    const ctx = document.getElementById('failedRulesByDayChart').getContext('2d');
    const periodData = data.period || data.last_7_days || [];
    
    if (failedRulesByDayChart) failedRulesByDayChart.destroy();
    
    // Coletar todas as regras que falharam no período
    const allRules = new Set();
    periodData.forEach(day => {
        Object.keys(day.failed_rules || {}).forEach(rule => allRules.add(rule));
    });
    
    if (allRules.size === 0) {
        failedRulesByDayChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Nenhuma regra falhou no período'],
                datasets: [{
                    data: [0],
                    backgroundColor: ['rgba(16, 185, 129, 0.8)']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
        return;
    }
    
    // Traduzir nomes de regras
    const ruleTranslations = {
        'validar_produtos': 'Validar Produtos',
        'validar_cnpj_fornecedor': 'Validar CNPJ Fornecedor',
        'validar_numero_pedido': 'Validar Número Pedido',
        'validar_valor_total': 'Validar Valor Total',
        'validar_data_emissao': 'Validar Data Emissão'
    };
    
    const rulesArray = Array.from(allRules);
    const colorPalette = [
        'rgba(239, 68, 68, 0.8)',
        'rgba(245, 158, 11, 0.8)',
        'rgba(139, 92, 246, 0.8)',
        'rgba(236, 72, 153, 0.8)',
        'rgba(6, 182, 212, 0.8)',
        'rgba(34, 197, 94, 0.8)',
        'rgba(251, 146, 60, 0.8)',
        'rgba(99, 102, 241, 0.8)'
    ];
    
    // Formatar datas
    const dates = periodData.map(d => {
        const date = new Date(d.date);
        return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    }).reverse();
    
    // Criar datasets para cada regra
    const datasets = rulesArray.slice(0, 8).map((rule, index) => ({
        label: ruleTranslations[rule] || rule,
        data: periodData.map(d => (d.failed_rules || {})[rule] || 0).reverse(),
        backgroundColor: colorPalette[index % colorPalette.length],
        borderColor: colorPalette[index % colorPalette.length].replace('0.8', '1'),
        borderWidth: 2,
        borderRadius: 4
    }));
    
    failedRulesByDayChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: dates,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 12,
                        font: { size: 11, weight: '600' }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 10
                }
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { font: { size: 11 } }
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    grid: { color: 'rgba(0, 0, 0, 0.05)' },
                    ticks: { font: { size: 11 }, stepSize: 1 }
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            }
        }
    });
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
            
            // Exibir metadados se existirem
            let metadataDisplay = '';
            const fileNameEscaped = f.file_name.replace(/'/g, "\\'");
            if (f.metadados && Object.keys(f.metadados).length > 0) {
                const metadataJson = JSON.stringify(f.metadados, null, 2);
                const metadataEscaped = metadataJson.replace(/</g, '&lt;').replace(/>/g, '&gt;');
                metadataDisplay = `
                    <div style="margin-top: 12px; padding: 12px; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                            <strong style="color: #495057; font-size: 0.9em;">📊 Metadados JSON</strong>
                            <button onclick="editMetadata('${fileNameEscaped}', 'DANFE')" 
                                    style="padding: 4px 12px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                                ✏️ Editar
                            </button>
                        </div>
                        <pre style="background: white; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.85em; max-height: 200px; overflow-y: auto; margin: 0;">${metadataEscaped}</pre>
                    </div>
                `;
            } else {
                metadataDisplay = `
                    <div style="margin-top: 12px;">
                        <button onclick="editMetadata('${fileNameEscaped}', 'DANFE')" 
                                style="padding: 6px 12px; background: #17a2b8; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                            ➕ Adicionar Metadados
                        </button>
                    </div>
                `;
            }
            
            return `
                <div class="file-item file-${statusClass}" style="margin-bottom: 15px; padding: 15px; border: 1px solid #e0e0e0; border-radius: 8px; background: white;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 500;">${statusIcon} ${f.file_name}</span>
                        <div style="display: flex; gap: 8px; align-items: center;">
                            <span class="file-status">${f.status}</span>
                            ${downloadBtn}
                        </div>
                    </div>
                    ${metadataDisplay}
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
            
            // Exibir metadados se existirem
            let metadataDisplay = '';
            const fileNameEscaped = f.file_name.replace(/'/g, "\\'");
            if (f.metadados && Object.keys(f.metadados).length > 0) {
                const metadataJson = JSON.stringify(f.metadados, null, 2);
                const metadataEscaped = metadataJson.replace(/</g, '&lt;').replace(/>/g, '&gt;');
                metadataDisplay = `
                    <div style="margin-top: 12px; padding: 12px; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                            <strong style="color: #495057; font-size: 0.9em;">📊 Metadados JSON</strong>
                            <button onclick="editMetadata('${fileNameEscaped}', 'ADDITIONAL')" 
                                    style="padding: 4px 12px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                                ✏️ Editar
                            </button>
                        </div>
                        <pre style="background: white; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.85em; max-height: 200px; overflow-y: auto; margin: 0;">${metadataEscaped}</pre>
                    </div>
                `;
            } else {
                metadataDisplay = `
                    <div style="margin-top: 12px;">
                        <button onclick="editMetadata('${fileNameEscaped}', 'ADDITIONAL')" 
                                style="padding: 6px 12px; background: #17a2b8; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                            ➕ Adicionar Metadados
                        </button>
                    </div>
                `;
            }
            
            return `
                <div class="file-item file-${statusClass}" style="margin-bottom: 15px; padding: 15px; border: 1px solid #e0e0e0; border-radius: 8px; background: white;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 500;">${statusIcon} ${f.file_name}</span>
                        <div style="display: flex; gap: 8px; align-items: center;">
                            <span class="file-status">${f.status}</span>
                            ${downloadBtn}
                        </div>
                    </div>
                    ${metadataDisplay}
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
    if (file) {
        const metadataText = document.getElementById('danfeMetadata').value.trim();
        let metadados = null;
        if (metadataText) {
            try {
                metadados = JSON.parse(metadataText);
            } catch (e) {
                showToast(`❌ Erro ao parsear JSON de metadados: ${e.message}`, 'error');
                return;
            }
        }
        uploadFile(file, 'DANFE', fileInput, metadados);
    }
}

function handleDocsSelect() {
    const fileInput = document.getElementById('docsInput');
    const files = Array.from(fileInput.files);
    const metadataText = document.getElementById('docsMetadata').value.trim();
    let metadados = null;
    if (metadataText) {
        try {
            metadados = JSON.parse(metadataText);
        } catch (e) {
            showToast(`❌ Erro ao parsear JSON de metadados: ${e.message}`, 'error');
            return;
        }
    }
    files.forEach(file => uploadFile(file, 'ADDITIONAL', fileInput, metadados));
}

async function uploadFile(file, docType, fileInput, userMetadata = null) {
    if (!file || !selectedProcess) return;

    try {
        const contentType = file.type || 'application/octet-stream';
        
        let urlResponse, upload_url;
        
        if (docType === 'DANFE') {
            const requestBody = {
                process_id: selectedProcess.process_id,
                file_name: file.name,
                file_type: contentType
            };
            
            // Adicionar metadados se fornecidos pelo usuário
            if (userMetadata) {
                requestBody.metadados = userMetadata;
            }
            
            urlResponse = await fetch(`${API_URL}/api/process/presigned-url/xml`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                },
                body: JSON.stringify(requestBody)
            });
            if (!urlResponse.ok) throw new Error('Falha ao gerar URL');
            const data = await urlResponse.json();
            upload_url = data.upload_url;
        } else {
            // Preparar metadados: usar metadados do usuário se fornecidos, senão gerar automáticos
            let metadados = userMetadata;
            if (!metadados) {
                // Gerar metadados baseados no nome do arquivo apenas se usuário não forneceu
                metadados = {
                tipo_documento: file.name.toLowerCase().includes('pedido') ? 'pedido_compra' : 'documento_adicional',
                tamanho_arquivo: file.size,
                data_upload: new Date().toISOString()
            };
            }
            
            urlResponse = await fetch(`${API_URL}/api/process/presigned-url/docs`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                },
                body: JSON.stringify({
                    process_id: selectedProcess.process_id,
                    file_name: file.name,
                    file_type: contentType,
                    metadados: metadados
                })
            });
            if (!urlResponse.ok) throw new Error('Falha ao gerar URL');
            const data = await urlResponse.json();
            upload_url = data.upload_url;
        }

        await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('PUT', upload_url, true);
            xhr.setRequestHeader('Content-Type', contentType);
            xhr.onload = () => xhr.status === 200 ? resolve() : reject(new Error('Falha no upload'));
            xhr.onerror = () => reject(new Error('Erro de rede'));
            xhr.send(file);
        });

        const metadataMsg = userMetadata ? ' com metadados' : '';
        showToast(`✓ ${docType === 'DANFE' ? 'DANFE' : 'Documento'} enviado${metadataMsg}!`, 'success');
        fileInput.value = '';
        
        // Limpar campos de metadados após upload bem-sucedido
        if (docType === 'DANFE') {
            document.getElementById('danfeMetadata').value = '';
        } else {
            document.getElementById('docsMetadata').value = '';
        }
        
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

let currentEditingFile = null;
let currentEditingFileType = null;

function editMetadata(fileName, fileType) {
    if (!selectedProcess) return;
    
    currentEditingFile = fileName;
    currentEditingFileType = fileType;
    
    // Buscar metadados existentes do arquivo
    let currentMetadata = null;
    const files = fileType === 'DANFE' 
        ? selectedProcess.files.danfe 
        : selectedProcess.files.additional;
    const file = files.find(f => f.file_name === fileName);
    if (file && file.metadados) {
        currentMetadata = file.metadados;
    }
    
    // Preencher textarea com metadados existentes ou JSON vazio
    const metadataTextarea = document.getElementById('editMetadataTextarea');
    if (currentMetadata) {
        metadataTextarea.value = JSON.stringify(currentMetadata, null, 2);
    } else {
        metadataTextarea.value = '{\n  \n}';
    }
    
    // Mostrar modal
    document.getElementById('editMetadataModal').style.display = 'block';
    document.getElementById('editMetadataFileName').textContent = fileName;
}

function closeEditMetadataModal() {
    document.getElementById('editMetadataModal').style.display = 'none';
    currentEditingFile = null;
    currentEditingFileType = null;
}

async function saveMetadata() {
    if (!selectedProcess || !currentEditingFile) return;
    
    const metadataTextarea = document.getElementById('editMetadataTextarea');
    const metadataText = metadataTextarea.value.trim();
    
    if (!metadataText) {
        showToast('❌ Metadados não podem estar vazios', 'error');
        return;
    }
    
    let metadados;
    try {
        metadados = JSON.parse(metadataText);
    } catch (e) {
        showToast(`❌ JSON inválido: ${e.message}`, 'error');
        return;
    }
    
    const saveBtn = document.getElementById('saveMetadataBtn');
    saveBtn.disabled = true;
    saveBtn.textContent = 'Salvando...';
    
    try {
        const response = await fetch(`${API_URL}/api/process/file/metadata`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': API_KEY
            },
            body: JSON.stringify({
                process_id: selectedProcess.process_id,
                file_name: currentEditingFile,
                metadados: metadados
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Falha ao atualizar metadados');
        }
        
        const data = await response.json();
        showToast('✓ Metadados atualizados com sucesso!', 'success');
        closeEditMetadataModal();
        
        // Recarregar processo para mostrar metadados atualizados
        setTimeout(() => selectProcess(selectedProcess.process_id, true), 1000);
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    } finally {
        saveBtn.disabled = false;
        saveBtn.textContent = 'Salvar';
    }
}
