// Função helper para normalizar URL (remover barra final)
function normalizeApiUrl(url) {
    if (!url) return url;
    return url.toString().replace(/\/+$/, ''); // Remove todas as barras no final
}

let API_URL = normalizeApiUrl(localStorage.getItem('apiUrl') || window.ENV?.API_URL);
let API_KEY = localStorage.getItem('apiKey') || window.ENV?.API_KEY;
let selectedProcess = null;
let refreshInterval = null;
let currentPage = 'dashboard';
let dailyProcessesChart, successErrorRateChart, hourlyChart, errorChart, typeChart, failedRulesChart, failedRulesByDayChart;
let regrasLabelsCatalog = null;

// Removido: PROCESS_RULES hardcoded - agora busca do backend

/** Catálogo de labels (API ou regras_labels_catalog.json estático). */
async function ensureRegrasLabelsCatalog(metricsPayload) {
    if (metricsPayload?.regras_labels) {
        regrasLabelsCatalog = metricsPayload.regras_labels;
        return;
    }
    if (regrasLabelsCatalog) return;
    try {
        const res = await fetch('regras_labels_catalog.json', { cache: 'no-cache' });
        if (res.ok) {
            regrasLabelsCatalog = await res.json();
        }
    } catch (e) {
        console.warn('[Dashboard] Catálogo de labels de regras não carregado:', e);
    }
}

function formatRegraLabel(regraId, catalog) {
    const cat = catalog || regrasLabelsCatalog;
    if (!regraId) return '';
    const meta = cat?.[regraId];
    if (meta) {
        return meta.label || meta.mensagem_resumo || regraId;
    }
    return regraId;
}

function regraLabelWithCategory(regraId, catalog) {
    const cat = catalog || regrasLabelsCatalog;
    const meta = cat?.[regraId];
    if (!meta) return formatRegraLabel(regraId, cat);
    const msg = meta.mensagem_resumo || meta.label || regraId;
    const categoria = meta.categoria;
    if (categoria && categoria !== msg) {
        return `${categoria}: ${msg}`;
    }
    return msg;
}

function truncateRegraLabel(text, maxLen = 56) {
    if (!text || text.length <= maxLen) return text;
    return text.slice(0, maxLen - 1) + '…';
}

document.addEventListener('DOMContentLoaded', async function() {
    console.log('[App] DOMContentLoaded - Iniciando...');
    console.log('[App] window.ENV disponível:', !!window.ENV);
    if (window.ENV) {
        console.log('[App] window.ENV keys:', Object.keys(window.ENV));
    }
    
    // Obter token OAuth2 ANTES de fazer qualquer requisição
    console.log('[App] Obtendo token OAuth2...');
    const tokenResult = await getOAuth2Token();
    if (tokenResult.success) {
        console.log('[App] ✅ Token OAuth2 obtido com sucesso', { 
            cached: tokenResult.cached,
            tokenLength: tokenResult.token?.length || 0
        });
        // Verificar se o token está disponível para uso
        const headers = getAuthHeaders();
        if (headers.Authorization) {
            console.log('[App] ✅ Token disponível para requisições:', headers.Authorization.substring(0, 30) + '...');
        } else {
            console.warn('[App] ⚠️ Token obtido mas não disponível em getAuthHeaders()');
        }
    } else {
        console.error('[App] ❌ Erro ao obter token OAuth2:', tokenResult.error);
        if (typeof showToast === 'function') {
            showToast('⚠️ Erro ao autenticar. Algumas funcionalidades podem não funcionar.', 'error');
        }
    }

    // Normalizar URL ao carregar (remove barra final se existir)
    API_URL = normalizeApiUrl(window.ENV?.API_URL || API_URL);
    
    if (document.getElementById('apiUrl')) {
        document.getElementById('apiUrl').value = API_URL;
    }

    // Carregar regras disponíveis do backend primeiro (página de regras)
    loadAvailableRules().then(() => {
        if (document.getElementById('rulesDisplay')) {
            showRules('AGROQUIMICOS');
        }
    });
    if (document.getElementById('processesList')) {
        loadProcesses();
    }
    loadDashboardMetrics();
});



// Função showPage mantida para compatibilidade, mas agora as páginas são arquivos separados
function showPage(pageName) {
    // Redirecionar para a página correta
    const pages = {
        dashboard: 'index.html',
        processes: 'processes.html',
        rules: 'rules.html',
        settings: 'settings.html'
    };
    if (pages[pageName]) {
        window.location.href = pages[pageName];
    }
}

let currentStartDate = null;
let currentEndDate = null;

// Helper: verifica se estamos na página do dashboard
function isDashboardPage() {
    // Verificar se estamos em index.html ou se o elemento principal do dashboard existe
    return window.location.pathname.endsWith('index.html') || 
           window.location.pathname.endsWith('/') ||
           document.getElementById('totalToday') !== null;
}

async function loadDashboardMetrics() {
    // Verificar se estamos na página do dashboard antes de fazer requisição
    if (!isDashboardPage()) {
        return; // Não fazer nada se não estiver na página do dashboard
    }
    
    try {
        let url = `${API_URL}/dashboard/metrics`;
        if (currentStartDate && currentEndDate) {
            url += `?start_date=${currentStartDate}&end_date=${currentEndDate}`;
        }
        
        const response = await fetch(url, {
            headers: getAuthHeaders()
        });
        if (!response.ok) {
            console.warn(`[Dashboard] Resposta não OK: ${response.status}`);
            return;
        }

        const data = await response.json();
        await ensureRegrasLabelsCatalog(data);
        
        // Verificar novamente se os elementos existem antes de atualizar
        if (isDashboardPage()) {
            updateMetricCards(data);
            createDailyProcessesChart(data);
            createSuccessErrorRateChart(data);
            createHourlyChart(data);
            createErrorChart(data);
            createTypeChart(data);
            createFailedRulesChart(data);
            createFailedRulesByDayChart(data);
        }
        
    } catch (error) {
        // Só mostrar erro se realmente for um erro de rede/API, não erro de elemento não encontrado
        if (error.name === 'TypeError' && error.message.includes('null')) {
            // Erro de elemento não encontrado - não mostrar toast, apenas log
            console.warn('[Dashboard] Elementos do dashboard não encontrados. Pode estar em outra página.');
            return;
        }
        console.error('Erro ao carregar métricas:', error);
        // Só mostrar toast se estiver na página do dashboard
        if (isDashboardPage() && typeof showToast === 'function') {
            showToast('❌ Erro ao carregar métricas', 'error');
        }
    }
}

function applyDateFilter() {
    // Verificar se estamos na página do dashboard
    if (!isDashboardPage()) return;
    
    const startDateEl = document.getElementById('startDateFilter');
    const endDateEl = document.getElementById('endDateFilter');
    
    if (!startDateEl || !endDateEl) return; // Elementos não existem
    
    const startDate = startDateEl.value;
    const endDate = endDateEl.value;
    
    if (!startDate || !endDate) {
        if (typeof showToast === 'function') {
            showToast('⚠️ Selecione data inicial e final', 'error');
        }
        return;
    }
    
    if (new Date(startDate) > new Date(endDate)) {
        if (typeof showToast === 'function') {
            showToast('⚠️ Data inicial deve ser anterior à data final', 'error');
        }
        return;
    }
    
    currentStartDate = startDate;
    currentEndDate = endDate;
    loadDashboardMetrics();
    if (typeof showToast === 'function') {
        showToast('✓ Filtro aplicado', 'success');
    }
}

function resetDateFilter() {
    // Verificar se estamos na página do dashboard
    if (!isDashboardPage()) return;
    
    currentStartDate = null;
    currentEndDate = null;
    
    const startDateEl = document.getElementById('startDateFilter');
    const endDateEl = document.getElementById('endDateFilter');
    
    if (startDateEl) startDateEl.value = '';
    if (endDateEl) endDateEl.value = '';
    
    loadDashboardMetrics();
    if (typeof showToast === 'function') {
        showToast('✓ Filtro resetado', 'success');
    }
}

function setFilterPeriod(period) {
    // Verificar se estamos na página do dashboard
    if (!isDashboardPage()) return;
    
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
    
    const startDateEl = document.getElementById('startDateFilter');
    const endDateEl = document.getElementById('endDateFilter');
    
    if (startDateEl) startDateEl.value = startDate;
    if (endDateEl) endDateEl.value = endDate;
    
    applyDateFilter();
}

function refreshDashboard() {
    loadDashboardMetrics();
    showToast('✓ Dashboard atualizado', 'success');
}

function updateMetricCards(data) {
    // Verificar se os elementos existem antes de tentar atualizar
    const totalTodayEl = document.getElementById('totalToday');
    const successTodayEl = document.getElementById('successToday');
    const failedTodayEl = document.getElementById('failedToday');
    const successRateEl = document.getElementById('successRate');
    const avgTimeEl = document.getElementById('avgTime');
    const totalLabelEl = document.getElementById('totalLabel');
    const successLabelEl = document.getElementById('successLabel');
    const failedLabelEl = document.getElementById('failedLabel');
    const prenotaTodayEl = document.getElementById('prenotaToday');
    const classifiedTodayEl = document.getElementById('classifiedToday');
    const prenotaLabelEl = document.getElementById('prenotaLabel');
    const classifiedLabelEl = document.getElementById('classifiedLabel');
    const processFailureTodayEl = document.getElementById('processFailureToday');
    const processFailureLabelEl = document.getElementById('processFailureLabel');
    
    // Se nenhum elemento existir, não fazer nada (não estamos na página do dashboard)
    if (!totalTodayEl && !successTodayEl && !failedTodayEl) {
        return;
    }
    
    // Suportar tanto formato antigo (today) quanto novo (period)
    const summary = data.summary || {};
    const todayData = data.today || {};
    
    // Determinar se há filtro de período ativo
    const hasPeriodFilter = currentStartDate && currentEndDate;
    
    let total, success, failed, processFailure, successRate, avgTime, periodLabel;
    let prenota = 0;
    let classified = 0;
    
    if (hasPeriodFilter) {
        // Filtro de período ativo → usar summary do período (inclui "hoje")
        total = summary.total ?? 0;
        success = summary.success ?? 0;
        failed = summary.failed ?? 0;
        processFailure = summary.skipped_operacional ?? 0;
        avgTime = summary.avg_processing_time ?? 0;
        prenota = summary.success_prenota ?? 0;
        classified = summary.success_classified ?? Math.max(0, success - prenota);
        const outcomeTotal = success + failed;
        successRate = outcomeTotal > 0 ? (success / outcomeTotal) * 100 : (summary.success_rate ?? 0);
        
        const startDate = parseDateLocal(currentStartDate);
        const endDate = parseDateLocal(currentEndDate);
        const startStr = startDate.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
        const endStr = endDate.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
        periodLabel = startDate.getTime() === endDate.getTime() 
            ? startStr 
            : `${startStr} - ${endStr}`;
    } else {
        // Sem filtro → usar dados de hoje
        total = todayData.total_count ?? 0;
        success = todayData.success_count ?? 0;
        failed = todayData.failed_count ?? 0;
        processFailure = todayData.skipped_operacional ?? 0;
        avgTime = todayData.avg_processing_time ?? 0;
        prenota = todayData.success_prenota_count ?? 0;
        classified = todayData.success_classified_count ?? Math.max(0, success - prenota);
        const outcomeTotalToday = success + failed;
        successRate = outcomeTotalToday > 0
            ? (success / outcomeTotalToday) * 100
            : (todayData.success_rate ?? 0);
        periodLabel = 'Hoje';
    }
    
    // Atualizar valores apenas se os elementos existirem
    if (totalTodayEl) totalTodayEl.textContent = total;
    if (successTodayEl) successTodayEl.textContent = success;
    if (prenotaTodayEl) prenotaTodayEl.textContent = prenota;
    if (classifiedTodayEl) classifiedTodayEl.textContent = classified;
    if (failedTodayEl) failedTodayEl.textContent = failed;
    if (processFailureTodayEl) processFailureTodayEl.textContent = processFailure ?? 0;
    if (successRateEl) successRateEl.textContent = successRate.toFixed(1) + '%';
    
    if (totalLabelEl) totalLabelEl.textContent = `Processos ${periodLabel}`;
    if (successLabelEl) successLabelEl.textContent = `Sucessos ${periodLabel}`;
    if (prenotaLabelEl) prenotaLabelEl.textContent = `Pré-notas ${periodLabel}`;
    if (classifiedLabelEl) classifiedLabelEl.textContent = `Classificadas ${periodLabel}`;
    if (failedLabelEl) failedLabelEl.textContent = `Falhas OCR ${periodLabel}`;
    if (processFailureLabelEl) processFailureLabelEl.textContent = `Falhas de Processos ${periodLabel}`;
    
    // Formatar tempo médio de processamento
    if (avgTimeEl) {
        let timeDisplay;
        if (avgTime >= 60) {
            const minutes = Math.floor(avgTime / 60);
            const seconds = Math.round(avgTime % 60);
            timeDisplay = `${minutes}m ${seconds}s`;
        } else {
            timeDisplay = `${Math.round(avgTime)}s`;
        }
        avgTimeEl.textContent = timeDisplay;
    }
}

// Helper: converte string de data YYYY-MM-DD para Date local (sem problema de timezone)
function parseDateLocal(dateStr) {
    // 'YYYY-MM-DD' sem hora é interpretado como UTC pelo JS, causando dia errado em fuso negativo
    // Adicionando T12:00:00 garante que mesmo com qualquer fuso o dia permanece correto
    if (typeof dateStr === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        return new Date(dateStr + 'T12:00:00');
    }
    return new Date(dateStr);
}

function createDailyProcessesChart(data) {
    const chartEl = document.getElementById('dailyProcessesChart');
    if (!chartEl) return; // Elemento não existe, não criar gráfico
    
    const ctx = chartEl.getContext('2d');
    const raw = data.period || data.last_7_days || [];
    // Ordem cronológica: mais antiga → mais recente (igual com ou sem filtro de datas)
    const periodData = [...raw].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    
    if (dailyProcessesChart) dailyProcessesChart.destroy();
    
    const labels = periodData.map(d => {
        const date = parseDateLocal(d.date);
        return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    });
    
    dailyProcessesChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Classificadas',
                data: periodData.map(d =>
                    d.success_classified ??
                    Math.max(0, (d.success || 0) - (d.success_prenota || 0))
                ),
                backgroundColor: 'rgba(5, 150, 105, 0.85)',
                borderColor: '#059669',
                borderWidth: 2,
                borderRadius: 6,
                order: 3,
                yAxisID: 'y'
            }, {
                label: 'Pré-notas',
                data: periodData.map(d => d.success_prenota ?? 0),
                backgroundColor: 'rgba(245, 158, 11, 0.85)',
                borderColor: '#f59e0b',
                borderWidth: 2,
                borderRadius: 6,
                order: 4,
                yAxisID: 'y'
            }, {
                label: 'Falhas',
                data: periodData.map(d => d.failed),
                backgroundColor: 'rgba(239, 68, 68, 0.8)',
                borderColor: '#ef4444',
                borderWidth: 2,
                borderRadius: 6,
                order: 5,
                yAxisID: 'y'
            }, {
                label: 'Taxa de acerto (%)',
                data: periodData.map(d => {
                    const s = Number(d.success) || 0;
                    const f = Number(d.failed) || 0;
                    const t = s + f;
                    return t > 0 ? (s / t) * 100 : (Number(d.success_rate) || 0);
                }),
                type: 'line',
                borderColor: '#0ea5e9',
                backgroundColor: 'rgba(14, 165, 233, 0.15)',
                borderWidth: 2,
                tension: 0.35,
                fill: false,
                pointRadius: 4,
                pointHoverRadius: 6,
                yAxisID: 'y1',
                order: 1
            }, {
                label: 'Total',
                data: periodData.map(d => d.total),
                type: 'line',
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                borderWidth: 3,
                tension: 0.4,
                fill: false,
                pointRadius: 5,
                pointHoverRadius: 7,
                order: 0,
                yAxisID: 'y'
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
                    bodyFont: { size: 13 },
                    callbacks: {
                        afterBody: function(items) {
                            const idx = items[0]?.dataIndex;
                            if (idx == null || !periodData[idx]) return '';
                            const row = periodData[idx];
                            const t = row.total || 0;
                            const s = row.success || 0;
                            if (t <= 0) return '';
                            const pct = ((s / t) * 100).toFixed(1);
                            return `Resumo: ${s} / ${t} ok (${pct}%)`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Quantidade de processos',
                        font: { size: 11 }
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        font: { size: 11 }
                    }
                },
                y1: {
                    beginAtZero: true,
                    max: 100,
                    position: 'right',
                    grid: {
                        drawOnChartArea: false
                    },
                    title: {
                        display: true,
                        text: '% acertos (dia)',
                        font: { size: 11 }
                    },
                    ticks: {
                        font: { size: 11 },
                        callback: function(value) {
                            return value + '%';
                        }
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
    const chartEl = document.getElementById('successErrorRateChart');
    if (!chartEl) return; // Elemento não existe, não criar gráfico
    
    const ctx = chartEl.getContext('2d');
    
    // Determinar se há filtro de período ativo
    const hasPeriodFilter = currentStartDate && currentEndDate;
    
    let total, success, failed, prenota, classified;
    if (hasPeriodFilter) {
        const summary = data.summary || {};
        total = summary.total ?? 0;
        success = summary.success ?? 0;
        failed = summary.failed ?? 0;
        prenota = summary.success_prenota ?? 0;
        classified = summary.success_classified ?? Math.max(0, success - prenota);
    } else {
        const todayData = data.today || {};
        total = todayData.total_count ?? 0;
        success = todayData.success_count ?? 0;
        failed = todayData.failed_count ?? 0;
        prenota = todayData.success_prenota_count ?? 0;
        classified = todayData.success_classified_count ?? Math.max(0, success - prenota);
    }
    
    if (successErrorRateChart) successErrorRateChart.destroy();
    
    successErrorRateChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Entrada classificada', 'Pré-notas', 'Falhas'],
            datasets: [{
                data: [classified, prenota, failed],
                backgroundColor: [
                    'rgba(5, 150, 105, 0.9)',
                    'rgba(245, 158, 11, 0.9)',
                    'rgba(239, 68, 68, 0.9)'
                ],
                borderColor: [
                    '#059669',
                    '#f59e0b',
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
    const chartEl = document.getElementById('hourlyChart');
    if (!chartEl) return; // Elemento não existe, não criar gráfico
    const ctx = chartEl.getContext('2d');
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
    const chartEl = document.getElementById('errorChart');
    if (!chartEl) return; // Elemento não existe, não criar gráfico
    const ctx = chartEl.getContext('2d');
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
    const chartEl = document.getElementById('typeChart');
    if (!chartEl) return; // Elemento não existe, não criar gráfico
    const ctx = chartEl.getContext('2d');
    
    // Determinar se há filtro de período ativo
    const hasPeriodFilter = currentStartDate && currentEndDate;
    
    let types;
    if (hasPeriodFilter) {
        // Com filtro de período (inclui "hoje") → usar dados do período
        types = data.processes_by_type || {};
    } else {
        // Sem filtro → usar dados de hoje
        types = data.today?.processes_by_type || {};
    }
    
    if (typeChart) typeChart.destroy();
    
    const labels = ['Agroquímicos', 'Barter (Commodities)'];
    const values = [
        types.AGROQUIMICOS || 0,
        types.BARTER || 0
    ];
    
    typeChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: [
                    'rgba(59, 130, 246, 0.9)',
                    'rgba(245, 158, 11, 0.9)'
                ],
                borderColor: [
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

const FAILED_RULES_DONUT_COLORS = [
    'rgba(239, 68, 68, 0.9)',
    'rgba(245, 158, 11, 0.9)',
    'rgba(139, 92, 246, 0.9)',
    'rgba(236, 72, 153, 0.9)',
    'rgba(6, 182, 212, 0.9)',
    'rgba(34, 197, 94, 0.9)',
    'rgba(99, 102, 241, 0.9)',
    'rgba(148, 163, 184, 0.9)'
];

function collectFailedRulesForDashboard(data) {
    const hasPeriodFilter = currentStartDate && currentEndDate;
    let ocrRules = {};
    let operacionalRules = {};
    if (hasPeriodFilter) {
        ocrRules = data.failed_rules || {};
        operacionalRules = data.failed_rules_operacional || {};
    } else {
        ocrRules = data.today?.failed_rules || {};
        operacionalRules = data.today?.failed_rules_operacional || {};
    }
    const merged = { ...ocrRules };
    Object.entries(operacionalRules).forEach(([rule, count]) => {
        merged[rule] = (Number(merged[rule]) || 0) + (Number(count) || 0);
    });
    return {
        ocrRules,
        operacionalRules,
        merged,
        operacionalRuleIds: new Set(Object.keys(operacionalRules || {}))
    };
}

function sortFailedRulesEntries(failedRules) {
    const labels = Object.keys(failedRules || {});
    const values = Object.values(failedRules || {});
    return labels
        .map((label, i) => ({ label, value: Number(values[i]) || 0 }))
        .filter(item => item.value > 0)
        .sort((a, b) => {
            const ao = a.label === 'Outros';
            const bo = b.label === 'Outros';
            if (ao !== bo) return ao ? 1 : -1;
            return b.value - a.value;
        });
}

function buildFailedRulesDonutSlices(sorted, topN = 7) {
    const withoutOutros = sorted.filter(x => x.label !== 'Outros');
    const outros = sorted.find(x => x.label === 'Outros');
    const top = withoutOutros.slice(0, topN);
    const rest = withoutOutros.slice(topN);
    const slices = top.map(item => ({
        regraId: item.label,
        value: item.value,
        shortLabel: truncateRegraLabel(formatRegraLabel(item.label), 36)
    }));
    let demais = rest.reduce((s, x) => s + x.value, 0);
    if (outros) demais += outros.value;
    if (demais > 0) {
        slices.push({
            regraId: '__demais__',
            value: demais,
            shortLabel: 'Demais regras'
        });
    }
    return slices;
}

function renderFailedRulesRanking(sorted, operacionalRuleIds = new Set()) {
    const container = document.getElementById('failedRulesRanking');
    if (!container) return;

    if (!sorted.length) {
        container.innerHTML = '<div class="failed-rules-ranking-empty">Nenhuma falha por regra neste recorte</div>';
        return;
    }

    const maxVal = Math.max(...sorted.map(s => s.value), 1);
    container.innerHTML = sorted.map((item, index) => {
        const regraId = item.label;
        const title = formatRegraLabel(regraId);
        const pct = Math.round((item.value / maxVal) * 100);
        const categoria = regrasLabelsCatalog?.[regraId]?.categoria;
        const isOp = operacionalRuleIds.has(regraId);
        const badge = isOp
            ? '<span class="failed-rules-badge-operacional">Falhas de Processos</span> '
            : '';
        const catLine = categoria && categoria !== title
            ? `<span class="failed-rules-ranking-code">${escapeHtml(categoria)}</span><br>`
            : '';
        return `
            <div class="failed-rules-ranking-item${isOp ? ' failed-rules-ranking-item--operacional' : ''}" title="${escapeHtml(regraLabelWithCategory(regraId))}">
                <div class="failed-rules-ranking-head">
                    <span class="failed-rules-ranking-rank">${index + 1}</span>
                    <span class="failed-rules-ranking-title">${badge}${escapeHtml(title)}</span>
                    <span class="failed-rules-ranking-count">${item.value}</span>
                </div>
                <div class="failed-rules-ranking-bar" aria-hidden="true">
                    <div style="width: ${pct}%"></div>
                </div>
                ${catLine}<span class="failed-rules-ranking-code">${escapeHtml(regraId)}</span>
            </div>`;
    }).join('');
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : String(text);
    return div.innerHTML;
}

function createFailedRulesChart(data) {
    const chartEl = document.getElementById('failedRulesChart');
    if (!chartEl) return;
    const ctx = chartEl.getContext('2d');

    const rulesBundle = collectFailedRulesForDashboard(data);
    const failedRules = rulesBundle.merged;

    if (failedRulesChart) failedRulesChart.destroy();

    const sorted = sortFailedRulesEntries(failedRules);
    const totalFailedAttributed = sorted.reduce((a, b) => a + b.value, 0);

    renderFailedRulesRanking(sorted, rulesBundle.operacionalRuleIds);

    if (totalFailedAttributed === 0) {
        failedRulesChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Sem falhas'],
                datasets: [{ data: [1], backgroundColor: ['rgba(16, 185, 129, 0.5)'] }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } }
            }
        });
        return;
    }

    const donutSlices = buildFailedRulesDonutSlices(sorted);
    const donutRuleIds = donutSlices.map(s => s.regraId);

    failedRulesChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: donutSlices.map(s => s.shortLabel),
            datasets: [{
                data: donutSlices.map(s => s.value),
                backgroundColor: donutSlices.map((_, i) => FAILED_RULES_DONUT_COLORS[i % FAILED_RULES_DONUT_COLORS.length]),
                borderWidth: 2,
                borderColor: '#fff',
                hoverOffset: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        boxWidth: 12,
                        padding: 8,
                        font: { size: 10, weight: '600' }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.85)',
                    padding: 12,
                    callbacks: {
                        title(items) {
                            if (!items?.length) return '';
                            const regraId = donutRuleIds[items[0].dataIndex];
                            if (regraId === '__demais__') return 'Demais regras (restante do ranking)';
                            return regraLabelWithCategory(regraId);
                        },
                        label(context) {
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                            return `Ocorrências: ${value} (${pct}%)`;
                        },
                        afterBody(items) {
                            if (!items?.length) return [];
                            const regraId = donutRuleIds[items[0].dataIndex];
                            if (regraId === '__demais__') return ['Ver lista ao lado para detalhes'];
                            return [`Código: ${regraId}`];
                        }
                    }
                }
            },
            cutout: '55%'
        }
    });
}

function createFailedRulesByDayChart(data) {
    const chartEl = document.getElementById('failedRulesByDayChart');
    if (!chartEl) return;
    const ctx = chartEl.getContext('2d');
    const raw = data.period || data.last_7_days || [];
    const periodData = [...raw].sort((a, b) => String(a.date).localeCompare(String(b.date)));

    if (failedRulesByDayChart) failedRulesByDayChart.destroy();

    const allRules = new Set();
    periodData.forEach(day => {
        Object.keys(day.failed_rules || {}).forEach(rule => allRules.add(rule));
        Object.keys(day.failed_rules_operacional || {}).forEach(rule => allRules.add(rule));
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

    let rulesArray = Array.from(allRules).filter(r => r !== 'Outros');
    rulesArray.sort((a, b) => a.localeCompare(b));
    if (allRules.has('Outros')) {
        rulesArray.push('Outros');
    }

    const MAX_RULES = 18;
    if (rulesArray.length > MAX_RULES) {
        const outrosAtEnd = rulesArray[rulesArray.length - 1] === 'Outros';
        const sliceEnd = outrosAtEnd ? MAX_RULES - 1 : MAX_RULES;
        const head = rulesArray.slice(0, sliceEnd);
        if (outrosAtEnd) head.push('Outros');
        rulesArray = head;
    }

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

    const dates = periodData.map(d => {
        const date = parseDateLocal(d.date);
        return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    });

    const datasets = rulesArray.map((rule, index) => ({
        label: truncateRegraLabel(formatRegraLabel(rule), 48),
        _regraId: rule,
        data: periodData.map(d => {
            const ocr = (d.failed_rules || {})[rule] || 0;
            const op = (d.failed_rules_operacional || {})[rule] || 0;
            return (Number(ocr) || 0) + (Number(op) || 0);
        }),
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
                    padding: 10,
                    callbacks: {
                        afterLabel(context) {
                            const regraId = context.dataset?._regraId;
                            if (!regraId) return '';
                            return `Código: ${regraId}`;
                        },
                        footer: function(items) {
                            let sum = 0;
                            items.forEach(it => {
                                sum += Number(it.parsed?.y ?? it.parsed ?? 0) || 0;
                            });
                            const di = items[0]?.dataIndex;
                            const failedDay = di != null && periodData[di] ? periodData[di].failed : null;
                            const extra = failedDay != null ? ` | Falhas no dia (métrica): ${failedDay}` : '';
                            return `Soma pilhas (dia): ${sum}${extra}`;
                        }
                    }
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
                    ticks: { font: { size: 11 }, precision: 0 }
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
    API_URL = normalizeApiUrl(document.getElementById('apiUrl').value.trim());
    API_KEY = document.getElementById('apiKey').value.trim();
    localStorage.setItem('apiUrl', API_URL);
    localStorage.setItem('apiKey', API_KEY);
    showToast('✓ Configuração salva!', 'success');
    loadProcesses();
}

function createNewProcess() {
    // Gerar UUID para o processo (será criado quando fizer upload)
    const processId = crypto.randomUUID();
    
    // Criar objeto de processo local (será criado no backend quando fizer upload)
    selectedProcess = {
        process_id: processId,
        process_type: null,
        status: 'CREATED',
        files: { danfe: [], additional: [] }
    };
    
    // Mostrar interface de upload
    document.getElementById('selectedProcessId').textContent = selectedProcess.process_id;
    document.getElementById('selectedProcessType').textContent = 'Aguardando upload...';
    
    const statusBadge = document.getElementById('selectedProcessStatus');
    statusBadge.textContent = 'CREATED';
    statusBadge.className = 'status-badge created';
    
    document.getElementById('processDetails').style.display = 'block';
    document.getElementById('processesList').style.display = 'none';
    
    // Limpar listas de arquivos
    document.getElementById('danfeList').innerHTML = '';
    document.getElementById('docsList').innerHTML = '';
    
    showToast(`✓ Novo processo preparado. Faça upload dos arquivos para criar.`, 'info');
}

async function loadProcesses(silent = false) {
    const list = document.getElementById('processesList');
    if (!list) {
        return;
    }
    try {
        if (!silent) {
            list.innerHTML = `
                <div style="grid-column: 1 / -1; text-align: center; padding: 60px 20px;">
                    <div style="font-size: 3em; margin-bottom: 20px;">⏳</div>
                    <p style="color: var(--gray);">Carregando processos...</p>
                </div>
            `;
        }
        
        const response = await fetch(`${API_URL}/process/`, {
            headers: getAuthHeaders()
        });
        if (!response.ok) {
            if (!silent) {
                list.innerHTML = `
                    <div style="grid-column: 1 / -1; text-align: center; padding: 60px 20px;">
                        <div style="font-size: 3em; margin-bottom: 20px;">❌</div>
                        <p style="color: var(--danger);">Erro ao carregar processos</p>
                    </div>
                `;
            }
            return;
        }

        const data = await response.json();

        if (data.processes.length === 0) {
            list.innerHTML = `
                <div class="empty-state" style="grid-column: 1 / -1; text-align: center; padding: 60px 20px;">
                    <div style="font-size: 5em; margin-bottom: 20px; opacity: 0.5;">📋</div>
                    <h3 style="color: var(--dark); margin-bottom: 10px; font-size: 1.5em;">Nenhum processo encontrado</h3>
                    <p style="color: var(--gray); margin-bottom: 30px; font-size: 1.1em;">Clique em "Novo Processo" para começar</p>
                    <button class="btn-primary" onclick="createNewProcess()" style="padding: 12px 24px; font-size: 1em;">
                        + Criar Novo Processo
                    </button>
                </div>
            `;
            return;
        }

        list.innerHTML = data.processes.map(p => {
            const createdDate = new Date(parseInt(p.created_at) * 1000);
            const now = new Date();
            const diffMs = now - createdDate;
            const diffMins = Math.floor(diffMs / 60000);
            const diffHours = Math.floor(diffMs / 3600000);
            const diffDays = Math.floor(diffMs / 86400000);
            
            let timeAgo = '';
            if (diffMins < 1) {
                timeAgo = 'Agora mesmo';
            } else if (diffMins < 60) {
                timeAgo = `${diffMins} min${diffMins > 1 ? 's' : ''} atrás`;
            } else if (diffHours < 24) {
                timeAgo = `${diffHours} hora${diffHours > 1 ? 's' : ''} atrás`;
            } else if (diffDays < 7) {
                timeAgo = `${diffDays} dia${diffDays > 1 ? 's' : ''} atrás`;
            } else {
                timeAgo = createdDate.toLocaleDateString('pt-BR');
            }
            
            const statusIcon = {
                'CREATED': '📝',
                'PROCESSING': '⏳',
                'COMPLETED': '✅',
                'SUCCESS': '✅',
                'VALIDATED': '✅',
                'FAILED': '❌',
                'VALIDATION_FAILURE': '⚠️'
            }[p.status] || '📄';
            
            const processTypeLabel = {
                'AGROQUIMICOS': '🧪 Agroquímicos',
                'BARTER': '🌾 Barter (Commodities)',
                'SEMENTES': '🌱 Sementes',
                'FERTILIZANTES': '💊 Fertilizantes'
            }[p.process_type] || `📦 ${p.process_type || 'N/A'}`;
            
            const statusColor = {
                'CREATED': '#6b7280',
                'PROCESSING': '#f59e0b',
                'COMPLETED': '#10b981',
                'SUCCESS': '#10b981',
                'VALIDATED': '#10b981',
                'FAILED': '#ef4444',
                'VALIDATION_FAILURE': '#f59e0b'
            }[p.status] || '#6b7280';
            
            return `
                <div class="process-item ${p.status === 'FAILED' || p.status === 'VALIDATION_FAILURE' ? 'failed' : ''}" onclick="selectProcess('${p.process_id}')">
                    <div class="process-item-header">
                        <div class="process-item-icon" style="background: ${statusColor}20; color: ${statusColor};">
                            ${statusIcon}
                        </div>
                        <div class="process-item-title">
                            <h3>${processTypeLabel}</h3>
                            <p class="process-id">${p.process_id}</p>
                        </div>
                    </div>
                    <div class="process-item-body">
                        <div class="process-info-row">
                            <span class="info-label">📅 Criado:</span>
                            <span class="info-value">${createdDate.toLocaleString('pt-BR', { 
                                day: '2-digit', 
                                month: '2-digit', 
                                year: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit'
                            })}</span>
                        </div>
                        <div class="process-info-row">
                            <span class="info-label">⏱️ Há:</span>
                            <span class="info-value">${timeAgo}</span>
                        </div>
                    </div>
                    <div class="process-item-footer">
                        <span class="status-badge ${p.status.toLowerCase().replace(/_/g, '-')}" style="background: ${statusColor}20; color: ${statusColor}; border-color: ${statusColor};">
                            ${p.status.replace(/_/g, ' ')}
                        </span>
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('Erro ao carregar processos:', error);
    }
}

async function selectProcess(processId, silent = false) {
    try {
        const response = await fetch(`${API_URL}/process/${processId}`, {
            headers: getAuthHeaders()
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
        
        // Exibir informações de erro se o processo falhou
        displayErrorInfo(selectedProcess);
        
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

function displayErrorInfo(process) {
    // Remover qualquer exibição de erro anterior
    const existingErrorDiv = document.getElementById('errorInfoDiv');
    if (existingErrorDiv) {
        existingErrorDiv.remove();
    }
    
    // Se o processo falhou e tem error_info, exibir
    if (process.status === 'FAILED' && process.error_info) {
        const errorInfo = process.error_info;
        const errorDiv = document.createElement('div');
        errorDiv.id = 'errorInfoDiv';
        errorDiv.className = 'details-card full-width';
        errorDiv.style.background = '#fff3cd';
        errorDiv.style.border = '2px solid #ffc107';
        errorDiv.style.marginTop = '20px';
        
        let errorMessage = errorInfo.message || 'Erro desconhecido';
        let errorType = errorInfo.type || 'UNKNOWN_ERROR';
        let lambdaName = errorInfo.lambda || 'N/A';
        let protheusCause = errorInfo.protheus_cause || null;
        let timestamp = errorInfo.timestamp || null;
        
        // Formatar mensagem de erro
        let formattedMessage = errorMessage;
        try {
            // Tentar parsear se for JSON string
            const parsed = JSON.parse(errorMessage);
            if (parsed.errorMessage) {
                formattedMessage = parsed.errorMessage;
            } else if (parsed.error) {
                formattedMessage = parsed.error;
            }
        } catch (e) {
            // Não é JSON, usar como está
        }
        
        // Escapar HTML para segurança
        const escapeHtml = (text) => {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        };
        
        let errorHtml = `
            <h4 style="color: #856404; margin-bottom: 15px; display: flex; align-items: center; gap: 10px;">
                ❌ Informações do Erro
            </h4>
            <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ffc107;">
                <div style="margin-bottom: 12px;">
                    <strong style="color: #856404; display: block; margin-bottom: 5px;">Tipo de Erro:</strong>
                    <span style="color: #333; font-family: monospace; background: #f8f9fa; padding: 4px 8px; border-radius: 4px; display: inline-block;">${escapeHtml(errorType)}</span>
                </div>
                <div style="margin-bottom: 12px;">
                    <strong style="color: #856404; display: block; margin-bottom: 5px;">Mensagem:</strong>
                    <div style="color: #333; background: #f8f9fa; padding: 12px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; max-height: 300px; overflow-y: auto;">${escapeHtml(formattedMessage)}</div>
                </div>
                <div style="margin-bottom: 12px;">
                    <strong style="color: #856404; display: block; margin-bottom: 5px;">Lambda/Etapa:</strong>
                    <span style="color: #333;">${escapeHtml(lambdaName)}</span>
                </div>
        `;
        
        // Adicionar causa do Protheus se existir
        if (protheusCause) {
            errorHtml += `
                <div style="margin-bottom: 12px;">
                    <strong style="color: #856404; display: block; margin-bottom: 5px;">Causa (Protheus):</strong>
                    <div style="color: #333; background: #f8f9fa; padding: 12px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; max-height: 200px; overflow-y: auto;">${escapeHtml(protheusCause)}</div>
                </div>
            `;
        }
        
        // Adicionar timestamp se existir
        if (timestamp) {
            try {
                const errorDate = new Date(timestamp);
                errorHtml += `
                    <div style="margin-bottom: 12px;">
                        <strong style="color: #856404; display: block; margin-bottom: 5px;">Data/Hora do Erro:</strong>
                        <span style="color: #333;">${errorDate.toLocaleString('pt-BR')}</span>
                    </div>
                `;
            } catch (e) {
                // Ignorar se não conseguir parsear data
            }
        }
        
        errorHtml += `
            </div>
        `;
        
        errorDiv.innerHTML = errorHtml;
        
        // Inserir após o card de informações do processo
        const detailsGrid = document.querySelector('.details-grid');
        if (detailsGrid) {
            // Buscar o primeiro card (informações do processo)
            const infoCard = detailsGrid.querySelector('.details-card');
            if (infoCard) {
                // Inserir logo após o primeiro card (informações do processo)
                infoCard.insertAdjacentElement('afterend', errorDiv);
            } else {
                // Se não encontrar o card, inserir no início do grid
                detailsGrid.insertBefore(errorDiv, detailsGrid.firstChild);
            }
        }
    }
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
            const isMetadataOnly = f.metadata_only === true;
            const statusClass = f.status === 'UPLOADED' ? 'uploaded' : f.status === 'LINKED' ? 'linked' : 'pending';
            const statusIcon = f.status === 'UPLOADED' ? '✅' : f.status === 'LINKED' ? '🔗' : '⏳';
            const downloadBtn = (f.status === 'UPLOADED' && f.file_key) ? `<button onclick="downloadFile('${f.file_key}', '${f.file_name}')" style="padding: 4px 8px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">📥 Baixar</button>` : '';
            
            // Exibir metadados se existirem
            let metadataDisplay = '';
            const fileNameEscaped = f.file_name.replace(/'/g, "\\'");
            if (f.metadados && Object.keys(f.metadados).length > 0) {
                const metadataJson = JSON.stringify(f.metadados, null, 2);
                const metadataEscaped = metadataJson.replace(/</g, '&lt;').replace(/>/g, '&gt;');
                const editButton = !isMetadataOnly ? `<button onclick="editMetadata('${fileNameEscaped}', 'ADDITIONAL')" 
                                    style="padding: 4px 12px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                                ✏️ Editar
                            </button>` : '';
                metadataDisplay = `
                    <div style="margin-top: 12px; padding: 12px; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                            <strong style="color: #495057; font-size: 0.9em;">📊 Metadados JSON</strong>
                            ${editButton}
                        </div>
                        <pre style="background: white; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.85em; max-height: 400px; overflow-y: auto; margin: 0; white-space: pre-wrap; word-wrap: break-word;">${metadataEscaped}</pre>
                    </div>
                `;
            } else if (!isMetadataOnly) {
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
        const response = await fetch(`${API_URL}/process/${selectedProcess.process_id}/validations`, {
            headers: getAuthHeaders()
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

async function sendMetadataOnly() {
    if (!selectedProcess) {
        showToast('❌ Selecione um processo primeiro', 'error');
        return;
    }
    
    const metadataText = document.getElementById('docsMetadata').value.trim();
    if (!metadataText) {
        showToast('❌ Metadados JSON são obrigatórios quando enviando apenas metadados', 'error');
        return;
    }
    
    let metadados;
    try {
        metadados = JSON.parse(metadataText);
    } catch (e) {
        showToast(`❌ Erro ao parsear JSON de metadados: ${e.message}`, 'error');
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/process/metadados/pedido`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            body: JSON.stringify({
                process_id: selectedProcess.process_id,
                metadados: metadados
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Erro desconhecido' }));
            throw new Error(errorData.detail || 'Falha ao enviar metadados');
        }
        
        const data = await response.json();
        showToast(`✓ Metadados do pedido de compra vinculados com sucesso!`, 'success');
        
        // Limpar campo de metadados
        document.getElementById('docsMetadata').value = '';
        
        // Atualizar processo após 1 segundo
        setTimeout(() => selectProcess(selectedProcess.process_id, true), 1000);
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

// Função removida - não há mais upload de documentos adicionais

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
            
            urlResponse = await fetch(`${API_URL}/process/presigned-url/xml`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    ...getAuthHeaders()
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
            
            urlResponse = await fetch(`${API_URL}/process/presigned-url/docs`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    ...getAuthHeaders()
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

    // Verificar se há metadados do pedido de compra vinculados
    const hasPedidoCompraMetadata = selectedProcess.files.additional && selectedProcess.files.additional.some(f => f.metadata_only === true);
    if (!hasPedidoCompraMetadata) {
        showToast('❌ Vincule os metadados do pedido de compra antes de iniciar o processamento', 'error');
        return;
    }

    const btn = document.getElementById('startBtn');
    btn.disabled = true;
    btn.textContent = 'Iniciando...';

    try {
        const response = await fetch(`${API_URL}/process/start`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            body: JSON.stringify({ 
                process_id: selectedProcess.process_id
            })
        });

        if (!response.ok) throw new Error('Falha ao iniciar processo');

        const processType = selectedProcess?.process_type || 'N/A';
        showToast(`✓ Processamento iniciado (${processType})!`, 'success');
        
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

let currentProcessType = 'AGROQUIMICOS';
let availableRules = []; // Cache de regras disponíveis do backend

// Carregar regras disponíveis do backend
async function loadAvailableRules() {
    try {
        const response = await fetch(`${API_URL}/rules/available`, {
            headers: { 
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            }
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('Erro ao carregar regras disponíveis:', response.status, response.statusText, errorText);
            availableRules = []; // Fallback: lista vazia
            return false;
        }
        
        const data = await response.json();
        availableRules = data.rules || [];
        console.log(`✓ Carregadas ${availableRules.length} regras disponíveis do backend`);
        return true;
    } catch (error) {
        console.error('Erro ao carregar regras disponíveis:', error);
        availableRules = []; // Fallback: lista vazia
        return false;
    }
}

async function showRules(processType) {
    currentProcessType = processType;
    document.querySelectorAll('.rule-tab').forEach(t => t.classList.remove('active'));
    if (event && event.target) {
    event.target.classList.add('active');
    }

    const display = document.getElementById('rulesDisplay');
    if (!display) return;
    
    display.innerHTML = '<p style="text-align: center; padding: 40px;">Carregando...</p>';

    try {
        // Garantir que as regras disponíveis foram carregadas
        if (availableRules.length === 0) {
            const loaded = await loadAvailableRules();
            if (!loaded) {
                display.innerHTML = `
                    <h3>Regras de Validação - ${processType}</h3>
                    <p style="color: red; text-align: center; padding: 40px;">
                        ❌ Erro: Não foi possível carregar as regras disponíveis do backend.<br>
                        <small>Verifique o console do navegador para mais detalhes.</small>
                    </p>
                `;
                return;
            }
        }

        // Buscar regras ativas para este tipo de processo
        const response = await fetch(`${API_URL}/rules/${processType}`, {
            headers: { 
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            }
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('Erro ao carregar regras ativas:', response.status, response.statusText, errorText);
            display.innerHTML = `
                <h3>Regras de Validação - ${processType}</h3>
                <p style="color: red; text-align: center; padding: 40px;">
                    ❌ Erro ao carregar regras ativas: ${response.status} ${response.statusText}
                </p>
            `;
            return;
        }
        
        const data = await response.json();
        const activeRules = data.rules || [];
        const activeRuleNames = activeRules.map(r => r.rule_name || r.RULE_NAME);

        if (availableRules.length === 0) {
            display.innerHTML = `
                <h3>Regras de Validação - ${processType}</h3>
                <p style="color: orange; text-align: center; padding: 40px;">
                    ⚠️ Nenhuma regra disponível foi encontrada no backend.
                </p>
            `;
            return;
        }

        display.innerHTML = `
            <h3>Regras de Validação - ${processType}</h3>
            <p style="background: #f0f4ff; padding: 15px; border-radius: 8px; margin: 15px 0; color: #666;">
                <strong>✓</strong> Marque as regras que deseja ativar para este tipo de processo.
            </p>
            <div style="display: grid; gap: 15px;">
                ${availableRules.map((rule, index) => {
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
            const response = await fetch(`${API_URL}/rules/`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    ...getAuthHeaders()
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
            const response = await fetch(`${API_URL}/rules/${processType}/${ruleName}`, {
                method: 'DELETE',
                headers: getAuthHeaders()
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
        const response = await fetch(`${API_URL}/process/download`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
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
        const response = await fetch(`${API_URL}/process/file/metadata`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
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

// ==================== Chave x CFOP ====================
let currentEditingCfopId = null;

async function loadCfopRules() {
    const container = document.getElementById('cfopRulesList');
    if (!container) {
        console.error('Container cfopRulesList não encontrado');
        return;
    }
    
    try {
        // Mostrar loading
        container.innerHTML = `
            <div style="text-align: center; padding: 40px; color: #999;">
                <p>Carregando regras...</p>
            </div>
        `;
        
        const response = await fetch(`${API_URL}/cfop-operation/`, {
            headers: getAuthHeaders()
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || 'Erro ao carregar regras CFOP');
        }
        
        const data = await response.json();
        console.log('Regras CFOP carregadas:', data);
        renderCfopRules(data.rules || []);
    } catch (error) {
        console.error('Erro ao carregar regras CFOP:', error);
        container.innerHTML = `
            <div style="text-align: center; padding: 20px; color: #ef4444;">
                <p>❌ Erro ao carregar regras: ${error.message}</p>
                <button onclick="loadCfopRules()" class="btn-secondary" style="margin-top: 10px;">🔄 Tentar novamente</button>
            </div>
        `;
    }
}

function renderCfopRules(rules) {
    const container = document.getElementById('cfopRulesList');
    
    const table = `
        <table style="width: 100%; border-collapse: collapse; background: white;">
            <thead style="position: sticky; top: 0; z-index: 10; background: #f8f9fa;">
                <tr style="border-bottom: 2px solid #dee2e6;">
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 80px;">Chave</th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 200px;">Descrição</th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 150px;">CFOP NF</th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 250px;">Regra</th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 250px;">Observação</th>
                    <!-- Pedido Compra - Oculto temporariamente, pode ser reativado depois -->
                    <!-- <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 120px;">Pedido Compra</th> -->
                    <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 100px;">Status</th>
                    <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 120px;">Ações</th>
                </tr>
            </thead>
            <tbody id="cfopTableBody">
                ${rules.length > 0 ? rules.map((rule, index) => `
                    <tr data-id="${rule.id}" style="border-bottom: 1px solid #e9ecef; transition: background 0.2s; ${!rule.ativo ? 'opacity: 0.6; background-color: #f8f9fa;' : ''}">
                        <td style="padding: 12px;">
                            <input type="text" 
                                   value="${(rule.chave || '').replace(/"/g, '&quot;')}" 
                                   data-field="chave"
                                   data-rule-id="${rule.id}"
                                   style="width: 100%; padding: 8px; border: 1px solid #ced4da; border-radius: 4px; font-weight: 600;"
                                   onchange="updateCfopField('${rule.id}', 'chave', this.value)">
                        </td>
                        <td style="padding: 12px;">
                            <input type="text" 
                                   value="${(rule.descricao || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;')}" 
                                   data-field="descricao"
                                   data-rule-id="${rule.id}"
                                   style="width: 100%; padding: 8px; border: 1px solid #ced4da; border-radius: 4px;"
                                   onchange="updateCfopField('${rule.id}', 'descricao', this.value)">
                        </td>
                        <td style="padding: 12px;">
                            <div class="cfop-tags-container" 
                                 data-rule-id="${rule.id}"
                                 style="display: flex; flex-wrap: wrap; gap: 6px; padding: 6px; border: 1px solid #ced4da; border-radius: 4px; min-height: 38px; align-items: center; background: white;">
                                ${renderCfopTags(rule.cfop || '', rule.id)}
                                <input type="text" 
                                       class="cfop-tag-input"
                                       data-rule-id="${rule.id}"
                                       placeholder="Digite CFOP e Enter"
                                       style="flex: 1; min-width: 120px; border: none; outline: none; padding: 4px; font-size: 0.9em; background: transparent;"
                                       onkeydown="handleCfopTagInput(event, '${rule.id}')"
                                       onblur="handleCfopTagBlur(event, '${rule.id}')">
                            </div>
                        </td>
                        <td style="padding: 12px;">
                            <textarea 
                                   data-field="regra"
                                   data-rule-id="${rule.id}"
                                   placeholder="Texto descritivo de quando usar"
                                   style="width: 100%; padding: 8px; border: 1px solid #ced4da; border-radius: 4px; font-size: 0.85em; resize: vertical; min-height: 60px; font-family: inherit;"
                                   onchange="updateCfopField('${rule.id}', 'regra', this.value)">${(rule.regra || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/\n/g, '&#10;')}</textarea>
                        </td>
                        <td style="padding: 12px;">
                            <textarea 
                                   data-field="observacao"
                                   data-rule-id="${rule.id}"
                                   placeholder="Observações adicionais"
                                   style="width: 100%; padding: 8px; border: 1px solid #ced4da; border-radius: 4px; font-size: 0.85em; resize: vertical; min-height: 60px; font-family: inherit;"
                                   onchange="updateCfopField('${rule.id}', 'observacao', this.value)">${(rule.observacao || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/\n/g, '&#10;')}</textarea>
                        </td>
                        <!-- Pedido Compra - Oculto temporariamente, pode ser reativado depois -->
                        <!-- <td style="padding: 12px; text-align: center;">
                            <input type="checkbox" 
                                   ${rule.pedido_compra ? 'checked' : ''}
                                   data-field="pedido_compra"
                                   data-rule-id="${rule.id}"
                                   style="width: 20px; height: 20px; cursor: pointer;"
                                   onchange="updateCfopField('${rule.id}', 'pedido_compra', this.checked)">
                        </td> -->
                        <td style="padding: 12px; text-align: center;">
                            <input type="checkbox" 
                                   ${rule.ativo !== false ? 'checked' : ''}
                                   data-field="ativo"
                                   data-rule-id="${rule.id}"
                                   style="width: 20px; height: 20px; cursor: pointer;"
                                   onchange="updateCfopField('${rule.id}', 'ativo', this.checked); updateStatusLabel(this)"
                                   title="${rule.ativo !== false ? 'Ativo' : 'Inativo'}">
                            <span class="status-label" style="margin-left: 5px; font-size: 0.85em; color: ${rule.ativo !== false ? '#10b981' : '#ef4444'}; font-weight: 600;">
                                ${rule.ativo !== false ? 'Ativo' : 'Inativo'}
                            </span>
                        </td>
                        <td style="padding: 12px; text-align: center;">
                            <button onclick="saveCfopRow('${rule.id}')" 
                                    class="btn-secondary" 
                                    style="padding: 6px 12px; font-size: 0.85em; margin-right: 5px; background: #10b981; color: white; border: none; cursor: pointer;"
                                    title="Salvar alterações">💾</button>
                            <button onclick="deleteCfopRule('${rule.id}', '${(rule.chave || '').replace(/'/g, "\\'")}')" 
                                    class="btn-secondary" 
                                    style="padding: 6px 12px; font-size: 0.85em; background: #ef4444; color: white; border: none; cursor: pointer;"
                                    title="Excluir">🗑️</button>
                        </td>
                    </tr>
                `).join('') : '<tr><td colspan="7" style="text-align: center; padding: 40px; color: #999;">Nenhuma regra cadastrada. Clique em "Adicionar Linha" para começar.</td></tr>'}
            </tbody>
        </table>
    `;
    
    container.innerHTML = table;
}

function addNewCfopRow() {
    let tbody = document.getElementById('cfopTableBody');
    if (!tbody) {
        // Se a tabela ainda não foi renderizada, criar estrutura básica
        const container = document.getElementById('cfopRulesList');
        container.innerHTML = `
            <table style="width: 100%; border-collapse: collapse; background: white;">
                <thead style="position: sticky; top: 0; z-index: 10; background: #f8f9fa;">
                    <tr style="border-bottom: 2px solid #dee2e6;">
                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 80px;">Chave</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 200px;">Descrição</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 150px;">CFOP NF</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 250px;">Regra</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; min-width: 250px;">Observação</th>
                        <!-- Pedido Compra - Oculto temporariamente, pode ser reativado depois -->
                        <!-- <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 120px;">Pedido Compra</th> -->
                        <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 100px;">Status</th>
                        <th style="padding: 12px; text-align: center; font-weight: 600; color: #333; background: #f8f9fa; border-bottom: 2px solid #dee2e6; width: 120px;">Ações</th>
                    </tr>
                </thead>
                <tbody id="cfopTableBody"></tbody>
            </table>
        `;
        tbody = document.getElementById('cfopTableBody');
    }
    
    const newRowId = 'new-' + Date.now();
    const newRow = document.createElement('tr');
    newRow.setAttribute('data-id', newRowId);
    newRow.style.borderBottom = '1px solid #e9ecef';
    newRow.style.backgroundColor = '#fff9e6';
    newRow.innerHTML = `
        <td style="padding: 12px;">
            <input type="text" 
                   data-field="chave"
                   data-rule-id="${newRowId}"
                   placeholder="Ex: 1B"
                   style="width: 100%; padding: 8px; border: 2px solid #fbbf24; border-radius: 4px; font-weight: 600;"
                   required>
        </td>
        <td style="padding: 12px;">
            <input type="text" 
                   data-field="descricao"
                   data-rule-id="${newRowId}"
                   placeholder="Ex: E-033-COMPRA PARA COMERCIALIZACAO"
                   style="width: 100%; padding: 8px; border: 2px solid #fbbf24; border-radius: 4px;"
                   required>
        </td>
        <td style="padding: 12px;">
            <div class="cfop-tags-container" 
                 data-rule-id="${newRowId}"
                 style="display: flex; flex-wrap: wrap; gap: 6px; padding: 6px; border: 2px solid #fbbf24; border-radius: 4px; min-height: 38px; align-items: center; background: #fff9e6;">
                <input type="text" 
                       class="cfop-tag-input"
                       data-rule-id="${newRowId}"
                       placeholder="Digite CFOP e Enter"
                       style="flex: 1; min-width: 120px; border: none; outline: none; padding: 4px; font-size: 0.9em; background: transparent;"
                       onkeydown="handleCfopTagInput(event, '${newRowId}')"
                       onblur="handleCfopTagBlur(event, '${newRowId}')">
            </div>
        </td>
        <td style="padding: 12px;">
            <textarea 
                   data-field="regra"
                   data-rule-id="${newRowId}"
                   placeholder="Texto descritivo de quando usar"
                   style="width: 100%; padding: 8px; border: 2px solid #fbbf24; border-radius: 4px; font-size: 0.85em; resize: vertical; min-height: 60px; font-family: inherit;"
            ></textarea>
        </td>
        <td style="padding: 12px;">
            <textarea 
                   data-field="observacao"
                   data-rule-id="${newRowId}"
                   placeholder="Observações adicionais"
                   style="width: 100%; padding: 8px; border: 2px solid #fbbf24; border-radius: 4px; font-size: 0.85em; resize: vertical; min-height: 60px; font-family: inherit;"
            ></textarea>
        </td>
        <!-- Pedido Compra - Oculto temporariamente, pode ser reativado depois -->
        <!-- <td style="padding: 12px; text-align: center;">
            <input type="checkbox" 
                   data-field="pedido_compra"
                   data-rule-id="${newRowId}"
                   style="width: 20px; height: 20px; cursor: pointer;"
                   checked>
        </td> -->
        <td style="padding: 12px; text-align: center;">
            <input type="checkbox" 
                   data-field="ativo"
                   data-rule-id="${newRowId}"
                   style="width: 20px; height: 20px; cursor: pointer;"
                   checked
                   onchange="updateStatusLabel(this)">
            <span class="status-label" style="margin-left: 5px; font-size: 0.85em; color: #10b981; font-weight: 600;">Ativo</span>
        </td>
        <td style="padding: 12px; text-align: center;">
            <button onclick="saveNewCfopRow('${newRowId}')" 
                    class="btn-secondary" 
                    style="padding: 6px 12px; font-size: 0.85em; margin-right: 5px; background: #10b981; color: white; border: none; cursor: pointer;"
                    title="Salvar">💾</button>
            <button onclick="cancelNewCfopRow('${newRowId}')" 
                    class="btn-secondary" 
                    style="padding: 6px 12px; font-size: 0.85em; background: #6b7280; color: white; border: none; cursor: pointer;"
                    title="Cancelar">✖️</button>
        </td>
    `;
    
    tbody.insertBefore(newRow, tbody.firstChild);
    
    // Focar no primeiro campo
    newRow.querySelector('input[data-field="chave"]').focus();
}

async function saveNewCfopRow(rowId) {
    const row = document.querySelector(`tr[data-id="${rowId}"]`);
    if (!row) return;
    
    const chave = row.querySelector('input[data-field="chave"]').value.trim();
    const descricao = row.querySelector('input[data-field="descricao"]').value.trim();
    // Coletar CFOPs das tags
    const cfop = getCfopFromTags(rowId);
    const regra = row.querySelector('textarea[data-field="regra"]').value.trim();
    const observacao = row.querySelector('textarea[data-field="observacao"]').value.trim();
    // Pedido Compra - Campo oculto, usando valor padrão (true). Pode ser reativado depois.
    const pedidoCompraInput = row.querySelector('input[data-field="pedido_compra"]');
    const pedidoCompra = pedidoCompraInput ? pedidoCompraInput.checked : true;
    const ativo = row.querySelector('input[data-field="ativo"]').checked;
    
    // Operação é igual à chave (mantido no backend mas não editável na UI)
    const operacao = chave;
    
    // Validação
    if (!chave) {
        showToast('⚠️ Chave é obrigatória', 'error');
        row.querySelector('input[data-field="chave"]').focus();
        return;
    }
    
    if (!descricao) {
        showToast('⚠️ Descrição é obrigatória', 'error');
        row.querySelector('input[data-field="descricao"]').focus();
        return;
    }
    
    if (!cfop) {
        showToast('⚠️ CFOP é obrigatório', 'error');
        const container = row.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
        if (container) {
            const input = container.querySelector('.cfop-tag-input');
            if (input) input.focus();
        }
        return;
    }
    
    // Desabilitar botões durante o salvamento
    const saveBtn = row.querySelector('button[onclick*="saveNewCfopRow"]');
    const cancelBtn = row.querySelector('button[onclick*="cancelNewCfopRow"]');
    if (saveBtn) saveBtn.disabled = true;
    if (cancelBtn) cancelBtn.disabled = true;
    if (saveBtn) saveBtn.textContent = 'Salvando...';
    
    try {
        const response = await fetch(`${API_URL}/cfop-operation/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            body: JSON.stringify({
                chave: chave,
                descricao: descricao,
                cfop: cfop,
                operacao: operacao,
                regra: regra,
                observacao: observacao,
                pedido_compra: pedidoCompra,
                ativo: ativo
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || 'Falha ao criar regra');
        }
        
        showToast('✓ Regra criada com sucesso!', 'success');
        // Recarregar a tabela completa para mostrar a nova regra
        await loadCfopRules();
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
        // Reabilitar botões em caso de erro
        if (saveBtn) {
            saveBtn.disabled = false;
            saveBtn.textContent = '💾';
        }
        if (cancelBtn) cancelBtn.disabled = false;
    }
}

function cancelNewCfopRow(rowId) {
    const row = document.querySelector(`tr[data-id="${rowId}"]`);
    if (row) {
        row.remove();
    }
}

async function updateCfopField(ruleId, field, value) {
    // Esta função é chamada quando um campo é alterado
    // Podemos adicionar validação em tempo real aqui se necessário
}

function updateStatusLabel(checkbox) {
    // Atualiza o texto "Ativo"/"Inativo" ao lado do checkbox
    const row = checkbox.closest('tr');
    if (!row) return;
    
    const statusLabel = row.querySelector('.status-label');
    if (!statusLabel) return;
    
    if (checkbox.checked) {
        statusLabel.textContent = 'Ativo';
        statusLabel.style.color = '#10b981';
    } else {
        statusLabel.textContent = 'Inativo';
        statusLabel.style.color = '#ef4444';
    }
}

function renderCfopTags(cfopString, ruleId) {
    // Separa CFOPs por espaço e renderiza como tags
    if (!cfopString || !cfopString.trim()) {
        return '';
    }
    
    const cfops = cfopString.split(/\s+/).filter(c => c.trim());
    return cfops.map(cfop => `
        <span class="cfop-tag" 
              data-cfop="${cfop.replace(/"/g, '&quot;')}"
              data-rule-id="${ruleId}"
              style="display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: #667eea; color: white; border-radius: 16px; font-size: 0.85em; font-weight: 500;">
            <span>${cfop.replace(/"/g, '&quot;')}</span>
            <button type="button" 
                    onclick="removeCfopTag('${ruleId}', '${cfop.replace(/"/g, '&quot;').replace(/'/g, "\\'")}')"
                    style="background: rgba(255,255,255,0.3); border: none; border-radius: 50%; width: 18px; height: 18px; cursor: pointer; color: white; font-size: 12px; line-height: 1; padding: 0; display: flex; align-items: center; justify-content: center; font-weight: bold;"
                    onmouseover="this.style.background='rgba(255,255,255,0.5)'"
                    onmouseout="this.style.background='rgba(255,255,255,0.3)'"
                    title="Remover CFOP">×</button>
        </span>
    `).join('');
}

function handleCfopTagInput(event, ruleId) {
    // Adiciona tag quando pressionar Enter
    const input = event.target;
    
    if (event.key === 'Enter' || event.key === ',') {
        event.preventDefault();
        const value = input.value.trim();
        
        if (value) {
            addCfopTag(ruleId, value);
            input.value = '';
        }
    } else if (event.key === 'Backspace' && input.value === '') {
        // Se backspace com input vazio, remover última tag
        const container = input.closest('.cfop-tags-container');
        if (container) {
            const tags = container.querySelectorAll('.cfop-tag');
            if (tags.length > 0) {
                const lastTag = tags[tags.length - 1];
                const cfopValue = lastTag.getAttribute('data-cfop');
                removeCfopTag(ruleId, cfopValue);
            }
        }
    }
}

function handleCfopTagBlur(event, ruleId) {
    // Adiciona tag quando perder foco (se houver valor)
    const input = event.target;
    const value = input.value.trim();
    
    if (value) {
        addCfopTag(ruleId, value);
        input.value = '';
    }
}

function addCfopTag(ruleId, cfopValue) {
    // Adiciona uma nova tag de CFOP
    if (!cfopValue || !cfopValue.trim()) {
        return;
    }
    
    const cfop = cfopValue.trim();
    const container = document.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
    if (!container) return;
    
    // Verificar se já existe
    const existingTags = container.querySelectorAll('.cfop-tag');
    for (let tag of existingTags) {
        if (tag.getAttribute('data-cfop') === cfop) {
            showToast('⚠️ Este CFOP já foi adicionado', 'error');
            return;
        }
    }
    
    // Criar nova tag
    const tag = document.createElement('span');
    tag.className = 'cfop-tag';
    tag.setAttribute('data-cfop', cfop);
    tag.setAttribute('data-rule-id', ruleId);
    tag.style.cssText = 'display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: #667eea; color: white; border-radius: 16px; font-size: 0.85em; font-weight: 500;';
    tag.innerHTML = `
        <span>${cfop.replace(/"/g, '&quot;')}</span>
        <button type="button" 
                onclick="removeCfopTag('${ruleId}', '${cfop.replace(/'/g, "\\'")}')"
                style="background: rgba(255,255,255,0.3); border: none; border-radius: 50%; width: 18px; height: 18px; cursor: pointer; color: white; font-size: 12px; line-height: 1; padding: 0; display: flex; align-items: center; justify-content: center; font-weight: bold;"
                onmouseover="this.style.background='rgba(255,255,255,0.5)'"
                onmouseout="this.style.background='rgba(255,255,255,0.3)'"
                title="Remover CFOP">×</button>
    `;
    
    // Inserir antes do input
    const input = container.querySelector('.cfop-tag-input');
    container.insertBefore(tag, input);
    
    // Atualizar campo hidden ou trigger change
    updateCfopFieldFromTags(ruleId);
}

function removeCfopTag(ruleId, cfopValue) {
    // Remove uma tag de CFOP
    const container = document.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
    if (!container) return;
    
    const tag = container.querySelector(`.cfop-tag[data-cfop="${cfopValue}"]`);
    if (tag) {
        tag.remove();
        updateCfopFieldFromTags(ruleId);
    }
}

function updateCfopFieldFromTags(ruleId) {
    // Atualiza o valor do campo CFOP baseado nas tags
    const container = document.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
    if (!container) return;
    
    const tags = container.querySelectorAll('.cfop-tag');
    const cfops = Array.from(tags).map(tag => tag.getAttribute('data-cfop')).filter(c => c);
    const cfopString = cfops.join(' ');
    
    // Trigger change event para atualizar no backend se necessário
    // O valor será coletado ao salvar a linha
}

function getCfopFromTags(ruleId) {
    // Coleta todos os CFOPs das tags e retorna como string separada por espaço
    const container = document.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
    if (!container) return '';
    
    const tags = container.querySelectorAll('.cfop-tag');
    const cfops = Array.from(tags).map(tag => tag.getAttribute('data-cfop')).filter(c => c);
    return cfops.join(' ');
}

async function saveCfopRow(ruleId) {
    const row = document.querySelector(`tr[data-id="${ruleId}"]`);
    if (!row) return;
    
    const chave = row.querySelector('input[data-field="chave"]').value.trim();
    const descricao = row.querySelector('input[data-field="descricao"]').value.trim();
    // Coletar CFOPs das tags
    const cfop = getCfopFromTags(ruleId);
    const regra = row.querySelector('textarea[data-field="regra"]').value.trim();
    const observacao = row.querySelector('textarea[data-field="observacao"]').value.trim();
    // Pedido Compra - Campo oculto, usando valor padrão (true). Pode ser reativado depois.
    const pedidoCompraInput = row.querySelector('input[data-field="pedido_compra"]');
    const pedidoCompra = pedidoCompraInput ? pedidoCompraInput.checked : true;
    const ativo = row.querySelector('input[data-field="ativo"]').checked;
    
    // Operação é igual à chave (mantido no backend mas não editável na UI)
    const operacao = chave;
    
    // Validação
    if (!chave) {
        showToast('⚠️ Chave é obrigatória', 'error');
        row.querySelector('input[data-field="chave"]').focus();
        return;
    }
    
    if (!descricao) {
        showToast('⚠️ Descrição é obrigatória', 'error');
        row.querySelector('input[data-field="descricao"]').focus();
        return;
    }
    
    if (!cfop) {
        showToast('⚠️ CFOP é obrigatório', 'error');
        const container = row.querySelector(`.cfop-tags-container[data-rule-id="${ruleId}"]`);
        if (container) {
            const input = container.querySelector('.cfop-tag-input');
            if (input) input.focus();
        }
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/cfop-operation/${ruleId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            body: JSON.stringify({
                chave: chave,
                descricao: descricao,
                cfop: cfop,
                operacao: operacao,
                regra: regra,
                observacao: observacao,
                pedido_compra: pedidoCompra,
                ativo: ativo
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Falha ao atualizar regra');
        }
        
        showToast('✓ Regra atualizada com sucesso!', 'success');
        loadCfopRules();
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}

async function deleteCfopRule(id, cfop) {
    if (!confirm(`Tem certeza que deseja excluir a regra CFOP ${cfop}?`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/cfop-operation/${id}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Falha ao excluir regra');
        }
        
        showToast('✓ Regra excluída com sucesso!', 'success');
        loadCfopRules();
        
    } catch (error) {
        showToast(`❌ Erro: ${error.message}`, 'error');
    }
}
