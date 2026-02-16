// Configuração do Cognito (pegar do output do CDK)
const COGNITO_CONFIG = {
    userPoolId: 'COLE_AQUI_O_USER_POOL_ID',
    clientId: 'COLE_AQUI_O_CLIENT_ID',
    region: 'us-east-1'
};

let authToken = localStorage.getItem('authToken');

async function login(username, password) {
    const url = `https://cognito-idp.${COGNITO_CONFIG.region}.amazonaws.com/`;
    
    const payload = {
        AuthFlow: 'USER_PASSWORD_AUTH',
        ClientId: COGNITO_CONFIG.clientId,
        AuthParameters: {
            USERNAME: username,
            PASSWORD: password
        }
    };
    
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-amz-json-1.1',
                'X-Amz-Target': 'AWSCognitoIdentityProviderService.InitiateAuth'
            },
            body: JSON.stringify(payload)
        });
        
        const data = await response.json();
        
        if (data.AuthenticationResult) {
            authToken = data.AuthenticationResult.IdToken;
            localStorage.setItem('authToken', authToken);
            return { success: true, token: authToken };
        }
        
        return { success: false, error: data.message || 'Login falhou' };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

function logout() {
    authToken = null;
    localStorage.removeItem('authToken');
    // Opcional: limpar também tokens OAuth2 ao fazer logout
    // clearAllOAuth2TokenCache();
}

function isAuthenticated() {
    return !!authToken;
}

function getAuthHeaders() {
    return authToken ? { 'Authorization': `Bearer ${authToken}` } : {};
}

/**
 * Cache de tokens OAuth2 no localStorage
 * Chave: `oauth2_token_${secretId}`
 * Valor: JSON com { token, expiresAt, tokenType }
 */
const TOKEN_CACHE_PREFIX = 'oauth2_token_';
const TOKEN_BUFFER_SECONDS = 300; // Renovar token 5 minutos antes de expirar

/**
 * Obtém token OAuth2 do cache ou faz nova requisição
 * 
 * @param {string} secretId - ID ou nome do secret no AWS Secrets Manager
 * @param {string} service - Nome do serviço (opcional, apenas para identificação)
 * @param {boolean} forceRefresh - Forçar renovação do token mesmo se válido
 * @returns {Promise<{success: boolean, token?: string, error?: string, cached?: boolean}>}
 */
async function getOAuth2Token(secretId, service = null, forceRefresh = false) {
    const cacheKey = `${TOKEN_CACHE_PREFIX}${secretId}`;
    
    // Verificar cache se não for refresh forçado
    if (!forceRefresh) {
        const cached = getCachedToken(secretId);
        if (cached && cached.token) {
            console.log(`[OAuth2] Token encontrado no cache para ${secretId}`);
            return { 
                success: true, 
                token: cached.token,
                tokenType: cached.tokenType || 'Bearer',
                expiresIn: cached.expiresIn,
                cached: true
            };
        }
    }
    
    // Buscar novo token
    console.log(`[OAuth2] Buscando novo token para ${secretId}${forceRefresh ? ' (refresh forçado)' : ''}`);
    const API_URL = normalizeApiUrl(localStorage.getItem('apiUrl') || window.ENV?.API_URL);
    const url = `${API_URL}/auth/token?secret_id=${encodeURIComponent(secretId)}${service ? `&service=${encodeURIComponent(service)}` : ''}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders() // Incluir token Cognito se disponível
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Erro desconhecido' }));
            return { 
                success: false, 
                error: errorData.detail || errorData.message || `Erro ${response.status}` 
            };
        }
        
        const data = await response.json();
        
        if (data.access_token) {
            // Calcular timestamp de expiração
            const expiresIn = data.expires_in || 3600; // Padrão 1 hora se não informado
            const expiresAt = Date.now() + (expiresIn * 1000);
            
            // Salvar no cache
            const cacheData = {
                token: data.access_token,
                tokenType: data.token_type || 'Bearer',
                expiresIn: expiresIn,
                expiresAt: expiresAt,
                secretId: secretId,
                service: service
            };
            localStorage.setItem(cacheKey, JSON.stringify(cacheData));
            
            console.log(`[OAuth2] Token salvo no cache para ${secretId}, expira em ${expiresIn}s`);
            
            return { 
                success: true, 
                token: data.access_token,
                tokenType: data.token_type || 'Bearer',
                expiresIn: expiresIn,
                cached: false
            };
        }
        
        return { success: false, error: 'Token não encontrado na resposta' };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

/**
 * Obtém token do cache se ainda for válido
 * @param {string} secretId - ID do secret
 * @returns {object|null} Token cacheado ou null se inválido/expirado
 */
function getCachedToken(secretId) {
    const cacheKey = `${TOKEN_CACHE_PREFIX}${secretId}`;
    const cached = localStorage.getItem(cacheKey);
    
    if (!cached) {
        return null;
    }
    
    try {
        const data = JSON.parse(cached);
        const now = Date.now();
        const expiresAt = data.expiresAt || 0;
        
        // Verificar se expirou (com buffer de segurança)
        const bufferTime = TOKEN_BUFFER_SECONDS * 1000;
        if (now >= (expiresAt - bufferTime)) {
            console.log(`[OAuth2] Token expirado ou próximo do vencimento para ${secretId}`);
            localStorage.removeItem(cacheKey);
            return null;
        }
        
        return data;
    } catch (e) {
        console.error(`[OAuth2] Erro ao ler cache para ${secretId}:`, e);
        localStorage.removeItem(cacheKey);
        return null;
    }
}

/**
 * Limpa o cache de token para um secret específico
 * @param {string} secretId - ID do secret
 */
function clearOAuth2TokenCache(secretId) {
    const cacheKey = `${TOKEN_CACHE_PREFIX}${secretId}`;
    localStorage.removeItem(cacheKey);
    console.log(`[OAuth2] Cache limpo para ${secretId}`);
}

/**
 * Limpa todos os caches de tokens OAuth2
 */
function clearAllOAuth2TokenCache() {
    const keys = Object.keys(localStorage);
    keys.forEach(key => {
        if (key.startsWith(TOKEN_CACHE_PREFIX)) {
            localStorage.removeItem(key);
        }
    });
    console.log('[OAuth2] Todos os caches de tokens limpos');
}

// Helper para normalizar URL (se não existir, usar do app.js)
function normalizeApiUrl(url) {
    if (!url) return '';
    return url.replace(/\/+$/, ''); // Remove barras no final
}

/**
 * Cache de Basic Auth do Protheus
 * Chave: 'protheus_basic_auth'
 * Valor: JSON com { authorization_header, expiresAt }
 */
const PROTHEUS_BASIC_AUTH_CACHE_KEY = 'protheus_basic_auth';
const PROTHEUS_BASIC_AUTH_CACHE_TTL = 3600000; // 1 hora em milissegundos

/**
 * Obtém o header Basic Auth do Protheus (com cache)
 * 
 * @param {boolean} forceRefresh - Forçar renovação mesmo se válido no cache
 * @returns {Promise<{success: boolean, authorization_header?: string, error?: string, cached?: boolean}>}
 */
async function getProtheusBasicAuth(forceRefresh = false) {
    const cacheKey = PROTHEUS_BASIC_AUTH_CACHE_KEY;
    
    // Verificar cache se não for refresh forçado
    if (!forceRefresh) {
        const cached = getCachedProtheusBasicAuth();
        if (cached && cached.authorization_header) {
            console.log('[Protheus Basic Auth] Header encontrado no cache');
            return { 
                success: true, 
                authorization_header: cached.authorization_header,
                cached: true
            };
        }
    }
    
    // Buscar novo header do backend
    console.log('[Protheus Basic Auth] Buscando novo header do backend' + (forceRefresh ? ' (refresh forçado)' : ''));
    const API_URL = normalizeApiUrl(localStorage.getItem('apiUrl') || window.ENV?.API_URL);
    const url = `${API_URL}/auth/protheus-basic`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders() // Incluir token Cognito se disponível
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Erro desconhecido' }));
            return { 
                success: false, 
                error: errorData.detail || errorData.message || `Erro ${response.status}` 
            };
        }
        
        const data = await response.json();
        
        if (data.authorization_header) {
            // Calcular timestamp de expiração
            const expiresAt = Date.now() + PROTHEUS_BASIC_AUTH_CACHE_TTL;
            
            // Salvar no cache
            const cacheData = {
                authorization_header: data.authorization_header,
                expiresAt: expiresAt
            };
            localStorage.setItem(cacheKey, JSON.stringify(cacheData));
            
            console.log('[Protheus Basic Auth] Header obtido e salvo no cache');
            return { 
                success: true, 
                authorization_header: data.authorization_header,
                cached: false
            };
        }
        
        return { success: false, error: 'Header não encontrado na resposta' };
    } catch (error) {
        console.error('[Protheus Basic Auth] Erro ao buscar header:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Obtém Basic Auth do Protheus do cache se ainda for válido
 * @returns {object|null} Header cacheado ou null se inválido/expirado
 */
function getCachedProtheusBasicAuth() {
    const cacheKey = PROTHEUS_BASIC_AUTH_CACHE_KEY;
    const cached = localStorage.getItem(cacheKey);
    
    if (!cached) {
        return null;
    }
    
    try {
        const data = JSON.parse(cached);
        const now = Date.now();
        const expiresAt = data.expiresAt || 0;
        
        // Verificar se expirou
        if (now >= expiresAt) {
            console.log('[Protheus Basic Auth] Header expirado no cache');
            localStorage.removeItem(cacheKey);
            return null;
        }
        
        return data;
    } catch (e) {
        console.error('[Protheus Basic Auth] Erro ao ler cache:', e);
        localStorage.removeItem(cacheKey);
        return null;
    }
}

/**
 * Limpa o cache do Basic Auth do Protheus
 */
function clearProtheusBasicAuthCache() {
    localStorage.removeItem(PROTHEUS_BASIC_AUTH_CACHE_KEY);
    console.log('[Protheus Basic Auth] Cache limpo');
}

/**
 * Faz uma requisição ao Protheus através do proxy do backend
 * 
 * Esta é a forma RECOMENDADA, pois:
 * - Não expõe credenciais no frontend
 * - Uma única requisição (frontend -> backend -> Protheus)
 * - Backend gerencia Basic Auth automaticamente
 * 
 * @param {string} method - Método HTTP (GET, POST, PUT, DELETE)
 * @param {string} path - Caminho da API do Protheus (ex: '/documento-entrada')
 * @param {object} body - Body da requisição (opcional)
 * @param {object} headers - Headers adicionais (opcional)
 * @param {string} tenantId - tenantId para adicionar no header (opcional)
 * @returns {Promise<{success: boolean, data?: object, error?: string}>}
 */
async function callProtheusProxy(method, path, body = null, headers = null, tenantId = null) {
    const API_URL = normalizeApiUrl(localStorage.getItem('apiUrl') || window.ENV?.API_URL);
    const url = `${API_URL}/auth/protheus-proxy`;
    
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders() // Token Cognito
            },
            body: JSON.stringify({
                method: method.toUpperCase(),
                path: path,
                body: body,
                headers: headers,
                tenant_id: tenantId
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Erro desconhecido' }));
            return { 
                success: false, 
                error: errorData.detail || errorData.message || `Erro ${response.status}` 
            };
        }
        
        const data = await response.json();
        return { 
            success: true, 
            data: data 
        };
    } catch (error) {
        console.error('[Protheus Proxy] Erro:', error);
        return { 
            success: false, 
            error: error.message 
        };
    }
}
