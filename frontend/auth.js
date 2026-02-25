// Verificar configuração OAuth2 ao carregar o script
(function checkOAuth2Config() {
    if (typeof window !== 'undefined' && window.ENV) {
        const hasOAuth2Config = !!(
            window.ENV.OAUTH2_FRONTEND_TOKEN_URL &&
            window.ENV.OAUTH2_FRONTEND_CLIENT_ID &&
            window.ENV.OAUTH2_FRONTEND_CLIENT_SECRET
        );
        
        if (!hasOAuth2Config) {
            console.warn('[OAuth2] ⚠️ Configuração OAuth2 não encontrada no window.ENV');
            console.warn('[OAuth2] Para corrigir:');
            console.warn('[OAuth2] 1. Defina as variáveis de ambiente:');
            console.warn('[OAuth2]    export OAUTH2_FRONTEND_TOKEN_URL="..."');
            console.warn('[OAuth2]    export OAUTH2_FRONTEND_CLIENT_ID="..."');
            console.warn('[OAuth2]    export OAUTH2_FRONTEND_CLIENT_SECRET="..."');
            console.warn('[OAuth2]    export OAUTH2_FRONTEND_SCOPE="..."');
            console.warn('[OAuth2] 2. Rode: python3 server.py (para regenerar config.js)');
        }
    }
})();

let authToken = null; // Será atualizado automaticamente via OAuth2

const TOKEN_BUFFER_SECONDS = 300; // Renovar token 5 minutos antes de expirar

// Obter configuração OAuth2 de window.ENV (gerado pelo server.py) ou usar valores padrão
function getOAuth2Config() {
    // Valores padrão para desenvolvimento (fallback)
    const defaults = {
        tokenUrl: 'https://api-auth-hml.agroamazonia.io/oauth2/token',
        clientId: '',
        clientSecret: '',
        scope: 'App_Fast/HML'
    };
    
    // Ler de window.ENV (gerado pelo server.py a partir de variáveis de ambiente)
    if (window.ENV) {
        const config = {
            tokenUrl: window.ENV.OAUTH2_FRONTEND_TOKEN_URL || defaults.tokenUrl,
            clientId: window.ENV.OAUTH2_FRONTEND_CLIENT_ID || defaults.clientId,
            clientSecret: window.ENV.OAUTH2_FRONTEND_CLIENT_SECRET || defaults.clientSecret,
            scope: window.ENV.OAUTH2_FRONTEND_SCOPE || defaults.scope
        };
        
        console.log('[OAuth2] Config carregada:', {
            tokenUrl: config.tokenUrl,
            clientId: config.clientId ? `${config.clientId.substring(0, 10)}...` : '(vazio)',
            clientSecret: config.clientSecret ? '***' : '(vazio)',
            scope: config.scope
        });
        
        return config;
    }
    
    console.warn('[OAuth2] window.ENV não encontrado, usando valores padrão');
    return defaults;
}

const OAUTH2_CACHE_KEY = 'oauth2_agroamazonia_token';

function logout() {
    authToken = null;
    localStorage.removeItem(OAUTH2_CACHE_KEY);
}

function isAuthenticated() {
    // Verificar se há token no cache ou na variável
    const cached = getCachedToken();
    return !!(authToken || (cached && cached.token));
}

function getAuthHeaders() {
    // Se authToken não estiver definido, tentar buscar do cache
    if (!authToken) {
        const cached = getCachedToken();
        if (cached && cached.token) {
            authToken = cached.token;
            console.log('[OAuth2] Token carregado do cache para getAuthHeaders()');
        }
    }
    
    if (authToken) {
        return { 'Authorization': `Bearer ${authToken}` };
    } else {
        console.warn('[OAuth2] ⚠️ getAuthHeaders() chamado mas nenhum token disponível');
        return {};
    }
}

async function getOAuth2Token(forceRefresh = false) {
    console.log('[OAuth2] Iniciando obtenção de token...', { forceRefresh });
    
    // Verificar cache se não for refresh forçado
    if (!forceRefresh) {
        const cached = getCachedToken();
        if (cached && cached.token) {
            console.log(`[OAuth2] Token encontrado no cache, expira em ${Math.round((cached.expiresAt - Date.now()) / 1000)}s`);
            // Atualizar authToken global
            authToken = cached.token;
            return { 
                success: true, 
                token: cached.token,
                tokenType: cached.tokenType || 'Bearer',
                cached: true
            };
        } else {
            console.log('[OAuth2] Nenhum token encontrado no cache');
        }
    }
    
    try {
        const config = getOAuth2Config();
        
        // Validar se as credenciais estão configuradas
        if (!config.clientId || !config.clientSecret) {
            const errorMsg = 'Credenciais OAuth2 não configuradas. Defina OAUTH2_FRONTEND_CLIENT_ID e OAUTH2_FRONTEND_CLIENT_SECRET nas variáveis de ambiente ou no arquivo .env';
            console.error('[OAuth2]', errorMsg);
            console.error('[OAuth2] Config atual:', {
                hasClientId: !!config.clientId,
                hasClientSecret: !!config.clientSecret,
                tokenUrl: config.tokenUrl,
                scope: config.scope
            });
            return { 
                success: false, 
                error: errorMsg
            };
        }
        
        console.log('[OAuth2] Fazendo requisição para obter token...', { tokenUrl: config.tokenUrl });
        
        const body = new URLSearchParams();
        body.append('grant_type', 'client_credentials');
        body.append('client_id', config.clientId);
        body.append('client_secret', config.clientSecret);
        body.append('scope', config.scope);

        const response = await fetch(config.tokenUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: body.toString()
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            let errorData;
            try {
                errorData = JSON.parse(errorText);
            } catch {
                errorData = { message: errorText };
            }
            return { 
                success: false, 
                error: errorData.detail || errorData.message || errorData.error || `Erro ${response.status}` 
            };
        }
        
        const data = await response.json();
        
        console.log('[OAuth2] Resposta completa recebida:', data);
        console.log('[OAuth2] Campos disponíveis:', Object.keys(data));
        
        // Verificar diferentes formatos de resposta (access_token ou accessToken)
        const token = data.access_token || data.accessToken || data.token;
        
        if (!token) {
            console.error('[OAuth2] ❌ Token não encontrado na resposta');
            console.error('[OAuth2] Resposta completa:', JSON.stringify(data, null, 2));
            return { success: false, error: data.error || 'Token não encontrado na resposta' };
        }
        
        console.log('[OAuth2] ✅ Token encontrado:', token.substring(0, 20) + '...');

        // Salvar no cache
        const cacheData = {
            token: token,
            tokenType: data.token_type || data.tokenType || 'Bearer',
            expiresAt: Date.now() + ((data.expires_in || data.expiresIn || 3600) * 1000)
        };
        localStorage.setItem(OAUTH2_CACHE_KEY, JSON.stringify(cacheData));

        // Atualizar authToken global
        authToken = token;

        console.log(`[OAuth2] ✅ Token obtido e salvo no cache, expira em ${data.expires_in || data.expiresIn || 3600}s`);
        console.log(`[OAuth2] Token (primeiros 20 chars): ${token.substring(0, 20)}...`);

        return { success: true, token: token, tokenType: data.token_type || data.tokenType || 'Bearer', cached: false };
    } catch (error) {
        console.error('[OAuth2] Erro ao obter token:', error);
        return { success: false, error: error.message };
    }
}

function getCachedToken() {
    const cached = localStorage.getItem(OAUTH2_CACHE_KEY);
    
    if (!cached) {
        return null;
    }
    
    try {
        const data = JSON.parse(cached);
        const now = Date.now();
        const expiresAt = data.expiresAt || 0;
        
        const bufferTime = TOKEN_BUFFER_SECONDS * 1000;
        if (now >= (expiresAt - bufferTime)) {
            localStorage.removeItem(OAUTH2_CACHE_KEY);
            return null;
        }
        
        return data;
    } catch (e) {
        localStorage.removeItem(OAUTH2_CACHE_KEY);
        return null;
    }
}

function clearOAuth2TokenCache() {
    authToken = null;
    localStorage.removeItem(OAUTH2_CACHE_KEY);
    console.log(`[OAuth2] Cache limpo`);
}

function clearAllOAuth2TokenCache() {
    const keys = Object.keys(localStorage);
    keys.forEach(key => {
        if (key.startsWith(OAUTH2_CACHE_KEY)) {
            localStorage.removeItem(key);
        }
    });
    console.log('[OAuth2] Todos os caches de tokens limpos');
}

function normalizeApiUrl(url) {
    if (!url) return '';
    return url.replace(/\/+$/, '');
}

// Interceptor para garantir que o token seja incluído em todas as requisições fetch
// (exceto PUTs para S3 presigned URLs)
(function setupFetchInterceptor() {
    const originalFetch = window.fetch;
    
    window.fetch = async function(...args) {
        const [url, options = {}] = args;
        const urlString = typeof url === 'string' ? url : url?.url || '';
        
        // Não adicionar token em PUTs para S3 (presigned URLs geralmente são PUT)
        const isPutToS3 = options.method === 'PUT' && (
            urlString.includes('s3.amazonaws.com') || 
            urlString.includes('s3.') ||
            urlString.includes('/upload') ||
            urlString.includes('presigned')
        );
        
        // Se não for PUT para S3, adicionar token se disponível e não já presente
        if (!isPutToS3) {
            const authHeaders = getAuthHeaders();
            if (authHeaders.Authorization) {
                // Garantir que options.headers existe
                if (!options.headers) {
                    options.headers = {};
                }
                
                // Se headers é um Headers object, converter para objeto
                if (options.headers instanceof Headers) {
                    const headersObj = {};
                    options.headers.forEach((value, key) => {
                        headersObj[key] = value;
                    });
                    options.headers = headersObj;
                }
                
                // Adicionar token apenas se não estiver presente
                if (!options.headers.Authorization && !options.headers.authorization) {
                    options.headers = {
                        ...options.headers,
                        ...authHeaders
                    };
                    console.log('[OAuth2] ✅ Token adicionado à requisição:', urlString.substring(0, 60));
                }
            } else {
                console.warn('[OAuth2] ⚠️ Requisição sem token disponível:', urlString.substring(0, 60));
            }
        } else {
            console.log('[OAuth2] PUT para S3 detectado, não adicionando token:', urlString.substring(0, 60));
        }
        
        return originalFetch.apply(this, args);
    };
    
    console.log('[OAuth2] ✅ Fetch interceptor configurado');
})();