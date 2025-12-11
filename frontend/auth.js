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
}

function isAuthenticated() {
    return !!authToken;
}

function getAuthHeaders() {
    return authToken ? { 'Authorization': `Bearer ${authToken}` } : {};
}
