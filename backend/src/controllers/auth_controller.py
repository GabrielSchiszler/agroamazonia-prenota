"""
Controller para autenticação e obtenção de tokens OAuth2
"""
import json
import logging
import os
import boto3
import requests
import base64
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["Auth"])

secrets_manager = boto3.client('secretsmanager')


class TokenResponse(BaseModel):
    """Resposta com o token de acesso"""
    access_token: str
    token_type: str = "Bearer"
    expires_in: Optional[int] = None


def get_secret(secret_id: str) -> dict:
    """Obtém secret do AWS Secrets Manager"""
    try:
        resp = secrets_manager.get_secret_value(SecretId=secret_id)
        if resp.get("SecretString"):
            return json.loads(resp["SecretString"])
        return json.loads(resp["SecretBinary"].decode("utf-8"))
    except Exception as e:
        logger.error(f"Erro ao buscar secret {secret_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar credenciais do Secrets Manager: {str(e)}"
        )


def get_oauth2_token_from_secret(secret_id: str) -> dict:
    """
    Obtém token OAuth2 usando credenciais do Secrets Manager.
    
    O secret deve conter:
    - auth_url: URL do endpoint de autenticação
    - client_id: ID do cliente OAuth2
    - client_secret: Secret do cliente OAuth2
    - username: (opcional) Username para password grant
    - password: (opcional) Password para password grant
    - grant_type: (opcional) Tipo de grant, padrão 'password' ou 'client_credentials'
    """
    
    try:
        # Buscar credenciais do Secrets Manager
        credentials = get_secret(secret_id)
        
        auth_url = credentials.get('auth_url') or credentials.get('token_url')
        client_id = credentials.get('client_id')
        client_secret = credentials.get('client_secret')
        username = credentials.get('username')
        password = credentials.get('password')
        grant_type = credentials.get('grant_type', 'password' if username and password else 'client_credentials')
        
        if not all([auth_url, client_id, client_secret]):
            raise HTTPException(
                status_code=500,
                detail="Credenciais incompletas no Secrets Manager. Necessário: auth_url, client_id, client_secret"
            )
        
        # Preparar autenticação Basic
        credentials_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('utf-8')
        
        headers = {
            'Authorization': f'Basic {credentials_b64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Preparar dados do request baseado no grant_type
        data = {'grant_type': grant_type}
        
        if grant_type == 'password' and username and password:
            data['username'] = username
            data['password'] = password
        elif grant_type == 'client_credentials':
            # Client credentials não precisa de username/password
            pass
        else:
            # Se grant_type requer scope, adicionar se disponível
            if 'scope' in credentials:
                data['scope'] = credentials['scope']
        
        # Fazer requisição OAuth2
        logger.info(f"Obtendo token OAuth2 de {auth_url} com grant_type={grant_type}")
        response = requests.post(auth_url, data=data, headers=headers, timeout=60)
        response.raise_for_status()
        
        token_data = response.json()
        
        return {
            'access_token': token_data.get('access_token'),
            'token_type': token_data.get('token_type', 'Bearer'),
            'expires_in': token_data.get('expires_in')
        }
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP ao obter token: {e.response.status_code} - {e.response.text}")
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao obter token do servidor de autenticação: {e.response.status_code}"
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão ao obter token: {str(e)}")
        raise HTTPException(
            status_code=502,
            detail=f"Erro de conexão ao servidor de autenticação: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao obter token: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao obter token: {str(e)}"
        )


@router.get("/token", response_model=TokenResponse, summary="Obter token OAuth2")
async def get_token(
    secret_id: str = Query(..., description="ID ou nome do secret no AWS Secrets Manager"),
    service: Optional[str] = Query(None, description="Nome do serviço (opcional, para facilitar identificação)")
):
    """
    Obtém um token OAuth2 usando credenciais armazenadas no AWS Secrets Manager.
    
    O secret deve conter as seguintes chaves:
    - `auth_url` ou `token_url`: URL do endpoint de autenticação OAuth2
    - `client_id`: ID do cliente OAuth2
    - `client_secret`: Secret do cliente OAuth2
    - `username` (opcional): Username para grant_type 'password'
    - `password` (opcional): Password para grant_type 'password'
    - `grant_type` (opcional): Tipo de grant ('password' ou 'client_credentials'), padrão: 'password' se username/password existirem
    
    **Exemplo de uso:**
    ```
    GET /auth/token?secret_id=my-oauth-credentials&service=servicenow
    ```
    
    **Exemplo de secret no Secrets Manager:**
    ```json
    {
        "auth_url": "https://example.com/oauth/token",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "username": "user@example.com",
        "password": "password123",
        "grant_type": "password"
    }
    ```
    """
    try:
        token_data = get_oauth2_token_from_secret(secret_id)
        
        logger.info(f"Token obtido com sucesso para secret_id={secret_id}, service={service}")
        
        return TokenResponse(**token_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar requisição: {str(e)}"
        )


class BasicAuthResponse(BaseModel):
    """Resposta com o header Basic Auth codificado"""
    authorization_header: str
    """Header Authorization completo, pronto para uso (ex: 'Basic dXNlcm5hbWU6cGFzc3dvcmQ=')"""


@router.get("/protheus-basic", response_model=BasicAuthResponse, summary="Obter Basic Auth para Protheus")
async def get_protheus_basic_auth(
    secret_id: Optional[str] = Query(None, description="ID do secret no Secrets Manager (opcional, usa PROTHEUS_SECRET_ID se não informado)")
):
    """
    Obtém o header Basic Auth para autenticação com a API do Protheus.
    
    Busca as credenciais (username/password) do AWS Secrets Manager e retorna
    o header Authorization já codificado em Base64, da mesma forma que o Lambda
    send_to_protheus faz.
    
    O secret deve conter:
    - `username` ou `user`: Nome de usuário
    - `password` ou `pass`: Senha
    
    **Exemplo de uso:**
    ```
    GET /auth/protheus-basic
    GET /auth/protheus-basic?secret_id=my-protheus-credentials
    ```
    
    **Resposta:**
    ```json
    {
        "authorization_header": "Basic dXNlcm5hbWU6cGFzc3dvcmQ="
    }
    ```
    
    **Uso no frontend:**
    ```javascript
    const response = await fetch('/auth/protheus-basic');
    const { authorization_header } = await response.json();
    // Usar em requisições:
    headers: {
        'Authorization': authorization_header
    }
    ```
    """
    try:
        # Se secret_id não foi informado, usar variável de ambiente
        if not secret_id:
            secret_id = os.environ.get('PROTHEUS_SECRET_ID')
            if not secret_id:
                raise HTTPException(
                    status_code=400,
                    detail="secret_id não informado e PROTHEUS_SECRET_ID não configurado"
                )
        
        # Buscar credenciais do Secrets Manager
        credentials = get_secret(secret_id)
        
        # Suportar tanto 'username'/'password' quanto 'user'/'pass'
        username = credentials.get('username') or credentials.get('user')
        password = credentials.get('password') or credentials.get('pass')
        
        if not username or not password:
            raise HTTPException(
                status_code=500,
                detail="Secret deve conter 'username'/'password' (ou 'user'/'pass')"
            )
        
        # Gerar Basic Auth header (mesma forma que send_to_protheus faz)
        basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        authorization_header = f"Basic {basic_auth}"
        
        logger.info(f"Basic Auth gerado com sucesso para secret_id={secret_id} (username: {username})")
        
        return BasicAuthResponse(authorization_header=authorization_header)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar Basic Auth: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao gerar Basic Auth: {str(e)}"
        )


class ProtheusProxyRequest(BaseModel):
    """Request para proxy do Protheus"""
    method: str = "POST"
    """Método HTTP (GET, POST, PUT, DELETE)"""
    path: str
    """Caminho da API do Protheus (ex: '/documento-entrada')"""
    body: Optional[dict] = None
    """Body da requisição (opcional)"""
    headers: Optional[dict] = None
    """Headers adicionais (opcional, tenantId será adicionado automaticamente se presente)"""
    tenant_id: Optional[str] = None
    """tenantId para adicionar no header (opcional)"""


@router.post("/protheus-proxy", summary="Proxy para API do Protheus")
async def protheus_proxy(
    request: ProtheusProxyRequest,
    secret_id: Optional[str] = Query(None, description="ID do secret no Secrets Manager (opcional, usa PROTHEUS_SECRET_ID se não informado)"),
    protheus_url: Optional[str] = Query(None, description="URL base do Protheus (opcional, usa PROTHEUS_API_URL se não informado)")
):
    """
    Endpoint proxy para fazer requisições à API do Protheus.
    
    O backend:
    1. Busca credenciais do Secrets Manager
    2. Gera Basic Auth header (mesma forma que send_to_protheus)
    3. Faz a requisição ao Protheus
    4. Retorna a resposta para o frontend
    
    **Vantagens:**
    - Frontend não precisa obter Basic Auth separadamente
    - Credenciais nunca saem do backend
    - Uma única requisição do frontend
    
    **Exemplo de uso:**
    ```javascript
    const response = await fetch('/auth/protheus-proxy', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            ...getAuthHeaders() // Token Cognito
        },
        body: JSON.stringify({
            method: 'POST',
            path: '/documento-entrada',
            body: { /* payload */ },
            tenant_id: '00,010108'
        })
    });
    ```
    """
    try:
        # Obter configurações
        if not secret_id:
            secret_id = os.environ.get('PROTHEUS_SECRET_ID')
            if not secret_id:
                raise HTTPException(
                    status_code=400,
                    detail="secret_id não informado e PROTHEUS_SECRET_ID não configurado"
                )
        
        if not protheus_url:
            protheus_url = os.environ.get('PROTHEUS_API_URL')
            if not protheus_url:
                raise HTTPException(
                    status_code=400,
                    detail="protheus_url não informado e PROTHEUS_API_URL não configurado"
                )
        
        # Buscar credenciais do Secrets Manager
        credentials = get_secret(secret_id)
        username = credentials.get('username') or credentials.get('user')
        password = credentials.get('password') or credentials.get('pass')
        
        if not username or not password:
            raise HTTPException(
                status_code=500,
                detail="Secret deve conter 'username'/'password' (ou 'user'/'pass')"
            )
        
        # Gerar Basic Auth header (mesma forma que send_to_protheus faz)
        basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        
        # Montar URL completa
        path = request.path.lstrip('/')
        full_url = f"{protheus_url.rstrip('/')}/{path}"
        
        # Montar headers
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {basic_auth}',
            'Accept': 'application/json'
        }
        
        # Adicionar headers customizados (exceto Authorization)
        if request.headers:
            for key, value in request.headers.items():
                if key.lower() != 'authorization':
                    headers[key] = value
        
        # Adicionar tenantId se fornecido
        if request.tenant_id:
            headers['tenantId'] = str(request.tenant_id)
        
        # Fazer requisição ao Protheus
        method = request.method.upper()
        timeout = int(os.environ.get('PROTHEUS_TIMEOUT', '100'))
        
        logger.info(f"Fazendo requisição {method} para {full_url} (timeout: {timeout}s)")
        
        try:
            if method == 'GET':
                resp = requests.get(full_url, headers=headers, timeout=timeout, params=request.body)
            elif method == 'POST':
                resp = requests.post(full_url, json=request.body, headers=headers, timeout=timeout)
            elif method == 'PUT':
                resp = requests.put(full_url, json=request.body, headers=headers, timeout=timeout)
            elif method == 'DELETE':
                resp = requests.delete(full_url, headers=headers, timeout=timeout)
            else:
                raise HTTPException(status_code=400, detail=f"Método HTTP não suportado: {method}")
            
            # Retornar resposta do Protheus
            try:
                response_body = resp.json()
            except:
                response_body = {'raw_response': resp.text}
            
            return {
                'status_code': resp.status_code,
                'headers': dict(resp.headers),
                'body': response_body
            }
            
        except requests.exceptions.Timeout:
            raise HTTPException(
                status_code=504,
                detail=f"Timeout ao conectar ao Protheus (após {timeout}s)"
            )
        except requests.exceptions.ConnectionError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Erro de conexão ao Protheus: {str(e)}"
            )
        except requests.exceptions.HTTPError as e:
            try:
                error_body = e.response.json() if e.response else {}
            except:
                error_body = {'raw_response': e.response.text if e.response else str(e)}
            
            raise HTTPException(
                status_code=e.response.status_code if e.response else 502,
                detail={
                    'error': 'Erro HTTP do Protheus',
                    'status_code': e.response.status_code if e.response else None,
                    'body': error_body
                }
            )
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Erro na requisição ao Protheus: {str(e)}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no proxy do Protheus: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro no proxy do Protheus: {str(e)}"
        )

