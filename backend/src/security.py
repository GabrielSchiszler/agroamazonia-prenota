from fastapi.security import APIKeyHeader
from fastapi.openapi.models import APIKey

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)

def get_api_key_scheme():
    return {
        "type": "apiKey",
        "in": "header",
        "name": "x-api-key",
        "description": "API Key no formato: agroamazonia_key_<codigo>"
    }
