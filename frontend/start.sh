#!/bin/bash
# Script para iniciar o frontend com configuração local

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Iniciando Frontend AgroAmazonia${NC}"
echo ""

# Verificar se existe arquivo .env
if [ ! -f .env ]; then
    echo -e "${YELLOW}⚠️  Arquivo .env não encontrado${NC}"
    echo "Criando arquivo .env com valores padrão..."
    cat > .env << 'EOF'
# Configuração da API para desenvolvimento local
API_URL=http://localhost:8001
API_KEY=dev
EOF
    echo -e "${GREEN}✓ Arquivo .env criado${NC}"
    echo ""
    echo "Para alterar, edite o arquivo .env ou defina variáveis de ambiente:"
    echo "  export API_URL='https://sua-api.com/v1'"
    echo "  export API_KEY='sua-api-key'"
    echo ""
fi

# Iniciar servidor
echo "Iniciando servidor na porta 8080..."
python3 server.py


