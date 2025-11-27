#!/bin/bash

set -e

echo "🚀 Deploy AgroAmazonia Serverless Solution"
echo ""

# Cores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# 1. Build Backend
echo -e "${BLUE}📦 Building backend...${NC}"
cd backend
pip install -r requirements.txt -t src/
cd ..

# 2. Build Infrastructure
echo -e "${BLUE}🏗️  Building infrastructure...${NC}"
cd infrastructure
npm install
npm run build

# 3. Bootstrap CDK (primeira vez)
echo -e "${BLUE}🔧 Bootstrapping CDK...${NC}"
cdk bootstrap || true

# 4. Deploy
echo -e "${BLUE}🚀 Deploying stack...${NC}"
cdk deploy --require-approval never

echo ""
echo -e "${GREEN}✅ Deploy completed successfully!${NC}"
echo ""
echo "📋 Next steps:"
echo "1. Check CloudFormation outputs for API URL"
echo "2. Upload documents to S3 bucket"
echo "3. Test API endpoints"
