#!/bin/bash
# Pegar API Key após deploy

API_KEY_ID=$(aws cloudformation describe-stacks \
  --stack-name AgroAmazoniaStack \
  --query "Stacks[0].Outputs[?OutputKey=='ApiKeyId'].OutputValue" \
  --output text)

echo "API Key ID: $API_KEY_ID"
echo ""
echo "API Key Value:"
aws apigateway get-api-key --api-key $API_KEY_ID --include-value \
  --query 'value' --output text
