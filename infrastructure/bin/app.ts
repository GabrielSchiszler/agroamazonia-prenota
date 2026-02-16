#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AgroAmazoniaStack } from '../lib/agroamazonia-stack';
import { FrontendStack } from '../lib/frontend-stack';

const app = new cdk.App();

// Obter ambiente da variável ENV (dev, stg, prd)
const environment = process.env.ENV || 'dev';

// Validar ambiente
if (!['dev', 'stg', 'prd', 'test'].includes(environment)) {
  throw new Error(`Invalid environment: ${environment}. Must be one of: dev, stg, prd`);
}

const stackProps = {
  env: { 
    account: process.env.CDK_DEFAULT_ACCOUNT, 
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
};

// Stack principal (backend)
const backendStack = new AgroAmazoniaStack(app, `AgroAmazoniaStack-${environment}`, {
  ...stackProps,
  environment: environment,
  stackName: `agroamazonia-backend-${environment}`
});

// Stack do frontend
// A URL da API pode ser fornecida via:
//   - CDK context: cdk deploy --context apiUrl=https://...
//   - Variável de ambiente: export API_URL=https://...
//   - Props no código (se necessário)
const apiUrl = app.node.tryGetContext('apiUrl') || process.env.API_URL;

new FrontendStack(app, `FrontendStack-${environment}`, {
  ...stackProps,
  environment: environment,
  stackName: `agroamazonia-frontend-${environment}`,
  apiUrl: apiUrl // Passar URL se fornecida via context ou env var
});
