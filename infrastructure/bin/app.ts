#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AgroAmazoniaStack } from '../lib/agroamazonia-stack';
import { FrontendStack } from '../lib/frontend-stack';

const app = new cdk.App();

// Obter ambiente da variável ENV (dev, stg, prd)
const environment = process.env.ENV || 'dev';

// Validar ambiente
if (!['dev', 'stg', 'prd'].includes(environment)) {
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

// Stack do frontend (recebe referência da API do backend)
// A URL da API será importada automaticamente do export do backend stack
new FrontendStack(app, `FrontendStack-${environment}`, {
  ...stackProps,
  environment: environment,
  stackName: `agroamazonia-frontend-${environment}`
  // apiUrl será importada automaticamente via Fn.importValue se não fornecida
});
