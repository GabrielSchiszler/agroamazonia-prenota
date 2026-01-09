#!/usr/bin/env node
/**
 * Script para validar que todos os recursos têm nomes padronizados com o ambiente
 * 
 * Uso: npx ts-node scripts/validate-names.ts
 */

import * as fs from 'fs';
import * as path from 'path';

const ENV_PATTERNS = ['dev', 'stg', 'prd'];
const REQUIRED_PATTERNS = [
  /lambda-[a-z0-9-]+-(dev|stg|prd)/,
  /tabela-[a-z0-9-]+-(dev|stg|prd)/,
  /bucket-[a-z0-9-]+-(dev|stg|prd)/,
  /api-[a-z0-9-]+-(dev|stg|prd)/,
  /state-machine-[a-z0-9-]+-(dev|stg|prd)/,
  /secret-[a-z0-9-]+-(dev|stg|prd)/,
  /topic-[a-z0-9-]+-(dev|stg|prd)/,
  /distribution-[a-z0-9-]+-(dev|stg|prd)/,
  /cache-policy-[a-z0-9-]+-(dev|stg|prd)/,
  /response-headers-policy-[a-z0-9-]+-(dev|stg|prd)/
];

interface ResourceCheck {
  file: string;
  line: number;
  resource: string;
  name: string;
  hasEnv: boolean;
  env?: string;
}

function checkFile(filePath: string): ResourceCheck[] {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  const issues: ResourceCheck[] = [];

  lines.forEach((line, index) => {
    // Verificar functionName
    if (line.includes('functionName:')) {
      const match = line.match(/functionName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'Lambda Function',
            name: nameValue,
            hasEnv: false
          });
        } else {
          const env = ENV_PATTERNS.find(e => nameValue.includes(`-${e}`));
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'Lambda Function',
            name: nameValue,
            hasEnv: true,
            env: env
          });
        }
      }
    }

    // Verificar tableName
    if (line.includes('tableName:')) {
      const match = line.match(/tableName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'DynamoDB Table',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar bucketName
    if (line.includes('bucketName:')) {
      const match = line.match(/bucketName:\s*[`'"]([^`'"]+)[`'"]/);
      if (match) {
        const nameValue = match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv && !nameValue.includes('${this.account}')) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'S3 Bucket',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar topicName
    if (line.includes('topicName:')) {
      const match = line.match(/topicName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'SNS Topic',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar secretName
    if (line.includes('secretName:')) {
      const match = line.match(/secretName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'Secrets Manager',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar stateMachineName
    if (line.includes('stateMachineName:')) {
      const match = line.match(/stateMachineName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'Step Functions State Machine',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar restApiName
    if (line.includes('restApiName:')) {
      const match = line.match(/restApiName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'API Gateway',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar distributionName
    if (line.includes('distributionName:')) {
      const match = line.match(/distributionName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'CloudFront Distribution',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar cachePolicyName
    if (line.includes('cachePolicyName:')) {
      const match = line.match(/cachePolicyName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'CloudFront Cache Policy',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }

    // Verificar responseHeadersPolicyName
    if (line.includes('responseHeadersPolicyName:')) {
      const match = line.match(/responseHeadersPolicyName:\s*(name\([^)]+\)|['"`]([^'"`]+)['"`])/);
      if (match) {
        const nameValue = match[2] || match[1];
        const hasEnv = ENV_PATTERNS.some(env => nameValue.includes(`-${env}`));
        if (!hasEnv) {
          issues.push({
            file: path.basename(filePath),
            line: index + 1,
            resource: 'CloudFront Response Headers Policy',
            name: nameValue,
            hasEnv: false
          });
        }
      }
    }
  });

  return issues;
}

function main() {
  const stackFiles = [
    path.join(__dirname, '../lib/agroamazonia-stack.ts'),
    path.join(__dirname, '../lib/frontend-stack.ts')
  ];

  console.log('🔍 Validando nomes de recursos...\n');

  let allIssues: ResourceCheck[] = [];
  let allValid: ResourceCheck[] = [];

  stackFiles.forEach(file => {
    if (fs.existsSync(file)) {
      const issues = checkFile(file);
      const valid = issues.filter(i => i.hasEnv);
      const invalid = issues.filter(i => !i.hasEnv);

      allIssues.push(...invalid);
      allValid.push(...valid);
    }
  });

  if (allIssues.length > 0) {
    console.log('❌ RECURSOS SEM AMBIENTE NO NOME:\n');
    allIssues.forEach(issue => {
      console.log(`  ${issue.resource} em ${issue.file}:${issue.line}`);
      console.log(`    Nome: ${issue.name}`);
      console.log('');
    });
    process.exit(1);
  }

  console.log('✅ Todos os recursos têm nomes padronizados com ambiente!\n');
  console.log(`📊 Total de recursos validados: ${allValid.length}\n`);

  // Agrupar por ambiente
  const byEnv: Record<string, number> = {};
  allValid.forEach(v => {
    if (v.env) {
      byEnv[v.env] = (byEnv[v.env] || 0) + 1;
    }
  });

  console.log('📈 Recursos por ambiente:');
  Object.entries(byEnv).forEach(([env, count]) => {
    console.log(`  ${env}: ${count} recursos`);
  });
}

main();

