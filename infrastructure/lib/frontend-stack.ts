import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as certificatemanager from 'aws-cdk-lib/aws-certificatemanager';
import { Construct } from 'constructs';

interface FrontendStackProps extends cdk.StackProps {
  environment: string;
  apiUrl?: string; // URL da API do backend (opcional, pode ser passada diretamente ou importada)
  apiKey?: string; // API Key para autenticação (opcional)
}

export class FrontendStack extends cdk.Stack {
  private readonly envName: string;

  constructor(scope: Construct, id: string, props: FrontendStackProps) {
    super(scope, id, props);
    this.envName = props.environment || 'dev';

    // Aplicar tags padrão em todos os recursos da stack
    // Stage é o padrão AWS para rastreamento de custos
    cdk.Tags.of(this).add('Application', 'Agroamazonia-Prenota');
    cdk.Tags.of(this).add('Name', 'Agroamazonia');
    cdk.Tags.of(this).add('Stage', this.envName);

    // Helper function para padronizar nomes
    const name = (resourceType: string, resourceName: string): string => {
      const normalizedName = resourceName.toLowerCase().replace(/_/g, '-');
      return `${resourceType}-${normalizedName}-${this.envName}`;
    };

    // Obter URL da API do backend
    // Prioridade: CDK context > variável de ambiente > props > importValue
    let apiUrl: string | undefined;
    
    // 1. Tentar obter do CDK context (--context apiUrl=...)
    const contextApiUrl = this.node.tryGetContext('apiUrl');
    if (contextApiUrl) {
      apiUrl = contextApiUrl;
      console.log(`[FrontendStack] API URL obtida do CDK context: ${apiUrl}`);
    }
    // 2. Tentar obter da variável de ambiente
    else if (process.env.API_URL) {
      apiUrl = process.env.API_URL;
      console.log(`[FrontendStack] API URL obtida da variável de ambiente: ${apiUrl}`);
    }
    // 3. Tentar obter das props (passada diretamente)
    else if (props.apiUrl) {
      apiUrl = props.apiUrl;
      console.log(`[FrontendStack] API URL obtida das props: ${apiUrl}`);
    }
    
    // 4. Se não foi fornecida, usar importValue como fallback (pode falhar se o export não existir)
    if (!apiUrl) {
      console.warn(`[FrontendStack] ⚠️  API URL não fornecida via context/env/props. Tentando importValue do backend stack...`);
      console.warn(`[FrontendStack] ⚠️  Se o export não existir, o deploy falhará.`);
      console.warn(`[FrontendStack] ⚠️  Recomendado: forneça via --context apiUrl=https://... ou export API_URL=https://...`);
      const backendStackName = `agroamazonia-backend-${this.envName}`;
      apiUrl = cdk.Fn.importValue(`${backendStackName}-ApiUrl`);
    }
    
    // Validar que a URL foi fornecida (apenas para strings reais, não tokens CloudFormation)
    if (apiUrl && typeof apiUrl === 'string' && apiUrl.trim() === '') {
      throw new Error(
        `API URL não pode ser vazia! Forneça via:\n` +
        `  - CDK context: cdk deploy --context apiUrl=https://...\n` +
        `  - Variável de ambiente: export API_URL=https://...\n` +
        `  - Props no código: new FrontendStack(..., { apiUrl: 'https://...' })`
      );
    }

    // API Key (pode vir de variável de ambiente ou ser definida)
    const apiKey = props.apiKey || process.env.API_KEY || 'agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx';

    // OAuth2 Frontend Config (lê de variáveis de ambiente)
    const oauth2TokenUrl = process.env.OAUTH2_FRONTEND_TOKEN_URL || 'https://api-auth-hml.agroamazonia.io/oauth2/token';
    const oauth2ClientId = process.env.OAUTH2_FRONTEND_CLIENT_ID || '';
    const oauth2ClientSecret = process.env.OAUTH2_FRONTEND_CLIENT_SECRET || '';
    const oauth2Scope = process.env.OAUTH2_FRONTEND_SCOPE || 'App_Fast/HML';
    
    // Log das configurações OAuth2 (sem expor o secret)
    console.log(`[FrontendStack] OAuth2 Config:`);
    console.log(`  Token URL: ${oauth2TokenUrl}`);
    console.log(`  Client ID: ${oauth2ClientId ? `${oauth2ClientId.substring(0, 10)}...` : '(não definido)'}`);
    console.log(`  Client Secret: ${oauth2ClientSecret ? '***' : '(não definido)'}`);
    console.log(`  Scope: ${oauth2Scope}`);

    // OAC (Origin Access Control) - nome obtido de variável de ambiente
    // PRD: oac-bucket-s3-fast-prd
    // STG: oac-bucket-s3-fast-stg
    // O OAC já existe, então vamos usar o ID diretamente ao invés de criar
    const oacName = process.env.OAC_NAME || `oac-bucket-s3-fast-${this.envName}`;
    const oacId = process.env.OAC_ID; // ID do OAC existente (obrigatório)
    
    if (!oacId) {
      throw new Error(
        `OAC_ID não fornecido! O OAC '${oacName}' já existe e precisa ser referenciado.\n` +
        `Forneça o ID do OAC via variável de ambiente: export OAC_ID=<id-do-oac>\n` +
        `Para obter o ID: aws cloudfront list-origin-access-controls --query "OriginAccessControlList.Items[?Name=='${oacName}'].Id" --output text`
      );
    }
    
    console.log(`[FrontendStack] OAC Name: ${oacName}`);
    console.log(`[FrontendStack] OAC ID: ${oacId}`);
    
    // Usar o ID do OAC diretamente (não criar um construct, apenas referenciar)
    // Isso evita tentar criar o OAC que já existe

    // S3 Bucket para frontend (website estático)
    // Nota: bucket names devem ser únicos globalmente, então incluímos account
    // IMPORTANTE: Usando OAC (Origin Access Control) ao invés de OAI (legacy)
    const accountId = this.account || 'unknown';
    const frontendBucket = new s3.Bucket(this, 'FrontendBucket', {
      bucketName: `bucket-agroamazonia-frontend-${this.envName}-${accountId}`,
      publicReadAccess: false, // CloudFront vai servir o conteúdo via OAC
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      // Desabilitar ACLs (obrigatório para buckets criados após abril de 2023)
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Pode deletar em dev/stg
      autoDeleteObjects: true, // Limpar objetos ao deletar bucket
      versioned: false, // Não precisa versionar frontend
      encryption: s3.BucketEncryption.S3_MANAGED,
      cors: [{
        allowedMethods: [s3.HttpMethods.GET, s3.HttpMethods.HEAD],
        allowedOrigins: ['*'],
        allowedHeaders: ['*'],
        maxAge: 3000
      }]
    });

    // Lambda@Edge Functions para autenticação
    // Lambda@Edge functions são versões de Lambda functions, não CloudFront Functions
    // Default behavior: CheckAuthHandler (viewer request) e HttpHeadersHandler (viewer response)
    const checkAuthHandler = lambda.Version.fromVersionArn(
      this,
      'CheckAuthHandler',
      'arn:aws:lambda:us-east-1:835671581949:function:serverlessrepo-cloudfront-authori-CheckAuthHandler-SR4JfK6RxAmC:5'
    );
    
    const httpHeadersHandler = lambda.Version.fromVersionArn(
      this,
      'HttpHeadersHandler',
      'arn:aws:lambda:us-east-1:835671581949:function:serverlessrepo-cloudfront-autho-HttpHeadersHandler-zzPlMPTQfHDy:1'
    );
    
    // /signout* path: SignOutHandler
    const signOutHandler = lambda.Version.fromVersionArn(
      this,
      'SignOutHandler',
      'arn:aws:lambda:us-east-1:835671581949:function:serverlessrepo-cloudfront-authoriza-SignOutHandler-s3TOmgrJDctZ:5'
    );
    
    // /refreshauth* path: RefreshAuthHandler
    const refreshAuthHandler = lambda.Version.fromVersionArn(
      this,
      'RefreshAuthHandler',
      'arn:aws:lambda:us-east-1:835671581949:function:serverlessrepo-cloudfront-autho-RefreshAuthHandler-Cw3LiS0O0IRV:5'
    );
    
    // /parseauth* path: ParseAuthHandler
    const parseAuthHandler = lambda.Version.fromVersionArn(
      this,
      'ParseAuthHandler',
      'arn:aws:lambda:us-east-1:835671581949:function:serverlessrepo-cloudfront-authori-ParseAuthHandler-gXHsjX3WUIcE:5'
    );

    // Cache Policy desabilitada (TTL = 0) para todos os behaviors
    const noCachePolicy = new cloudfront.CachePolicy(this, 'NoCachePolicy', {
      cachePolicyName: name('cache-policy', 'no-cache'),
      defaultTtl: cdk.Duration.seconds(0),
      minTtl: cdk.Duration.seconds(0),
      maxTtl: cdk.Duration.seconds(0),
      comment: 'Cache policy desabilitada para todos os paths'
    });

    // CloudFront Distribution com OAC, Lambda@Edge e domínio customizado
    // Nota: A distribuição existente pode ter OAI configurado, então precisamos
    // garantir que apenas OAC seja usado (não ambos)
    const distribution = new cloudfront.Distribution(this, 'FrontendDistribution', {
      defaultBehavior: {
        origin: new origins.S3Origin(frontendBucket, {
          originAccessControlId: oacId!
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
        cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
        compress: true,
        cachePolicy: noCachePolicy, // Cache desabilitado
        // Lambda@Edge: CheckAuthHandler (viewer request) e HttpHeadersHandler (viewer response)
        edgeLambdas: [
          {
            functionVersion: checkAuthHandler,
            eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
          },
          {
            functionVersion: httpHeadersHandler,
            eventType: cloudfront.LambdaEdgeEventType.VIEWER_RESPONSE
          }
        ],
        responseHeadersPolicy: new cloudfront.ResponseHeadersPolicy(this, 'FrontendHeadersPolicy', {
          responseHeadersPolicyName: name('response-headers-policy', 'agroamazonia-frontend'),
          comment: `Headers policy for AgroAmazonia Frontend - ${this.envName}`,
          securityHeadersBehavior: {
            strictTransportSecurity: {
              accessControlMaxAge: cdk.Duration.seconds(31536000),
              includeSubdomains: true,
              override: true
            },
            contentTypeOptions: {
              override: true
            },
            frameOptions: {
              frameOption: cloudfront.HeadersFrameOption.DENY,
              override: true
            },
            referrerPolicy: {
              referrerPolicy: cloudfront.HeadersReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN,
              override: true
            }
          }
        })
      },
      // Comportamentos adicionais para paths específicos
      additionalBehaviors: {
        // Behaviors antigos: assets, HTML e JS
        '/assets/*': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          // Lambda@Edge do default behavior (CheckAuthHandler e HttpHeadersHandler)
          edgeLambdas: [
            {
              functionVersion: checkAuthHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            },
            {
              functionVersion: httpHeadersHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_RESPONSE
            }
          ]
        },
        '*.html': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          // Lambda@Edge do default behavior (CheckAuthHandler e HttpHeadersHandler)
          edgeLambdas: [
            {
              functionVersion: checkAuthHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            },
            {
              functionVersion: httpHeadersHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_RESPONSE
            }
          ]
        },
        '*.js': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          // Lambda@Edge do default behavior (CheckAuthHandler e HttpHeadersHandler)
          edgeLambdas: [
            {
              functionVersion: checkAuthHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            },
            {
              functionVersion: httpHeadersHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_RESPONSE
            }
          ]
        },
        // Novos behaviors para autenticação
        '/signout*': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          edgeLambdas: [
            {
              functionVersion: signOutHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            }
          ]
        },
        '/refreshauth*': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          edgeLambdas: [
            {
              functionVersion: refreshAuthHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            }
          ]
        },
        '/parseauth*': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessControlId: oacId!
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          cachePolicy: noCachePolicy, // Cache desabilitado
          edgeLambdas: [
            {
              functionVersion: parseAuthHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_REQUEST
            },
            {
              functionVersion: httpHeadersHandler,
              eventType: cloudfront.LambdaEdgeEventType.VIEWER_RESPONSE
            }
          ]
        }
      },
      errorResponses: [
        {
          httpStatus: 404,
          responseHttpStatus: 200,
          responsePagePath: '/index.html', // SPA fallback
          ttl: cdk.Duration.seconds(0)
        },
        {
          httpStatus: 403,
          responseHttpStatus: 200,
          responsePagePath: '/index.html', // SPA fallback
          ttl: cdk.Duration.seconds(0)
        }
      ],
      // Domínio customizado e certificado SSL
      domainNames: this.envName === 'prd' 
        ? ['fast-dash-prd.agroamazonia.com']
        : this.envName === 'stg'
        ? ['fast-dash-hml.agroamazonia.com']
        : [],
      certificate: this.envName === 'prd' || this.envName === 'stg'
        ? certificatemanager.Certificate.fromCertificateArn(
            this,
            'CloudFrontCertificate',
            'arn:aws:acm:us-east-1:835671581949:certificate/2d797bdd-f51d-4fdb-8ae3-ebc6d1db10d2'
          )
        : undefined,
      minimumProtocolVersion: cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
      priceClass: cloudfront.PriceClass.PRICE_CLASS_100, // Apenas EUA e Europa
      comment: `AgroAmazonia Frontend Distribution - ${this.envName}`
    });
    
    // Forçar remoção do OAI (legacy) da distribuição existente usando Cfn construct
    // Isso é necessário quando migrando de OAI para OAC
    // O CloudFormation não permite ter ambos (OAI e OAC) ao mesmo tempo
    // IMPORTANTE: S3OriginConfig ainda precisa existir, mas sem OriginAccessIdentity
    const cfnDistribution = distribution.node.defaultChild as cloudfront.CfnDistribution;
    
    // Remover OriginAccessIdentity do S3OriginConfig e garantir OAC para todos os origins
    // Origin 0: default behavior
    cfnDistribution.addOverride('Properties.DistributionConfig.Origins.0.S3OriginConfig.OriginAccessIdentity', undefined);
    cfnDistribution.addOverride('Properties.DistributionConfig.Origins.0.OriginAccessControlId', oacId!);
    
    // Origins 1-6: behaviors adicionais
    // 1-3: /assets/*, *.html, *.js (antigos)
    // 4-6: /signout*, /refreshauth*, /parseauth* (novos)
    for (let i = 1; i <= 6; i++) {
      cfnDistribution.addOverride(`Properties.DistributionConfig.Origins.${i}.S3OriginConfig.OriginAccessIdentity`, undefined);
      cfnDistribution.addOverride(`Properties.DistributionConfig.Origins.${i}.OriginAccessControlId`, oacId!);
    }
    
    // Adicionar política do bucket para permitir acesso do OAC
    // Nota: A política precisa ser adicionada após criar a distribuição para ter o ARN correto
    frontendBucket.addToResourcePolicy(new iam.PolicyStatement({
      sid: 'AllowCloudFrontOAC',
      effect: iam.Effect.ALLOW,
      principals: [new iam.ServicePrincipal('cloudfront.amazonaws.com')],
      actions: ['s3:GetObject'],
      resources: [frontendBucket.arnForObjects('*')],
      conditions: {
        StringEquals: {
          'AWS:SourceArn': distribution.distributionArn
        }
      }
    }));

    // Criar arquivo config.js dinâmico com a URL da API, API Key e OAuth2 Config
    // Este arquivo será gerado durante o deploy e sobrescreverá o config.js estático
    // A URL será normalizada no JavaScript do frontend (removendo barra final)
    const configContent = cdk.Fn.sub(
      `// Configuração da API - gerada automaticamente durante o deploy
// Environment: ${this.envName}
// A URL será normalizada no app.js para remover barra final
window.ENV = {
    API_URL: '\${ApiUrl}',
    API_KEY: '${apiKey}',
    OAUTH2_FRONTEND_TOKEN_URL: '${oauth2TokenUrl}',
    OAUTH2_FRONTEND_CLIENT_ID: '${oauth2ClientId}',
    OAUTH2_FRONTEND_CLIENT_SECRET: '${oauth2ClientSecret}',
    OAUTH2_FRONTEND_SCOPE: '${oauth2Scope}'
};`,
      {
        ApiUrl: apiUrl
      }
    );

    // Deploy do frontend para S3
    // Este deployment vai:
    // 1. Limpar o bucket antes de fazer upload (prune: true)
    // 2. Fazer upload de todos os arquivos do diretório frontend
    // 3. Criar arquivo config.js dinâmico com a URL da API (sobrescreve o estático)
    // 4. Invalidar o cache do CloudFront após o deploy
    // Nota: Atualizado para CDK 2.241.0 com urllib3 >= 2.6.3 (CVE-2025-66418, CVE-2026-21441)
    // ephemeralStorageSize força atualização do Lambda sem afetar o bucket S3
    const deployment = new s3deploy.BucketDeployment(this, 'FrontendDeployment', {
      sources: [
        s3deploy.Source.asset('../frontend'),
        // Adicionar config.js dinâmico (sobrescreve o config.js estático se existir)
        s3deploy.Source.data('config.js', configContent)
      ],
      destinationBucket: frontendBucket,
      distribution: distribution,
      distributionPaths: ['/*'], // Invalidar todos os paths
      prune: true, // Remover arquivos que não estão mais no source
      retainOnDelete: false, // Não reter arquivos ao deletar stack
      memoryLimit: 512,
      ephemeralStorageSize: cdk.Size.mebibytes(512), // Força atualização do Lambda (não afeta o bucket S3)
      // Invalidar cache após deploy
      cacheControl: [
        s3deploy.CacheControl.setPublic(),
        s3deploy.CacheControl.maxAge(cdk.Duration.days(0)) // Sem cache para HTML/JS
      ]
    });

    // Outputs
    new cdk.CfnOutput(this, 'FrontendBucketName', {
      value: frontendBucket.bucketName,
      description: 'S3 Bucket Name for Frontend'
    });

    new cdk.CfnOutput(this, 'CloudFrontDistributionId', {
      value: distribution.distributionId,
      description: 'CloudFront Distribution ID'
    });

    new cdk.CfnOutput(this, 'CloudFrontDomainName', {
      value: distribution.distributionDomainName,
      description: 'CloudFront Distribution Domain Name'
    });

    new cdk.CfnOutput(this, 'FrontendUrl', {
      value: `https://${distribution.distributionDomainName}`,
      description: 'Frontend URL'
    });
  }
}

