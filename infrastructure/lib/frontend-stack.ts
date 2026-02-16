import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as iam from 'aws-cdk-lib/aws-iam';
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

    // S3 Bucket para frontend (website estático)
    // Nota: bucket names devem ser únicos globalmente, então incluímos account
    // IMPORTANTE: Não usar website hosting quando usar OAI com CloudFront
    const accountId = this.account || 'unknown';
    const frontendBucket = new s3.Bucket(this, 'FrontendBucket', {
      bucketName: `bucket-agroamazonia-frontend-${this.envName}-${accountId}`,
      // Não usar websiteIndexDocument/websiteErrorDocument com OAI
      publicReadAccess: false, // CloudFront vai servir o conteúdo via OAI
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
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

    // OAI (Origin Access Identity) para CloudFront acessar o bucket privado
    const originAccessIdentity = new cloudfront.OriginAccessIdentity(this, 'FrontendOAI', {
      comment: `OAI for AgroAmazonia Frontend - ${this.envName}`
    });

    // Permitir que CloudFront leia o bucket via OAI
    // O grantRead() adiciona automaticamente a política de bucket necessária
    frontendBucket.grantRead(originAccessIdentity);

    // IPs permitidos para acesso ao CloudFront (mesmos do bucket S3)
    const allowedIpRanges = [
      '10.255.0.0/24',
      '10.65.0.0/24'
    ];

    // CloudFront Function para validar IP de origem (GRÁTIS)
    // Limite: 10.000 req/s por distribuição (suficiente para a maioria dos casos)
    const ipRestrictionFunction = new cloudfront.Function(this, 'IpRestrictionFunction', {
      functionName: name('function', 'ip-restriction'),
      code: cloudfront.FunctionCode.fromInline(`
function handler(event) {

    var request = event.request;
    var uri = request.uri;

    // IP correto no CloudFront Functions
    var clientIp = event.viewer && event.viewer.ip 
        ? event.viewer.ip 
        : null;

    // ===============================
    // DEBUG MODE
    // ===============================
    if (uri === '/debug-ip' || uri === '/debug-ip.html') {

        return {
            statusCode: 200,
            statusDescription: 'OK',
            headers: {
                'content-type': { value: 'application/json' }
            },
            body: JSON.stringify({
                clientIp: clientIp || 'NOT_AVAILABLE',
                ipType: clientIp 
                    ? (clientIp.indexOf(':') !== -1 ? 'IPv6' : 'IPv4')
                    : 'UNKNOWN'
            }, null, 2)
        };
    }

    // Se IP não disponível → bloquear
    if (!clientIp) {
        return {
            statusCode: 403,
            statusDescription: 'Forbidden',
            body: 'IP not detected.'
        };
    }

    // Se for IPv6 → bloquear (ou ajuste se quiser permitir)
    if (clientIp.indexOf(':') !== -1) {
        return {
            statusCode: 403,
            statusDescription: 'Forbidden',
            body: 'IPv6 not allowed.'
        };
    }

    // ===============================
    // RANGES PERMITIDOS
    // ===============================
    var allowedRanges = [
        { network: '10.255.0.0', mask: 24 },
        { network: '10.65.0.0', mask: 24 }
    ];

    function ipToNumber(ip) {
        var parts = ip.split('.');
        if (parts.length !== 4) return null;

        var p0 = parseInt(parts[0], 10);
        var p1 = parseInt(parts[1], 10);
        var p2 = parseInt(parts[2], 10);
        var p3 = parseInt(parts[3], 10);

        if (
            p0 < 0 || p0 > 255 ||
            p1 < 0 || p1 > 255 ||
            p2 < 0 || p2 > 255 ||
            p3 < 0 || p3 > 255
        ) return null;

        // >>> 0 força unsigned
        return (((p0 << 24) >>> 0) +
                ((p1 << 16) >>> 0) +
                ((p2 << 8) >>> 0) +
                (p3 >>> 0)) >>> 0;
    }

    function isInRange(ip, network, mask) {
        var ipNum = ipToNumber(ip);
        var networkNum = ipToNumber(network);

        if (ipNum === null || networkNum === null) return false;

        var maskNum = (0xFFFFFFFF << (32 - mask)) >>> 0;

        return (ipNum & maskNum) === (networkNum & maskNum);
    }

    for (var i = 0; i < allowedRanges.length; i++) {
        if (isInRange(clientIp, allowedRanges[i].network, allowedRanges[i].mask)) {
            return request; // permitido
        }
    }

    // ===============================
    // BLOQUEAR
    // ===============================
    return {
        statusCode: 403,
        statusDescription: 'Forbidden',
        headers: {
            'content-type': { value: 'text/plain' }
        },
        body: 'Access denied. Your IP (' + clientIp + ') is not allowed.'
    };
}
      `)
    });

    // CloudFront Distribution
    // Nota: CloudFront Distribution não suporta nome customizado diretamente
    // O CDK gera um nome único automaticamente baseado no construct ID
    const distribution = new cloudfront.Distribution(this, 'FrontendDistribution', {
      defaultBehavior: {
        origin: new origins.S3Origin(frontendBucket, {
          originAccessIdentity: originAccessIdentity
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
        cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
        compress: true,
        cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
        // CloudFront Function para validar IP de origem
        functionAssociations: [{
          function: ipRestrictionFunction,
          eventType: cloudfront.FunctionEventType.VIEWER_REQUEST
        }],
        // Invalidar cache para arquivos HTML e JS
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
      // Comportamento para arquivos estáticos (cache longo)
      additionalBehaviors: {
        '/assets/*': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessIdentity: originAccessIdentity
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          // CloudFront Function para validar IP de origem
          functionAssociations: [{
            function: ipRestrictionFunction,
            eventType: cloudfront.FunctionEventType.VIEWER_REQUEST
          }],
          // Cache longo para assets (CSS, JS, imagens)
          cachePolicy: new cloudfront.CachePolicy(this, 'AssetsCachePolicy', {
            cachePolicyName: name('cache-policy', 'assets-long-cache'),
            defaultTtl: cdk.Duration.days(30),
            minTtl: cdk.Duration.days(30),
            maxTtl: cdk.Duration.days(365),
            enableAcceptEncodingGzip: true,
            enableAcceptEncodingBrotli: true
          })
        },
        '*.html': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessIdentity: originAccessIdentity
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          // CloudFront Function para validar IP de origem
          functionAssociations: [{
            function: ipRestrictionFunction,
            eventType: cloudfront.FunctionEventType.VIEWER_REQUEST
          }],
          // Cache curto para HTML (para facilitar invalidação)
          // Nota: Quando TTL = 0 (cache desabilitado), não podemos habilitar compressão
          cachePolicy: new cloudfront.CachePolicy(this, 'HtmlCachePolicy', {
            cachePolicyName: name('cache-policy', 'html-no-cache'),
            defaultTtl: cdk.Duration.seconds(0), // Sem cache por padrão
            minTtl: cdk.Duration.seconds(0),
            maxTtl: cdk.Duration.seconds(0)
            // enableAcceptEncodingGzip e enableAcceptEncodingBrotli não podem ser usados com TTL = 0
          })
        },
        '*.js': {
          origin: new origins.S3Origin(frontendBucket, {
            originAccessIdentity: originAccessIdentity
          }),
          viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
          allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
          cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
          compress: true,
          // CloudFront Function para validar IP de origem
          functionAssociations: [{
            function: ipRestrictionFunction,
            eventType: cloudfront.FunctionEventType.VIEWER_REQUEST
          }],
          // Cache curto para JS (para facilitar invalidação)
          // Nota: Quando TTL = 0 (cache desabilitado), não podemos habilitar compressão
          cachePolicy: new cloudfront.CachePolicy(this, 'JsCachePolicy', {
            cachePolicyName: name('cache-policy', 'js-short-cache'),
            defaultTtl: cdk.Duration.seconds(0), // Sem cache por padrão
            minTtl: cdk.Duration.seconds(0),
            maxTtl: cdk.Duration.seconds(0)
            // enableAcceptEncodingGzip e enableAcceptEncodingBrotli não podem ser usados com TTL = 0
          })
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
      priceClass: cloudfront.PriceClass.PRICE_CLASS_100, // Apenas EUA e Europa
      comment: `AgroAmazonia Frontend Distribution - ${this.envName}`
    });

    // Criar arquivo config.js dinâmico com a URL da API e API Key
    // Este arquivo será gerado durante o deploy e sobrescreverá o config.js estático
    // A URL será normalizada no JavaScript do frontend (removendo barra final)
    const configContent = cdk.Fn.sub(
      `// Configuração da API - gerada automaticamente durante o deploy
// Environment: ${this.envName}
// A URL será normalizada no app.js para remover barra final
window.ENV = {
    API_URL: '\${ApiUrl}',
    API_KEY: '${apiKey}'
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

