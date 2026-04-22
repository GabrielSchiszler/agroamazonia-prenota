import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import * as path from 'path';

interface AgroAmazoniaStackProps extends cdk.StackProps {
  environment: string;
}

export class AgroAmazoniaStack extends cdk.Stack {
  private readonly envName: string;

  constructor(scope: Construct, id: string, props: AgroAmazoniaStackProps) {
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

    // VPC e Subnet para todas as Lambdas
    const vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', {
      vpcId: 'vpc-0f7e58cab8170d2d5'
    });

    const privateSubnet = ec2.Subnet.fromSubnetAttributes(this, 'PrivateSubnet', {
      subnetId: 'subnet-04bd6b0e1367cfffe',
      availabilityZone: 'sa-east-1a'
    });

    const privateSubnet2 = ec2.Subnet.fromSubnetAttributes(this, 'PrivateSubnet2', {
      subnetId: 'subnet-0897daaebdab91405',
      availabilityZone: 'sa-east-1b'
    });

    // Security Group existente para as Lambdas na VPC
    const lambdaSg = ec2.SecurityGroup.fromSecurityGroupId(this, 'LambdaSecurityGroup', 'sg-00197d66a726cc77b');

    // Configuração comum de VPC para todas as Lambdas
    const vpcConfig = {
      vpc,
      vpcSubnets: { subnets: [privateSubnet, privateSubnet2] },
      securityGroups: [lambdaSg]
    };

    // S3 Bucket para documentos brutos
    // Nota: bucket names devem ser únicos globalmente, então incluímos account
    const accountId = this.account || 'unknown';
    
    // IPs permitidos para acesso ao bucket (ranges de rede privada)
    const allowedIpRanges = [
      '10.255.0.0/24',
      '10.65.0.0/24'
    ];
    
    const rawDocumentsBucket = new s3.Bucket(this, 'RawDocumentsBucket', {
      bucketName: `bucket-agroamazonia-raw-documents-${this.envName}-${accountId}`,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      // Ativar todas as opções de Block Public Access
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      // Desabilitar ACLs (obrigatório para buckets criados após abril de 2023)
      // Usar apenas políticas de bucket para controle de acesso
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      // Remover CORS público - acesso será apenas via IPs permitidos
      // CORS removido para garantir privacidade total
      lifecycleRules: [{
        transitions: [{
          storageClass: s3.StorageClass.INTELLIGENT_TIERING,
          transitionAfter: cdk.Duration.days(30)
        }]
      }]
    });
    
    // Bucket Policy: Permitir acesso apenas dos ranges de IP especificados
    // IMPORTANTE: Presigned URLs geradas por Lambdas não respeitam restrições de IP
    // As Lambdas continuam tendo acesso via IAM roles (grantRead/grantReadWrite)
    
    // Permitir GetObject apenas dos IPs permitidos
    rawDocumentsBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'AllowGetObjectFromAllowedIpRanges',
        effect: iam.Effect.ALLOW,
        principals: [new iam.AnyPrincipal()],
        actions: [
          's3:GetObject',
          's3:GetObjectVersion'
        ],
        resources: [
          rawDocumentsBucket.arnForObjects('*')
        ],
        conditions: {
          IpAddress: {
            'aws:SourceIp': allowedIpRanges
          }
        }
      })
    );
    
    // Permitir ListBucket apenas dos IPs permitidos
    rawDocumentsBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'AllowListBucketFromAllowedIpRanges',
        effect: iam.Effect.ALLOW,
        principals: [new iam.AnyPrincipal()],
        actions: [
          's3:ListBucket'
        ],
        resources: [
          rawDocumentsBucket.bucketArn
        ],
        conditions: {
          IpAddress: {
            'aws:SourceIp': allowedIpRanges
          }
        }
      })
    );
    
    // IMPORTANTE: Presigned URLs geradas por Lambdas NÃO respeitam restrições de IP
    // Presigned URLs são tokens assinados que funcionam de qualquer lugar
    // Para bloquear completamente, seria necessário:
    // 1. Remover presigned URLs do código backend
    // 2. Fazer uploads passarem pela Lambda (proxy)
    // 3. Ou usar VPC endpoints com políticas de rede
    
    // A política abaixo bloqueia acesso direto ao bucket de IPs não permitidos
    // Mas presigned URLs ainda funcionarão de qualquer IP
    // Isso é uma limitação do S3 - presigned URLs bypassam bucket policies baseadas em IP

    // SNS Topic para erros de Lambda (unificado para sucesso e falha)
    const errorTopic = new sns.Topic(this, 'LambdaErrorTopic', {
      topicName: name('topic', 'agroamazonia-lambda-errors'),
      displayName: `AgroAmazonia Lambda Errors - ${this.envName}`
    });

    // DynamoDB Table
    const documentTable = new dynamodb.Table(this, 'DocumentProcessorTable', {
      tableName: name('tabela', 'document-processor'),
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      deletionProtection: true
    });

    // Lambda: Notificação de Recebimento
    const notifyReceiptLambda = new lambda.Function(this, 'NotifyReceiptFunction', {
      functionName: name('lambda', 'notify-receipt'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'notify_receipt.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/notify_receipt'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.seconds(30),
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(notifyReceiptLambda);
    rawDocumentsBucket.grantRead(notifyReceiptLambda);



    // Lambda: Processor
    const processorLambda = new lambda.Function(this, 'ProcessorFunction', {
      functionName: name('lambda', 'processor'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'processor.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/processor'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(processorLambda);

    // Lambda: Validate Rules (inclui ../utils — handler importa utils.primary_xml)
    const validateRulesLambda = new lambda.Function(this, 'ValidateRulesFunction', {
      functionName: name('lambda', 'validate-rules'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../backend/lambdas'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd validate_rules && cp -au . /asset-output/ && cp -au ../utils /asset-output/utils',
          ],
        },
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(validateRulesLambda);
    validateRulesLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: Report OCR Failure
    // OAuth2 credentials from environment variables or CDK context
    const ocrFailureApiUrl = this.node.tryGetContext('ocrFailureApiUrl') || process.env.OCR_FAILURE_API_URL || '';
    const ocrFailureAuthUrl = this.node.tryGetContext('ocrFailureAuthUrl') || process.env.OCR_FAILURE_AUTH_URL || '';
    const ocrFailureClientId = this.node.tryGetContext('ocrFailureClientId') || process.env.OCR_FAILURE_CLIENT_ID || '';
    const ocrFailureClientSecret = this.node.tryGetContext('ocrFailureClientSecret') || process.env.OCR_FAILURE_CLIENT_SECRET || '';
    const ocrFailureUsername = this.node.tryGetContext('ocrFailureUsername') || process.env.OCR_FAILURE_USERNAME || '';
    const ocrFailurePassword = this.node.tryGetContext('ocrFailurePassword') || process.env.OCR_FAILURE_PASSWORD || '';
    
    // ServiceNow Feedback API URL
    const servicenowFeedbackApiUrl = this.node.tryGetContext('servicenowFeedbackApiUrl') || process.env.SERVICENOW_FEEDBACK_API_URL || '';

    const reportOcrFailureLambda = new lambda.Function(this, 'ReportOcrFailureFunction', {
      functionName: name('lambda', 'report-ocr-failure'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd report_ocr_failure && pip install -r requirements.txt -t /asset-output && cp -au . /asset-output && cp -au ../utils /asset-output/'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0',
        OCR_FAILURE_API_URL: ocrFailureApiUrl,
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(reportOcrFailureLambda);
    reportOcrFailureLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: Send to Protheus (via HTTP direto com Basic Auth)
    const protheusSecretId = this.node.tryGetContext('protheusSecretId') || process.env.PROTHEUS_SECRET_ID || '';
    const protheusUrl = this.node.tryGetContext('protheusUrl') || process.env.PROTHEUS_API_URL || '';

    const sendToProtheusLambda = new lambda.Function(this, 'SendToProtheusFunction', {
      functionName: name('lambda', 'send-to-protheus'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd send_to_protheus && pip install -r requirements.txt -t /asset-output && cp -au . /asset-output && cp -au ../utils /asset-output/'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        PROTHEUS_SECRET_ID: protheusSecretId,
        PROTHEUS_API_URL: protheusUrl,
        PROTHEUS_TIMEOUT: '100', // Timeout em segundos
        // Variáveis para reportar falhas do Protheus para SCTASK
        OCR_FAILURE_API_URL: ocrFailureApiUrl,
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 512,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(sendToProtheusLambda);
    sendToProtheusLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));
    // Permissão para ler Secrets Manager (credenciais do Protheus)
    if (protheusSecretId) {
      // Construir ARN do secret (suporta ARN completo ou nome do secret)
      let secretArn: string;
      if (protheusSecretId.startsWith('arn:')) {
        secretArn = protheusSecretId;
      } else {
        // Se for apenas o nome, usar wildcard para cobrir o sufixo aleatório do AWS
        secretArn = `arn:aws:secretsmanager:${this.region}:${this.account}:secret:${protheusSecretId}*`;
      }
      
      sendToProtheusLambda.addToRolePolicy(new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [secretArn]
      }));
    }

    // Lambda: Update Metrics
    const updateMetricsLambda = new lambda.Function(this, 'UpdateMetricsFunction', {
      functionName: name('lambda', 'update-metrics'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/update_metrics'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(updateMetricsLambda);

    // Lambda: Notify Success - Busca dados, Bedrock summary, feedback API e SNS (mesmo payload que send_feedback)
    const notifySuccessLambda = new lambda.Function(this, 'NotifySuccessFunction', {
      functionName: name('lambda', 'notify-success'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd notify_success && pip install -r requirements.txt -t /asset-output && cp -au . /asset-output && cp -au ../utils /asset-output/'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        SNS_TOPIC_ARN: errorTopic.topicArn,
        ENVIRONMENT: this.envName,
        SERVICENOW_FEEDBACK_API_URL: servicenowFeedbackApiUrl,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0',
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadData(notifySuccessLambda);
    errorTopic.grantPublish(notifySuccessLambda);
    notifySuccessLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: Send Feedback to ServiceNow e SNS
    const sendFeedbackLambda = new lambda.Function(this, 'SendFeedbackFunction', {
      functionName: name('lambda', 'send-feedback'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd send_feedback && pip install -r requirements.txt -t /asset-output && cp -au . /asset-output && cp -au ../utils /asset-output/'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        SERVICENOW_FEEDBACK_API_URL: servicenowFeedbackApiUrl,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0',
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword,
        SNS_TOPIC_ARN: errorTopic.topicArn,
        ENVIRONMENT: this.envName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadData(sendFeedbackLambda);
    errorTopic.grantPublish(sendFeedbackLambda);
    sendFeedbackLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: Parse XML
    const parseXmlLambda = new lambda.Function(this, 'ParseXmlFunction', {
      functionName: name('lambda', 'parse-xml'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/parse_xml'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(parseXmlLambda);
    rawDocumentsBucket.grantRead(parseXmlLambda);

    // Lambda: Extract Documents (Textract on non-XML files — multi-anexo)
    const extractDocumentsLambda = new lambda.Function(this, 'ExtractDocumentsFunction', {
      functionName: name('lambda', 'extract-documents'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../backend/lambdas'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd extract_documents && cp -au . /asset-output/ && cp -au ../utils /asset-output/utils',
          ],
        },
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BUCKET_NAME: rawDocumentsBucket.bucketName,
        // Textract não existe em sa-east-1; AnalyzeDocument usa Bytes após S3 GetObject local.
        TEXTRACT_REGION: 'us-east-1',
      },
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(extractDocumentsLambda);
    rawDocumentsBucket.grantRead(extractDocumentsLambda);
    extractDocumentsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:AnalyzeDocument', 'textract:StartDocumentAnalysis', 'textract:GetDocumentAnalysis'],
      resources: ['*']
    }));
    extractDocumentsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject', 's3:GetObjectVersion'],
      resources: [rawDocumentsBucket.arnForObjects('*')]
    }));

    // Lambda: Merge Extractions (unify XML + Textract into canonical JSON)
    const mergeExtractionsLambda = new lambda.Function(this, 'MergeExtractionsFunction', {
      functionName: name('lambda', 'merge-extractions'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../backend/lambdas'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'cd merge_extractions && cp -au . /asset-output/ && cp -au ../utils /asset-output/utils',
          ],
        },
      }),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(mergeExtractionsLambda);

    // Lambda: listar FILE# para Step Functions Map (xml / textract / skip)
    const listAttachmentsLambda = new lambda.Function(this, 'ListAttachmentsFunction', {
      functionName: name('lambda', 'list-attachments'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/list_attachments'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadData(listAttachmentsLambda);

    // Lambda: rejeitar anexo não suportado (Map branch skip — sem Textract)
    const rejectAttachmentLambda = new lambda.Function(this, 'RejectAttachmentFunction', {
      functionName: name('lambda', 'reject-attachment'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/reject_attachment'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(rejectAttachmentLambda);

    // Lambda: Bedrock Extract Fields (AI enrichment for Protheus payload)
    const bedrockExtractFieldsLambda = new lambda.Function(this, 'BedrockExtractFieldsFunction', {
      functionName: name('lambda', 'bedrock-extract-fields'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/bedrock_extract_fields'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.minutes(3),
      memorySize: 512,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(bedrockExtractFieldsLambda);
    bedrockExtractFieldsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: Update Process Status (para atualizar status quando houver erro)
    const updateProcessStatusLambda = new lambda.Function(this, 'UpdateProcessStatusFunction', {
      functionName: name('lambda', 'update-process-status'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/update_process_status'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(updateProcessStatusLambda);

    // Lambda: S3 Upload Handler
    const s3UploadHandler = new lambda.Function(this, 'S3UploadHandler', {
      functionName: name('lambda', 's3-upload-handler'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/s3_upload_handler'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID || 'amazon.nova-pro-v1:0'
      },
      timeout: cdk.Duration.seconds(30),
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(s3UploadHandler);
    rawDocumentsBucket.grantRead(s3UploadHandler);

    // S3 Event Notification
    rawDocumentsBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(s3UploadHandler),
      { prefix: 'processes/' }
    );

    // Step Functions — Map por anexo + Choice: XML→parse_xml | binário→Textract | skip→reject (sem OCR).
    // Depois: merge → Bedrock → validação.

    const notifyTask = new tasks.LambdaInvoke(this, 'NotifyReceipt', {
      stateName: 'NotificarRecebimento',
      comment: 'Confirma recebimento do processo e prepara execução.',
      lambdaFunction: notifyReceiptLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'process_type.$': '$.process_type',
        'files.$': '$.files'
      }),
      outputPath: '$.Payload',
      resultPath: '$'  // Substituir contexto pelo resultado (apenas process_id)
    });

    const listAttachmentsTask = new tasks.LambdaInvoke(this, 'ListAttachments', {
      stateName: 'ListarAnexosFILE',
      comment: 'Monta attachments[] com handler xml | textract | skip (um item por FILE# com FILE_KEY).',
      lambdaFunction: listAttachmentsLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    // Map primeiro; Choice + tasks do iterator DEVEM ser filhos do Map (escopo `this` causa
    // IDs duplicados no grafo → RangeError Maximum call stack em registerContainedState).
    const attachmentsMap = sfn.Map.jsonPath(this, 'MapForEachAttachment', {
      stateName: 'ParaCadaAnexo_ChoiceXmlOuTextract',
      comment:
        'Um branch por anexo; Choice só dispara parse XML ou Textract ou rejeição — sem Lambda de OCR para XML.',
      itemsPath: '$.attachments',
      itemSelector: {
        'process_id.$': '$.process_id',
        'attachment.$': '$$.Map.Item.Value'
      },
      maxConcurrency: 8,
      resultPath: '$.attachmentResults'
    });

    const parseXmlSingleTask = new tasks.LambdaInvoke(attachmentsMap, 'ParseXmlSingle', {
      stateName: 'MapItem_XmlParaJson',
      comment: 'Iteração Map: um .xml → PARSED_XML= (NF-e ou resumo genérico).',
      lambdaFunction: parseXmlLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'file_name.$': '$.attachment.file_name',
        'file_key.$': '$.attachment.file_key'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const extractTextractSingleTask = new tasks.LambdaInvoke(attachmentsMap, 'ExtractTextractSingle', {
      stateName: 'MapItem_TextractOCR',
      comment: 'Iteração Map: um PDF/imagem → TEXTRACT# (XML não passa aqui).',
      lambdaFunction: extractDocumentsLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'file_name.$': '$.attachment.file_name',
        'file_key.$': '$.attachment.file_key',
        'file_sk.$': '$.attachment.file_sk'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const rejectUnsupportedTask = new tasks.LambdaInvoke(attachmentsMap, 'RejectUnsupportedAttachment', {
      stateName: 'MapItem_RejeitarSemOCR',
      comment: 'Iteração Map: DOCX etc. → REJECTED no FILE#, sem Textract.',
      lambdaFunction: rejectAttachmentLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'attachment.$': '$.attachment'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const routeAttachmentKind = new sfn.Choice(attachmentsMap, 'RouteAttachmentKind')
      .when(
        sfn.Condition.stringEquals('$.attachment.handler', 'xml'),
        parseXmlSingleTask
      )
      .when(
        sfn.Condition.stringEquals('$.attachment.handler', 'textract'),
        extractTextractSingleTask
      )
      .otherwise(rejectUnsupportedTask);

    attachmentsMap.itemProcessor(routeAttachmentKind);

    const afterAttachmentsPass = new sfn.Pass(this, 'AfterAttachmentsNormalize', {
      stateName: 'AposMap_SoProcessId',
      comment: 'Contexto mínimo para merge (PARSED_XML / TEXTRACT# já no Dynamo).',
      parameters: {
        'process_id.$': '$.process_id'
      },
      resultPath: '$'
    });

    const faseDepoisTextractAntesMerge = new sfn.Pass(this, 'FaseDepoisTextractAntesMerge', {
      stateName: 'Fase_Unificar_fontes_JSON',
      comment:
        'Transição: próximo estado monta um único registro MERGED_EXTRACTION (xml_documents + textract_documents).',
    });

    const mergeExtractionsTask = new tasks.LambdaInvoke(this, 'MergeExtractions', {
      stateName: 'UnificarXmlMaisTextract_Json',
      comment:
        'Lê todos PARSED_XML=* e TEXTRACT#*, grava MERGED_EXTRACTION + PARSED_OCR compatível com send_to_protheus.',
      lambdaFunction: mergeExtractionsLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const faseDepoisMergeAntesBedrock = new sfn.Pass(this, 'FaseDepoisMergeAntesBedrock', {
      stateName: 'Fase_Bedrock_formato_Protheus',
      comment:
        'Transição: próximo estado chama Bedrock para extrair/normalizar campos no formato esperado antes da validação e do Protheus.',
    });

    const bedrockExtractFieldsTask = new tasks.LambdaInvoke(this, 'BedrockExtractFields', {
      stateName: 'Bedrock_PadronizarParaProtheus',
      comment:
        'LLM sobre MERGED_EXTRACTION → BEDROCK_EXTRACTION (campos padronizados reutilizáveis na integração).',
      lambdaFunction: bedrockExtractFieldsLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const validateTask = new tasks.LambdaInvoke(this, 'ValidateRules', {
      stateName: 'ValidarRegrasNegocio',
      comment: 'Regras de negócio sobre NF-e + pedido + documentos (resultado em validation_result).',
      lambdaFunction: validateRulesLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      resultPath: '$.validation_result'
    });

    const reportFailureTask = new tasks.LambdaInvoke(this, 'ReportOcrFailure', {
      lambdaFunction: reportOcrFailureLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'failed_rules.$': '$.validation_result.Payload.failed_rules'
      }),
      resultPath: '$.failure_result'
    });

    const sendToProtheusTask = new tasks.LambdaInvoke(this, 'SendToProtheus', {
      lambdaFunction: sendToProtheusLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      resultPath: '$.protheus_result'
    });

    // Preparar dados para UpdateMetrics quando há sucesso (protheus_result)
    const prepareMetricsDataSuccess = new sfn.Pass(this, 'PrepareMetricsDataSuccess', {
      parameters: {
        'process_id.$': '$.process_id',
        'status': 'SUCCESS',
        'protheus_response.$': '$.protheus_result.Payload.protheus_response',
        'protheus_result.$': '$.protheus_result',
        'failure_result': {}
      },
      resultPath: '$.metrics_input'
    });

    // Preparar dados para UpdateMetrics quando há falha (failure_result)
    const prepareMetricsDataFailure = new sfn.Pass(this, 'PrepareMetricsDataFailure', {
      parameters: {
        'process_id.$': '$.process_id',
        'status': 'FAILED',
        'protheus_response': {},
        'failure_result': {
          'status.$': '$.failure_result.Payload.status'
        }
      },
      resultPath: '$.metrics_input'
    });

    // Preparar dados para UpdateMetrics quando há erro de etapa (LAMBDA_ERROR)
    const prepareMetricsDataForError = new sfn.Pass(this, 'PrepareMetricsDataForError', {
      parameters: {
        'process_id.$': '$.process_id',
        'status': 'FAILED',
        'protheus_response': {},
        'failure_result': {},
        'error': {
          'Error.$': '$.error.Error',
          'Cause.$': '$.error.Cause'
        }
      },
      resultPath: '$.metrics_input'
    });

    // Task para atualizar métricas no fluxo de SUCESSO (quando tudo deu certo)
    // IMPORTANTE: Usar resultPath ao invés de outputPath para preservar protheus_result no estado
    const updateMetricsTaskSuccess = new tasks.LambdaInvoke(this, 'UpdateMetricsSuccess', {
      lambdaFunction: updateMetricsLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
      resultPath: '$.metrics_result'  // Preserva o estado anterior (incluindo protheus_result)
    });
    
    // Task para atualizar métricas no fluxo de FALHA DE VALIDAÇÃO
    const updateMetricsTaskValidationFailure = new tasks.LambdaInvoke(this, 'UpdateMetricsValidationFailure', {
      lambdaFunction: updateMetricsLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
      resultPath: sfn.JsonPath.DISCARD
    });
    
    // IMPORTANTE: no fluxo de erro de LAMBDA, não podemos perder $.error (usado no NotifyError).
    // Então descartamos o output da Lambda e mantemos o input original.
    const updateMetricsTaskError = new tasks.LambdaInvoke(this, 'UpdateMetricsError', {
      lambdaFunction: updateMetricsLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
      resultPath: sfn.JsonPath.DISCARD
    });

    // Lambda task para atualizar status para FAILED antes de notificar erro
    const updateStatusBeforeErrorTask = new tasks.LambdaInvoke(this, 'UpdateStatusBeforeError', {
      lambdaFunction: updateProcessStatusLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'error': {
          'Error.$': '$.error.Error',
          'Cause.$': '$.error.Cause'
        },
        'error_type': 'LAMBDA_ERROR',
        'lambda_name.$': '$$.State.Name'
      }),
      // Não sobrescrever o input do fluxo (precisamos manter $.error para os próximos passos)
      resultPath: sfn.JsonPath.DISCARD
    });

    // SNS removido do Step Functions - agora é enviado pelas Lambdas (notify_success e send_feedback)

    // Estado de falha específico para falhas de validação
    const validationFailureState = new sfn.Fail(this, 'ValidationFailed', {
      error: 'ValidationFailed',
      cause: 'O processo falhou na validação de regras. Verifique os detalhes no failure_result.'
    });

    // Task Lambda para notificar sucesso (busca dados completos e envia SNS)
    const notifySuccessTask = new tasks.LambdaInvoke(this, 'NotifySuccessTask', {
      lambdaFunction: notifySuccessLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'protheus_result.$': '$.protheus_result'
      }),
      resultPath: '$.notification_result'
    });

    // Removido: prepareFeedbackSuccess e sendFeedbackSuccessTask
    // notifySuccessTask agora envia para API e SNS diretamente

    // Preparar dados para feedback de falha de validação
    const prepareFeedbackValidationFailure = new sfn.Pass(this, 'PrepareFeedbackValidationFailure', {
      parameters: {
        'process_id.$': '$.process_id',
        'success': false,
        'details': {
          'status': 'VALIDATION_FAILURE',
          'validation_status': 'FAILED',
          'failed_rules.$': '$.failure_result.Payload.failed_rules',
          'failed_rules_details.$': '$.failure_result.Payload.failed_rules_details',
          'failure_result.$': '$.failure_result.Payload',
          'timestamp.$': '$$.State.EnteredTime'
        }
      },
      resultPath: '$.feedback_input'
    });

    // Task para enviar feedback de falha de validação para ServiceNow
    const sendFeedbackValidationFailureTask = new tasks.LambdaInvoke(this, 'SendFeedbackValidationFailure', {
      lambdaFunction: sendFeedbackLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.feedback_input'),
      resultPath: sfn.JsonPath.DISCARD
    });

    // Preparar dados para feedback de falha de Lambda
    const prepareFeedbackLambdaFailure = new sfn.Pass(this, 'PrepareFeedbackLambdaFailure', {
      parameters: {
        'process_id.$': '$.process_id',
        'success': false,
        'details': {
          'status': 'LAMBDA_ERROR',
          'error_details': {
            'Error.$': '$.error.Error',
            'Cause.$': '$.error.Cause',
            'state_name.$': '$$.State.Name'
          },
          'timestamp.$': '$$.State.EnteredTime'
        }
      },
      resultPath: '$.feedback_input'
    });

    // Task para enviar feedback de falha de Lambda para ServiceNow
    const sendFeedbackLambdaFailureTask = new tasks.LambdaInvoke(this, 'SendFeedbackLambdaFailure', {
      lambdaFunction: sendFeedbackLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.feedback_input'),
      resultPath: sfn.JsonPath.DISCARD
    });

    const successState = new sfn.Succeed(this, 'ProcessSuccess');
    const failureState = new sfn.Fail(this, 'ProcessFailed', {
      error: 'ProcessingFailed',
      cause: 'O processo falhou durante a execução. Verifique os logs para mais detalhes.'
    });

    // Choice após validação
    const validationChoice = new sfn.Choice(this, 'HasValidationFailures?')
      .when(sfn.Condition.stringEquals('$.validation_result.Payload.validation_status', 'FAILED'), reportFailureTask)
      .otherwise(sendToProtheusTask);

    // =============================================
    // FLUXO DE SUCESSO: Protheus → Métricas → Notificar Sucesso (envia API + SNS) → Success
    // notifySuccessTask já envia feedback para API e SNS
    // =============================================
    sendToProtheusTask.next(prepareMetricsDataSuccess);
    prepareMetricsDataSuccess.next(updateMetricsTaskSuccess);
    updateMetricsTaskSuccess.next(notifySuccessTask);
    notifySuccessTask.next(successState);
    
    // =============================================
    // FLUXO DE FALHA DE VALIDAÇÃO: Report → Métricas → Feedback (envia API + SNS) → Fail
    // =============================================
    reportFailureTask.next(prepareMetricsDataFailure);
    prepareMetricsDataFailure.next(updateMetricsTaskValidationFailure);
    updateMetricsTaskValidationFailure.next(prepareFeedbackValidationFailure);
    prepareFeedbackValidationFailure.next(sendFeedbackValidationFailureTask);
    sendFeedbackValidationFailureTask.next(validationFailureState);
    
    // =============================================
    // FLUXO DE ERRO DE LAMBDA: Update Status → Métricas → Feedback (envia API + SNS) → Fail
    // =============================================
    updateStatusBeforeErrorTask.next(prepareMetricsDataForError);
    prepareMetricsDataForError.next(updateMetricsTaskError);
    updateMetricsTaskError.next(prepareFeedbackLambdaFailure);
    prepareFeedbackLambdaFailure.next(sendFeedbackLambdaFailureTask);
    sendFeedbackLambdaFailureTask.next(failureState);
    
    // =============================================
    // CATCH: Capturar erros de Lambda e redirecionar para fluxo de erro
    // =============================================
    updateMetricsTaskSuccess.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    updateMetricsTaskValidationFailure.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    updateMetricsTaskError.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    notifyTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    listAttachmentsTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    // Só Catch no Map: Catch nos estados internos + alvo fora do iterator gerava ciclo infinito no CDK (registerContainedState).
    attachmentsMap.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    mergeExtractionsTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    bedrockExtractFieldsTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    validateTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    sendToProtheusTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    reportFailureTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    // Se notifySuccessTask falhar, não deve falhar o processo inteiro (é apenas notificação)
    // Então ele vai direto para successState em caso de erro
    notifySuccessTask.addCatch(successState, { resultPath: '$.notification_error' });
    // Se sendFeedbackValidationFailureTask falhar, ainda assim deve terminar como falha
    sendFeedbackValidationFailureTask.addCatch(validationFailureState, { resultPath: '$.feedback_error' });
    // Se sendFeedbackLambdaFailureTask falhar, ainda assim deve terminar como falha
    sendFeedbackLambdaFailureTask.addCatch(failureState, { resultPath: '$.feedback_error' });

    attachmentsMap
      .next(afterAttachmentsPass)
      .next(faseDepoisTextractAntesMerge)
      .next(mergeExtractionsTask)
      .next(faseDepoisMergeAntesBedrock)
      .next(bedrockExtractFieldsTask)
      .next(validateTask)
      .next(validationChoice);

    const definition = notifyTask
      .next(listAttachmentsTask)
      .next(attachmentsMap);



    const stateMachine = new sfn.StateMachine(this, 'DocumentProcessorStateMachine', {
      stateMachineName: name('state-machine', 'document-processor-workflow'),
      comment:
        'Listar anexos → Map por arquivo (Choice: XML→parse | binário→Textract | skip→reject) → merge JSON → Bedrock → validação. XML não passa pela Lambda de Textract.',
      definition,
      timeout: cdk.Duration.minutes(15)
    });

    // Grant S3 read to Step Functions
    rawDocumentsBucket.grantRead(stateMachine);
    
    // Grant Textract permissions to read S3
    stateMachine.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject', 's3:GetObjectVersion'],
      resources: [rawDocumentsBucket.arnForObjects('*')]
    }));
    
    stateMachine.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:*'],
      resources: ['*']
    }));

    // Lambda: API FastAPI
    const apiLambda = new lambda.Function(this, 'ApiFunction', {
      functionName: name('lambda', 'api'),
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'src.main.handler',
      code: lambda.Code.fromAsset('../backend', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au src /asset-output/'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        STATE_MACHINE_ARN: stateMachine.stateMachineArn,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(apiLambda);
    // IMPORTANTE: grantReadWrite permite gerar presigned URLs que funcionam de qualquer IP
    // Para bloquear completamente, seria necessário remover presigned URLs e fazer upload via Lambda
    // Por enquanto, mantemos grantReadWrite mas adicionamos restrições na bucket policy
    rawDocumentsBucket.grantReadWrite(apiLambda);
    stateMachine.grantStartExecution(apiLambda);
    
    // Permissão para acessar Secrets Manager (para endpoint de autenticação)
    apiLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['secretsmanager:GetSecretValue'],
      resources: ['*'] // Permite acessar qualquer secret (pode ser restringido depois)
    }));

    // Outputs
    new cdk.CfnOutput(this, 'ApiLambdaArn', {
      value: apiLambda.functionArn,
      description: 'API Lambda Function ARN (vincular ao API Gateway existente)'
    });

    new cdk.CfnOutput(this, 'BucketName', {
      value: rawDocumentsBucket.bucketName,
      description: 'S3 Bucket Name'
    });

    new cdk.CfnOutput(this, 'TableName', {
      value: documentTable.tableName,
      description: 'DynamoDB Table Name'
    });

    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: stateMachine.stateMachineArn,
      description: 'Step Functions State Machine ARN'
    });

    new cdk.CfnOutput(this, 'ErrorTopicArn', {
      value: errorTopic.topicArn,
      description: 'SNS Topic ARN for Lambda Errors'
    });

    new cdk.CfnOutput(this, 'AllowedIpRanges', {
      value: allowedIpRanges.join(', '),
      description: 'IP ranges allowed for S3 bucket access (CIDR format)'
    });

  }
}
