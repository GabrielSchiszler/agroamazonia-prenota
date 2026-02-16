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
import { Construct } from 'constructs';

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

    // Security Group existente para as Lambdas na VPC
    const lambdaSg = ec2.SecurityGroup.fromSecurityGroupId(this, 'LambdaSecurityGroup', 'sg-048ca965b1065aa57');

    // Configuração comum de VPC para todas as Lambdas
    const vpcConfig = {
      vpc,
      vpcSubnets: { subnets: [privateSubnet] },
      securityGroups: [lambdaSg]
    };

    // S3 Bucket para documentos brutos
    // Nota: bucket names devem ser únicos globalmente, então incluímos account
    const accountId = this.account || 'unknown';
    const rawDocumentsBucket = new s3.Bucket(this, 'RawDocumentsBucket', {
      bucketName: `bucket-agroamazonia-raw-documents-${this.envName}-${accountId}`,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      cors: [{
        allowedMethods: [
          s3.HttpMethods.GET,
          s3.HttpMethods.PUT,
          s3.HttpMethods.POST,
          s3.HttpMethods.DELETE,
          s3.HttpMethods.HEAD
        ],
        allowedOrigins: ['*'],
        allowedHeaders: ['*'],
        exposedHeaders: ['ETag'],
        maxAge: 3000
      }],
      lifecycleRules: [{
        transitions: [{
          storageClass: s3.StorageClass.INTELLIGENT_TIERING,
          transitionAfter: cdk.Duration.days(30)
        }]
      }]
    });

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
      removalPolicy: cdk.RemovalPolicy.RETAIN
    });



    // Lambda: Notificação de Recebimento
    const notifyReceiptLambda = new lambda.Function(this, 'NotifyReceiptFunction', {
      functionName: name('lambda', 'notify-receipt'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'notify_receipt.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/notify_receipt'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.seconds(30),
      ...vpcConfig
    });

    documentTable.grantReadWriteData(notifyReceiptLambda);
    rawDocumentsBucket.grantRead(notifyReceiptLambda);



    // Lambda: Processor
    const processorLambda = new lambda.Function(this, 'ProcessorFunction', {
      functionName: name('lambda', 'processor'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'processor.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/processor'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(processorLambda);

    // Lambda: Validate Rules
    const validateRulesLambda = new lambda.Function(this, 'ValidateRulesFunction', {
      functionName: name('lambda', 'validate-rules'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/validate_rules'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
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
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/report_ocr_failure', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        OCR_FAILURE_API_URL: ocrFailureApiUrl,
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(reportOcrFailureLambda);

    // Lambda: Send to Protheus (via HTTP direto com Basic Auth)
    const protheusSecretId = this.node.tryGetContext('protheusSecretId') || process.env.PROTHEUS_SECRET_ID || '';
    const protheusUrl = this.node.tryGetContext('protheusUrl') || process.env.PROTHEUS_API_URL || '';

    const sendToProtheusLambda = new lambda.Function(this, 'SendToProtheusFunction', {
      functionName: name('lambda', 'send-to-protheus'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/send_to_protheus', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        PROTHEUS_SECRET_ID: protheusSecretId,
        PROTHEUS_API_URL: protheusUrl,
        PROTHEUS_TIMEOUT: '30', // Timeout em segundos
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
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/update_metrics'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(updateMetricsLambda);

    // Lambda: Notify Success - Busca dados, envia feedback para API e SNS
    const notifySuccessLambda = new lambda.Function(this, 'NotifySuccessFunction', {
      functionName: name('lambda', 'notify-success'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/notify_success', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        SNS_TOPIC_ARN: errorTopic.topicArn,
        ENVIRONMENT: this.envName,
        SERVICENOW_FEEDBACK_API_URL: servicenowFeedbackApiUrl,
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      ...vpcConfig
    });

    documentTable.grantReadData(notifySuccessLambda);
    errorTopic.grantPublish(notifySuccessLambda);

    // Lambda: Send Feedback to ServiceNow e SNS
    const sendFeedbackLambda = new lambda.Function(this, 'SendFeedbackFunction', {
      functionName: name('lambda', 'send-feedback'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../backend/lambdas/send_feedback', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ]
        }
      }),
      environment: {
        TABLE_NAME: documentTable.tableName,
        SERVICENOW_FEEDBACK_API_URL: servicenowFeedbackApiUrl,
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
      ...vpcConfig
    });

    documentTable.grantReadData(sendFeedbackLambda);
    errorTopic.grantPublish(sendFeedbackLambda);

    // Lambda: Parse XML
    const parseXmlLambda = new lambda.Function(this, 'ParseXmlFunction', {
      functionName: name('lambda', 'parse-xml'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/parse_xml'),
      environment: {
        TABLE_NAME: documentTable.tableName,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(parseXmlLambda);
    rawDocumentsBucket.grantRead(parseXmlLambda);

    // Lambda: Update Process Status (para atualizar status quando houver erro)
    const updateProcessStatusLambda = new lambda.Function(this, 'UpdateProcessStatusFunction', {
      functionName: name('lambda', 'update-process-status'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/update_process_status'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      ...vpcConfig
    });

    documentTable.grantReadWriteData(updateProcessStatusLambda);

    // Lambda: S3 Upload Handler
    const s3UploadHandler = new lambda.Function(this, 'S3UploadHandler', {
      functionName: name('lambda', 's3-upload-handler'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/s3_upload_handler'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30),
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

    // Step Functions State Machine
    // Cada Lambda recebe apenas o necessário e retorna apenas o necessário para o próximo passo
    
    const notifyTask = new tasks.LambdaInvoke(this, 'NotifyReceipt', {
      lambdaFunction: notifyReceiptLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'process_type.$': '$.process_type',
        'files.$': '$.files'
      }),
      outputPath: '$.Payload',
      resultPath: '$'  // Substituir contexto pelo resultado (apenas process_id)
    });

    const parseXmlTask = new tasks.LambdaInvoke(this, 'ParseXml', {
      lambdaFunction: parseXmlLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id'
      }),
      outputPath: '$.Payload',
      resultPath: '$'
    });

    const validateTask = new tasks.LambdaInvoke(this, 'ValidateRules', {
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
    parseXmlTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
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

    // Conectar fluxo principal
    const definition = notifyTask
      .next(parseXmlTask)
      .next(validateTask)
      .next(validationChoice);



    const stateMachine = new sfn.StateMachine(this, 'DocumentProcessorStateMachine', {
      stateMachineName: name('state-machine', 'document-processor-workflow'),
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
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'src.main.handler',
      code: lambda.Code.fromAsset('../backend', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
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
      ...vpcConfig
    });

    documentTable.grantReadWriteData(apiLambda);
    rawDocumentsBucket.grantReadWrite(apiLambda);
    stateMachine.grantStartExecution(apiLambda);

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

  }
}
