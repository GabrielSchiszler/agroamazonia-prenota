import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';

interface AgroAmazoniaStackProps extends cdk.StackProps {
  environment: string;
}

export class AgroAmazoniaStack extends cdk.Stack {
  private readonly envName: string;
  public readonly apiUrl: string; // Expor URL da API para outras stacks

  constructor(scope: Construct, id: string, props: AgroAmazoniaStackProps) {
    super(scope, id, props);
    this.envName = props.environment || 'dev';

    // Aplicar tags padrão em todos os recursos da stack
    cdk.Tags.of(this).add('Application', 'Agroamazonia-Prenota');
    cdk.Tags.of(this).add('Name', 'Agroamazonia');

    // Helper function para padronizar nomes
    const name = (resourceType: string, resourceName: string): string => {
      const normalizedName = resourceName.toLowerCase().replace(/_/g, '-');
      return `${resourceType}-${normalizedName}-${this.envName}`;
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

    // SNS Topic para erros de Lambda
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
      timeout: cdk.Duration.seconds(30)
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
      memorySize: 256
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
      memorySize: 512
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
      memorySize: 256
    });

    documentTable.grantReadWriteData(reportOcrFailureLambda);

    // Lambda: Send to Protheus
    // OAuth2 credentials from environment variables or CDK context
    const protheusApiUrl = this.node.tryGetContext('protheusApiUrl') || process.env.PROTHEUS_API_URL || 'https://api.agroamazonia.com/hom-ocr';
    const protheusAuthUrl = this.node.tryGetContext('protheusAuthUrl') || process.env.PROTHEUS_AUTH_URL || '';
    const protheusClientId = this.node.tryGetContext('protheusClientId') || process.env.PROTHEUS_CLIENT_ID || '';
    const protheusClientSecret = this.node.tryGetContext('protheusClientSecret') || process.env.PROTHEUS_CLIENT_SECRET || '';

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
        PROTHEUS_API_URL: protheusApiUrl,
        PROTHEUS_AUTH_URL: protheusAuthUrl,
        PROTHEUS_CLIENT_ID: protheusClientId,
        PROTHEUS_CLIENT_SECRET: protheusClientSecret,
        // Variáveis para reportar falhas do Protheus para SCTASK
        OCR_FAILURE_API_URL: ocrFailureApiUrl,
        OCR_FAILURE_AUTH_URL: ocrFailureAuthUrl,
        OCR_FAILURE_CLIENT_ID: ocrFailureClientId,
        OCR_FAILURE_CLIENT_SECRET: ocrFailureClientSecret,
        OCR_FAILURE_USERNAME: ocrFailureUsername,
        OCR_FAILURE_PASSWORD: ocrFailurePassword
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 512
    });

    documentTable.grantReadWriteData(sendToProtheusLambda);
    sendToProtheusLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

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
      memorySize: 256
    });

    documentTable.grantReadWriteData(updateMetricsLambda);

    // Lambda: Check Textract
    const checkTextractLambda = new lambda.Function(this, 'CheckTextractFunction', {
      functionName: name('lambda', 'check-textract'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/check_textract'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30)
    });

    documentTable.grantReadData(checkTextractLambda);

    // Lambda: Get Textract Results
    const getTextractLambda = new lambda.Function(this, 'GetTextractFunction', {
      functionName: name('lambda', 'get-textract'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/get_textract'),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512
    });

    getTextractLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:GetDocumentAnalysis'],
      resources: ['*']
    }));

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
      memorySize: 256
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
      memorySize: 128
    });

    documentTable.grantReadWriteData(updateProcessStatusLambda);

    // Lambda: Parse OCR
    const parseOcrLambda = new lambda.Function(this, 'ParseOcrFunction', {
      functionName: name('lambda', 'parse-ocr'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/parse_ocr'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.minutes(5),
      memorySize: 512
    });

    documentTable.grantReadWriteData(parseOcrLambda);
    parseOcrLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

    // Lambda: S3 Upload Handler
    const s3UploadHandler = new lambda.Function(this, 'S3UploadHandler', {
      functionName: name('lambda', 's3-upload-handler'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('../backend/lambdas/s3_upload_handler'),
      environment: {
        TABLE_NAME: documentTable.tableName
      },
      timeout: cdk.Duration.seconds(30)
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
    // Nota: start_time é salvo no DynamoDB pelo notifyTask, não precisa passar entre estados
    const notifyTask = new tasks.LambdaInvoke(this, 'NotifyReceipt', {
      lambdaFunction: notifyReceiptLambda,
      resultPath: '$.notify_result'
    });

    const parseXmlTask = new tasks.LambdaInvoke(this, 'ParseXml', {
      lambdaFunction: parseXmlLambda,
      resultPath: '$.xml_result'
    });

    const validateTask = new tasks.LambdaInvoke(this, 'ValidateRules', {
      lambdaFunction: validateRulesLambda,
      resultPath: '$.validation_result'
    });

    const reportFailureTask = new tasks.LambdaInvoke(this, 'ReportOcrFailure', {
      lambdaFunction: reportOcrFailureLambda,
      resultPath: '$.failure_result'
    });

    const sendToProtheusTask = new tasks.LambdaInvoke(this, 'SendToProtheus', {
      lambdaFunction: sendToProtheusLambda,
      resultPath: '$.protheus_result'
    });

    // Preparar dados para UpdateMetrics quando há sucesso (protheus_result)
    // start_time será buscado do DynamoDB pelo Lambda, não precisa passar
    const prepareMetricsDataSuccess = new sfn.Pass(this, 'PrepareMetricsDataSuccess', {
      parameters: {
        'process_id.$': '$.process_id',
        'status': 'COMPLETED',
        'protheus_response.$': '$.protheus_result.Payload',
        'failure_result': {}
      },
      resultPath: '$.metrics_input'
    });

    // Preparar dados para UpdateMetrics quando há falha (failure_result)
    // start_time será buscado do DynamoDB pelo Lambda, não precisa passar
    const prepareMetricsDataFailure = new sfn.Pass(this, 'PrepareMetricsDataFailure', {
      parameters: {
        'process_id.$': '$.process_id',
        'status': 'FAILED',
        'protheus_response': {},
        'failure_result.$': '$.failure_result.Payload'
      },
      resultPath: '$.metrics_input'
    });

    // Usar fromJsonPathAt para passar todo o objeto metrics_input diretamente
    // Isso evita ter que acessar campos individuais que podem não existir
    const updateMetricsTask = new tasks.LambdaInvoke(this, 'UpdateMetrics', {
      lambdaFunction: updateMetricsLambda,
      payload: sfn.TaskInput.fromJsonPathAt('$.metrics_input'),
      outputPath: '$.Payload'
    });

    // Lambda task para atualizar status para FAILED antes de notificar erro
    const updateStatusBeforeErrorTask = new tasks.LambdaInvoke(this, 'UpdateStatusBeforeError', {
      lambdaFunction: updateProcessStatusLambda,
      payload: sfn.TaskInput.fromObject({
        'process_id.$': '$.process_id',
        'error': sfn.TaskInput.fromJsonPathAt('$.error'),
        'error_type': 'LAMBDA_ERROR',
        'lambda_name.$': '$$.State.Name',
        'state_name.$': '$$.State.Name'
      }),
      resultPath: '$.status_update'
    });

    const notifyErrorTask = new tasks.SnsPublish(this, 'NotifyError', {
      topic: errorTopic,
      subject: sfn.JsonPath.format('Erro no Processamento - AgroAmazonia - {}', sfn.JsonPath.stringAt('$$.State.Name')),
      message: sfn.TaskInput.fromObject({
        'application': 'AgroAmazonia-Prenota',
        'error': sfn.JsonPath.stringAt('$.error.Error'),
        'cause': sfn.JsonPath.stringAt('$.error.Cause'),
        'process_id': sfn.JsonPath.stringAt('$.process_id'),
        'timestamp': sfn.JsonPath.stringAt('$$.State.EnteredTime'),
        'state_name': sfn.JsonPath.stringAt('$$.State.Name'),
        'error_type': 'LAMBDA_ERROR',
        'environment': this.envName
      })
    });

    const successState = new sfn.Succeed(this, 'ProcessSuccess');
    const failureState = new sfn.Fail(this, 'ProcessFailed');

    // Choice após validação
    const validationChoice = new sfn.Choice(this, 'HasValidationFailures?')
      .when(sfn.Condition.stringEquals('$.validation_result.Payload.validation_status', 'FAILED'), reportFailureTask)
      .otherwise(sendToProtheusTask);

    sendToProtheusTask.next(prepareMetricsDataSuccess);
    reportFailureTask.next(prepareMetricsDataFailure);
    prepareMetricsDataSuccess.next(updateMetricsTask);
    prepareMetricsDataFailure.next(updateMetricsTask);
    updateMetricsTask.next(successState);
    
    // Conectar atualização de status antes de notificar erro (global para todos os erros)
    updateStatusBeforeErrorTask.next(notifyErrorTask);
    notifyErrorTask.next(failureState);
    
    // Catch para capturar falhas: primeiro atualiza status, depois envia SNS (global)
    updateMetricsTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    notifyTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    parseXmlTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    validateTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    sendToProtheusTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });
    reportFailureTask.addCatch(updateStatusBeforeErrorTask, { resultPath: '$.error' });

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

    // API Key padrão para o frontend
    // Esta é a mesma chave usada no frontend-stack.ts
    const defaultApiKey = 'agroamazonia_key_UPXsb8Hb8sjbxWBQqouzYnTL5w-V_dJx';
    
    // Secrets Manager for API Keys
    const apiKeysSecret = new secretsmanager.Secret(this, 'ApiKeysSecret', {
      secretName: name('secret', 'agroamazonia-api-keys'),
      description: `API Keys for AgroAmazonia clients - ${this.envName}`,
      generateSecretString: {
        secretStringTemplate: JSON.stringify({}),
        generateStringKey: 'placeholder'
      }
    });
    
    // Custom Resource Lambda para garantir que a API key padrão esteja sempre presente
    // Isso faz merge com chaves existentes, não sobrescreve
    const ensureApiKeyLambda = new lambda.Function(this, 'EnsureApiKeyFunction', {
      functionName: name('lambda', 'ensure-api-key'),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
import json
import boto3
from datetime import datetime

def handler(event, context):
    secrets_client = boto3.client('secretsmanager')
    # O Provider do CDK passa as propriedades em event['ResourceProperties']
    props = event.get('ResourceProperties', event)
    secret_arn = props['SecretArn']
    api_key = props['ApiKey']
    
    # Obter secret existente
    try:
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        current_keys = json.loads(response['SecretString'])
    except secrets_client.exceptions.ResourceNotFoundException:
        current_keys = {}
    
    # Adicionar ou atualizar a API key padrão
    if api_key not in current_keys:
        current_keys[api_key] = {
            'status': 'active',
            'client_name': 'frontend',
            'created_at': datetime.utcnow().isoformat() + 'Z'
        }
    else:
        current_keys[api_key]['status'] = 'active'
        current_keys[api_key]['client_name'] = 'frontend'
        current_keys[api_key]['updated_at'] = datetime.utcnow().isoformat() + 'Z'
    
    # Atualizar o secret
    secrets_client.update_secret(
        SecretId=secret_arn,
        SecretString=json.dumps(current_keys, indent=2)
    )
    
    return {
        'Message': f'API key {api_key[:20]}... ensured',
        'TotalKeys': len(current_keys)
    }
`),
      timeout: cdk.Duration.seconds(30)
    });
    
    // Permissão para a Lambda acessar o Secrets Manager
    apiKeysSecret.grantRead(ensureApiKeyLambda);
    apiKeysSecret.grantWrite(ensureApiKeyLambda);
    
    // Custom Resource para garantir que a API key esteja presente
    const ensureApiKeyProvider = new cr.Provider(this, 'EnsureApiKeyProvider', {
      onEventHandler: ensureApiKeyLambda
    });
    
    new cdk.CustomResource(this, 'EnsureApiKeyResource', {
      serviceToken: ensureApiKeyProvider.serviceToken,
      properties: {
        SecretArn: apiKeysSecret.secretArn,
        ApiKey: defaultApiKey
      }
    });

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
        BUCKET_NAME: rawDocumentsBucket.bucketName,
        API_KEYS_SECRET_ARN: apiKeysSecret.secretArn
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 512
    });

    documentTable.grantReadWriteData(apiLambda);
    rawDocumentsBucket.grantReadWrite(apiLambda);
    stateMachine.grantStartExecution(apiLambda);
    apiKeysSecret.grantRead(apiLambda);

    // API Gateway
    const api = new apigateway.RestApi(this, 'DocumentApi', {
      restApiName: name('api', 'agroamazonia-document-api'),
      deployOptions: {
        stageName: 'v1',
        throttlingRateLimit: 100,
        throttlingBurstLimit: 200
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ['Content-Type', 'x-api-key', 'Authorization']
      }
    });

    // Lambda integration
    const lambdaIntegration = new apigateway.LambdaIntegration(apiLambda);

    // Root method
    api.root.addMethod('ANY', lambdaIntegration);
    
    // Catch-all proxy
    api.root.addProxy({
      defaultIntegration: lambdaIntegration,
      anyMethod: true
    });

    // Expor URL da API como propriedade pública para outras stacks
    this.apiUrl = api.url;

    // Outputs
    const apiUrlOutput = new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
      description: 'API Gateway URL',
      exportName: `agroamazonia-backend-${this.envName}-ApiUrl` // Export para uso em outras stacks
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

    new cdk.CfnOutput(this, 'ApiKeysSecretArn', {
      value: apiKeysSecret.secretArn,
      description: 'Secrets Manager ARN for API Keys'
    });

    new cdk.CfnOutput(this, 'ErrorTopicArn', {
      value: errorTopic.topicArn,
      description: 'SNS Topic ARN for Lambda Errors'
    });
  }
}
