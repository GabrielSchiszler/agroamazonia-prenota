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
import { Construct } from 'constructs';

export class AgroAmazoniaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Bucket para documentos brutos
    const rawDocumentsBucket = new s3.Bucket(this, 'RawDocumentsBucket', {
      bucketName: `agroamazonia-raw-documents-${this.account}`,
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
      topicName: 'agroamazonia-lambda-errors',
      displayName: 'AgroAmazonia Lambda Errors'
    });

    // DynamoDB Table
    const documentTable = new dynamodb.Table(this, 'DocumentProcessorTable', {
      tableName: 'DocumentProcessorTable',
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN
    });



    // Lambda: Notificação de Recebimento
    const notifyReceiptLambda = new lambda.Function(this, 'NotifyReceiptFunction', {
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
    const ocrFailureApiUrl = this.node.tryGetContext('ocrFailureApiUrl') || process.env.OCR_FAILURE_API_URL || 'https://agroamazoniad.service-now.com/api/x_aapas_fast_ocr/ocr/reportar-falha';
    const ocrFailureAuthUrl = this.node.tryGetContext('ocrFailureAuthUrl') || process.env.OCR_FAILURE_AUTH_URL || 'https://agroamazoniad.service-now.com/oauth_token.do';
    const ocrFailureClientId = this.node.tryGetContext('ocrFailureClientId') || process.env.OCR_FAILURE_CLIENT_ID || '';
    const ocrFailureClientSecret = this.node.tryGetContext('ocrFailureClientSecret') || process.env.OCR_FAILURE_CLIENT_SECRET || '';
    const ocrFailureUsername = this.node.tryGetContext('ocrFailureUsername') || process.env.OCR_FAILURE_USERNAME || '';
    const ocrFailurePassword = this.node.tryGetContext('ocrFailurePassword') || process.env.OCR_FAILURE_PASSWORD || '';

    const reportOcrFailureLambda = new lambda.Function(this, 'ReportOcrFailureFunction', {
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
    const sendToProtheusLambda = new lambda.Function(this, 'SendToProtheusFunction', {
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
        PROTHEUS_API_URL: 'https://virtserver.swaggerhub.com/agroamazonia/fast-ocr/1.0.0/documentos-entrada'
      },
      timeout: cdk.Duration.minutes(2),
      memorySize: 512
    });

    documentTable.grantReadWriteData(sendToProtheusLambda);

    // Lambda: Update Metrics
    const updateMetricsLambda = new lambda.Function(this, 'UpdateMetricsFunction', {
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

    // Lambda: Parse OCR
    const parseOcrLambda = new lambda.Function(this, 'ParseOcrFunction', {
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

    const checkTextractTask = new tasks.LambdaInvoke(this, 'CheckTextract', {
      lambdaFunction: checkTextractLambda,
      resultPath: '$.textract_check'
    });

    // Textract Start Job
    const startTextract = new tasks.CallAwsService(this, 'StartTextractJob', {
      service: 'textract',
      action: 'startDocumentAnalysis',
      parameters: {
        DocumentLocation: {
          S3Object: {
            Bucket: rawDocumentsBucket.bucketName,
            Name: sfn.JsonPath.stringAt('$.FILE_KEY')
          }
        },
        FeatureTypes: ['TABLES']
      },
      iamResources: ['*'],
      resultPath: '$.textract_job'
    });

    // Get and Parse Textract Results
    const getTextract = new tasks.LambdaInvoke(this, 'GetTextractResults', {
      lambdaFunction: getTextractLambda,
      outputPath: '$.Payload'
    });

    const textractFlow = startTextract.next(getTextract);

    const mapState = new sfn.Map(this, 'ProcessFiles', {
      maxConcurrency: 5,
      itemsPath: '$.files',
      resultPath: '$.textract_results'
    });

    mapState.iterator(textractFlow);

    const skipTextract = new sfn.Pass(this, 'SkipTextract', {
      resultPath: '$.textract_results',
      result: sfn.Result.fromArray([])
    });

    const checkChoice = new sfn.Choice(this, 'NeedsTextract?')
      .when(sfn.Condition.booleanEquals('$.textract_check.Payload.needs_textract', true), mapState)
      .otherwise(skipTextract);

    const processorTask = new tasks.LambdaInvoke(this, 'ProcessResults', {
      lambdaFunction: processorLambda,
      resultPath: '$.processor_result'
    });

    const parseXmlTask = new tasks.LambdaInvoke(this, 'ParseXml', {
      lambdaFunction: parseXmlLambda,
      resultPath: '$.xml_result'
    });

    const parseOcrTask = new tasks.LambdaInvoke(this, 'ParseOcr', {
      lambdaFunction: parseOcrLambda,
      resultPath: '$.ocr_result'
    });

    const parallelParsing = new sfn.Parallel(this, 'ParallelParsing', {
      resultPath: '$.parsing_results'
    });

    parallelParsing.branch(parseXmlTask);
    parallelParsing.branch(parseOcrTask);

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

    const notifyErrorTask = new tasks.SnsPublish(this, 'NotifyError', {
      topic: errorTopic,
      subject: 'Lambda Error - AgroAmazonia',
      message: sfn.TaskInput.fromObject({
        'error': sfn.JsonPath.stringAt('$.error.Error'),
        'cause': sfn.JsonPath.stringAt('$.error.Cause'),
        'process_id': sfn.JsonPath.stringAt('$.process_id'),
        'timestamp': sfn.JsonPath.stringAt('$$.State.EnteredTime'),
        'state_name': sfn.JsonPath.stringAt('$$.State.Name')
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
    
    // Catch para capturar falhas e enviar para SNS
    updateMetricsTask.addCatch(notifyErrorTask, { resultPath: '$.error' });
    notifyErrorTask.next(failureState);
    
    // Adicionar catch em todos os tasks principais para notificar erros
    notifyTask.addCatch(notifyErrorTask, { resultPath: '$.error' });
    checkTextractTask.addCatch(notifyErrorTask, { resultPath: '$.error' });
    processorTask.addCatch(notifyErrorTask, { resultPath: '$.error' });
    parallelParsing.addCatch(notifyErrorTask, { resultPath: '$.error' });
    validateTask.addCatch(notifyErrorTask, { resultPath: '$.error' });
    sendToProtheusTask.addCatch(notifyErrorTask, { resultPath: '$.error' });

    mapState.next(processorTask);
    skipTextract.next(processorTask);
    
    processorTask
      .next(parallelParsing)
      .next(validateTask)
      .next(validationChoice);

    const definition = notifyTask
      .next(checkTextractTask)
      .next(checkChoice);



    const stateMachine = new sfn.StateMachine(this, 'DocumentProcessorStateMachine', {
      stateMachineName: 'DocumentProcessorWorkflow',
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

    // Secrets Manager for API Keys
    const apiKeysSecret = new secretsmanager.Secret(this, 'ApiKeysSecret', {
      secretName: 'agroamazonia/api-keys',
      description: 'API Keys for AgroAmazonia clients',
      generateSecretString: {
        secretStringTemplate: JSON.stringify({}),
        generateStringKey: 'placeholder'
      }
    });

    // Lambda: API FastAPI
    const apiLambda = new lambda.Function(this, 'ApiFunction', {
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
      restApiName: 'AgroAmazonia Document API',
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

    // Outputs
    new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
      description: 'API Gateway URL'
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
