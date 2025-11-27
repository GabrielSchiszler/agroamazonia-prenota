import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
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
    const notifyTask = new tasks.LambdaInvoke(this, 'NotifyReceipt', {
      lambdaFunction: notifyReceiptLambda,
      outputPath: '$.Payload'
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

    const processorTask = new tasks.LambdaInvoke(this, 'ProcessResults', {
      lambdaFunction: processorLambda,
      outputPath: '$.Payload'
    });

    const definition = notifyTask
      .next(mapState)
      .next(processorTask);

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

    // Lambda: API FastAPI
    const apiLambda = new lambda.Function(this, 'ApiFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'main.handler',
      code: lambda.Code.fromAsset('../backend/src', {
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
        STATE_MACHINE_ARN: stateMachine.stateMachineArn,
        BUCKET_NAME: rawDocumentsBucket.bucketName
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 512
    });

    documentTable.grantReadData(apiLambda);
    rawDocumentsBucket.grantRead(apiLambda);
    stateMachine.grantStartExecution(apiLambda);

    // API Gateway
    const api = new apigateway.LambdaRestApi(this, 'DocumentApi', {
      handler: apiLambda,
      restApiName: 'AgroAmazonia Document API',
      proxy: true,
      deployOptions: {
        stageName: 'v1',
        throttlingRateLimit: 100,
        throttlingBurstLimit: 200
      }
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
  }
}
