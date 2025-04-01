/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ecr from "aws-cdk-lib/aws-ecr";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as logs from "aws-cdk-lib/aws-logs";
import * as path from "path";
import { AMZN_GUARD_DUTY_ECR_ARNS_BY_REGION } from "./guardduty_ecr_repo_arns";
import { Result } from "aws-cdk-lib/aws-stepfunctions";

export interface OsmlModelValidationToolStackProps extends cdk.StackProps {
  reportBucketName?: string;
  testImageryBucketName?: string;
  logLevel?: string;
}

export class OsmlModelValidationToolStack extends cdk.Stack {
  public readonly stateMachine: sfn.StateMachine;
  public readonly invokeRequestTopic: sns.Topic;
  public readonly successTopic: sns.Topic;
  public readonly failureTopic: sns.Topic;
  public readonly reportBucket: s3.Bucket;
  public readonly testImageryBucket: s3.Bucket;
  public readonly sageMakerExecutionRole: iam.Role;

  constructor(scope: Construct, id: string, props?: OsmlModelValidationToolStackProps) {
    super(scope, id, props);

    this.reportBucket = new s3.Bucket(this, 'ReportBucket', {
      bucketName: props?.reportBucketName || `${this.account}-${this.region}-validation-reports`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true,
    });

    // Create S3 bucket for test imagery used in OversightML compatibility tests
    this.testImageryBucket = new s3.Bucket(this, 'TestImageryBucket', {
      bucketName: props?.testImageryBucketName || `${this.account}-${this.region}-osml-test-imagery`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true,
    });

    this.sageMakerExecutionRole = new iam.Role(this, 'SageMakerExecutionRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      description: 'Role used by SageMaker for model creation and inference in the validation tool',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess')
      ]
    });

    this.sageMakerExecutionRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
        's3:PutObject',
        's3:ListBucket'
      ],
      resources: [
        this.reportBucket.bucketArn,
        this.reportBucket.arnForObjects('*')
      ]
    }));

    // Create the SNS topic to be used to start state machine execution
    this.invokeRequestTopic = new sns.Topic(this, 'ModelValidationInvokeRequestTopic', {
      displayName: 'Model Validation Invoke Request Topic',
    });

    // Create SNS topics for success/failure notifications
    this.successTopic = new sns.Topic(this, 'ModelValidationSuccessTopic', {
      displayName: 'Model Validation Success Topic',
    });
    this.failureTopic = new sns.Topic(this, 'ModelValidationFailureTopic', {
      displayName: 'Model Validation Failure Topic',
    });

    // Create Lambda functions for validation tasks
    // Note: These lambda's depend on common python code in aws.osml.model_runner_validation_tool.common, which are bundled by
    // scripts/bundle-lambda.js. package.json is configured to run this script `via npm run build:lambda` but if that changes the
    // handlers will fail to import the dependent modules.
    const sageMakerCompatibilityLambda = new lambda.Function(this, 'SageMakerCompatibilityLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'sagemaker_compatibility.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../dist/lambda')),
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      environment: {
        REPORT_BUCKET: this.reportBucket.bucketName,
        SAGEMAKER_EXECUTION_ROLE_ARN: this.sageMakerExecutionRole.roleArn,
        LOG_LEVEL: props?.logLevel || 'INFO',
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    const oversightMLCompatibilityLambda = new lambda.Function(this, 'OversightMLCompatibilityLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'oversight_ml_compatibility.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../dist/lambda')),
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      environment: {
        REPORT_BUCKET: this.reportBucket.bucketName,
        TEST_IMAGERY_BUCKET: this.testImageryBucket.bucketName,
        LOG_LEVEL: props?.logLevel || 'INFO',
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    const compileReportLambda = new lambda.Function(this, 'CompileReportLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'compile_report.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../dist/lambda')),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: {
        REPORT_BUCKET: this.reportBucket.bucketName,
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    const cleanupResourcesLambda = new lambda.Function(this, 'CleanupResourcesLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'cleanup_resources.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../dist/lambda')),
      timeout: cdk.Duration.minutes(10),
      memorySize: 512,
      environment: {
        SAGEMAKER_EXECUTION_ROLE_ARN: this.sageMakerExecutionRole.roleArn,
        LOG_LEVEL: props?.logLevel || 'INFO',
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    this.reportBucket.grantReadWrite(sageMakerCompatibilityLambda);
    this.reportBucket.grantReadWrite(oversightMLCompatibilityLambda);
    this.testImageryBucket.grantRead(oversightMLCompatibilityLambda);
    this.reportBucket.grantReadWrite(compileReportLambda);
    this.reportBucket.grantReadWrite(cleanupResourcesLambda);

    cleanupResourcesLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:ListEndpoints',
        'sagemaker:ListEndpointConfigs',
        'sagemaker:ListModels',
        'sagemaker:DeleteEndpoint',
        'sagemaker:DeleteEndpointConfig',
        'sagemaker:DeleteModel',
        'sagemaker:DescribeEndpoint',
        'sagemaker:DescribeEndpointConfig',
        'sagemaker:DescribeModel',
      ],
      resources: ['*'],
    }));

    sageMakerCompatibilityLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:DescribeModel',
        'sagemaker:InvokeEndpoint',
        'sagemaker:CreateModel',
        'sagemaker:CreateEndpointConfig',
        'sagemaker:CreateEndpoint',
        'sagemaker:GetWaiter',
        'sagemaker:DeleteModel',
        'sagemaker:DeleteEndpointConfig',
        'sagemaker:DeleteEndpoint',
        'sagemaker:DescribeEndpoint',
        'cloudwatch:PutMetricData',
        'iam:PassRole', // TODO: Do I need this?
      ],
      resources: ['*'],
    }));

    oversightMLCompatibilityLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
      ],
      resources: [
        this.testImageryBucket.arnForObjects('*')
      ]
    }));

    oversightMLCompatibilityLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:InvokeEndpoint',
        'cloudwatch:PutMetricData',
      ],
      resources: ['*']}));

    // VPC and ECS Cluster for Fargate tasks
    const vpc = new ec2.Vpc(this, 'ModelValidationVpc', {
      maxAzs: 2,
      natGateways: 1,
    });
    const cluster = new ecs.Cluster(this, 'ModelValidationCluster', {
      vpc: vpc
    });

    const benchmarkingTaskDef = new ecs.FargateTaskDefinition(this, 'BenchmarkingTaskDef', {
      memoryLimitMiB: 4096,
      cpu: 2048,
      executionRole: new iam.Role(this, 'BenchmarkingTaskExecutionRole', {
        assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchLogsFullAccess'),
        ],
      }),
    });

    const benchmarkingEcrRepositoryArn= ecr.Repository.fromRepositoryArn(this,
      "OSMLValidationToolBenchmarking", `arn:aws:ecr:${this.region}:${this.account}:repository/osml-validation-tool/benchmarking`);

    benchmarkingTaskDef.addContainer('BenchmarkingContainer', {
      // TODO: Using docker image already uploaded ECR. It may be possible to auto-build using fromAsset, but would require docker
      //  to be on the host doing CDK build/deploy.
      // for now, you can update manually on a host with docker cli
      // ./aws/osml/model_runner/model_runner_validation_tool/scripts/mr_validation_tool_update_ecs_images.sh [acct] [region] benchmarking
      image: ecs.ContainerImage.fromEcrRepository(benchmarkingEcrRepositoryArn, "latest"),
      // image: ecs.ContainerImage.fromAsset(path.join(__dirname, '../../../docker/aws/osml/model_runner_validation_tool/benchmarking')),
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'benchmarking',
        logRetention: logs.RetentionDays.ONE_WEEK,
      }),
      environment: {
        REPORT_BUCKET: this.reportBucket.bucketName,
      },
    });

    const inferenceRecommenderTaskDef = new ecs.FargateTaskDefinition(this, 'InferenceRecommenderTaskDef', {
      memoryLimitMiB: 4096,
      cpu: 2048,
      executionRole: new iam.Role(this, 'InferenceRecommenderTaskExecutionRole', {
        assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchLogsFullAccess'),
        ],
      }),
    });

    const inferenceRecommenderEcrRepositoryArn= ecr.Repository.fromRepositoryArn(this,
      "OSMLValidationToolInferenceRecommender", `arn:aws:ecr:${this.region}:${this.account}:repository/osml-validation-tool/inference_recommender`);
    inferenceRecommenderTaskDef.addContainer('InferenceRecommenderContainer', {
      // TODO: See above comment in benchmarkingTaskDef
      // Update docker image manually on a host with docker cli
      // ./aws/osml/model_runner/model_runner_validation_tool/scripts/mr_validation_tool_update_ecs_images.sh [acct] [region] inference_recommender
      image: ecs.ContainerImage.fromEcrRepository(inferenceRecommenderEcrRepositoryArn, "latest"),
      // image: ecs.ContainerImage.fromAsset(path.join(__dirname, '../../../docker/aws/osml/model_runner_validation_tool/inference_recommender')),
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'inference-recommender',
        logRetention: logs.RetentionDays.ONE_WEEK,
      }),
      environment: {
        REPORT_BUCKET: this.reportBucket.bucketName,
      },
    });

    benchmarkingTaskDef.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        's3:PutObject',
        's3:GetObject',
        'sagemaker:CreateEndpointConfig',
        'sagemaker:CreateEndpoint',
        'sagemaker:DeleteEndpointConfig',
        'sagemaker:DeleteEndpoint',
        'sagemaker:DescribeEndpointConfig',
        'sagemaker:DescribeEndpoint',
        'sagemaker:InvokeEndpoint',
        'sagemaker:ListEndpoints',
        'sagemaker:ListEndpointConfigs',
        'sagemaker:UpdateEndpoint',
        'sagemaker:UpdateEndpointWeightsAndCapacities',
        'cloudwatch:PutMetricData',
      ],
      resources: ['*'],
    }));

    // Permissions to download the GuardDuty security agent container image
    benchmarkingTaskDef.executionRole?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetAuthorizationToken'
      ],
      resources: ['*']  // GetAuthorizationToken requires * resource
    }));

    benchmarkingTaskDef.executionRole?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetDownloadUrlForLayer',
        'ecr:BatchGetImage',
        'ecr:BatchCheckLayerAvailability'
      ],
      resources: [
        AMZN_GUARD_DUTY_ECR_ARNS_BY_REGION.get(this.region)!
      ]
    }));

    inferenceRecommenderTaskDef.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:CreateInferenceRecommendationsJob',
        'sagemaker:DescribeInferenceRecommendationsJob',
        'sagemaker:StopInferenceRecommendationsJob',
        's3:PutObject',
        's3:GetObject',
      ],
      resources: ['*'],
    }));

    // Permissions to download the GuardDuty security agent container image
    inferenceRecommenderTaskDef.executionRole?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetAuthorizationToken'
      ],
      resources: ['*']  // GetAuthorizationToken requires * resource
    }));

    inferenceRecommenderTaskDef.executionRole?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetDownloadUrlForLayer',
        'ecr:BatchGetImage',
        'ecr:BatchCheckLayerAvailability'
      ],
      resources: [
        AMZN_GUARD_DUTY_ECR_ARNS_BY_REGION.get(this.region)!
      ]
    }));

    const runSageMakerCompatibilityTest = new tasks.LambdaInvoke(this, 'RunSageMakerCompatibilityTest', {
      lambdaFunction: sageMakerCompatibilityLambda,
      resultPath: '$.sageMakerCompatibilityResults',
    });

    const runOversightMLCompatibilityTest = new tasks.LambdaInvoke(this, 'RunOversightMLCompatibilityTest', {
      lambdaFunction: oversightMLCompatibilityLambda,
      resultPath: '$.oversightMLCompatibilityResults',
    });

    // Factory function to create Benchmarking Task - unique name is necessary since it can be used more than once based on test parameters
    const createBenchmarkingTask = () => new tasks.EcsRunTask(this, `RunBenchmarkingTask-${Math.random().toString(36).substring(2, 8)}`, {
      cluster,
      taskDefinition: benchmarkingTaskDef,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      containerOverrides: [{
        containerDefinition: benchmarkingTaskDef.findContainer('BenchmarkingContainer')!,
        environment: [
          {
            'name': 'ECS_IMAGE_URI',
            'value': sfn.JsonPath.stringAt('$.modelInfo.ecsImageUri')
          },
          {
            'name': 'S3_MODEL_DATA_URI',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.s3ModelDataUri')
          },
          {
            'name': 'MODEL_NAME',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.modelName')
          },
          {
            'name': 'EXISTING_ENDPOINT_NAME',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.existingEndpointName')
          }
        ]
      }],
    });

    // Factory function to create Inference Recommender Task - unique name is necessary since it can be used more than once based on test parameters
    const createInferenceRecommenderTask = () => new tasks.EcsRunTask(this, `RunInferenceRecommenderTask-${Math.random().toString(36).substring(2, 8)}`, {
      cluster,
      taskDefinition: inferenceRecommenderTaskDef,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      containerOverrides: [{
        containerDefinition: inferenceRecommenderTaskDef.findContainer('InferenceRecommenderContainer')!,
        environment: [
          {
            'name': 'ECS_IMAGE_URI',
            'value': sfn.JsonPath.stringAt('$.modelInfo.ecsImageUri')
          },
          {
            'name': 'S3_MODEL_DATA_URI',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.s3ModelDataUri')
          },
          {
            'name': 'MODEL_NAME',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.modelName')
          },
          {
            'name': 'EXISTING_ENDPOINT_NAME',
            'value': sfn.JsonPath.stringAt('$.sageMakerCompatibilityResults.Payload.sageMakerCompatibilityResults.existingEndpointName')
          }
        ]
      }],
    });

    const compileFinalReport = new tasks.LambdaInvoke(this, 'CompileFinalReport', {
      lambdaFunction: compileReportLambda,
      resultPath: '$.validationTestReport',
    });

    const cleanupResources = new tasks.LambdaInvoke(this, 'CleanupResources', {
      lambdaFunction: cleanupResourcesLambda,
      resultPath: '$.cleanupResults',
    });

    const sendSuccessNotification = new tasks.SnsPublish(this, 'SendSuccessNotification', {
      topic: this.successTopic,
      message: sfn.TaskInput.fromObject({
        message: 'Model validation completed successfully',
        details: sfn.JsonPath.stringAt('$'),
      }),
    });

    // TODO: This isn't used yet
    const sendFailureNotification = new tasks.SnsPublish(this, 'SendFailureNotification', {
      topic: this.failureTopic,
      message: sfn.TaskInput.fromObject({
        message: 'Model validation failed',
        error: sfn.JsonPath.stringAt('$.Error'),
        cause: sfn.JsonPath.stringAt('$.Cause'),
      }),
    });

    // Define workflow logic

    const joinAfterOversightCheck = new sfn.Pass(this, 'JoinAfterOversightCheck');

    const checkOversightMLCompatibility = new sfn.Choice(this, 'CheckOversightMLCompatibility')
      .when(sfn.Condition.booleanEquals('$.runOversightMLCompatibilityTest', true),
        runOversightMLCompatibilityTest)
      .otherwise(new sfn.Pass(this, 'SkipOversightMLCompatibilityTest', {
        result: Result.fromString('OSML Compatibility Test Skipped'),
        resultPath: '$.oversightMLCompatibilityResults',
      }))
      .afterwards()
      .next(joinAfterOversightCheck)

    // Create a parallel state that can run both tasks if needed
    const runParallelEcsTasks = new sfn.Parallel(this, 'RunParallelEcsTasks', {
      resultPath: '$.parallelResults'
    })
      .branch(createBenchmarkingTask())
      .branch(createInferenceRecommenderTask());

    // Create individual task states
    const runBenchmarkingOnly = new sfn.Parallel(this, 'RunBenchmarkingOnly', {
      resultPath: '$.parallelResults'
    })
      .branch(createBenchmarkingTask());

    const runInferenceRecommenderOnly = new sfn.Parallel(this, 'RunInferenceRecommenderOnly', {
      resultPath: '$.parallelResults'
    })
      .branch(createInferenceRecommenderTask());

    const reshapeParallelOutput = new sfn.Pass(this, 'ReshapeParallelOutput', {
      parameters: {
        'modelInfo.$': '$.modelInfo',
        'runBenchmarkingTask.$': '$.runBenchmarkingTask',
        'runInferenceRecommender.$': '$.runInferenceRecommender',
        'runOversightMLCompatibilityTest.$': '$.runOversightMLCompatibilityTest',

        'sageMakerCompatibilityResults.$': '$.sageMakerCompatibilityResults',
        'oversightMLCompatibilityResults.$': '$.oversightMLCompatibilityResults',

        'benchmarkingResults.$': '$.parallelResults[0]',
        'inferenceRecommenderResults.$': '$.parallelResults[1]'
      }
    });

    const reshapeBenchmarkingOutput = new sfn.Pass(this, 'ReshapeBenchmarkingOutput', {
      parameters: {
        'modelInfo.$': '$.modelInfo',
        'runBenchmarkingTask.$': '$.runBenchmarkingTask',
        'runInferenceRecommender.$': '$.runInferenceRecommender',
        'runOversightMLCompatibilityTest.$': '$.runOversightMLCompatibilityTest',

        'sageMakerCompatibilityResults.$': '$.sageMakerCompatibilityResults',
        'oversightMLCompatibilityResults.$': '$.oversightMLCompatibilityResults',

        'benchmarkingResults.$': '$.parallelResults[0]',
        'inferenceRecommenderResults.$': null
      }
    });

    const reshapeInferenceRecommenderOutput = new sfn.Pass(this, 'ReshapeInferenceRecommenderOutput', {
      parameters: {
        'modelInfo.$': '$.modelInfo',
        'runBenchmarkingTask.$': '$.runBenchmarkingTask',
        'runInferenceRecommender.$': '$.runInferenceRecommender',
        'runOversightMLCompatibilityTest.$': '$.runOversightMLCompatibilityTest',

        'sageMakerCompatibilityResults.$': '$.sageMakerCompatibilityResults',
        'oversightMLCompatibilityResults.$': '$.oversightMLCompatibilityResults',

        'inferenceRecommenderResults.$': '$.parallelResults[0]',
        'benchmarkingResults.$': null
      }
    });

    runParallelEcsTasks.next(reshapeParallelOutput);
    runBenchmarkingOnly.next(reshapeBenchmarkingOutput);
    runInferenceRecommenderOnly.next(reshapeInferenceRecommenderOutput);

    const joinAfterEcsTasks = new sfn.Pass(this, 'JoinAfterEcsTasks', {
      outputPath: '$'
    });

    reshapeParallelOutput.next(joinAfterEcsTasks);
    reshapeBenchmarkingOutput.next(joinAfterEcsTasks);
    reshapeInferenceRecommenderOutput.next(joinAfterEcsTasks);

    joinAfterEcsTasks.next(compileFinalReport);

    const skipAllEcsTasks = new sfn.Pass(this, 'SkipAllEcsTasks', {
      parameters: {
        'modelInfo.$': '$.modelInfo',
        'runBenchmarkingTask.$': '$.runBenchmarkingTask',
        'runInferenceRecommender.$': '$.runInferenceRecommender',
        'runOversightMLCompatibilityTest.$': '$.runOversightMLCompatibilityTest',

        'sageMakerCompatibilityResults.$': '$.sageMakerCompatibilityResults',
        'oversightMLCompatibilityResults.$': '$.oversightMLCompatibilityResults',

        'inferenceRecommenderResults.$': null,
        'benchmarkingResults.$': null
      }
    })
      .next(joinAfterEcsTasks);

    // Create a choice state with all possible combinations
    const checkParallelEcsTasks = new sfn.Choice(this, 'CheckParallelEcsTasks')
      // Both tasks
      .when(
        sfn.Condition.and(
          sfn.Condition.booleanEquals('$.runBenchmarkingTask', true),
          sfn.Condition.booleanEquals('$.runInferenceRecommender', true)
        ),
        runParallelEcsTasks
      )
      // Only benchmarking
      .when(
        sfn.Condition.and(
          sfn.Condition.booleanEquals('$.runBenchmarkingTask', true),
          sfn.Condition.booleanEquals('$.runInferenceRecommender', false)
        ),
        runBenchmarkingOnly
      )
      // Only inference recommender
      .when(
        sfn.Condition.and(
          sfn.Condition.booleanEquals('$.runBenchmarkingTask', false),
          sfn.Condition.booleanEquals('$.runInferenceRecommender', true)
        ),
        runInferenceRecommenderOnly
      )
      // Neither task
      .otherwise(skipAllEcsTasks);

    // Connect the remaining workflow steps
    compileFinalReport.next(cleanupResources);
    cleanupResources.next(sendSuccessNotification);

    const definition = runSageMakerCompatibilityTest
      .next(checkOversightMLCompatibility)
      .next(checkParallelEcsTasks);

    const stateMachineLogGroup = new logs.LogGroup(this, 'ModelValidationStateMachineLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
    });

    this.stateMachine = new sfn.StateMachine(this, 'ModelValidationWorkflow', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: cdk.Duration.hours(24),
      tracingEnabled: true,
      stateMachineType: sfn.StateMachineType.STANDARD,
      logs: {
        destination: stateMachineLogGroup,
        level: sfn.LogLevel.ALL,
      },
    });

    this.stateMachine.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sns:Publish'],
      resources: [this.failureTopic.topicArn],
    }));

    // Create a Lambda function to process SNS messages and start Step Functions workflow
    const workflowTriggerLambda = new lambda.Function(this, 'WorkflowTriggerLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'workflow_trigger.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../dist/lambda')),
      timeout: cdk.Duration.minutes(1),
      memorySize: 128,
      environment: {
        STATE_MACHINE_ARN: this.stateMachine.stateMachineArn,
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    workflowTriggerLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['states:StartExecution'],
      resources: [this.stateMachine.stateMachineArn],
    }));

    this.invokeRequestTopic.addSubscription(
      new subscriptions.LambdaSubscription(workflowTriggerLambda)
    );

    // Publish important resources in the CloudFormation stack
    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: this.stateMachine.stateMachineArn,
      description: 'ARN of the Model Validation State Machine',
    });

    new cdk.CfnOutput(this, 'InvokeRequestTopicArn', {
      value: this.invokeRequestTopic.topicArn,
      description: 'ARN of the Model Validation Invoke Request Topic',
    });

    new cdk.CfnOutput(this, 'SuccessTopicArn', {
      value: this.successTopic.topicArn,
      description: 'ARN of the Model Validation Success Topic',
    });

    new cdk.CfnOutput(this, 'FailureTopicArn', {
      value: this.failureTopic.topicArn,
      description: 'ARN of the Model Validation Failure Topic',
    });

    new cdk.CfnOutput(this, 'ReportBucketName', {
      value: this.reportBucket.bucketName,
      description: 'Name of the Report S3 Bucket',
    });

    new cdk.CfnOutput(this, 'TestImageryBucketName', {
      value: this.testImageryBucket.bucketName,
      description: 'Name of the Test Imagery S3 Bucket for OversightML compatibility tests',
    });

    const testImagesPath = path.join(__dirname, '../assets/test_images');
    new s3deploy.BucketDeployment(this, 'DeployTestImages', {
      sources: [s3deploy.Source.asset(testImagesPath)],
      destinationBucket: this.testImageryBucket,
      retainOnDelete: true,
      prune: false,
    });

    new cdk.CfnOutput(this, 'SageMakerExecutionRoleArn', {
      value: this.sageMakerExecutionRole.roleArn,
      description: 'ARN of the SageMaker execution role for model creation and inference',
    });
  }
}
