/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { region_info } from "aws-cdk-lib";
import {
  CompositePrincipal,
  Effect,
  IRole,
  ManagedPolicy,
  PolicyStatement,
  Role,
  ServicePrincipal
} from "aws-cdk-lib/aws-iam";
import { NagSuppressions } from "cdk-nag";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";

/**
 * Properties for creating ECS roles.
 */
export interface ECSRolesProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The name for the task role. */
  readonly taskRoleName: string;
  /** The name for the execution role. */
  readonly executionRoleName: string;
  /** Optional existing task role to use instead of creating one. */
  readonly existingTaskRole?: IRole;
  /** Optional existing execution role to use instead of creating one. */
  readonly existingExecutionRole?: IRole;
  /** Optional specific SQS queue ARNs to restrict permissions to. */
  readonly sqsQueueArns?: string[];
  /** Optional specific SNS topic ARNs to restrict permissions to. */
  readonly snsTopicArns?: string[];
  /** Optional specific S3 bucket ARNs to restrict permissions to. */
  readonly s3BucketArns?: string[];
  /** Optional specific DynamoDB table ARNs to restrict permissions to. */
  readonly dynamoTableArns?: string[];
  /** Optional specific ECS service/cluster ARNs to restrict permissions to. */
  readonly ecsResourceArns?: string[];
}

/**
 * Construct that manages both ECS task and execution roles.
 *
 * This construct encapsulates the creation and configuration of both the ECS
 * task role and execution role required by the Model Runner, providing a
 * unified interface for role management.
 */
export class ECSRoles extends Construct {
  /** The ECS task role. */
  public readonly taskRole: IRole;

  /** The ECS execution role. */
  public readonly executionRole: IRole;

  /** The AWS partition in which the roles will operate. */
  public readonly partition: string;

  /**
   * Creates a new ECSRoles construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: ECSRolesProps) {
    super(scope, id);

    this.partition = region_info.Fact.find(
      props.account.region,
      region_info.FactName.PARTITION
    )!;

    // Create or use existing task role
    this.taskRole = props.existingTaskRole || this.createTaskRole(props);

    // Create or use existing execution role
    this.executionRole =
      props.existingExecutionRole || this.createExecutionRole(props);
  }

  /**
   * Creates the ECS task role.
   *
   * @param props - The ECS roles properties
   * @returns The created task role
   */
  private createTaskRole(props: ECSRolesProps): IRole {
    const taskRole = new Role(this, "ECSTaskRole", {
      roleName: props.taskRoleName,
      assumedBy: new CompositePrincipal(
        new ServicePrincipal("ecs-tasks.amazonaws.com"),
        new ServicePrincipal("lambda.amazonaws.com")
      ),
      description:
        "Allows the Oversight Model Runner to access necessary AWS services (S3, SQS, DynamoDB, ...)"
    });

    const taskPolicy = new ManagedPolicy(this, "EcsTaskPolicy", {
      managedPolicyName: "ECSTaskPolicy"
    });

    // Add permissions to assume roles
    const stsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["sts:AssumeRole"],
      resources: ["*"]
    });

    // Add permissions for AWS Key Management Service (KMS)
    const kmsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["kms:Decrypt", "kms:GenerateDataKey", "kms:Encrypt"],
      resources: [
        `arn:${this.partition}:kms:${props.account.region}:${props.account.id}:key/*`
      ]
    });

    // Add permissions for Amazon Kinesis
    const kinesisPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "kinesis:DescribeStream"
      ],
      resources: [
        `arn:${this.partition}:kinesis:${props.account.region}:${props.account.id}:stream/*`
      ]
    });

    // Add permissions to describe EC2 instance types
    const ec2PolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["ec2:DescribeInstanceTypes", "ec2:DescribeSubnets"],
      resources: ["*"]
    });

    // Add permissions for SQS permissions
    const sqsResources =
      props.sqsQueueArns && props.sqsQueueArns.length > 0
        ? props.sqsQueueArns
        : [
            `arn:${this.partition}:sqs:${props.account.region}:${props.account.id}:*`
          ];
    const sqsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "sqs:DeleteMessage",
        "sqs:ListQueues",
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:SendMessage",
        "sqs:GetQueueAttributes"
      ],
      resources: sqsResources
    });

    // Add permissions for S3 permissions
    const s3Resources =
      props.s3BucketArns && props.s3BucketArns.length > 0
        ? [
            ...props.s3BucketArns.map((arn) => `${arn}/*`),
            ...props.s3BucketArns
          ]
        : [`arn:${this.partition}:s3:::*`];
    const s3PolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "s3:GetBucketAcl",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:GetObjectAcl",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      resources: s3Resources
    });

    // Add permissions for SNS permissions
    const snsResources =
      props.snsTopicArns && props.snsTopicArns.length > 0
        ? props.snsTopicArns
        : [
            `arn:${this.partition}:sns:${props.account.region}:${props.account.id}:*`
          ];
    const snsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["sns:Publish"],
      resources: snsResources
    });

    // Add permissions for dynamodb permissions
    const ddbResources =
      props.dynamoTableArns && props.dynamoTableArns.length > 0
        ? props.dynamoTableArns
        : [
            `arn:${this.partition}:dynamodb:${props.account.region}:${props.account.id}:*`
          ];
    const ddbPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem",
        "dynamodb:ListTables",
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:UpdateItem",
        "dynamodb:UpdateTable"
      ],
      resources: ddbResources
    });

    // Add permission for autoscaling ECS permissions
    const ecsResources =
      props.ecsResourceArns && props.ecsResourceArns.length > 0
        ? props.ecsResourceArns
        : [
            `arn:${this.partition}:ecs:${props.account.region}:${props.account.id}:*`
          ];
    const autoScalingEcsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["ecs:DescribeServices", "ecs:UpdateService"],
      resources: ecsResources
    });

    // Add permission for CW ECS permissions
    const cwPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "logs:PutLogEvents",
        "logs:GetLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:CreateLogStream",
        "logs:CreateLogGroup"
      ],
      resources: [
        `arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
      ]
    });

    // Add permission for autoscaling CW permissions
    const autoScalingCwPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["cloudwatch:DescribeAlarms"],
      resources: [`*`]
    });

    // Add permissions for SageMaker permissions
    const sagemakerPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "sagemaker:ListEndpointConfigs",
        "sagemaker:DescribeEndpointConfig",
        "sagemaker:InvokeEndpoint",
        "sagemaker:DescribeEndpoint",
        "sagemaker:ListEndpoints",
        "sagemaker:InvokeEndpointAsync",
        "sagemaker:DescribeModel",
        "sagemaker:ListModels",
        "sagemaker:DescribeModelPackage",
        "sagemaker:DescribeModelPackageGroup",
        "sagemaker:BatchDescribeModelPackage",
        "sagemaker:ListModelMetadata",
        "sagemaker:BatchGetRecord",
        "sagemaker:BatchGetMetrics",
        "sagemaker:BatchPutMetrics",
        "sagemaker:ListTags"
      ],
      resources: [
        `arn:${this.partition}:sagemaker:${props.account.region}:${props.account.id}:*`
      ]
    });

    taskPolicy.addStatements(
      stsPolicyStatement,
      kmsPolicyStatement,
      sagemakerPolicyStatement,
      s3PolicyStatement,
      ec2PolicyStatement,
      kinesisPolicyStatement,
      snsPolicyStatement,
      sqsPolicyStatement,
      ddbPolicyStatement,
      autoScalingEcsPolicyStatement,
      autoScalingCwPolicyStatement,
      cwPolicyStatement
    );

    taskRole.addManagedPolicy(taskPolicy);

    // Suppress acceptable wildcard permissions
    NagSuppressions.addResourceSuppressions(
      taskPolicy,
      [
        {
          id: "AwsSolutions-IAM5",
          reason:
            "sts:AssumeRole requires wildcard resource for cross-account and dynamic role assumption scenarios",
          appliesTo: ["Resource::*"]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "EC2 DescribeInstanceTypes and DescribeSubnets are read-only actions that require wildcard resource",
          appliesTo: ["Resource::*"]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "CloudWatch DescribeAlarms requires wildcard resource for autoscaling integration",
          appliesTo: ["Resource::*"]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "KMS key wildcard allows access to account-managed keys needed for encryption of various resources",
          appliesTo: [
            `Resource::arn:${this.partition}:kms:${props.account.region}:${props.account.id}:key/*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "SageMaker wildcard allows access to any SageMaker endpoint in the account for flexible model invocation",
          appliesTo: [
            `Resource::arn:${this.partition}:sagemaker:${props.account.region}:${props.account.id}:*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "S3 wildcard may be used when specific bucket ARNs are not provided, allows access to S3 resources needed by the model runner",
          appliesTo: [`Resource::arn:${this.partition}:s3:::*`]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "Kinesis stream wildcard allows writing to any stream in the account for flexible sink configuration",
          appliesTo: [
            `Resource::arn:${this.partition}:kinesis:${props.account.region}:${props.account.id}:stream/*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "ECS wildcard may be used when specific service ARNs are not provided, allows autoscaling operations",
          appliesTo: [
            `Resource::arn:${this.partition}:ecs:${props.account.region}:${props.account.id}:*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "CloudWatch Logs log-group wildcard allows access to log groups created dynamically by ECS tasks",
          appliesTo: [
            `Resource::arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "SQS queue wildcard may be used when specific queue ARNs are not provided, allows access to queues needed by the model runner",
          appliesTo: [
            `Resource::arn:${this.partition}:sqs:${props.account.region}:${props.account.id}:*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "SNS topic wildcard may be used when specific topic ARNs are not provided, allows publishing to topics needed by the model runner",
          appliesTo: [
            `Resource::arn:${this.partition}:sns:${props.account.region}:${props.account.id}:*`
          ]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "DynamoDB table wildcard may be used when specific table ARNs are not provided, allows access to tables needed by the model runner",
          appliesTo: [
            `Resource::arn:${this.partition}:dynamodb:${props.account.region}:${props.account.id}:*`
          ]
        }
      ],
      true
    );

    return taskRole;
  }

  /**
   * Creates the ECS execution role.
   *
   * @param props - The ECS roles properties
   * @returns The created execution role
   */
  private createExecutionRole(props: ECSRolesProps): IRole {
    const executionRole = new Role(this, "EcsExecutionRole", {
      roleName: props.executionRoleName,
      assumedBy: new CompositePrincipal(
        new ServicePrincipal("ecs-tasks.amazonaws.com")
      ),
      description: "Allows ECS tasks to access necessary AWS services."
    });

    const executionPolicy = new ManagedPolicy(this, "EcsExecutionPolicy", {
      managedPolicyName: "ECSExecutionPolicy"
    });

    executionPolicy.addStatements(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["ecr:GetAuthorizationToken"],
        resources: ["*"]
      }),
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "logs:CreateLogStream",
          "logs:CreateLogGroup"
        ],
        resources: [
          `arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
        ]
      })
    );

    executionRole.addManagedPolicy(executionPolicy);

    // Suppress acceptable wildcard permissions
    NagSuppressions.addResourceSuppressions(
      executionPolicy,
      [
        {
          id: "AwsSolutions-IAM5",
          reason:
            "ecr:GetAuthorizationToken requires wildcard resource per AWS documentation",
          appliesTo: ["Resource::*"]
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "CloudWatch Logs log-group wildcard allows ECS tasks to create and write to log groups dynamically",
          appliesTo: [
            `Resource::arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
          ]
        }
      ],
      true
    );

    return executionRole;
  }
}
