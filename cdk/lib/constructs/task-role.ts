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
import { Construct } from "constructs";

import { OSMLAccount } from "osml-cdk-constructs";

/**
 * Represents the properties required to define a model runner ECS task role.
 *
 * @interface TaskRoleProps
 */
export interface TaskRoleProps {
  /**
   * The OSML (OversightML) deployment account associated with this role.
   *
   * @type {OSMLAccount}
   */
  account: OSMLAccount;

  /**
   * The name to give to the role.
   *
   * @type {string}
   */
  roleName: string;
}

/**
 * Represents a TaskRole construct.
 */
export class TaskRole extends Construct {
  /**
   * The AWS IAM role associated with this TaskRole.
   */
  public role: IRole;

  /**
   * The AWS partition to be used for this TaskRole.
   */
  public partition: string;

  /**
   * Creates an TaskRole construct.
   * @param {Construct} scope - The scope/stack in which to define this construct.
   * @param {string} id - The id of this construct within the current scope.
   * @param {TaskRoleProps} props - The properties of this construct.
   * @returns TaskRole - The TaskRole construct.
   */
  constructor(scope: Construct, id: string, props: TaskRoleProps) {
    super(scope, id);

    // Determine the AWS partition based on the provided AWS region
    this.partition = region_info.Fact.find(
      props.account.region,
      region_info.FactName.PARTITION
    )!;

    // Create an AWS IAM role for the Model Runner Fargate ECS task
    const mrTaskRole = new Role(this, "TaskRole", {
      roleName: props.roleName,
      assumedBy: new CompositePrincipal(
        new ServicePrincipal("ecs-tasks.amazonaws.com"),
        new ServicePrincipal("lambda.amazonaws.com")
      ),
      description:
        "Allows the Oversight Model Runner to access necessary AWS services (S3, SQS, DynamoDB, ...)"
    });

    const mrPolicy = new ManagedPolicy(this, "MRTaskPolicy", {
      managedPolicyName: "MRTaskPolicy"
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
      resources: [
        `arn:${this.partition}:sqs:${props.account.region}:${props.account.id}:*`
      ]
    });

    // Add permissions for S3 permissions
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
      resources: [`arn:${this.partition}:s3:::*`]
    });

    // Add permissions for SNS permissions
    const snsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["sns:Publish"],
      resources: [
        `arn:${this.partition}:sns:${props.account.region}:${props.account.id}:*`
      ]
    });

    // Add permissions for dynamodb permissions
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
      resources: [
        `arn:${this.partition}:dynamodb:${props.account.region}:${props.account.id}:*`
      ]
    });

    // Add permission for autoscaling ECS permissions
    const autoScalingEcsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["ecs:DescribeServices", "ecs:UpdateService"],
      resources: [
        `arn:${this.partition}:ecs:${props.account.region}:${props.account.id}:*`
      ]
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
        "sagemaker:BatchPutMetrics"
      ],
      resources: [
        `arn:${this.partition}:sagemaker:${props.account.region}:${props.account.id}:*`
      ]
    });

    mrPolicy.addStatements(
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

    mrTaskRole.addManagedPolicy(mrPolicy);

    // Set the TaskRole property to the created role
    this.role = mrTaskRole;
  }
}
