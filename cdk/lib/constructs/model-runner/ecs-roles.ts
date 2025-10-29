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

    return executionRole;
  }
}
