/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";
import {
  Effect,
  PolicyStatement,
  Role,
  ServicePrincipal
} from "aws-cdk-lib/aws-iam";
import {
  BlockPublicAccess,
  Bucket,
  BucketAccessControl,
  BucketEncryption,
  ObjectOwnership
} from "aws-cdk-lib/aws-s3";
import {
  BucketDeployment,
  ServerSideEncryption,
  Source
} from "aws-cdk-lib/aws-s3-deployment";
import { NagSuppressions } from "cdk-nag";
import { Construct, IConstruct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount } from "../types";

/**
 * Configuration class for TestImagery Construct.
 */
export class TestImageryConfig extends BaseConfig {
  /**
   * The name of the S3 bucket where images will be stored.
   * @default "mr-test-imagery""
   */
  public S3_IMAGE_BUCKET_PREFIX: string;

  /**
   * The local path to the test images to deploy.
   * @default "../test/data/integ/"
   */
  public S3_TEST_IMAGES_PATH: string;

  /**
   * Creates an instance of TestImageryConfig.
   * @param config - The configuration object for TestImagery.
   */
  constructor(config: ConfigType = {}) {
    super({
      S3_IMAGE_BUCKET_PREFIX: "mr-test-imagery",
      S3_TEST_IMAGES_PATH: "assets/imagery/",
      ...config
    });
  }
}

/**
 * Represents the properties for configuring the TestImagery Construct.
 *
 * @interface TestImageryProps
 * @property {OSMLAccount} account - The OSML account to use.
 * @property {IVpc} vpc - The Model Runner VPC configuration.
 * @property {TestImageryConfig|undefined} [config] - Optional custom resource configuration.
 * @property {string|undefined} [securityGroupId] - Optional security group ID to apply to the VPC config for SM endpoints.
 */
export interface TestImageryProps {
  /**
   * The OSML account to use.
   *
   * @type {OSMLAccount}
   */
  account: OSMLAccount;

  /**
   * The target vpc for the s3 bucket deployment.
   *
   * @type {IVpc}
   */
  vpc: IVpc;

  /**
   * Optional custom configuration for TestImagery.
   *
   * @type {TestImageryConfig|undefined}
   */
  config?: TestImageryConfig;
}

/**
 * Represents a TestImagery construct for managing test imagery resources.
 */
export class TestImagery extends Construct {
  /**
   * The image bucket where OSML imagery data is stored.
   */
  public imageBucket: Bucket;

  /**
   * The removal policy for this resource.
   * @default RemovalPolicy.DESTROY
   */
  public removalPolicy: RemovalPolicy;

  /**
   * Configuration options for TestImagery.
   */
  public config: TestImageryConfig;

  /**
   * Creates a TestImagery cdk construct.
   * @param scope The scope/stack in which to define this construct.
   * @param id The id of this construct within the current scope.
   * @param props The properties of this construct.
   */
  constructor(scope: Construct, id: string, props: TestImageryProps) {
    super(scope, id);

    // Check if a custom configuration was provided
    if (props.config != undefined) {
      this.config = props.config;
    } else {
      // Create a new default configuration
      this.config = new TestImageryConfig();
    }

    // Set up a removal policy based on the 'prodLike' property
    this.removalPolicy = props.account.prodLike
      ? RemovalPolicy.RETAIN
      : RemovalPolicy.DESTROY;

    // Create access logging bucket
    const accessLogBucket = new Bucket(this, "TestImageryAccessLogs", {
      bucketName: `${this.config.S3_IMAGE_BUCKET_PREFIX}-access-logs-${props.account.id}`,
      autoDeleteObjects: !props.account.prodLike,
      enforceSSL: true,
      encryption: BucketEncryption.KMS_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: this.removalPolicy
    });

    // Create an image bucket to store OSML test imagery
    this.imageBucket = new Bucket(this, `TestImageryBucket`, {
      bucketName: `${this.config.S3_IMAGE_BUCKET_PREFIX}-${props.account.id}`,
      autoDeleteObjects: !props.account.prodLike,
      enforceSSL: true,
      encryption: BucketEncryption.KMS_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: this.removalPolicy,
      objectOwnership: ObjectOwnership.OBJECT_WRITER,
      versioned: props.account.prodLike,
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      serverAccessLogsBucket: accessLogBucket,
      serverAccessLogsPrefix: "access-logs/"
    });

    // Create custom role for bucket deployment (replaces AWS managed policies)
    const deploymentRole = new Role(this, "BucketDeploymentRole", {
      assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
      description:
        "Custom role for S3 bucket deployment without AWS managed policies"
    });

    // Add CloudWatch Logs permissions (replacing AWSLambdaBasicExecutionRole)
    deploymentRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        resources: [
          `arn:aws:logs:${props.account.region}:${props.account.id}:log-group:/aws/lambda/*`
        ]
      })
    );

    // Add VPC access permissions (replacing AWSLambdaVPCAccessExecutionRole)
    deploymentRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses"
        ],
        resources: ["*"]
      })
    );

    // Add S3 permissions (restricted to specific buckets)
    // Note: CDK assets bucket is accessed via a wildcard pattern that's managed by CDK
    // We restrict to the destination bucket specifically, and allow CDK assets bucket pattern
    const cdkAssetsBucketPattern = `arn:aws:s3:::cdk-*-assets-${props.account.id}-${props.account.region}`;
    deploymentRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketAcl"
        ],
        resources: [
          this.imageBucket.bucketArn,
          `${this.imageBucket.bucketArn}/*`,
          `${cdkAssetsBucketPattern}`,
          `${cdkAssetsBucketPattern}/*`
        ]
      })
    );

    // Deploy test images into the bucket
    const bucketDeployment = new BucketDeployment(
      this,
      "TestImageryDeployment",
      {
        sources: [Source.asset(this.config.S3_TEST_IMAGES_PATH)],
        destinationBucket: this.imageBucket,
        accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
        memoryLimit: 10240,
        useEfs: true,
        vpc: props.vpc,
        retainOnDelete: props.account.prodLike,
        serverSideEncryption: ServerSideEncryption.AES_256,
        role: deploymentRole
      }
    );

    // Suppress acceptable wildcard permissions on the role's default policy
    // Note: CDK creates a DefaultPolicy automatically for roles
    const defaultPolicy = deploymentRole.node.tryFindChild("DefaultPolicy") as
      | IConstruct
      | undefined;
    if (defaultPolicy) {
      NagSuppressions.addResourceSuppressions(
        defaultPolicy,
        [
          {
            id: "AwsSolutions-IAM5",
            reason:
              "EC2 network interface actions require wildcard resource for VPC endpoint creation",
            appliesTo: ["Resource::*"]
          },
          {
            id: "AwsSolutions-IAM5",
            reason:
              "CloudWatch Logs wildcard allows Lambda function to create log groups dynamically",
            appliesTo: [
              `Resource::arn:aws:logs:${props.account.region}:${props.account.id}:log-group:/aws/lambda/*`
            ]
          },
          {
            id: "AwsSolutions-IAM5",
            reason:
              "S3 bucket object wildcard is scoped to specific bucket ARN, required for bucket deployment operations. Bucket ARN is a CloudFormation reference.",
            appliesTo: [
              `Resource::<TestImageryTestImageryBucket4E193533.Arn>/*`
            ]
          },
          {
            id: "AwsSolutions-IAM5",
            reason:
              "S3 action wildcards (GetObject*, GetBucket*, List*, DeleteObject*, Abort*) are more specific than s3:* and necessary for bucket deployment",
            appliesTo: [
              "Action::s3:GetObject*",
              "Action::s3:GetBucket*",
              "Action::s3:List*",
              "Action::s3:DeleteObject*",
              "Action::s3:Abort*"
            ]
          },
          {
            id: "AwsSolutions-IAM5",
            reason:
              "CDK assets bucket pattern is required for CDK bucket deployment functionality to access staging assets",
            appliesTo: [
              `Resource::arn:aws:s3:::cdk-*-assets-${props.account.id}-${props.account.region}`,
              `Resource::arn:aws:s3:::cdk-*-assets-${props.account.id}-${props.account.region}/*`,
              `Resource::arn:<AWS::Partition>:s3:::cdk-hnb659fds-assets-${props.account.id}-${props.account.region}/*`
            ]
          }
        ],
        true
      );
    }

    // Suppress CDK-managed bucket deployment policy wildcards
    NagSuppressions.addResourceSuppressions(
      bucketDeployment,
      [
        {
          id: "AwsSolutions-IAM5",
          reason:
            "CDK BucketDeployment requires access to CDK assets bucket which uses dynamic naming",
          appliesTo: [
            `Resource::arn:<AWS::Partition>:s3:::cdk-hnb659fds-assets-${props.account.id}-${props.account.region}/*`
          ]
        }
      ],
      true
    );
  }
}
