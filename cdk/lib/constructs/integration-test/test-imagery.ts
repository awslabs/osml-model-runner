/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy, Stack } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";
import { IRole, Role } from "aws-cdk-lib/aws-iam";
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
import { Construct } from "constructs";

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
   * The name of an existing IAM role to use for bucket deployment.
   * If not provided, BucketDeployment will create its own default role.
   * @default undefined
   */
  public DEPLOYMENT_ROLE_NAME?: string;

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
 * @property {IRole|undefined} [deploymentRole] - Optional IAM role for bucket deployment. If not provided, a role will be created.
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

  /**
   * Optional IAM role for bucket deployment.
   * If not provided, a role will be created with the necessary permissions.
   *
   * @type {IRole|undefined}
   */
  deploymentRole?: IRole;
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

    // Determine which role to use for bucket deployment
    // Priority: props.deploymentRole > config.DEPLOYMENT_ROLE_NAME > CDK default
    // If neither is provided, BucketDeployment will create its own default role
    let deploymentRole: IRole | undefined;
    if (props.deploymentRole) {
      // Use provided role from props
      deploymentRole = props.deploymentRole;
    } else if (this.config.DEPLOYMENT_ROLE_NAME) {
      // Import existing role by name from config
      deploymentRole = Role.fromRoleName(
        this,
        "ImportedDeploymentRole",
        this.config.DEPLOYMENT_ROLE_NAME,
        {
          mutable: false
        }
      );
    }

    // Deploy test images into the bucket
    new BucketDeployment(this, "TestImageryDeployment", {
      sources: [Source.asset(this.config.S3_TEST_IMAGES_PATH)],
      destinationBucket: this.imageBucket,
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      memoryLimit: 10240,
      useEfs: true,
      vpc: props.vpc,
      retainOnDelete: props.account.prodLike,
      serverSideEncryption: ServerSideEncryption.AES_256,
      ...(deploymentRole && { role: deploymentRole })
    });

    // If no deploymentRole is provided, CDK creates a default role and policy
    // Suppress nag errors for the auto-generated ServiceRole and DefaultPolicy
    if (!deploymentRole) {
      const stack = Stack.of(this);

      // Add stack-level suppressions for BucketDeployment's ServiceRole, DefaultPolicy, and Lambda runtime
      // These are deeply nested constructs that resource-level suppressions may not catch
      NagSuppressions.addStackSuppressions(stack, [
        {
          id: "AwsSolutions-IAM4",
          reason:
            "CDK BucketDeployment creates a default service role with AWS managed policies when no deploymentRole is provided"
        },
        {
          id: "AwsSolutions-IAM5",
          reason:
            "CDK BucketDeployment creates a default policy with wildcard permissions when no deploymentRole is provided. These are required for the deployment functionality."
        },
        {
          id: "AwsSolutions-L1",
          reason:
            "CDK BucketDeployment uses a Lambda function with a runtime version managed by CDK. The runtime version is controlled by the CDK framework and cannot be directly configured."
        }
      ]);
    }
  }
}
