/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";
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
   * Creates an instance of TestImageryConfig.
   * @param config - The configuration object for TestImagery.
   */
  constructor(config: ConfigType = {}) {
    super({
      S3_IMAGE_BUCKET_PREFIX: "mr-test-imagery",
      S3_TEST_IMAGES_PATH: "../test/integration/test_suites/imagery/",
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
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL
    });

    // Deploy test images into the bucket
    new BucketDeployment(this, "TestImageryDeployment", {
      sources: [Source.asset(this.config.S3_TEST_IMAGES_PATH)],
      destinationBucket: this.imageBucket,
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      memoryLimit: 10240,
      useEfs: true,
      vpc: props.vpc,
      retainOnDelete: props.account.prodLike,
      serverSideEncryption: ServerSideEncryption.AES_256
    });
  }
}
