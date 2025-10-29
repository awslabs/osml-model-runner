/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { Stream, StreamEncryption, StreamMode } from "aws-cdk-lib/aws-kinesis";
import { CfnStream } from "aws-cdk-lib/aws-kinesis";
import { Bucket, BucketEncryption } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount } from "../types";

/**
 * Configuration class for Sinks Construct.
 *
 * This class provides a strongly-typed configuration interface for the
 * output sinks, with validation and default values.
 */
export class SinksConfig extends BaseConfig {
  /** Whether to deploy a kinesis output sink stream. */
  public readonly MR_ENABLE_KINESIS_SINK: boolean;
  /** Whether to deploy a s3 output sink bucket. */
  public readonly MR_ENABLE_S3_SINK: boolean;
  /** The prefix to assign the deployed Kinesis stream output sink. */
  public readonly MR_KINESIS_SINK_STREAM_PREFIX: string;
  /** The prefix to assign the deployed S3 bucket output sink. */
  public readonly S3_SINK_BUCKET_PREFIX: string;

  /**
   * Constructor for SinksConfig.
   *
   * @param config - The configuration object for Sinks
   */
  constructor(config: Partial<ConfigType> = {}) {
    const mergedConfig = {
      MR_ENABLE_KINESIS_SINK: true,
      MR_ENABLE_S3_SINK: true,
      MR_KINESIS_SINK_STREAM_PREFIX: "mr-stream-sink",
      S3_SINK_BUCKET_PREFIX: "mr-bucket-sink",
      ...config
    };
    super(mergedConfig);
  }
}

/**
 * Interface representing properties for configuring the Sinks Construct.
 */
export interface SinksProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** Custom configuration for the Sinks Construct (optional). */
  readonly config?: SinksConfig;
  /** The removal policy for resources created by this construct. */
  readonly removalPolicy: RemovalPolicy;
}

/**
 * Represents the Sinks construct responsible for managing output sinks
 * (S3 bucket and Kinesis stream) for the model runner application.
 */
export class Sinks extends Construct {
  /** The S3 bucket output sink. */
  public readonly sinkBucket?: Bucket;
  /** The Kinesis stream output sink. */
  public readonly sinkStream?: Stream;
  /** The configuration for the Sinks. */
  public readonly config: SinksConfig;

  /**
   * Constructs an instance of Sinks.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: SinksProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new SinksConfig();

    // Create S3 bucket sink if enabled
    if (this.config.MR_ENABLE_S3_SINK) {
      this.sinkBucket = new Bucket(this, "BucketSink", {
        bucketName: `${this.config.S3_SINK_BUCKET_PREFIX}-${props.account.id}`,
        encryption: BucketEncryption.S3_MANAGED,
        versioned: props.account.prodLike,
        removalPolicy: props.removalPolicy,
        autoDeleteObjects: !props.account.prodLike,
        blockPublicAccess: {
          blockPublicAcls: true,
          blockPublicPolicy: true,
          ignorePublicAcls: true,
          restrictPublicBuckets: true
        }
      });
    }

    // Create Kinesis stream sink if enabled
    if (this.config.MR_ENABLE_KINESIS_SINK) {
      this.sinkStream = new Stream(this, "KinesisSink", {
        streamName: `${this.config.MR_KINESIS_SINK_STREAM_PREFIX}-${props.account.id}`,
        streamMode: StreamMode.PROVISIONED,
        shardCount: 1,
        encryption: StreamEncryption.MANAGED,
        removalPolicy: props.removalPolicy
      });

      // Handle ADC-specific configuration
      if (props.account.isAdc) {
        const cfnStream = this.sinkStream.node.defaultChild as CfnStream;
        cfnStream.addPropertyDeletionOverride("StreamModeDetails");
      }
    }
  }
}
