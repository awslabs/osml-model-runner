/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { ISecurityGroup, IVpc, SecurityGroup } from "aws-cdk-lib/aws-ec2";
import { IRole, Role } from "aws-cdk-lib/aws-iam";
import { Stream, StreamEncryption, StreamMode } from "aws-cdk-lib/aws-kinesis";
import { CfnStream } from "aws-cdk-lib/aws-kinesis";
import { Construct } from "constructs";

import {
  OSMLAccount,
  OSMLBucket,
  BaseConfig,
  ConfigType,
  RegionalConfig
} from "osml-cdk-constructs";
import { ExecutionRole } from "./execution-role";
import { TaskRole } from "./task-role";
import { DatabaseTables } from "./database-tables";
import { Messaging } from "./messaging";
import { Containers } from "./containers";
import { Monitoring } from "./monitoring";
import { Autoscaling } from "./autoscaling";

/**
 * Configuration class for ModelRunnerDataplane Construct.
 *
 * This class provides a strongly-typed configuration interface for the
 * Model Runner dataplane, with validation and default values.
 */
export class ModelRunnerDataplaneConfig extends BaseConfig {
  /** Whether to build container resources from source. */
  public readonly BUILD_FROM_SOURCE: boolean;
  /** The namespace for metrics. */
  public readonly CW_METRICS_NAMESPACE: string;
  /** The path for the Model Runner container to build from when building from source. */
  public readonly CONTAINER_BUILD_PATH: string;
  /** The target for the Model Runner Dockerfile container build. */
  public readonly CONTAINER_BUILD_TARGET: string;
  /** The relative Dockerfile to use to build the Model Runner container. */
  public readonly CONTAINER_DOCKERFILE: string;
  /** The default container image to import when not building from source. */
  public readonly CONTAINER_URI: string;
  /** The name of the DynamoDB table for endpoint processing statistics. */
  public readonly DDB_ENDPOINT_PROCESSING_TABLE: string;
  /** The name of the DynamoDB table for image processing features. */
  public readonly DDB_FEATURES_TABLE: string;
  /** The name of the DynamoDB table for job status. */
  public readonly DDB_JOB_STATUS_TABLE: string;
  /** The name of the DynamoDB table for outstanding image jobs. */
  public readonly DDB_OUTSTANDING_IMAGE_JOBS_TABLE: string;
  /** The name of the DynamoDB table for region request status. */
  public readonly DDB_REGION_REQUEST_TABLE: string;
  /** The attribute name for expiration time in DynamoDB. */
  public readonly DDB_TTL_ATTRIBUTE: string;
  /** The time to live in days for DDB records used in tables. */
  public readonly DDB_TTL_IN_DAYS: string;
  /** The maximum number of tasks allowed in the cluster. */
  public readonly ECS_AUTOSCALING_TASK_MAX_COUNT: number;
  /** The minimum number of tasks required in the cluster. */
  public readonly ECS_AUTOSCALING_TASK_MIN_COUNT: number;
  /** The cooldown period (in minutes) after scaling in tasks. */
  public readonly ECS_AUTOSCALING_TASK_IN_COOLDOWN: number;
  /** The number of tasks to increment when scaling in. */
  public readonly ECS_AUTOSCALING_TASK_IN_INCREMENT: number;
  /** The cooldown period (in minutes) after scaling out tasks. */
  public readonly ECS_AUTOSCALING_TASK_OUT_COOLDOWN: number;
  /** The number of tasks to increment when scaling out. */
  public readonly ECS_AUTOSCALING_TASK_OUT_INCREMENT: number;
  /** The CPU configuration for MR containers. */
  public readonly ECS_CONTAINER_CPU: number;
  /** The memory configuration for MR containers. */
  public readonly ECS_CONTAINER_MEMORY: number;
  /** The name to assign the Model Runner ECS container. */
  public readonly ECS_CONTAINER_NAME: string;
  /** The name to assign the Model Runner ECS cluster. */
  public readonly ECS_CLUSTER_NAME: string;
  /** The desired number of tasks to use for the service. */
  public readonly ECS_DEFAULT_DESIRE_COUNT: number;
  /** The security group ID to use for the ECS Fargate service. */
  public readonly ECS_SECURITY_GROUP_ID?: string;
  /** The CPU configuration for MR tasks. */
  public readonly ECS_TASK_CPU: number;
  /** The memory configuration for MR tasks. */
  public readonly ECS_TASK_MEMORY: number;
  /** The name of the MR ECS execution role to import. */
  public readonly ECS_EXECUTION_ROLE_NAME?: string;
  /** The name of the MR ECS task role to import. */
  public readonly ECS_TASK_ROLE_NAME?: string;
  /** Whether to deploy image status messages. */
  public readonly MR_ENABLE_IMAGE_STATUS: boolean;
  /** Whether to deploy a kinesis output sink stream. */
  public readonly MR_ENABLE_KINESIS_SINK: boolean;
  /** Whether to deploy a monitoring dashboard for model runner. */
  public readonly MR_ENABLE_MONITORING: boolean;
  /** Whether to deploy region status messages. */
  public readonly MR_ENABLE_REGION_STATUS: boolean;
  /** Whether to deploy a s3 output sink bucket. */
  public readonly MR_ENABLE_S3_SINK: boolean;
  /** The prefix to assign the deployed Kinesis stream output sink. */
  public readonly MR_KINESIS_SINK_STREAM_PREFIX: string;
  /** The size of MR regions in the format "(width, height)". */
  public readonly MR_REGION_SIZE: string;
  /** The URI for the terrain to use for geolocation. */
  public readonly MR_TERRAIN_URI?: string;
  /** The number of workers per CPU. */
  public readonly MR_WORKERS_PER_CPU: number;
  /** The prefix to assign the deployed S3 bucket output sink. */
  public readonly S3_SINK_BUCKET_PREFIX: string;
  /** The name of the SNS topic for image status. */
  public readonly SNS_IMAGE_STATUS_TOPIC: string;
  /** The ARN of the Image Status Topic to be imported. */
  public readonly SNS_IMAGE_STATUS_TOPIC_ARN?: string;
  /** The name of the SNS topic for region status. */
  public readonly SNS_REGION_STATUS_TOPIC: string;
  /** The ARN of the Image Region Topic to be imported. */
  public readonly SNS_REGION_STATUS_TOPIC_ARN?: string;
  /** The name of the SQS queue for image requests. */
  public readonly SQS_IMAGE_REQUEST_QUEUE: string;
  /** The name of the SQS queue for image status. */
  public readonly SQS_IMAGE_STATUS_QUEUE: string;
  /** The name of the SQS queue for region requests. */
  public readonly SQS_REGION_REQUEST_QUEUE: string;
  /** The name of the SQS queue for region status. */
  public readonly SQS_REGION_STATUS_QUEUE: string;

  /**
   * Constructor for ModelRunnerDataplaneConfig.
   *
   * @param config - The configuration object for ModelRunnerDataplane
   */
  constructor(config: Partial<ConfigType> = {}) {
    const mergedConfig = {
      BUILD_FROM_SOURCE: false,
      CW_METRICS_NAMESPACE: "OSML",
      CONTAINER_BUILD_PATH: "lib/osml-model-runner",
      CONTAINER_BUILD_TARGET: "model_runner",
      CONTAINER_DOCKERFILE: "Dockerfile",
      CONTAINER_URI: "awsosml/osml-model-runner:latest",
      DDB_ENDPOINT_PROCESSING_TABLE: "EndpointProcessingStatistics",
      DDB_FEATURES_TABLE: "ImageProcessingFeatures",
      DDB_JOB_STATUS_TABLE: "ImageProcessingJobStatus",
      DDB_OUTSTANDING_IMAGE_JOBS_TABLE: "OutstandingImageProcessingJobs",
      DDB_REGION_REQUEST_TABLE: "RegionProcessingJobStatus",
      DDB_TTL_ATTRIBUTE: "expire_time",
      DDB_TTL_IN_DAYS: "7",
      ECS_AUTOSCALING_TASK_MAX_COUNT: 40,
      ECS_AUTOSCALING_TASK_MIN_COUNT: 3,
      ECS_AUTOSCALING_TASK_IN_COOLDOWN: 1,
      ECS_AUTOSCALING_TASK_IN_INCREMENT: 8,
      ECS_AUTOSCALING_TASK_OUT_COOLDOWN: 3,
      ECS_AUTOSCALING_TASK_OUT_INCREMENT: 8,
      ECS_CONTAINER_CPU: 8192,
      ECS_CONTAINER_MEMORY: 16384,
      ECS_CONTAINER_NAME: "MRContainer",
      ECS_CLUSTER_NAME: "MRCluster",
      ECS_DEFAULT_DESIRE_COUNT: 1,
      ECS_TASK_CPU: 8192,
      ECS_TASK_MEMORY: 16384,
      MR_ENABLE_IMAGE_STATUS: true,
      MR_ENABLE_KINESIS_SINK: true,
      MR_ENABLE_MONITORING: true,
      MR_ENABLE_REGION_STATUS: false,
      MR_ENABLE_S3_SINK: true,
      MR_KINESIS_SINK_STREAM_PREFIX: "mr-stream-sink",
      MR_REGION_SIZE: "(8192, 8192)",
      MR_WORKERS_PER_CPU: 2,
      S3_SINK_BUCKET_PREFIX: "mr-bucket-sink",
      SNS_IMAGE_STATUS_TOPIC: "ImageStatusTopic",
      SNS_REGION_STATUS_TOPIC: "RegionStatusTopic",
      SQS_IMAGE_REQUEST_QUEUE: "ImageRequestQueue",
      SQS_IMAGE_STATUS_QUEUE: "ImageStatusQueue",
      SQS_REGION_REQUEST_QUEUE: "RegionRequestQueue",
      SQS_REGION_STATUS_QUEUE: "RegionStatusQueue",
      ...config
    };
    super(mergedConfig);

    // Validate configuration values
    this.validateConfig(mergedConfig);
  }

  /**
   * Validates the configuration values.
   *
   * @param config - The configuration to validate
   * @throws Error if validation fails
   */
  private validateConfig(config: any): void {
    const errors: string[] = [];

    // Validate ECS constraints (AWS Fargate limits)
    if (config.ECS_TASK_CPU < 256) {
      errors.push('ECS_TASK_CPU must be at least 256 (0.25 vCPU)');
    }
    if (config.ECS_TASK_CPU > 16384) {
      errors.push('ECS_TASK_CPU must be at most 16384 (16 vCPU)');
    }
    if (config.ECS_TASK_MEMORY < 512) {
      errors.push('ECS_TASK_MEMORY must be at least 512 MiB');
    }
    if (config.ECS_TASK_MEMORY > 122880) {
      errors.push('ECS_TASK_MEMORY must be at most 122880 MiB (120 GB)');
    }

    // Validate autoscaling constraints
    if (config.ECS_AUTOSCALING_TASK_MIN_COUNT < 1) {
      errors.push('ECS_AUTOSCALING_TASK_MIN_COUNT must be at least 1');
    }
    if (config.ECS_AUTOSCALING_TASK_MAX_COUNT > 100) {
      errors.push('ECS_AUTOSCALING_TASK_MAX_COUNT must be at most 100');
    }
    if (config.ECS_AUTOSCALING_TASK_MIN_COUNT > config.ECS_AUTOSCALING_TASK_MAX_COUNT) {
      errors.push('ECS_AUTOSCALING_TASK_MIN_COUNT cannot be greater than ECS_AUTOSCALING_TASK_MAX_COUNT');
    }

    if (errors.length > 0) {
      throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
    }
  }
}

/**
 * Interface representing properties for configuring the ModelRunnerDataplane Construct.
 */
export interface ModelRunnerDataplaneProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The VPC (Virtual Private Cloud) for the ModelRunnerDataplane. */
  readonly vpc: IVpc;
  /** Custom configuration for the ModelRunnerDataplane Construct (optional). */
  readonly config?: ModelRunnerDataplaneConfig;
}

/**
 * Represents the ModelRunnerDataplane construct responsible for managing the data plane
 * of the model runner application. It handles various AWS resources and configurations
 * required for the application's operation.
 *
 * This refactored version uses separate resource classes to improve maintainability
 * and reduce complexity.
 */
export class ModelRunnerDataplane extends Construct {
  /** The IAM role for the ECS task. */
  public readonly taskRole: IRole;
  /** The IAM role for the ECS task execution. */
  public readonly executionRole: IRole;
  /** The configuration for the ModelRunnerDataplane. */
  public readonly config: ModelRunnerDataplaneConfig;
  /** The removal policy for resources created by this construct. */
  public readonly removalPolicy: RemovalPolicy;
  /** The regional S3 endpoint. */
  public readonly regionalS3Endpoint: string;
  /** The security groups for the Fargate service. */
  public readonly securityGroups?: ISecurityGroup[];

  // Resource classes
  /** The database tables. */
  public readonly databaseTables: DatabaseTables;
  /** The messaging resources. */
  public readonly messaging: Messaging;
  /** The container resources. */
  public readonly containers: Containers;
  /** The monitoring resources. */
  public readonly monitoringResources?: Monitoring;

  /** The autoscaling configuration. */
  public readonly autoscaling?: Autoscaling;

  // Output sinks
  /** The S3 bucket output sink. */
  public sinkBucket?: OSMLBucket;
  /** The Kinesis stream output sink. */
  public sinkStream?: Stream;

  /**
   * Constructs an instance of ModelRunnerDataplane.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: ModelRunnerDataplaneProps) {
    super(scope, id);

    // Initialize configuration and basic properties
    this.config = this.initializeConfig(props);
    this.removalPolicy = this.initializeRemovalPolicy(props);
    this.regionalS3Endpoint = this.initializeRegionalS3Endpoint(props);
    this.securityGroups = this.initializeSecurityGroups(props);

    // Initialize IAM roles
    this.taskRole = this.initializeTaskRole(props);
    this.executionRole = this.initializeExecutionRole(props);

    // Create resource classes
    this.databaseTables = this.createDatabaseTables(props);
    this.messaging = this.createMessaging(props);
    this.containers = this.createContainers(props);
    this.monitoringResources = this.createMonitoring(props);
    this.autoscaling = this.createAutoscaling(props);

    // Create output sinks
    this.createOutputSinks(props);
  }

  /**
   * Initializes the configuration.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The initialized configuration
   */
  private initializeConfig(props: ModelRunnerDataplaneProps): ModelRunnerDataplaneConfig {
    return props.config ?? new ModelRunnerDataplaneConfig();
  }

  /**
   * Initializes the removal policy based on account type.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The removal policy
   */
  private initializeRemovalPolicy(props: ModelRunnerDataplaneProps): RemovalPolicy {
    return props.account.prodLike ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;
  }

  /**
   * Initializes the regional S3 endpoint.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The regional S3 endpoint
   */
  private initializeRegionalS3Endpoint(props: ModelRunnerDataplaneProps): string {
    return RegionalConfig.getConfig(props.account.region).s3Endpoint;
  }

  /**
   * Initializes security groups if specified.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The security groups or undefined
   */
  private initializeSecurityGroups(props: ModelRunnerDataplaneProps): ISecurityGroup[] | undefined {
    if (this.config.ECS_SECURITY_GROUP_ID) {
      return [
        SecurityGroup.fromSecurityGroupId(
          this,
          "MRImportSecurityGroup",
          this.config.ECS_SECURITY_GROUP_ID
        )
      ];
    }
    return undefined;
  }

  /**
   * Initializes the ECS task role.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The task role
   */
  private initializeTaskRole(props: ModelRunnerDataplaneProps): IRole {
    if (this.config.ECS_TASK_ROLE_NAME) {
      return Role.fromRoleName(
        this,
        "ImportedMRECSTaskRole",
        this.config.ECS_TASK_ROLE_NAME,
        { mutable: false }
      );
    }

    return new TaskRole(this, "MRECSTaskRole", {
      account: props.account,
      roleName: "OSMLTaskRole"
    }).role;
  }

  /**
   * Initializes the ECS execution role.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The execution role
   */
  private initializeExecutionRole(props: ModelRunnerDataplaneProps): IRole {
    if (this.config.ECS_EXECUTION_ROLE_NAME) {
      return Role.fromRoleName(
        this,
        "ImportedMRECSExecutionRole",
        this.config.ECS_EXECUTION_ROLE_NAME,
        { mutable: false }
      );
    }

    return new ExecutionRole(this, "MRECSExecutionRole", {
      account: props.account,
      roleName: "MRECSExecutionRole"
    }).role;
  }

  /**
   * Creates the database tables.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The database tables
   */
  private createDatabaseTables(props: ModelRunnerDataplaneProps): DatabaseTables {
    return new DatabaseTables(this, "DatabaseTables", {
      account: props.account,
      config: this.config,
      removalPolicy: this.removalPolicy
    });
  }

  /**
   * Creates the messaging resources.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The messaging resources
   */
  private createMessaging(props: ModelRunnerDataplaneProps): Messaging {
    return new Messaging(this, "Messaging", {
      account: props.account,
      config: this.config
    });
  }

  /**
   * Creates the container resources.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The container resources
   */
  private createContainers(props: ModelRunnerDataplaneProps): Containers {
    return new Containers(this, "Containers", {
      account: props.account,
      vpc: props.vpc,
      config: this.config,
      taskRole: this.taskRole,
      executionRole: this.executionRole,
      removalPolicy: this.removalPolicy,
      regionalS3Endpoint: this.regionalS3Endpoint,
      securityGroups: this.securityGroups
    });
  }

  /**
   * Creates the monitoring dashboard.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The monitoring dashboard or undefined
   */
  private createMonitoring(props: ModelRunnerDataplaneProps): Monitoring | undefined {
    if (this.config.MR_ENABLE_MONITORING) {
      return new Monitoring(this, "Monitoring", {
        account: props.account,
        imageRequestQueue: this.messaging.imageRequestQueue.queue,
        regionRequestQueue: this.messaging.regionRequestQueue.queue,
        imageRequestDlQueue: this.messaging.imageRequestQueue.dlQueue,
        regionRequestDlQueue: this.messaging.regionRequestQueue.dlQueue,
        service: this.containers.fargateService,
        mrDataplaneConfig: this.config
      });
    }
    return undefined;
  }

  /**
   * Creates the autoscaling configuration.
   *
   * @param props - The ModelRunnerDataplane properties
   * @returns The autoscaling configuration
   */
  private createAutoscaling(props: ModelRunnerDataplaneProps): Autoscaling {
    return new Autoscaling(this, "Autoscaling", {
      account: props.account,
      config: this.config,
      fargateService: this.containers.fargateService,
      taskRole: this.taskRole,
      cluster: this.containers.cluster,
      imageRequestQueue: this.messaging.imageRequestQueue.queue,
      regionRequestQueue: this.messaging.regionRequestQueue.queue
    });
  }

  /**
   * Creates the output sinks (S3 bucket and Kinesis stream).
   *
   * @param props - The ModelRunnerDataplane properties
   */
  private createOutputSinks(props: ModelRunnerDataplaneProps): void {
    // Create S3 bucket sink if enabled
    if (this.config.MR_ENABLE_S3_SINK) {
      this.sinkBucket = new OSMLBucket(this, "MRSinkBucket", {
        bucketName: `${this.config.S3_SINK_BUCKET_PREFIX}-${props.account.id}`,
        prodLike: props.account.prodLike,
        removalPolicy: this.removalPolicy
      });
    }

    // Create Kinesis stream sink if enabled
    if (this.config.MR_ENABLE_KINESIS_SINK) {
      this.sinkStream = new Stream(this, "MRSinkStream", {
        streamName: `${this.config.MR_KINESIS_SINK_STREAM_PREFIX}-${props.account.id}`,
        streamMode: StreamMode.PROVISIONED,
        shardCount: 1,
        encryption: StreamEncryption.MANAGED,
        removalPolicy: this.removalPolicy
      });

      // Handle ADC-specific configuration
      if (props.account.isAdc) {
        const cfnStream = this.sinkStream.node.defaultChild as CfnStream;
        cfnStream.addPropertyDeletionOverride("StreamModeDetails");
      }
    }
  }

}
