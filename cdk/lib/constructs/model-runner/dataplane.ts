/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { ISecurityGroup, IVpc, SecurityGroup } from "aws-cdk-lib/aws-ec2";
import { Role } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount, RegionalConfig } from "../types";
import { Autoscaling } from "./autoscaling";
import { DatabaseTables } from "./database-tables";
import { ECSService } from "./ecs-service";
import { Messaging } from "./messaging";
import { Monitoring } from "./monitoring";
import { Sinks } from "./sinks";

/**
 * Configuration class for Dataplane Construct.
 *
 * This class provides a strongly-typed configuration interface for the
 * Model Runner dataplane, with validation and default values.
 */
export class DataplaneConfig extends BaseConfig {
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
  /** The name of the DynamoDB table for image request status. */
  public readonly DDB_IMAGE_REQUEST_TABLE: string;
  /** The name of the DynamoDB table for outstanding image requests. */
  public readonly DDB_OUTSTANDING_IMAGE_REQUESTS_TABLE: string;
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
  /** The CPU configuration for containers. */
  public readonly ECS_CONTAINER_CPU: number;
  /** The memory configuration for containers. */
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
   * Constructor for DataplaneConfig.
   *
   * @param config - The configuration object for Dataplane
   */
  constructor(config: Partial<ConfigType> = {}) {
    const mergedConfig = {
      BUILD_FROM_SOURCE: false,
      CW_METRICS_NAMESPACE: "OSML",
      CONTAINER_BUILD_PATH: "../",
      CONTAINER_BUILD_TARGET: "model_runner",
      CONTAINER_DOCKERFILE: "docker/Dockerfile.model-runner",
      CONTAINER_URI: "awsosml/osml-model-runner:latest",
      DDB_ENDPOINT_PROCESSING_TABLE: "EndpointProcessingStatistics",
      DDB_FEATURES_TABLE: "ImageProcessingFeatures",
      DDB_IMAGE_REQUEST_TABLE: "ImageRequestTable",
      DDB_OUTSTANDING_IMAGE_REQUESTS_TABLE: "OutstandingImageRequests",
      DDB_REGION_REQUEST_TABLE: "RegionRequestTable",
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
      ECS_CONTAINER_NAME: "ModelRunnerContainer",
      ECS_CLUSTER_NAME: "ModelRunnerCluster",
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
  private validateConfig(config: Record<string, unknown>): void {
    const errors: string[] = [];

    // Validate ECS constraints (AWS Fargate limits)
    const ecsTaskCpu =
      typeof config.ECS_TASK_CPU === "number" ? config.ECS_TASK_CPU : 0;
    if (ecsTaskCpu < 256) {
      errors.push("ECS_TASK_CPU must be at least 256 (0.25 vCPU)");
    }
    if (ecsTaskCpu > 16384) {
      errors.push("ECS_TASK_CPU must be at most 16384 (16 vCPU)");
    }

    const ecsTaskMemory =
      typeof config.ECS_TASK_MEMORY === "number" ? config.ECS_TASK_MEMORY : 0;
    if (ecsTaskMemory < 512) {
      errors.push("ECS_TASK_MEMORY must be at least 512 MiB");
    }
    if (ecsTaskMemory > 122880) {
      errors.push("ECS_TASK_MEMORY must be at most 122880 MiB (120 GB)");
    }

    // Validate autoscaling constraints
    const minCount =
      typeof config.ECS_AUTOSCALING_TASK_MIN_COUNT === "number"
        ? config.ECS_AUTOSCALING_TASK_MIN_COUNT
        : 0;
    const maxCount =
      typeof config.ECS_AUTOSCALING_TASK_MAX_COUNT === "number"
        ? config.ECS_AUTOSCALING_TASK_MAX_COUNT
        : 0;
    if (minCount < 1) {
      errors.push("ECS_AUTOSCALING_TASK_MIN_COUNT must be at least 1");
    }
    if (maxCount > 100) {
      errors.push("ECS_AUTOSCALING_TASK_MAX_COUNT must be at most 100");
    }
    if (minCount > maxCount) {
      errors.push(
        "ECS_AUTOSCALING_TASK_MIN_COUNT cannot be greater than ECS_AUTOSCALING_TASK_MAX_COUNT"
      );
    }

    if (errors.length > 0) {
      throw new Error(`Configuration validation failed:\n${errors.join("\n")}`);
    }
  }
}

/**
 * Interface representing properties for configuring the Dataplane Construct.
 */
export interface DataplaneProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The VPC (Virtual Private Cloud) for the Dataplane. */
  readonly vpc: IVpc;
  /** Custom configuration for the Dataplane Construct (optional). */
  readonly config?: DataplaneConfig;
}

/**
 * Represents the Dataplane construct responsible for managing the data plane
 * of the model runner application. It handles various AWS resources and configurations
 * required for the application's operation.
 *
 * This refactored version uses separate resource classes to improve maintainability
 * and reduce complexity.
 */
export class Dataplane extends Construct {
  /** The configuration for the Dataplane. */
  public readonly config: DataplaneConfig;
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
  /** The ECS service resources. */
  public readonly ecsService: ECSService;
  /** The monitoring resources. */
  public readonly monitoringResources?: Monitoring;

  /** The autoscaling configuration. */
  public readonly autoscaling?: Autoscaling;

  /** The output sinks. */
  public readonly sinks: Sinks;

  /**
   * Constructs an instance of Dataplane.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: DataplaneProps) {
    super(scope, id);

    // Initialize configuration and basic properties
    this.config = this.initializeConfig(props);
    this.removalPolicy = this.initializeRemovalPolicy(props);
    this.regionalS3Endpoint = this.initializeRegionalS3Endpoint(props);
    this.securityGroups = this.initializeSecurityGroups();

    // Create resource classes
    this.databaseTables = this.createDatabaseTables(props);
    this.messaging = this.createMessaging(props);
    this.ecsService = this.createECSService(props);
    this.monitoringResources = this.createMonitoring(props);
    this.autoscaling = this.createAutoscaling(props);
    this.sinks = this.createSinks(props);
  }

  /**
   * Initializes the configuration.
   *
   * @param props - The Dataplane properties
   * @returns The initialized configuration
   */
  private initializeConfig(props: DataplaneProps): DataplaneConfig {
    if (props.config instanceof DataplaneConfig) {
      return props.config;
    }
    return new DataplaneConfig(
      (props.config as unknown as Partial<ConfigType>) ?? {}
    );
  }

  /**
   * Initializes the removal policy based on account type.
   *
   * @param props - The Dataplane properties
   * @returns The removal policy
   */
  private initializeRemovalPolicy(props: DataplaneProps): RemovalPolicy {
    return props.account.prodLike
      ? RemovalPolicy.RETAIN
      : RemovalPolicy.DESTROY;
  }

  /**
   * Initializes the regional S3 endpoint.
   *
   * @param props - The Dataplane properties
   * @returns The regional S3 endpoint
   */
  private initializeRegionalS3Endpoint(props: DataplaneProps): string {
    return RegionalConfig.getConfig(props.account.region).s3Endpoint;
  }

  /**
   * Initializes security groups if specified.
   *
   * @returns The security groups or undefined
   */
  private initializeSecurityGroups(): ISecurityGroup[] | undefined {
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
   * Creates the database tables.
   *
   * @param props - The Dataplane properties
   * @returns The database tables
   */
  private createDatabaseTables(props: DataplaneProps): DatabaseTables {
    return new DatabaseTables(this, "DatabaseTables", {
      account: props.account,
      config: this.config,
      removalPolicy: this.removalPolicy
    });
  }

  /**
   * Creates the messaging resources.
   *
   * @param props - The Dataplane properties
   * @returns The messaging resources
   */
  private createMessaging(props: DataplaneProps): Messaging {
    return new Messaging(this, "Messaging", {
      account: props.account,
      config: this.config
    });
  }

  /**
   * Creates the ECS service resources.
   *
   * @param props - The Dataplane properties
   * @returns The ECS service resources
   */
  private createECSService(props: DataplaneProps): ECSService {
    // Get existing roles if specified in config
    const existingTaskRole = this.config.ECS_TASK_ROLE_NAME
      ? Role.fromRoleName(
          this,
          "ImportedEcsTaskRole",
          this.config.ECS_TASK_ROLE_NAME,
          { mutable: false }
        )
      : undefined;

    const existingExecutionRole = this.config.ECS_EXECUTION_ROLE_NAME
      ? Role.fromRoleName(
          this,
          "ImportedECSExecutionRole",
          this.config.ECS_EXECUTION_ROLE_NAME,
          { mutable: false }
        )
      : undefined;

    return new ECSService(this, "ECSService", {
      account: props.account,
      vpc: props.vpc,
      config: this.config,
      taskRole: existingTaskRole,
      executionRole: existingExecutionRole,
      removalPolicy: this.removalPolicy,
      regionalS3Endpoint: this.regionalS3Endpoint,
      securityGroups: this.securityGroups,
      imageRequestTable: this.databaseTables.imageRequestTable,
      outstandingImageRequestsTable:
        this.databaseTables.outstandingImageRequestsTable,
      featureTable: this.databaseTables.featureTable,
      endpointStatisticsTable: this.databaseTables.endpointStatisticsTable,
      regionRequestTable: this.databaseTables.regionRequestTable,
      imageRequestQueue: this.messaging.imageRequestQueue,
      imageRequestDlQueue: this.messaging.imageRequestDlQueue,
      regionRequestQueue: this.messaging.regionRequestQueue,
      imageStatusTopic: this.messaging.imageStatusTopic,
      regionStatusTopic: this.messaging.regionStatusTopic
    });
  }

  /**
   * Creates the monitoring dashboard.
   *
   * @param props - The Dataplane properties
   * @returns The monitoring dashboard or undefined
   */
  private createMonitoring(props: DataplaneProps): Monitoring | undefined {
    if (this.config.MR_ENABLE_MONITORING) {
      return new Monitoring(this, "Monitoring", {
        account: props.account,
        imageRequestQueue: this.messaging.imageRequestQueue,
        regionRequestQueue: this.messaging.regionRequestQueue,
        imageRequestDlQueue: this.messaging.imageRequestDlQueue,
        regionRequestDlQueue: this.messaging.regionRequestDlQueue,
        service: this.ecsService.fargateService,
        mrDataplaneConfig: this.config
      });
    }
    return undefined;
  }

  /**
   * Creates the autoscaling configuration.
   *
   * @param props - The Dataplane properties
   * @returns The autoscaling configuration
   */
  private createAutoscaling(props: DataplaneProps): Autoscaling {
    return new Autoscaling(this, "Autoscaling", {
      account: props.account,
      config: this.config,
      fargateService: this.ecsService.fargateService,
      taskRole: this.ecsService.ecsRoles.taskRole,
      cluster: this.ecsService.cluster,
      imageRequestQueue: this.messaging.imageRequestQueue,
      regionRequestQueue: this.messaging.regionRequestQueue
    });
  }

  /**
   * Creates the output sinks.
   *
   * @param props - The Dataplane properties
   * @returns The output sinks
   */
  private createSinks(props: DataplaneProps): Sinks {
    return new Sinks(this, "Sinks", {
      account: props.account,
      config: this.config,
      removalPolicy: this.removalPolicy
    });
  }
}
