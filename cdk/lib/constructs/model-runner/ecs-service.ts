/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Duration, RemovalPolicy } from "aws-cdk-lib";
import { ITable } from "aws-cdk-lib/aws-dynamodb";
import { ISecurityGroup, IVpc } from "aws-cdk-lib/aws-ec2";
import { Platform } from "aws-cdk-lib/aws-ecr-assets";
import {
  AwsLogDriver,
  Cluster,
  Compatibility,
  ContainerDefinition,
  ContainerImage,
  ContainerInsights,
  FargateService,
  Protocol,
  TaskDefinition
} from "aws-cdk-lib/aws-ecs";
import { IRole } from "aws-cdk-lib/aws-iam";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { ITopic } from "aws-cdk-lib/aws-sns";
import { IQueue } from "aws-cdk-lib/aws-sqs";
import { NagSuppressions } from "cdk-nag";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";
import { DataplaneConfig } from "./dataplane";
import { ECSRoles } from "./ecs-roles";

/**
 * Properties for creating ECS service resources.
 */
export interface ECSServiceProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The VPC configuration. */
  readonly vpc: IVpc;
  /** The MR dataplane configuration. */
  readonly config: DataplaneConfig;
  /** Optional ECS task role. If not provided, will be created. */
  readonly taskRole?: IRole;
  /** Optional ECS execution role. If not provided, will be created. */
  readonly executionRole?: IRole;
  /** The removal policy for resources. */
  readonly removalPolicy: RemovalPolicy;
  /** The regional S3 endpoint. */
  readonly regionalS3Endpoint: string;
  /** The security groups for the Fargate service. */
  readonly securityGroups?: ISecurityGroup[];
  /** The DynamoDB table for image request status. */
  readonly imageRequestTable: ITable;
  /** The DynamoDB table for outstanding image requests. */
  readonly outstandingImageRequestsTable: ITable;
  /** The DynamoDB table for feature data. */
  readonly featureTable: ITable;
  /** The DynamoDB table for endpoint statistics. */
  readonly endpointStatisticsTable: ITable;
  /** The DynamoDB table for region request status. */
  readonly regionRequestTable: ITable;
  /** The SQS queue for image processing requests. */
  readonly imageRequestQueue: IQueue;
  /** The dead letter queue for image requests. */
  readonly imageRequestDlQueue: IQueue;
  /** The SQS queue for region processing requests. */
  readonly regionRequestQueue: IQueue;
  /** The SNS topic for image status notifications. */
  readonly imageStatusTopic?: ITopic;
  /** The SNS topic for region status notifications. */
  readonly regionStatusTopic?: ITopic;
}

/**
 * Construct that manages all ECS service resources for the Model Runner.
 *
 * This construct encapsulates the creation and configuration of all ECS
 * resources required by the Model Runner, including the ECS cluster, task
 * definition, Fargate service, and container image.
 */
export class ECSService extends Construct {
  /** The ECS cluster for running tasks. */
  public readonly cluster: Cluster;

  /** The ECS task definition. */
  public readonly taskDefinition: TaskDefinition;

  /** The Fargate service for the container. */
  public readonly fargateService: FargateService;

  /** The container definition for the service. */
  public readonly containerDefinition: ContainerDefinition;

  /** The container image for the service. */
  public readonly containerImage: ContainerImage;

  /** The log group for the service. */
  public readonly logGroup: LogGroup;

  /** The ECS roles (task and execution roles). */
  public readonly ecsRoles: ECSRoles;

  /**
   * Creates a new ECSService construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: ECSServiceProps) {
    super(scope, id);

    // Create ECS roles
    this.ecsRoles = this.createECSRoles(props);

    // Create log group
    this.logGroup = this.createLogGroup(props);

    // Create container image
    this.containerImage = this.createContainerImage(props);

    // Create ECS cluster
    this.cluster = this.createCluster(props);

    // Create task definition
    this.taskDefinition = this.createTaskDefinition(props);

    // Create container definition
    this.containerDefinition = this.createContainerDefinition(props);

    // Create Fargate service
    this.fargateService = this.createFargateService(props);
  }

  /**
   * Creates the ECS roles.
   *
   * @param props - The ECS service properties
   * @returns The created ECSRoles
   */
  private createECSRoles(props: ECSServiceProps): ECSRoles {
    // Collect SQS queue ARNs
    const sqsQueueArns: string[] = [
      props.imageRequestQueue.queueArn,
      props.imageRequestDlQueue.queueArn,
      props.regionRequestQueue.queueArn
    ];

    // Collect SNS topic ARNs
    const snsTopicArns: string[] = [];
    if (props.imageStatusTopic) {
      snsTopicArns.push(props.imageStatusTopic.topicArn);
    }
    if (props.regionStatusTopic) {
      snsTopicArns.push(props.regionStatusTopic.topicArn);
    }

    // Collect DynamoDB table ARNs
    const dynamoTableArns: string[] = [
      props.imageRequestTable.tableArn,
      props.outstandingImageRequestsTable.tableArn,
      props.featureTable.tableArn,
      props.endpointStatisticsTable.tableArn,
      props.regionRequestTable.tableArn
    ];

    return new ECSRoles(this, "ECSRoles", {
      account: props.account,
      taskRoleName: "ECSTaskRole",
      executionRoleName: "ECSExecutionRole",
      existingTaskRole: props.taskRole,
      existingExecutionRole: props.executionRole,
      sqsQueueArns: sqsQueueArns,
      snsTopicArns: snsTopicArns.length > 0 ? snsTopicArns : undefined,
      dynamoTableArns: dynamoTableArns
    });
  }

  /**
   * Creates the CloudWatch log group.
   *
   * @param props - The ECS service properties
   * @returns The created LogGroup
   */
  private createLogGroup(props: ECSServiceProps): LogGroup {
    return new LogGroup(this, "MRServiceLogGroup", {
      logGroupName: "/aws/OSML/MRService",
      retention: RetentionDays.TEN_YEARS,
      removalPolicy: props.removalPolicy
    });
  }

  /**
   * Creates the container image.
   *
   * @param props - The ECS service properties
   * @returns The created ContainerImage
   */
  private createContainerImage(props: ECSServiceProps): ContainerImage {
    if (props.config.BUILD_FROM_SOURCE) {
      // Build from source using Docker
      return ContainerImage.fromAsset(props.config.CONTAINER_BUILD_PATH, {
        target: props.config.CONTAINER_BUILD_TARGET,
        file: props.config.CONTAINER_DOCKERFILE,
        platform: Platform.LINUX_AMD64
      });
    } else {
      // Use pre-built image from registry
      return ContainerImage.fromRegistry(props.config.CONTAINER_URI);
    }
  }

  /**
   * Creates the ECS cluster.
   *
   * @param props - The ECS service properties
   * @returns The created Cluster
   */
  private createCluster(props: ECSServiceProps): Cluster {
    return new Cluster(this, "Cluster", {
      clusterName: props.config.ECS_CLUSTER_NAME,
      vpc: props.vpc,
      containerInsightsV2: props.account.prodLike
        ? ContainerInsights.ENABLED
        : ContainerInsights.ENHANCED
    });
  }

  /**
   * Creates the ECS task definition.
   *
   * @param props - The ECS service properties
   * @returns The created TaskDefinition
   */
  private createTaskDefinition(props: ECSServiceProps): TaskDefinition {
    return new TaskDefinition(this, "TaskDefinition", {
      memoryMiB: props.config.ECS_TASK_MEMORY.toString(),
      cpu: props.config.ECS_TASK_CPU.toString(),
      compatibility: Compatibility.FARGATE,
      taskRole: this.ecsRoles.taskRole,
      executionRole: this.ecsRoles.executionRole
    });
  }

  /**
   * Creates the container definition.
   *
   * @param props - The ECS service properties
   * @returns The created ContainerDefinition
   */
  private createContainerDefinition(
    props: ECSServiceProps
  ): ContainerDefinition {
    // Add port mapping to task definition
    this.taskDefinition.defaultContainer?.addPortMappings({
      containerPort: 80,
      hostPort: 80,
      protocol: Protocol.TCP
    });

    const containerDef = this.taskDefinition.addContainer(
      "ContainerDefinition",
      {
        containerName: props.config.ECS_CONTAINER_NAME,
        image: this.containerImage,
        memoryLimitMiB: props.config.ECS_CONTAINER_MEMORY,
        cpu: props.config.ECS_CONTAINER_CPU,
        environment: this.buildContainerEnvironment(props),
        startTimeout: Duration.minutes(1),
        stopTimeout: Duration.minutes(1),
        disableNetworking: false,
        logging: new AwsLogDriver({
          logGroup: this.logGroup,
          streamPrefix: props.config.CW_METRICS_NAMESPACE
        })
      }
    );

    // Suppress ECS2 findings - environment variables are used for configuration
    NagSuppressions.addResourceSuppressions(
      this.taskDefinition,
      [
        {
          id: "AwsSolutions-ECS2",
          reason:
            "ECS task definition uses environment variables for container configuration. Secrets Manager integration can be added if required."
        }
      ],
      true
    );

    return containerDef;
  }

  /**
   * Creates the Fargate service.
   *
   * @param props - The ECS service properties
   * @returns The created FargateService
   */
  private createFargateService(props: ECSServiceProps): FargateService {
    // Set desired count to match min capacity to avoid immediate autoscaling
    // Autoscaling will manage the count from this initial value
    const desiredCount = Math.max(
      props.config.ECS_DEFAULT_DESIRE_COUNT,
      props.config.ECS_AUTOSCALING_TASK_MIN_COUNT
    );

    const service = new FargateService(this, "MRService", {
      taskDefinition: this.taskDefinition,
      cluster: this.cluster,
      minHealthyPercent: 100,
      securityGroups: props.securityGroups,
      vpcSubnets: props.vpc.selectSubnets(),
      desiredCount: desiredCount
    });

    return service;
  }

  /**
   * Builds the container environment variables.
   *
   * @param props - The ECS service properties
   * @returns The environment variables object
   */
  private buildContainerEnvironment(props: ECSServiceProps): {
    [key: string]: string;
  } {
    const workers = Math.ceil(
      (props.config.ECS_CONTAINER_CPU / 1024) * props.config.MR_WORKERS_PER_CPU
    ).toString();

    const environment: { [key: string]: string } = {
      AWS_DEFAULT_REGION: props.account.region,
      DDB_TTL_IN_DAYS: props.config.DDB_TTL_IN_DAYS,
      IMAGE_REQUEST_TABLE: props.imageRequestTable.tableName,
      OUTSTANDING_IMAGE_REQUEST_TABLE:
        props.outstandingImageRequestsTable.tableName,
      FEATURE_TABLE: props.featureTable.tableName,
      ENDPOINT_TABLE: props.endpointStatisticsTable.tableName,
      REGION_REQUEST_TABLE: props.regionRequestTable.tableName,
      IMAGE_QUEUE: props.imageRequestQueue.queueUrl,
      IMAGE_DLQ: props.imageRequestDlQueue.queueUrl,
      REGION_QUEUE: props.regionRequestQueue.queueUrl,
      AWS_S3_ENDPOINT: props.regionalS3Endpoint,
      WORKERS_PER_CPU: props.config.MR_WORKERS_PER_CPU.toString(),
      WORKERS: workers,
      REGION_SIZE: props.config.MR_REGION_SIZE
    };

    // Add optional environment variables
    if (props.config.MR_TERRAIN_URI) {
      environment.ELEVATION_DATA_LOCATION = props.config.MR_TERRAIN_URI;
    }

    if (props.imageStatusTopic) {
      environment.IMAGE_STATUS_TOPIC = props.imageStatusTopic.topicArn;
    }

    if (props.regionStatusTopic) {
      environment.REGION_STATUS_TOPIC = props.regionStatusTopic.topicArn;
    }

    return environment;
  }
}
