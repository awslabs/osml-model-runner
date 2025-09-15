/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Duration, RemovalPolicy } from "aws-cdk-lib";
import {
  AwsLogDriver,
  Cluster,
  Compatibility,
  ContainerDefinition,
  ContainerInsights,
  FargateService,
  Protocol,
  TaskDefinition
} from "aws-cdk-lib/aws-ecs";
import { IRole } from "aws-cdk-lib/aws-iam";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { ISecurityGroup, IVpc } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

import { OSMLAccount, OSMLContainer } from "osml-cdk-constructs";
import { ModelRunnerDataplaneConfig } from "./model-runner-dataplane";


/**
 * Properties for creating container resources.
 */
export interface ContainersProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The VPC configuration. */
  readonly vpc: IVpc;
  /** The MR dataplane configuration. */
  readonly config: ModelRunnerDataplaneConfig;
  /** The ECS task role. */
  readonly taskRole: IRole;
  /** The ECS execution role. */
  readonly executionRole: IRole;
  /** The removal policy for resources. */
  readonly removalPolicy: RemovalPolicy;
  /** The regional S3 endpoint. */
  readonly regionalS3Endpoint: string;
  /** The security groups for the Fargate service. */
  readonly securityGroups?: ISecurityGroup[];
}

/**
 * Construct that manages all ECS container resources for the Model Runner.
 *
 * This construct encapsulates the creation and configuration of all container
 * resources required by the Model Runner, including the ECS cluster, task
 * definition, Fargate service, and container image.
 */
export class Containers extends Construct {
  /** The ECS cluster for running tasks. */
  public readonly cluster: Cluster;

  /** The ECS task definition. */
  public readonly taskDefinition: TaskDefinition;

  /** The Fargate service for the MR container. */
  public readonly fargateService: FargateService;

  /** The container definition for the MR service. */
  public readonly containerDefinition: ContainerDefinition;

  /** The container for the MR service. */
  public readonly mrContainer: OSMLContainer;

  /** The log group for the MR service. */
  public readonly logGroup: LogGroup;

  /**
   * Creates a new Containers construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: ContainersProps) {
    super(scope, id);

    // Create log group
    this.logGroup = this.createLogGroup(props);

    // Create container image
    this.mrContainer = this.createContainer(props);

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
   * Creates the CloudWatch log group.
   *
   * @param props - The container properties
   * @returns The created LogGroup
   */
  private createLogGroup(props: ContainersProps): LogGroup {
    return new LogGroup(this, "MRServiceLogGroup", {
      logGroupName: "/aws/OSML/MRService",
      retention: RetentionDays.TEN_YEARS,
      removalPolicy: props.removalPolicy
    });
  }

  /**
   * Creates the container image.
   *
   * @param props - The container properties
   * @returns The created OSMLContainer
   */
  private createContainer(props: ContainersProps): OSMLContainer {
    return new OSMLContainer(this, "MRContainer", {
      account: props.account,
      buildFromSource: props.config.BUILD_FROM_SOURCE,
      config: {
        CONTAINER_URI: props.config.CONTAINER_URI,
        CONTAINER_BUILD_PATH: props.config.CONTAINER_BUILD_PATH,
        CONTAINER_BUILD_TARGET: props.config.CONTAINER_BUILD_TARGET,
        CONTAINER_DOCKERFILE: props.config.CONTAINER_DOCKERFILE
      }
    });
  }

  /**
   * Creates the ECS cluster.
   *
   * @param props - The container properties
   * @returns The created Cluster
   */
  private createCluster(props: ContainersProps): Cluster {
    return new Cluster(this, "MRCluster", {
      clusterName: props.config.ECS_CLUSTER_NAME,
      vpc: props.vpc,
      containerInsightsV2: props.account.prodLike
        ? ContainerInsights.ENABLED
        : ContainerInsights.DISABLED
    });
  }

  /**
   * Creates the ECS task definition.
   *
   * @param props - The container properties
   * @returns The created TaskDefinition
   */
  private createTaskDefinition(props: ContainersProps): TaskDefinition {
    return new TaskDefinition(this, "MRTaskDefinition", {
      memoryMiB: props.config.ECS_TASK_MEMORY.toString(),
      cpu: props.config.ECS_TASK_CPU.toString(),
      compatibility: Compatibility.FARGATE,
      taskRole: props.taskRole,
      executionRole: props.executionRole
    });
  }

  /**
   * Creates the container definition.
   *
   * @param props - The container properties
   * @returns The created ContainerDefinition
   */
  private createContainerDefinition(props: ContainersProps): ContainerDefinition {
    // Add port mapping to task definition
    this.taskDefinition.defaultContainer?.addPortMappings({
      containerPort: 80,
      hostPort: 80,
      protocol: Protocol.TCP
    });

    return this.taskDefinition.addContainer("MRContainerDefinition", {
      containerName: props.config.ECS_CONTAINER_NAME,
      image: this.mrContainer.containerImage,
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
    });
  }

  /**
   * Creates the Fargate service.
   *
   * @param props - The container properties
   * @returns The created FargateService
   */
  private createFargateService(props: ContainersProps): FargateService {
    const service = new FargateService(this, "MRService", {
      taskDefinition: this.taskDefinition,
      cluster: this.cluster,
      minHealthyPercent: 100,
      securityGroups: props.securityGroups,
      vpcSubnets: props.vpc.selectSubnets(),
      desiredCount: props.config.ECS_DEFAULT_DESIRE_COUNT
    });

    service.node.addDependency(this.mrContainer);
    return service;
  }

  /**
   * Builds the container environment variables.
   *
   * @param props - The container properties
   * @returns The environment variables object
   */
  private buildContainerEnvironment(props: ContainersProps): { [key: string]: string } {
    const workers = Math.ceil(
      (props.config.ECS_CONTAINER_CPU / 1024) * props.config.MR_WORKERS_PER_CPU
    ).toString();

    const environment: { [key: string]: string } = {
      AWS_DEFAULT_REGION: props.account.region,
      DDB_TTL_IN_DAYS: props.config.DDB_TTL_IN_DAYS,
      AWS_S3_ENDPOINT: props.regionalS3Endpoint,
      WORKERS_PER_CPU: props.config.MR_WORKERS_PER_CPU.toString(),
      WORKERS: workers,
      REGION_SIZE: props.config.MR_REGION_SIZE
    };

    // Add optional environment variables
    if (props.config.MR_TERRAIN_URI) {
      environment.ELEVATION_DATA_LOCATION = props.config.MR_TERRAIN_URI;
    }

    return environment;
  }
}
