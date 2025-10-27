/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Duration, RemovalPolicy } from "aws-cdk-lib";
import {
  AwsLogDriver,
  Cluster,
  Compatibility,
  ContainerDefinition,
  ContainerImage,
  FargateService,
  Protocol,
  TaskDefinition
} from "aws-cdk-lib/aws-ecs";
import { IRole, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { IVpc, ISecurityGroup, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

import { OSMLAccount, BaseConfig, ConfigType } from "../types";
import { ModelContainer } from "./model-container";

/**
 * Configuration class for HTTPEndpoint Construct.
 */
export class HTTPEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the HTTP model endpoint.
   * @default true
   */
  public DEPLOY_HTTP_ENDPOINT: boolean;

  /**
   * The CPU allocation for the HTTP endpoint.
   * @default 4096
   */
  public HTTP_ENDPOINT_CPU: number;

  /**
   * The container port for the HTTP endpoint.
   * @default 8080
   */
  public HTTP_ENDPOINT_CONTAINER_PORT: number;

  /**
   * The domain name for the HTTP endpoint.
   * @default "test-http-model-endpoint"
   */
  public HTTP_ENDPOINT_DOMAIN_NAME: string;

  /**
   * The name of the HTTP endpoint cluster.
   * @default "HTTPModelCluster"
   */
  public HTTP_ENDPOINT_NAME: string;

  /**
   * The host port for the HTTP endpoint.
   * @default 8080
   */
  public HTTP_ENDPOINT_HOST_PORT: number;

  /**
   * The health check path for the HTTP endpoint.
   * @default "/ping"
   */
  public HTTP_ENDPOINT_HEALTHCHECK_PATH: string;

  /**
   * The memory allocation for the HTTP endpoint.
   * @default 16384
   */
  public HTTP_ENDPOINT_MEMORY: number;

  /**
   * The name of the HTTP endpoint execution role.
   * @default undefined
   */
  public HTTP_ENDPOINT_ROLE_NAME?: string | undefined;

  /**
   * A security group to use for these resources.
   */
  public SECURITY_GROUP_ID?: string | undefined;

  /**
   * Constructor for HTTPEndpointConfig.
   * @param config - The configuration object for HTTPEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_HTTP_ENDPOINT: true,
      HTTP_ENDPOINT_CPU: 4096,
      HTTP_ENDPOINT_CONTAINER_PORT: 8080,
      HTTP_ENDPOINT_DOMAIN_NAME: "test-http-model-endpoint",
      HTTP_ENDPOINT_NAME: "HTTPModelCluster",
      HTTP_ENDPOINT_HOST_PORT: 8080,
      HTTP_ENDPOINT_HEALTHCHECK_PATH: "/ping",
      HTTP_ENDPOINT_MEMORY: 16384,
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the HTTPEndpoint Construct.
 */
export interface HTTPEndpointProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The VPC where the model will be deployed. */
  readonly vpc: IVpc;
  /** The selected subnets within the VPC for deployment. */
  readonly selectedSubnets: SubnetSelection;
  /** The default security group for the VPC. */
  readonly securityGroup?: ISecurityGroup;
  /** The OSML container. */
  readonly container: ModelContainer;
  /** Custom configuration for the HTTPEndpoint Construct (optional). */
  readonly config?: HTTPEndpointConfig;
}

/**
 * Represents an HTTPEndpoint construct responsible for managing an
 * HTTP-based model endpoint using ECS Fargate.
 */
export class HTTPEndpoint extends Construct {
  /** The configuration for the HTTPEndpoint. */
  public readonly config: HTTPEndpointConfig;
  /** The ECS cluster for the HTTP endpoint. */
  public readonly cluster?: Cluster;
  /** The ECS task definition. */
  public readonly taskDefinition?: TaskDefinition;
  /** The Fargate service for the HTTP endpoint. */
  public readonly service?: FargateService;
  /** The log group for the service. */
  public readonly logGroup?: LogGroup;

  /**
   * Constructs an instance of HTTPEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: HTTPEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new HTTPEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_HTTP_ENDPOINT) {
      // Determine removal policy
      const removalPolicy = props.account.prodLike
        ? RemovalPolicy.RETAIN
        : RemovalPolicy.DESTROY;

      // Create log group
      this.logGroup = new LogGroup(this, "HTTPEndpointLogGroup", {
        logGroupName: `/aws/OSML/${this.config.HTTP_ENDPOINT_NAME}`,
        retention: RetentionDays.TEN_YEARS,
        removalPolicy: removalPolicy
      });

      // Create ECS cluster
      this.cluster = new Cluster(this, "Cluster", {
        clusterName: this.config.HTTP_ENDPOINT_NAME,
        vpc: props.vpc
      });

      // Create execution role
      const executionRole = this.createExecutionRole();

      // Create task definition
      this.taskDefinition = new TaskDefinition(this, "TaskDefinition", {
        memoryMiB: this.config.HTTP_ENDPOINT_MEMORY.toString(),
        cpu: this.config.HTTP_ENDPOINT_CPU.toString(),
        compatibility: Compatibility.FARGATE,
        executionRole: executionRole
      });

      // Create container definition
      this.createContainerDefinition(props);

      // Create Fargate service
      const securityGroupId = this.config.SECURITY_GROUP_ID ??
        props.securityGroup?.securityGroupId;

      this.service = new FargateService(this, "HTTPEndpointService", {
        taskDefinition: this.taskDefinition,
        cluster: this.cluster,
        minHealthyPercent: 100,
        maxHealthyPercent: 200,
        securityGroups: securityGroupId ?
          [props.securityGroup!] :
          undefined,
        vpcSubnets: props.selectedSubnets,
        desiredCount: 1
      });

      // Add dependency on container
      this.service.node.addDependency(props.container);
    }
  }

  /**
   * Creates the execution role for the HTTP endpoint.
   *
   * @returns The created execution role
   */
  private createExecutionRole(): IRole {
    if (this.config.HTTP_ENDPOINT_ROLE_NAME) {
      return Role.fromRoleName(
        this,
        "ImportedExecutionRole",
        this.config.HTTP_ENDPOINT_ROLE_NAME,
        {
          mutable: false
        }
      );
    }

    // Create new execution role
    return new Role(this, "ExecutionRole", {
      roleName: `${this.config.HTTP_ENDPOINT_NAME}-ExecutionRole`,
      assumedBy: new ServicePrincipal("ecs-tasks.amazonaws.com"),
      description: "Allows ECS tasks to access necessary AWS services for HTTP endpoint"
    });
  }

  /**
   * Creates the container definition for the HTTP endpoint.
   *
   * @param props - The HTTP endpoint properties
   */
  private createContainerDefinition(props: HTTPEndpointProps): void {
    // Use the container image from ModelContainer
    const containerImage = props.container.containerImage;

    // Add port mapping
    this.taskDefinition!.defaultContainer?.addPortMappings({
      containerPort: this.config.HTTP_ENDPOINT_CONTAINER_PORT,
      hostPort: this.config.HTTP_ENDPOINT_HOST_PORT,
      protocol: Protocol.TCP
    });

    // Add container to task definition
    this.taskDefinition!.addContainer("HTTPEndpointContainer", {
      containerName: "http-model-endpoint",
      image: containerImage,
      memoryLimitMiB: this.config.HTTP_ENDPOINT_MEMORY,
      cpu: this.config.HTTP_ENDPOINT_CPU,
      environment: {
        MODEL_SELECTION: "centerpoint"
      },
      logging: new AwsLogDriver({
        logGroup: this.logGroup!,
        streamPrefix: this.config.HTTP_ENDPOINT_NAME
      })
    });
  }
}
