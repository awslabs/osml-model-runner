/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Duration, RemovalPolicy } from "aws-cdk-lib";
import { ISecurityGroup, IVpc, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import {
  AwsLogDriver,
  Cluster,
  ContainerInsights,
  FargateTaskDefinition,
  Protocol
} from "aws-cdk-lib/aws-ecs";
import { ApplicationLoadBalancedFargateService } from "aws-cdk-lib/aws-ecs-patterns";
import { IRole, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { NagSuppressions } from "cdk-nag";
import { Construct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount } from "../types";
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
  public readonly taskDefinition?: FargateTaskDefinition;
  /** The Application Load Balanced Fargate service for the HTTP endpoint. */
  public readonly service?: ApplicationLoadBalancedFargateService;
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
        vpc: props.vpc,
        containerInsightsV2: ContainerInsights.ENABLED
      });

      // Create execution role
      const executionRole = this.createExecutionRole();

      // Create task definition
      this.taskDefinition = new FargateTaskDefinition(this, "TaskDefinition", {
        memoryLimitMiB: this.config.HTTP_ENDPOINT_MEMORY,
        cpu: this.config.HTTP_ENDPOINT_CPU,
        executionRole: executionRole
      });

      // Create container definition
      this.createContainerDefinition(props);

      // Create Application Load Balanced Fargate service
      this.service = new ApplicationLoadBalancedFargateService(
        this,
        "HTTPEndpointService",
        {
          cluster: this.cluster,
          loadBalancerName: this.config.HTTP_ENDPOINT_DOMAIN_NAME,
          healthCheckGracePeriod: Duration.seconds(120),
          taskDefinition: this.taskDefinition,
          taskSubnets: props.selectedSubnets,
          publicLoadBalancer: false,
          minHealthyPercent: 100,
          maxHealthyPercent: 200
        }
      );

      // Suppress ELB access logging requirement since ApplicationLoadBalancedFargateService
      // does not support access logging configuration
      NagSuppressions.addResourceSuppressions(
        this.service.loadBalancer,
        [
          {
            id: "AwsSolutions-ELB2",
            reason:
              "ApplicationLoadBalancedFargateService does not support access logging configuration. Use ApplicationLoadBalancer construct directly if access logs are required."
          }
        ],
        true
      );

      // Configure health check for the target group
      this.service.targetGroup.configureHealthCheck({
        path: this.config.HTTP_ENDPOINT_HEALTHCHECK_PATH,
        port: this.config.HTTP_ENDPOINT_HOST_PORT.toString()
      });

      // Suppress NAG findings for HTTP endpoint resources
      // These are related to default configurations that are acceptable for test endpoints
      if (this.taskDefinition) {
        NagSuppressions.addResourceSuppressions(
          this.taskDefinition,
          [
            {
              id: "AwsSolutions-ECS2",
              reason:
                "HTTP endpoint task definition uses environment variables for test configuration. Secrets Manager integration can be added if required."
            }
          ],
          true
        );
      }

      if (this.service) {
        if (this.service.loadBalancer.connections.securityGroups.length > 0) {
          NagSuppressions.addResourceSuppressions(
            this.service.loadBalancer.connections.securityGroups[0],
            [
              {
                id: "AwsSolutions-EC23",
                reason:
                  "HTTP endpoint security group uses default egress rules required for ALB functionality"
              }
            ],
            true
          );
        }
      }
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
      description:
        "Allows ECS tasks to access necessary AWS services for HTTP endpoint"
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

    // Add container to task definition with port mappings
    this.taskDefinition!.addContainer("HTTPEndpointContainer", {
      image: containerImage,
      portMappings: [
        {
          containerPort: this.config.HTTP_ENDPOINT_CONTAINER_PORT,
          hostPort: this.config.HTTP_ENDPOINT_HOST_PORT,
          protocol: Protocol.TCP
        }
      ],
      environment: {
        MODEL_SELECTION: "centerpoint",
        ENABLE_SEGMENTATION: "true"
      },
      logging: new AwsLogDriver({
        logGroup: this.logGroup!,
        streamPrefix: this.config.HTTP_ENDPOINT_NAME
      })
    });
  }
}
