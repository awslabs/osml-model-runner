/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { IRole } from "aws-cdk-lib/aws-iam";
import { IVpc, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount } from "../types";
import { ModelContainer, ModelContainerConfig } from "./model-container";
import { SageMakerRole, SageMakerRoleConfig } from "./sagemaker-role";
import { CenterpointEndpoint, CenterpointEndpointConfig } from "./centerpoint-endpoint";
import { FloodEndpoint, FloodEndpointConfig } from "./flood-endpoint";

/**
 * Configuration class for defining endpoints for OSML model endpoints.
 */
export class TestEndpointsConfig extends BaseConfig {
  /**
   * Whether to build container resources from source.
   * @default false
   */
  public BUILD_FROM_SOURCE?: boolean;

  /**
   * The build path for the container.
   * @default "../"
   */
  public CONTAINER_BUILD_PATH?: string;

  /**
   * The build target for the container.
   * @default "osml_model"
   */
  public CONTAINER_BUILD_TARGET?: string;

  /**
   * The Dockerfile to build the container.
   * @default "docker/Dockerfile.test-models"
   */
  public CONTAINER_DOCKERFILE?: string;

  /**
   * The default container image.
   * Can only be specified if ECR_REPOSITORY_ARN is not set.
   * @default "awsosml/osml-models:latest"
   */
  public CONTAINER_URI?: string;

  /**
   * (Optional) ARN of an existing container to be used.
   * Can only be specified if CONTAINER_URI is not set.
   */
  public ECR_REPOSITORY_ARN?: string;

  /**
   * (Optional) Tag of an existing container to be used.
   * Only used if ECR_REPOSITORY_ARN is set and will default to "latest".
   */
  public ECR_REPOSITORY_TAG?: string;

  /**
   * Whether to deploy the SageMaker centerpoint model endpoint.
   * @default true
   */
  public DEPLOY_SM_CENTERPOINT_ENDPOINT: boolean;

  /**
   * Whether to deploy the SageMaker flood model endpoint.
   * @default true
   */
  public DEPLOY_SM_FLOOD_ENDPOINT: boolean;

  /**
   * Whether to deploy the SageMaker multi-container model endpoint.
   * @default true
   */
  public DEPLOY_MULTI_CONTAINER_ENDPOINT: boolean;

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
   * The name of the SageMaker endpoint for the centerpoint model.
   * @default "centerpoint"
   */
  public SM_CENTER_POINT_MODEL: string;

  /**
   * The name of the multi-container SageMaker endpoint.
   * @default "multi-container"
   */
  public SM_MULTI_CONTAINER_ENDPOINT: string;

  /**
   * The SageMaker CPU instance type.
   * @default "ml.m5.xlarge"
   */
  public SM_CPU_INSTANCE_TYPE: string;

  /**
   * The name of the SageMaker endpoint for the flood model.
   * @default "flood"
   */
  public SM_FLOOD_MODEL: string;

  /**
   * The name of the SageMaker execution role.
   * @default undefined
   */
  public SM_ROLE_NAME?: string | undefined;

  /**
   * Constructor for TestEndpointsConfig.
   * @param config - The configuration object for TestEndpoints.
   */
  constructor(config: ConfigType = {}) {
    super({
      BUILD_FROM_SOURCE: false,
      CONTAINER_BUILD_PATH: "../",
      CONTAINER_BUILD_TARGET: "osml_model",
      CONTAINER_DOCKERFILE: "docker/Dockerfile.test-models",
      CONTAINER_URI: "awsosml/osml-models:latest",
      ECR_REPOSITORY_TAG: "latest",
      DEPLOY_SM_CENTERPOINT_ENDPOINT: true,
      DEPLOY_SM_FLOOD_ENDPOINT: true,
      DEPLOY_MULTI_CONTAINER_ENDPOINT: true,
      HTTP_ENDPOINT_CPU: 4096,
      HTTP_ENDPOINT_CONTAINER_PORT: 8080,
      HTTP_ENDPOINT_DOMAIN_NAME: "test-http-model-endpoint",
      HTTP_ENDPOINT_NAME: "HTTPModelCluster",
      HTTP_ENDPOINT_HOST_PORT: 8080,
      HTTP_ENDPOINT_HEALTHCHECK_PATH: "/ping",
      HTTP_ENDPOINT_MEMORY: 16384,
      SM_CENTER_POINT_MODEL: "centerpoint",
      SM_FLOOD_MODEL: "flood",
      SM_MULTI_CONTAINER_ENDPOINT: "multi-container",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      ...config
    });
  }
}

/**
 * Represents the properties required to configure an MR (Model Router) model endpoints.
 *
 * @interface TestEndpointsProps
 */
export interface TestEndpointsProps {
  /**
   * The OSML (OversightML) account associated with the model endpoints.
   *
   * @type {OSMLAccount}
   */
  account: OSMLAccount;

  /**
   * The VPC (Virtual Private Cloud) where the model will be deployed.
   *
   * @type {IVpc}
   */
  vpc: IVpc;

  /**
   * The selected subnets within the VPC for deployment.
   *
   * @type {SubnetSelection}
   */
  selectedSubnets: SubnetSelection;

  /**
   * The default security group ID for the VPC.
   *
   * @type {string}
   */
  defaultSecurityGroup?: string;

  /**
   * (Optional) Configuration settings for test model endpoints.
   *
   * @type {TestEndpointsConfig}
   */
  config?: TestEndpointsConfig;

  /**
   * (Optional) A Role to use for the SMEndpoints.
   *
   * @type {IRole}
   */
  smRole?: IRole;
}

/**
 * Represents an AWS CDK Construct for managing Model Registry (MR) endpoints.
 */
export class TestEndpoints extends Construct {
  /**
   * The removal policy for the construct.
   */
  public removalPolicy: RemovalPolicy;

  /**
   * Configuration for MR Model Endpoints.
   */
  public config: TestEndpointsConfig;

  /**
   * Optional HTTP Endpoint role for MR operations.
   */
  public httpEndpointRole?: IRole;

  /**
   * Security Group ID associated with the endpoints.
   */
  public securityGroupId: string;

  // Resource classes
  /** The container resources. */
  public readonly container: ModelContainer;
  /** The SageMaker role. */
  public readonly smRole: SageMakerRole;
  /** The centerpoint model endpoint. */
  public readonly centerpointEndpoint?: CenterpointEndpoint;
  /** The flood model endpoint. */
  public readonly floodEndpoint?: FloodEndpoint;

  /**
   * Creates an TestEndpoints construct.
   * @param {Construct} scope - The scope/stack in which to define this construct.
   * @param {string} id - The id of this construct within the current scope.
   * @param {TestEndpointsProps} props - The properties of this construct.
   * @returns TestEndpoints - The TestEndpoints construct.
   */
  constructor(scope: Construct, id: string, props: TestEndpointsProps) {
    super(scope, id);

    // Initialize configuration and basic properties
    this.config = props.config ?? new TestEndpointsConfig();
    this.removalPolicy = props.account.prodLike
      ? RemovalPolicy.RETAIN
      : RemovalPolicy.DESTROY;

    // Determine security group ID
    this.securityGroupId = this.config.SECURITY_GROUP_ID ?? props.defaultSecurityGroup ?? "";

    // Create resource classes
    this.container = this.createContainer(props);
    this.smRole = this.createSageMakerRole(props);
    this.centerpointEndpoint = this.createCenterpointEndpoint(props);
    this.floodEndpoint = this.createFloodEndpoint(props);
  }

  /**
   * Creates the container resources.
   *
   * @param props - The TestEndpoints properties
   * @returns The container resources
   */
  private createContainer(props: TestEndpointsProps): ModelContainer {
    return new ModelContainer(this, "Container", {
      account: props.account,
      buildFromSource: this.config.BUILD_FROM_SOURCE,
      config: new ModelContainerConfig({
        CONTAINER_URI: this.config.CONTAINER_URI,
        CONTAINER_BUILD_PATH: this.config.CONTAINER_BUILD_PATH,
        CONTAINER_BUILD_TARGET: this.config.CONTAINER_BUILD_TARGET,
        CONTAINER_DOCKERFILE: this.config.CONTAINER_DOCKERFILE,
        ECR_REPOSITORY_ARN: this.config.ECR_REPOSITORY_ARN,
        ECR_REPOSITORY_TAG: this.config.ECR_REPOSITORY_TAG
      })
    });
  }

  /**
   * Creates the SageMaker role.
   *
   * @param props - The TestEndpoints properties
   * @returns The SageMaker role
   */
  private createSageMakerRole(props: TestEndpointsProps): SageMakerRole {
    return new SageMakerRole(this, "SageMakerRole", {
      account: props.account,
      roleName: "SageMakerRole",
      config: new SageMakerRoleConfig({
        SM_ROLE_NAME: this.config.SM_ROLE_NAME
      }),
      existingRole: props.smRole
    });
  }

  /**
   * Creates the centerpoint model endpoint.
   *
   * @param props - The TestEndpoints properties
   * @returns The centerpoint model endpoint
   */
  private createCenterpointEndpoint(props: TestEndpointsProps): CenterpointEndpoint | undefined {
    if (this.config.DEPLOY_SM_CENTERPOINT_ENDPOINT) {
      return new CenterpointEndpoint(this, "CenterpointEndpoint", {
        account: props.account,
        vpc: props.vpc,
        selectedSubnets: props.selectedSubnets,
        defaultSecurityGroup: props.defaultSecurityGroup,
        smRole: this.smRole.role,
        container: this.container,
        config: new CenterpointEndpointConfig({
          DEPLOY_SM_CENTERPOINT_ENDPOINT: this.config.DEPLOY_SM_CENTERPOINT_ENDPOINT,
          SM_CENTER_POINT_MODEL: this.config.SM_CENTER_POINT_MODEL,
          SM_CPU_INSTANCE_TYPE: this.config.SM_CPU_INSTANCE_TYPE,
          SECURITY_GROUP_ID: this.securityGroupId
        })
      });
    }
    return undefined;
  }

  /**
   * Creates the flood model endpoint.
   *
   * @param props - The TestEndpoints properties
   * @returns The flood model endpoint
   */
  private createFloodEndpoint(props: TestEndpointsProps): FloodEndpoint | undefined {
    if (this.config.DEPLOY_SM_FLOOD_ENDPOINT) {
      return new FloodEndpoint(this, "FloodEndpoint", {
        account: props.account,
        vpc: props.vpc,
        selectedSubnets: props.selectedSubnets,
        defaultSecurityGroup: props.defaultSecurityGroup,
        smRole: this.smRole.role,
        container: this.container,
        config: new FloodEndpointConfig({
          DEPLOY_SM_FLOOD_ENDPOINT: this.config.DEPLOY_SM_FLOOD_ENDPOINT,
          SM_FLOOD_MODEL: this.config.SM_FLOOD_MODEL,
          SM_CPU_INSTANCE_TYPE: this.config.SM_CPU_INSTANCE_TYPE,
          SECURITY_GROUP_ID: this.securityGroupId
        })
      });
    }
    return undefined;
  }
}
