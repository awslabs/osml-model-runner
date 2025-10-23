/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { IRole } from "aws-cdk-lib/aws-iam";
import { IVpc, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

import { OSMLAccount, BaseConfig, ConfigType } from "../types";
import { SageMakerEndpoint, SageMakerEndpointConfig } from "./sagemaker-endpoint";
import { ModelContainer } from "./model-container";

/**
 * Configuration class for FloodEndpoint Construct.
 */
export class FloodEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the SageMaker flood model endpoint.
   * @default true
   */
  public DEPLOY_SM_FLOOD_ENDPOINT: boolean;

  /**
   * The name of the SageMaker endpoint for the flood model.
   * @default "flood"
   */
  public SM_FLOOD_MODEL: string;

  /**
   * The SageMaker CPU instance type.
   * @default "ml.m5.xlarge"
   */
  public SM_CPU_INSTANCE_TYPE: string;

  /**
   * A security group to use for these resources.
   */
  public SECURITY_GROUP_ID?: string | undefined;

  /**
   * Constructor for FloodEndpointConfig.
   * @param config - The configuration object for FloodEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_SM_FLOOD_ENDPOINT: true,
      SM_FLOOD_MODEL: "flood",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the FloodEndpoint Construct.
 */
export interface FloodEndpointProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The VPC where the model will be deployed. */
  readonly vpc: IVpc;
  /** The selected subnets within the VPC for deployment. */
  readonly selectedSubnets: SubnetSelection;
  /** The default security group ID for the VPC. */
  readonly defaultSecurityGroup?: string;
  /** The SageMaker execution role. */
  readonly smRole: IRole;
  /** The OSML container. */
  readonly container: ModelContainer;
  /** Custom configuration for the FloodEndpoint Construct (optional). */
  readonly config?: FloodEndpointConfig;
}

/**
 * Represents a FloodEndpoint construct responsible for managing the
 * flood model SageMaker endpoint with multiple variants.
 */
export class FloodEndpoint extends Construct {
  /** The configuration for the FloodEndpoint. */
  public readonly config: FloodEndpointConfig;
  /** The flood model endpoint. */
  public readonly endpoint?: SageMakerEndpoint;

  /**
   * Constructs an instance of FloodEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: FloodEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new FloodEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_SM_FLOOD_ENDPOINT) {
      // Determine security group ID
      const securityGroupId = this.config.SECURITY_GROUP_ID ?? props.defaultSecurityGroup ?? "";

      // Create the flood model endpoint with multiple variants
      this.endpoint = new SageMakerEndpoint(
        this,
        "FloodModelEndpoint",
        {
          containerImageUri: props.container.containerUri,
          modelName: this.config.SM_FLOOD_MODEL,
          roleArn: props.smRole.roleArn,
          instanceType: this.config.SM_CPU_INSTANCE_TYPE,
          subnetIds: props.selectedSubnets.subnets?.map((subnet) => subnet.subnetId) ?? [],
          config: [
            new SageMakerEndpointConfig({
              VARIANT_NAME: "flood-50",
              CONTAINER_ENV: {
                FLOOD_VOLUME: 50,
                MODEL_SELECTION: this.config.SM_FLOOD_MODEL
              },
              SECURITY_GROUP_ID: securityGroupId,
              REPOSITORY_ACCESS_MODE: props.container.repositoryAccessMode
            }),
            new SageMakerEndpointConfig({
              VARIANT_NAME: "flood-100",
              CONTAINER_ENV: {
                FLOOD_VOLUME: 100,
                MODEL_SELECTION: this.config.SM_FLOOD_MODEL
              },
              SECURITY_GROUP_ID: securityGroupId,
              REPOSITORY_ACCESS_MODE: props.container.repositoryAccessMode
            })
          ]
        }
      );
      this.endpoint.node.addDependency(props.container);
    }
  }
}
