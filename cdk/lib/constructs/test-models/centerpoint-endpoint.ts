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
 * Configuration class for CenterpointEndpoint Construct.
 */
export class CenterpointEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the SageMaker centerpoint model endpoint.
   * @default true
   */
  public DEPLOY_SM_CENTERPOINT_ENDPOINT: boolean;

  /**
   * The name of the SageMaker endpoint for the centerpoint model.
   * @default "centerpoint"
   */
  public SM_CENTER_POINT_MODEL: string;

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
   * Constructor for CenterpointEndpointConfig.
   * @param config - The configuration object for CenterpointEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_SM_CENTERPOINT_ENDPOINT: true,
      SM_CENTER_POINT_MODEL: "centerpoint",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the CenterpointEndpoint Construct.
 */
export interface CenterpointEndpointProps {
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
  /** Custom configuration for the CenterpointEndpoint Construct (optional). */
  readonly config?: CenterpointEndpointConfig;
}

/**
 * Represents a CenterpointEndpoint construct responsible for managing the
 * centerpoint model SageMaker endpoint.
 */
export class CenterpointEndpoint extends Construct {
  /** The configuration for the CenterpointEndpoint. */
  public readonly config: CenterpointEndpointConfig;
  /** The centerpoint model endpoint. */
  public readonly endpoint?: SageMakerEndpoint;

  /**
   * Constructs an instance of CenterpointEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: CenterpointEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new CenterpointEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_SM_CENTERPOINT_ENDPOINT) {
      // Determine security group ID
      const securityGroupId = this.config.SECURITY_GROUP_ID ?? props.defaultSecurityGroup ?? "";

      // Create the centerpoint model endpoint
      this.endpoint = new SageMakerEndpoint(
        this,
        "CenterpointModelEndpoint",
        {
          containerImageUri: props.container.containerUri,
          modelName: this.config.SM_CENTER_POINT_MODEL,
          roleArn: props.smRole.roleArn,
          instanceType: this.config.SM_CPU_INSTANCE_TYPE,
          subnetIds: props.selectedSubnets.subnets?.map((subnet) => subnet.subnetId) ?? [],
          config: [
            new SageMakerEndpointConfig({
              CONTAINER_ENV: {
                MODEL_SELECTION: this.config.SM_CENTER_POINT_MODEL
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
