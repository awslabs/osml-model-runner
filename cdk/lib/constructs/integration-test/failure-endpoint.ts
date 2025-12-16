/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { ISecurityGroup, IVpc, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { IRole } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

import { BaseConfig, ConfigType, OSMLAccount } from "../types";
import { ModelContainer } from "./model-container";
import {
  SageMakerInference,
  SageMakerInferenceConfig
} from "./sagemaker-inference";

/**
 * Configuration class for FailureEndpoint Construct.
 */
export class FailureEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the SageMaker failure model endpoint.
   * @default true
   */
  public DEPLOY_SM_FAILURE_ENDPOINT: boolean;

  /**
   * The name of the SageMaker endpoint for the failure model.
   * @default "failure"
   */
  public SM_FAILURE_MODEL: string;

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
   * Constructor for FailureEndpointConfig.
   * @param config - The configuration object for FailureEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_SM_FAILURE_ENDPOINT: true,
      SM_FAILURE_MODEL: "failure",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the FailureEndpoint Construct.
 */
export interface FailureEndpointProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The VPC where the model will be deployed. */
  readonly vpc: IVpc;
  /** The selected subnets within the VPC for deployment. */
  readonly selectedSubnets: SubnetSelection;
  /** The default security group for the VPC. */
  readonly securityGroup?: ISecurityGroup;
  /** The SageMaker execution role. */
  readonly smRole: IRole;
  /** The OSML container. */
  readonly container: ModelContainer;
  /** Custom configuration for the FailureEndpoint Construct (optional). */
  readonly config?: FailureEndpointConfig;
}

/**
 * Represents a FailureEndpoint construct responsible for managing the
 * failure model SageMaker endpoint.
 */
export class FailureEndpoint extends Construct {
  /** The configuration for the FailureEndpoint. */
  public readonly config: FailureEndpointConfig;
  /** The failure model endpoint. */
  public readonly endpoint?: SageMakerInference;

  /**
   * Constructs an instance of FailureEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: FailureEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new FailureEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_SM_FAILURE_ENDPOINT) {
      // Create the failure model endpoint
      this.endpoint = new SageMakerInference(this, "FailureModelEndpoint", {
        containerImageUri: props.container.containerUri,
        modelName: this.config.SM_FAILURE_MODEL,
        roleArn: props.smRole.roleArn,
        instanceType: this.config.SM_CPU_INSTANCE_TYPE,
        subnetIds:
          props.selectedSubnets.subnets?.map((subnet) => subnet.subnetId) ?? [],
        config: [
          new SageMakerInferenceConfig({
            CONTAINER_ENV: {
              MODEL_SELECTION: this.config.SM_FAILURE_MODEL
            },
            SECURITY_GROUP_ID:
              this.config.SECURITY_GROUP_ID ??
              props.securityGroup?.securityGroupId ??
              "",
            REPOSITORY_ACCESS_MODE: props.container.repositoryAccessMode
          })
        ]
      });
      this.endpoint.node.addDependency(props.container);
    }
  }
}
