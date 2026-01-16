/*
 * Copyright 2023-2026 Amazon.com, Inc. or its affiliates.
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
 * Configuration class for TestEndpoint Construct.
 */
export class TestEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the SageMaker test model endpoint.
   * @default true
   */
  public DEPLOY_SM_TEST_ENDPOINT: boolean;

  /**
   * The name of the SageMaker endpoint for the test model.
   * @default "test-models"
   */
  public SM_TEST_MODEL: string;

  /**
   * Default model selection for the test endpoint.
   * @default "centerpoint"
   */
  public DEFAULT_MODEL_SELECTION: string;

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
   * Constructor for TestEndpointConfig.
   * @param config - The configuration object for TestEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_SM_TEST_ENDPOINT: true,
      SM_TEST_MODEL: "test-models",
      DEFAULT_MODEL_SELECTION: "centerpoint",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the TestEndpoint Construct.
 */
export interface TestEndpointProps {
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
  /** Custom configuration for the TestEndpoint Construct (optional). */
  readonly config?: TestEndpointConfig;
}

/**
 * Represents a TestEndpoint construct responsible for managing the
 * unified test model SageMaker endpoint.
 */
export class TestEndpoint extends Construct {
  /** The configuration for the TestEndpoint. */
  public readonly config: TestEndpointConfig;
  /** The test model endpoint. */
  public readonly endpoint?: SageMakerInference;

  /**
   * Constructs an instance of TestEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: TestEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new TestEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_SM_TEST_ENDPOINT) {
      // Create the test model endpoint
      this.endpoint = new SageMakerInference(this, "TestModelEndpoint", {
        containerImageUri: props.container.containerUri,
        modelName: this.config.SM_TEST_MODEL,
        roleArn: props.smRole.roleArn,
        instanceType: this.config.SM_CPU_INSTANCE_TYPE,
        subnetIds:
          props.selectedSubnets.subnets?.map((subnet) => subnet.subnetId) ?? [],
        config: [
          new SageMakerInferenceConfig({
            CONTAINER_ENV: {
              DEFAULT_MODEL_SELECTION: this.config.DEFAULT_MODEL_SELECTION,
              ENABLE_SEGMENTATION: "true"
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
