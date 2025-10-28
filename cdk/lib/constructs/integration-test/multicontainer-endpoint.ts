/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { IRole } from "aws-cdk-lib/aws-iam";
import { IVpc, ISecurityGroup, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

import { OSMLAccount, BaseConfig, ConfigType } from "../types";
import { SageMakerInference, SageMakerInferenceConfig, ContainerDefinition } from "./sagemaker-inference";
import { ModelContainer } from "./model-container";

/**
 * Configuration for a single container in the multi-container endpoint.
 */
export interface ContainerConfig {
  /** The model selection name (e.g., "centerpoint", "flood", "aircraft"). */
  modelSelection: string;
  /** The hostname for the container (e.g., "centerpoint-container"). */
  hostname: string;
}

/**
 * Configuration class for MulticontainerEndpoint Construct.
 */
export class MulticontainerEndpointConfig extends BaseConfig {
  /**
   * Whether to deploy the SageMaker multi-container model endpoint.
   * @default true
   */
  public DEPLOY_MULTI_CONTAINER_ENDPOINT: boolean;

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
   * List of models to include in the multi-container endpoint.
   * Each model will run as a separate container.
   * @default [{ modelSelection: "centerpoint", hostname: "centerpoint-container" },
   *            { modelSelection: "flood", hostname: "flood-container" }]
   */
  public MODELS?: ContainerConfig[];

  /**
   * A security group to use for these resources.
   */
  public SECURITY_GROUP_ID?: string | undefined;

  /**
   * Constructor for MulticontainerEndpointConfig.
   * @param config - The configuration object for MulticontainerEndpoint
   */
  constructor(config: ConfigType = {}) {
    super({
      DEPLOY_MULTI_CONTAINER_ENDPOINT: true,
      SM_MULTI_CONTAINER_ENDPOINT: "multi-container",
      SM_CPU_INSTANCE_TYPE: "ml.m5.xlarge",
      MODELS: [
        { modelSelection: "centerpoint", hostname: "centerpoint-container" },
        { modelSelection: "flood", hostname: "flood-container" }
      ],
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the MulticontainerEndpoint Construct.
 */
export interface MulticontainerEndpointProps {
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
  /** Custom configuration for the MulticontainerEndpoint Construct (optional). */
  readonly config?: MulticontainerEndpointConfig;
}

/**
 * Represents a MulticontainerEndpoint construct responsible for managing a
 * multi-container SageMaker endpoint that supports both centerpoint and flood models.
 */
export class MulticontainerEndpoint extends Construct {
  /** The configuration for the MulticontainerEndpoint. */
  public readonly config: MulticontainerEndpointConfig;
  /** The multi-container model endpoint. */
  public readonly endpoint?: SageMakerInference;

  /**
   * Constructs an instance of MulticontainerEndpoint.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: MulticontainerEndpointProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new MulticontainerEndpointConfig();

    // Only create the endpoint if deployment is enabled
    if (this.config.DEPLOY_MULTI_CONTAINER_ENDPOINT) {
      // Determine security group ID
      const securityGroupId = this.config.SECURITY_GROUP_ID ??
        props.securityGroup?.securityGroupId ?? "";

      // Create multi-container definitions from config
      const containers: ContainerDefinition[] = (this.config.MODELS || []).map((model) => {
        const environment: Record<string, unknown> = {
          MODEL_SELECTION: model.modelSelection
        };

        // Enable segmentation for centerpoint model
        if (model.modelSelection === "centerpoint") {
          environment.ENABLE_SEGMENTATION = "true";
        }

        return {
          imageUri: props.container.containerUri,
          environment,
          repositoryAccessMode: props.container.repositoryAccessMode,
          containerHostname: model.hostname
        };
      });

      // Create the multi-container endpoint
      this.endpoint = new SageMakerInference(
        this,
        "MultiContainerEndpoint",
        {
          containerImageUri: props.container.containerUri,
          modelName: this.config.SM_MULTI_CONTAINER_ENDPOINT,
          roleArn: props.smRole.roleArn,
          instanceType: this.config.SM_CPU_INSTANCE_TYPE,
          subnetIds: props.selectedSubnets.subnets?.map((subnet) => subnet.subnetId) ?? [],
          config: [
            new SageMakerInferenceConfig({
              CONTAINERS: containers,
              INITIAL_INSTANCE_COUNT: 1,
              INITIAL_VARIANT_WEIGHT: 1,
              VARIANT_NAME: "AllTraffic",
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
