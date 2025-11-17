/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import {
  CfnEndpoint,
  CfnEndpointConfig,
  CfnModel
} from "aws-cdk-lib/aws-sagemaker";
import { Construct } from "constructs";

import { BaseConfig, ConfigType } from "../types";

/**
 * Configuration for a container in a SageMaker model
 */
export interface ContainerDefinition {
  /**
   * The URI of the container image
   */
  imageUri: string;

  /**
   * Environment variables for the container
   */
  environment?: Record<string, unknown>;

  /**
   * Repository access mode for the container
   */
  repositoryAccessMode?: string;

  /**
   * Hostname for the container (required for multi-container Direct mode)
   * If required and not specified, will default to an index-based name
   * example: "container-0"
   */
  containerHostname?: string;
}

/**
 * Configuration class for SageMakerInference Construct.
 */
export class SageMakerInferenceConfig extends BaseConfig {
  /**
   * The initial number of instances to run for the endpoint.
   * @default 1
   */
  public INITIAL_INSTANCE_COUNT: number;

  /**
   * The initial weight for the model variant when using traffic splitting.
   * @default 1
   */
  public INITIAL_VARIANT_WEIGHT: number;

  /**
   * The name of the model variant.
   * @default "AllTraffic"
   */
  public VARIANT_NAME: string;

  /**
   * The security group ID to associate with the endpoint.
   */
  public SECURITY_GROUP_ID: string;

  /**
   * List of container definitions for the model
   */
  public CONTAINERS: ContainerDefinition[];

  /**
   * A JSON object which includes ENV variables to be put into the model container.
   * @deprecated Use CONTAINERS (ContainerDefinition[]) instead
   */
  public CONTAINER_ENV: Record<string, unknown>;

  /**
   * The repository access mode to use for the SageMaker endpoint container.
   * @deprecated Use CONTAINERS (ContainerDefinition) instead
   */
  public REPOSITORY_ACCESS_MODE: string;
  /**
   * Creates an instance of SageMakerInferenceConfig.
   * @param config - The configuration object for SageMakerInference.
   */
  constructor(config: ConfigType = {}) {
    super({
      INITIAL_INSTANCE_TYPE: 1,
      INITIAL_VARIANT_WEIGHT: 1,
      INITIAL_INSTANCE_COUNT: 1,
      VARIANT_NAME: "AllTraffic",
      CONTAINERS: [],
      ...config
    });

    // Convert deprecated interface to container list if needed
    if (
      this.CONTAINERS.length === 0 &&
      config.CONTAINER_ENV !== undefined &&
      typeof config.CONTAINER_ENV === "object" &&
      config.CONTAINER_ENV !== null
    ) {
      this.CONTAINERS = [
        {
          imageUri: "", // Populated later with props.containerImageUri
          environment: config.CONTAINER_ENV as Record<string, unknown>,
          repositoryAccessMode:
            (config.REPOSITORY_ACCESS_MODE as string) || "Platform"
        }
      ];
    } else if (this.CONTAINERS.length === 0) {
      // Ensure we always have a CONTAINERS array - default to an empty container definition
      this.CONTAINERS = [
        {
          imageUri: "",
          environment: {} as Record<string, unknown>,
          repositoryAccessMode: "Platform"
        }
      ];
    }
  }
}

/**
 * Represents the properties required to configure an OSML model inference endpoint.
 *
 * @interface SageMakerInferenceProps
 */
export interface SageMakerInferenceProps {
  /**
   * The Amazon Resource Name (ARN) of the role that provides permissions for the endpoint.
   *
   * @type {string}
   */
  roleArn: string;

  /**
   * The URI of the Amazon Elastic Container Registry (ECR) container image.
   *
   * @type {string}
   */
  containerImageUri: string;

  /**
   * The name of the machine learning model.
   *
   * @type {string}
   */
  modelName: string;

  /**
   * The instance type for the endpoint.
   *
   * @type {string}
   */
  instanceType: string;

  /**
   * The instance type for the endpoint.
   *
   * @type {string}
   */
  subnetIds: string[];

  /**
   * (Optional) Configuration settings for SageMakerInference resources.
   *
   * @type {SageMakerInferenceConfig}
   */
  config?: SageMakerInferenceConfig | SageMakerInferenceConfig[];
}

/**
 * Represents an AWS SageMaker inference endpoint for a specified model.
 */
export class SageMakerInference extends Construct {
  /**
   * The SageMaker endpoint configuration.
   */
  public endpointConfig: CfnEndpointConfig;

  /**
   * The SageMaker endpoint.
   */
  public endpoint: CfnEndpoint;

  /**
   * The configuration for the SageMakerInference.
   */
  public config: SageMakerInferenceConfig[];

  /**
   * Creates a SageMaker inference endpoint for the specified model.
   *
   * @param {Construct} scope - The scope/stack in which to define this construct.
   * @param {string} id - The id of this construct within the current scope.
   * @param {SageMakerInferenceProps} props - The properties of this construct.
   * @returns SageMakerInference - The SageMakerInference construct.
   */
  constructor(scope: Construct, id: string, props: SageMakerInferenceProps) {
    super(scope, id);

    // Check if a custom configuration was provided for the model container
    if (!props.config) {
      this.config = [new SageMakerInferenceConfig()];
    } else if (Array.isArray(props.config)) {
      this.config = props.config;
    } else {
      this.config = [props.config];
    }

    const models = this.config.map((config) => {
      // Set the imageUri for containers that don't have one specified. This
      //  handles the legacy conversion case where imageUri was initially empty.
      config.CONTAINERS = config.CONTAINERS.map((container) => ({
        ...container,
        imageUri: container.imageUri || props.containerImageUri
      }));

      // Map to the SageMaker container format
      const containers = config.CONTAINERS.map((container, index) => ({
        image: container.imageUri,
        environment: container.environment || {},
        imageConfig: {
          repositoryAccessMode: container.repositoryAccessMode || "Platform"
        },
        containerHostname: container.containerHostname || `container-${index}`
      }));

      return new CfnModel(this, `${id}-${config.VARIANT_NAME}`, {
        executionRoleArn: props.roleArn,
        containers: containers,
        inferenceExecutionConfig:
          containers.length > 1 ? { mode: "Direct" } : undefined,
        vpcConfig: {
          subnets: props.subnetIds,
          securityGroupIds: [config.SECURITY_GROUP_ID]
        }
      });
    });

    this.endpointConfig = new CfnEndpointConfig(this, `${id}-EndpointConfig`, {
      productionVariants: this.config.map((config, i) => ({
        initialInstanceCount: config.INITIAL_INSTANCE_COUNT,
        initialVariantWeight: config.INITIAL_VARIANT_WEIGHT,
        instanceType: props.instanceType,
        modelName: models[i].attrModelName,
        variantName: config.VARIANT_NAME
      })),
      tags: [
        { key: "Name", value: props.modelName },
        { key: "Timestamp", value: new Date().toISOString() }
      ]
    });

    this.endpoint = new CfnEndpoint(this, `${id}-Endpoint`, {
      endpointConfigName: this.endpointConfig.attrEndpointConfigName,
      endpointName: props.modelName
    });
  }
}
