/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy, SymlinkFollowMode } from "aws-cdk-lib";
import { IRepository, Repository } from "aws-cdk-lib/aws-ecr";
import { DockerImageAsset } from "aws-cdk-lib/aws-ecr-assets";
import { ContainerImage } from "aws-cdk-lib/aws-ecs";
import { DockerImageCode } from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import * as fs from "fs";
import * as path from "path";

import { OSMLAccount, BaseConfig, ConfigType, RegionalConfig } from "../types";

/**
 * Configuration class for ModelContainer Construct.
 */
export class ModelContainerConfig extends BaseConfig {
  /**
   * The default container image.
   * Can only be specified if ECR_REPOSITORY_ARN is not set.
   */
  public CONTAINER_URI?: string;

  /**
   * The build path for the container.
   * @default "../.."
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
   * Constructor for ModelContainerConfig.
   * @param config - The configuration object for ModelContainer
   */
  constructor(config: ConfigType = {}) {
    super({
      CONTAINER_BUILD_PATH: "../",
      CONTAINER_BUILD_TARGET: "osml_model",
      CONTAINER_DOCKERFILE: "docker/Dockerfile.test-models",
      CONTAINER_URI: "awsosml/osml-models:latest",
      ECR_REPOSITORY_TAG: "latest",
      ...config
    });
  }
}

/**
 * Interface representing properties for configuring the ModelContainer Construct.
 */
export interface ModelContainerProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** (Optional) Flag to instruct building the Docker image code for the container. */
  readonly buildDockerImageCode?: boolean;
  /** (Optional) Flag to instruct building the model container from source. */
  readonly buildFromSource?: boolean;
  /** (Optional) Custom configuration for the ModelContainer Construct. */
  readonly config?: ModelContainerConfig;
}

/**
 * Represents a ModelContainer construct responsible for managing container
 * resources for test model endpoints.
 */
export class ModelContainer extends Construct {
  /**
   * The container image used for the Construct.
   */
  public containerImage: ContainerImage;

  /**
   * The URI of the container image.
   */
  public containerUri: string;

  /**
   * The removal policy for the Construct resources.
   */
  public removalPolicy: RemovalPolicy;

  /**
   * Configuration for the Construct.
   */
  public config: ModelContainerConfig;

  /**
   * The ECR repository for the container image.
   */
  public repository: IRepository;

  /**
   * The Docker image code for Lambda functions.
   */
  public dockerImageCode: DockerImageCode;

  /**
   * The Docker image asset for the container.
   */
  public dockerImageAsset: DockerImageAsset;

  /**
   * Regional config imputed for this resource.
   */
  public regionConfig: any;

  /**
   * The repository access mode to assign when building model containers for SageMaker.
   */
  public repositoryAccessMode: string = "Platform";

  /**
   * The repository access mode to assign when building model containers for SageMaker.
   */
  public buildDockerImageCode?: boolean | undefined;

  /**
   * Creates an instance of ModelContainer.
   * @param {Construct} scope - The scope/stack in which to define this construct.
   * @param {string} id - The id of this construct within the current scope.
   * @param {ModelContainerProps} props - The properties of this construct.
   * @returns ModelContainer - The ModelContainer instance.
   */
  constructor(scope: Construct, id: string, props: ModelContainerProps) {
    super(scope, id);

    // Set the removal policy based on the account type
    this.removalPolicy = props.account.prodLike
      ? RemovalPolicy.RETAIN
      : RemovalPolicy.DESTROY;

    // Set whether to build Docker image code for this resource
    this.buildDockerImageCode = props.buildDockerImageCode;

    // Check if a custom configuration was provided
    this.config = props.config ?? new ModelContainerConfig();

    // Impute a regional config to use for the resources
    this.regionConfig = RegionalConfig.getConfig(props.account.region);

    // Build the container resources
    if (props.buildFromSource) {
      this.buildFromSource();
    } else {
      this.buildFromRepository();
    }
  }

  /**
   * Builds the container from source using the Dockerfile and build path specified in the config.
   * Validates the configuration to ensure that the necessary parameters are provided.
   *
   * @throws {Error} If CONTAINER_DOCKERFILE or CONTAINER_BUILD_PATH are not set in the configuration.
   */
  private buildFromSource(): void {
    // Validate that both the Dockerfile and build path are provided in the configuration.
    if (
      !this.config.CONTAINER_DOCKERFILE ||
      !this.config.CONTAINER_BUILD_PATH
    ) {
      throw new Error(
        "CONTAINER_DOCKERFILE and CONTAINER_BUILD_PATH must be set in the configuration to build from source."
      );
    }

    // Create a DockerImageAsset, which builds a Docker image from the specified Dockerfile and build path.
    this.dockerImageAsset = new DockerImageAsset(this, `DockerImageAsset`, {
      directory: this.config.CONTAINER_BUILD_PATH,
      file: this.config.CONTAINER_DOCKERFILE,
      followSymlinks: SymlinkFollowMode.ALWAYS,
      target: this.config.CONTAINER_BUILD_TARGET
    });

    // Create a ContainerImage object from the DockerImageAsset, which will be used by ECS services.
    this.containerImage = ContainerImage.fromDockerImageAsset(
      this.dockerImageAsset
    );

    // Store the URI of the built Docker image in the containerUri property for later use.
    this.containerUri = this.dockerImageAsset.imageUri;

    if (this.buildDockerImageCode) {
      // Create a DockerImageCode object for Lambda using the same Docker image built from the source.
      this.dockerImageCode = DockerImageCode.fromImageAsset(
        this.config.CONTAINER_BUILD_PATH,
        {
          file: this.config.CONTAINER_DOCKERFILE,
          followSymlinks: SymlinkFollowMode.ALWAYS,
          target: this.config.CONTAINER_BUILD_TARGET
        }
      );
    }
  }

  /**
   * Builds the container from an existing repository using the specified repository URI or ARN in the config.
   * Ensures that only one of `CONTAINER_URI` or `ECR_REPOSITORY_ARN` is provided.
   *
   * @throws {Error} If both `CONTAINER_URI` and `ECR_REPOSITORY_ARN` are provided or neither is set.
   */
  private buildFromRepository(): void {
    const { CONTAINER_URI, ECR_REPOSITORY_ARN } = this.config;

    this.validateContainerConfig(CONTAINER_URI, ECR_REPOSITORY_ARN);

    if (ECR_REPOSITORY_ARN) {
      this.importRepositoryByArn(ECR_REPOSITORY_ARN);
    } else {
      this.importContainerByUri(CONTAINER_URI!);
    }
  }

  /**
   * Validates the configuration to ensure only one of CONTAINER_URI or ECR_REPOSITORY_ARN is provided.
   *
   * @param {string | undefined} uri - The container URI.
   * @param {string | undefined} arn - The container ARN.
   * @throws {Error} If both or neither are provided.
   */
  private validateContainerConfig(uri?: string, arn?: string): void {
    if (uri && arn) {
      throw new Error(
        "Only one of CONTAINER_URI or ECR_REPOSITORY_ARN can be set."
      );
    }
    if (!uri && !arn) {
      throw new Error(
        "Either CONTAINER_URI or ECR_REPOSITORY_ARN must be set in the configuration."
      );
    }
  }

  /**
   * Imports an ECR repository by ARN.
   *
   * @param {string} arn - The ECR repository ARN.
   */
  private importRepositoryByArn(arn: string): void {
    this.repository = Repository.fromRepositoryArn(
      this,
      "ImportedECRRepo",
      arn
    );
    this.containerImage = ContainerImage.fromEcrRepository(
      this.repository,
      this.config.ECR_REPOSITORY_TAG
    );
    this.containerUri = this.repository.repositoryUri;

    if (this.buildDockerImageCode) {
      this.dockerImageCode = DockerImageCode.fromEcr(this.repository);
    }
  }

  /**
   * Imports a container by URI, handling ECR repositories and external registries.
   *
   * @param {string} uri - The container URI.
   */
  private importContainerByUri(uri: string): void {
    if (uri.includes("dkr.ecr")) {
      this.importEcrContainer(uri);
    } else {
      this.importExternalContainer(uri);
    }
  }

  /**
   * Imports a container from an ECR repository.
   *
   * @param {string} uri - The container URI.
   */
  private importEcrContainer(uri: string): void {
    // Extract the repository name and tag from the URI
    const tag = this.extractTagFromUri(uri);
    const repoName = uri.split("/").pop()?.split(":")[0] ?? "";

    this.repository = Repository.fromRepositoryName(
      this,
      "ImportedECRRepo",
      repoName
    );
    this.containerImage = ContainerImage.fromEcrRepository(
      this.repository,
      tag
    );
    this.containerUri = `${this.repository.repositoryUri}:${tag}`;

    if (this.buildDockerImageCode) {
      this.dockerImageCode = DockerImageCode.fromEcr(this.repository, {
        tagOrDigest: tag
      });
    }
  }

  /**
   * Imports a container from an external registry such as Docker Hub.
   *
   * @param {string} uri - The container URI.
   */
  private importExternalContainer(uri: string): void {
    // Set the repository access mode to Vpc for external registries like Docker Hub.
    // SageMaker requires Vpc mode for non-ECR images.
    this.repositoryAccessMode = "Vpc";
    this.containerImage = ContainerImage.fromRegistry(uri);
    this.containerUri = uri;

    if (this.buildDockerImageCode) {
      this.createDockerImageFromRegistry(uri);
    }
  }

  /**
   * Creates a temporary Dockerfile to use an external registry image.
   *
   * @param {string} uri - The container URI.
   */
  private createDockerImageFromRegistry(uri: string): void {
    const tmpDockerfile = path.join(__dirname, "Dockerfile.tmp");
    fs.writeFileSync(tmpDockerfile, `FROM ${uri}`);

    this.dockerImageCode = DockerImageCode.fromImageAsset(__dirname, {
      file: "Dockerfile.tmp",
      followSymlinks: SymlinkFollowMode.ALWAYS
    });
  }

  /**
   * Extracts the tag from a container URI.
   * If no tag is found, returns `"latest"` as the default.
   *
   * @param {string} uri - The container URI.
   * @returns {string} The extracted tag or `"latest"` if none is found.
   */
  private extractTagFromUri(uri: string): string {
    if (!uri) {
      throw new Error("Container URI is undefined or empty.");
    }

    // Find the last `:` in the URI to check for a tag
    const tagSeparatorIndex = uri.lastIndexOf(":");
    const lastSlashIndex = uri.lastIndexOf("/");

    // Ensure the colon comes after the last slash (indicating a tag, not a protocol separator)
    if (tagSeparatorIndex > lastSlashIndex) {
      return uri.substring(tagSeparatorIndex + 1);
    }

    // Default if no tag is found
    return "latest";
  }
}
