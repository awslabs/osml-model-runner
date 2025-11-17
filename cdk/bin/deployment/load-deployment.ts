/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Utility to load and validate the deployment configuration file.
 *
 * This module provides a strongly typed interface for reading the `deployment.json`
 * configuration, performing required validations, and returning a structured result.
 *
 * Expected structure of `deployment.json`:
 * ```json
 * {
 *   "projectName": "example-stack",
 *   "account": {
 *     "id": "123456789012",
 *     "region": "us-west-2",
 *     "prodLike": false,  // Optional: defaults to false if not specified
 *     "isAdc": false      // Optional: defaults to false if not specified
 *   },
 *   "networkConfig": {
 *     "vpcId": "vpc-abc123",  // Optional: if not provided, a new VPC will be created
 *     "targetSubnets": ["subnet-12345", "subnet-67890"],  // Required when vpcId is provided: specific subnets to use
 *     "securityGroupId": "sg-1234567890abcdef0"  // Optional: security group for test endpoints
 *   },
 *   "dataplaneConfig": {
 *     "CONTAINER_URI": "awsosml/osml-model-runner:latest",
 *     "ECS_TASK_CPU": 2048,
 *     "ECS_TASK_MEMORY": 4096
 *   },
 *   "deployIntegrationTests": true,  // Optional: whether to deploy integration test infrastructure
 *   "testModelsConfig": {
 *     "BUILD_FROM_SOURCE": true,  // Optional: build containers from source instead of pulling from Docker Hub
 *     "CONTAINER_URI": "awsosml/osml-models:latest"  // Optional: container image to use
 *   },
 *   "testImageryConfig": {
 *     "S3_IMAGE_BUCKET_PREFIX": "mr-test-imagery",  // Optional: S3 bucket prefix for test imagery
 *     "S3_TEST_IMAGES_PATH": "assets/imagery/",  // Optional: local path to test images
 *     "DEPLOYMENT_ROLE_NAME": "my-deployment-role"  // Optional: IAM role name for bucket deployment
 *   }
 * }
 * ```
 *
 * @packageDocumentation
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";

import { TestImageryConfig } from "../../lib/constructs/integration-test/test-imagery";
import { TestModelsConfig } from "../../lib/constructs/integration-test/test-models";
import { NetworkConfig } from "../../lib/constructs/model-runner/network";

/**
 * Represents the structure of the deployment configuration file.
 */
export interface DeploymentConfig {
  /** Logical name of the project, used for the CDK stack ID. */
  projectName: string;

  /** AWS account configuration. */
  account: {
    /** AWS Account ID. */
    id: string;
    /** AWS region for deployment. */
    region: string;
    /** Whether the account is prod-like. Defaults to false if not specified. */
    prodLike?: boolean;
    /** Whether this is an ADC (Application Data Center) environment. Defaults to false if not specified. */
    isAdc?: boolean;
  };
  /** Networking configuration. If VPC_ID is provided, an existing VPC will be imported. Otherwise, a new VPC will be created. */
  networkConfig?: NetworkConfig;

  /** Optional Dataplane configuration. Can be a partial config object passed to DataplaneConfig constructor. */
  dataplaneConfig?: Partial<Record<string, unknown>>;

  /** Whether to deploy integration test infrastructure (test models and test imagery stacks). */
  deployIntegrationTests?: boolean;

  /** Optional Test Models configuration. */
  testModelsConfig?: TestModelsConfig;

  /** Optional Test Imagery configuration. */
  testImageryConfig?: TestImageryConfig;
}

/**
 * Validation error class for deployment configuration issues.
 */
class DeploymentConfigError extends Error {
  /**
   * Creates a new DeploymentConfigError.
   *
   * @param message - The error message
   * @param field - Optional field name that caused the error
   */
  constructor(
    message: string,
    // eslint-disable-next-line no-unused-vars
    public field?: string
  ) {
    super(message);
    this.name = "DeploymentConfigError";
  }
}

/**
 * Validates and trims a string field, checking for required value and whitespace.
 *
 * @param value - The value to validate
 * @param fieldName - The name of the field being validated (for error messages)
 * @param isRequired - Whether the field is required (default: true)
 * @returns The trimmed string value
 * @throws {DeploymentConfigError} If validation fails
 */
function validateStringField(
  value: unknown,
  fieldName: string,
  isRequired: boolean = true
): string {
  if (value === undefined || value === null) {
    if (isRequired) {
      throw new DeploymentConfigError(
        `Missing required field: ${fieldName}`,
        fieldName
      );
    }
    return "";
  }

  if (typeof value !== "string") {
    throw new DeploymentConfigError(
      `Field '${fieldName}' must be a string, got ${typeof value}`,
      fieldName
    );
  }

  const trimmed = value.trim();
  if (isRequired && trimmed === "") {
    throw new DeploymentConfigError(
      `Field '${fieldName}' cannot be empty or contain only whitespace`,
      fieldName
    );
  }

  return trimmed;
}

/**
 * Validates AWS account ID format.
 *
 * @param accountId - The account ID to validate
 * @returns The validated account ID
 * @throws {DeploymentConfigError} If the account ID format is invalid
 */
function validateAccountId(accountId: string): string {
  if (!/^\d{12}$/.test(accountId)) {
    throw new DeploymentConfigError(
      `Invalid AWS account ID format: '${accountId}'. Must be exactly 12 digits.`,
      "account.id"
    );
  }
  return accountId;
}

/**
 * Validates AWS region format using pattern matching.
 *
 * @param region - The region to validate
 * @returns The validated region
 * @throws {DeploymentConfigError} If the region format is invalid
 */
function validateRegion(region: string): string {
  // AWS region pattern: letters/numbers, hyphen, letters/numbers, optional hyphen and numbers
  if (!/^[a-z0-9]+-[a-z0-9]+(?:-[a-z0-9]+)*$/.test(region)) {
    throw new DeploymentConfigError(
      `Invalid AWS region format: '${region}'. Must follow pattern like 'us-east-1', 'eu-west-2', etc.`,
      "account.region"
    );
  }
  return region;
}

/**
 * Validates VPC ID format.
 *
 * @param vpcId - The VPC ID to validate
 * @returns The validated VPC ID
 * @throws {DeploymentConfigError} If the VPC ID format is invalid
 */
function validateVpcId(vpcId: string): string {
  if (!/^vpc-[a-f0-9]{8}(?:[a-f0-9]{9})?$/.test(vpcId)) {
    throw new DeploymentConfigError(
      `Invalid VPC ID format: '${vpcId}'. Must start with 'vpc-' followed by 8 or 17 hexadecimal characters.`,
      "vpcConfig.vpcId"
    );
  }
  return vpcId;
}

/**
 * Validates security group ID format.
 *
 * @param securityGroupId - The security group ID to validate
 * @returns The validated security group ID
 * @throws {DeploymentConfigError} If the security group ID format is invalid
 */
function validateSecurityGroupId(securityGroupId: string): string {
  if (!/^sg-[a-f0-9]{8}(?:[a-f0-9]{9})?$/.test(securityGroupId)) {
    throw new DeploymentConfigError(
      `Invalid security group ID format: '${securityGroupId}'. Must start with 'sg-' followed by 8 or 17 hexadecimal characters.`,
      "vpcConfig.securityGroupId"
    );
  }
  return securityGroupId;
}

/**
 * Loads and validates the deployment configuration from `deployment/deployment.json`.
 *
 * @returns A validated {@link DeploymentConfig} object
 * @throws {DeploymentConfigError} If the file is missing, malformed, or contains invalid values
 */
export function loadDeploymentConfig(): DeploymentConfig {
  const deploymentPath = join(__dirname, "deployment.json");

  if (!existsSync(deploymentPath)) {
    throw new DeploymentConfigError(
      `Missing deployment.json file at ${deploymentPath}. Please create it by copying deployment.json.example`
    );
  }

  let parsed: unknown;
  try {
    const rawContent = readFileSync(deploymentPath, "utf-8");
    parsed = JSON.parse(rawContent) as unknown;
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new DeploymentConfigError(
        `Invalid JSON format in deployment.json: ${error.message}`
      );
    }
    throw new DeploymentConfigError(
      `Failed to read deployment.json: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }

  // Validate top-level structure
  if (!parsed || typeof parsed !== "object" || parsed === null) {
    throw new DeploymentConfigError(
      "deployment.json must contain a valid JSON object"
    );
  }

  const parsedObj = parsed as Record<string, unknown>;

  // Validate project name
  const projectName = validateStringField(parsedObj.projectName, "projectName");
  if (projectName.length === 0) {
    throw new DeploymentConfigError("projectName cannot be empty");
  }

  // Validate account section
  if (!parsedObj.account || typeof parsedObj.account !== "object") {
    throw new DeploymentConfigError(
      "Missing or invalid account section in deployment.json",
      "account"
    );
  }

  const accountObj = parsedObj.account as Record<string, unknown>;

  const accountId = validateAccountId(
    validateStringField(accountObj.id, "account.id")
  );
  const region = validateRegion(
    validateStringField(accountObj.region, "account.region")
  );

  // Parse and validate networking configuration
  let networkConfig: DeploymentConfig["networkConfig"] = undefined;
  if (
    "networkConfig" in parsedObj &&
    parsedObj.networkConfig &&
    typeof parsedObj.networkConfig === "object"
  ) {
    // Convert the parsed networkConfig to the format expected by NetworkConfig
    const networkConfigData: Record<string, unknown> = {};
    const parsedNetworkConfig = parsedObj.networkConfig as Record<
      string,
      unknown
    >;

    // Map vpcId to VPC_ID
    if (
      parsedNetworkConfig.vpcId !== undefined &&
      parsedNetworkConfig.vpcId !== null
    ) {
      networkConfigData.VPC_ID = validateVpcId(
        validateStringField(parsedNetworkConfig.vpcId, "networkConfig.vpcId")
      );
    }

    // Map targetSubnets to TARGET_SUBNETS
    if (
      parsedNetworkConfig.targetSubnets !== undefined &&
      parsedNetworkConfig.targetSubnets !== null
    ) {
      if (Array.isArray(parsedNetworkConfig.targetSubnets)) {
        networkConfigData.TARGET_SUBNETS =
          parsedNetworkConfig.targetSubnets.map(
            (subnetId: unknown, index: number) =>
              validateStringField(
                subnetId,
                `networkConfig.targetSubnets[${index}]`
              )
          );
      } else {
        throw new DeploymentConfigError(
          "Field 'networkConfig.targetSubnets' must be an array",
          "networkConfig.targetSubnets"
        );
      }
    }

    // Map securityGroupId to SECURITY_GROUP_ID
    if (
      parsedNetworkConfig.securityGroupId !== undefined &&
      parsedNetworkConfig.securityGroupId !== null
    ) {
      networkConfigData.SECURITY_GROUP_ID = validateSecurityGroupId(
        validateStringField(
          parsedNetworkConfig.securityGroupId,
          "networkConfig.securityGroupId"
        )
      );
    }

    // Validate that TARGET_SUBNETS is required when VPC_ID is provided
    if (
      networkConfigData.VPC_ID &&
      (!networkConfigData.TARGET_SUBNETS ||
        !Array.isArray(networkConfigData.TARGET_SUBNETS) ||
        networkConfigData.TARGET_SUBNETS.length === 0)
    ) {
      throw new DeploymentConfigError(
        "When vpcId is provided, targetSubnets must also be specified with at least one subnet ID",
        "networkConfig.targetSubnets"
      );
    }

    // Create NetworkConfig instance
    networkConfig = new NetworkConfig(networkConfigData);
  }

  // Parse optional Dataplane configuration
  let dataplaneConfig: DeploymentConfig["dataplaneConfig"] = undefined;
  if (
    parsedObj.dataplaneConfig &&
    typeof parsedObj.dataplaneConfig === "object" &&
    parsedObj.dataplaneConfig !== null
  ) {
    dataplaneConfig = parsedObj.dataplaneConfig as Record<string, unknown>;
  }

  // Parse optional deployIntegrationTests flag
  let deployIntegrationTests: boolean = false;
  if (
    parsedObj.deployIntegrationTests !== undefined &&
    typeof parsedObj.deployIntegrationTests === "boolean"
  ) {
    deployIntegrationTests = parsedObj.deployIntegrationTests;
  }

  // Parse optional testModelsConfig
  let testModelsConfig: TestModelsConfig | undefined = undefined;
  if (
    parsedObj.testModelsConfig &&
    typeof parsedObj.testModelsConfig === "object"
  ) {
    testModelsConfig = new TestModelsConfig(
      parsedObj.testModelsConfig as Record<string, unknown>
    );
  }

  // Parse optional testImageryConfig
  let testImageryConfig: TestImageryConfig | undefined = undefined;
  if (
    parsedObj.testImageryConfig &&
    typeof parsedObj.testImageryConfig === "object"
  ) {
    testImageryConfig = new TestImageryConfig(
      parsedObj.testImageryConfig as Record<string, unknown>
    );
  }

  const validatedConfig: DeploymentConfig = {
    projectName,
    account: {
      id: accountId,
      region: region,
      prodLike: (accountObj.prodLike as boolean | undefined) ?? false,
      isAdc: (accountObj.isAdc as boolean | undefined) ?? false
    },
    networkConfig,
    dataplaneConfig: dataplaneConfig,
    deployIntegrationTests: deployIntegrationTests,
    testModelsConfig: testModelsConfig,
    testImageryConfig: testImageryConfig
  };

  // Only log non-sensitive configuration details (prevent duplicate logging)
  const globalObj = global as { __deploymentConfigLoaded?: boolean };
  if (!globalObj.__deploymentConfigLoaded) {
    console.log(
      `🚀 Using environment from deployment.json: projectName=${validatedConfig.projectName}, region=${validatedConfig.account.region}`
    );
    globalObj.__deploymentConfigLoaded = true;
  }

  return validatedConfig;
}
