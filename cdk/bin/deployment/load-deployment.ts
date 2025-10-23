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
 *   "vpcConfig": {
 *     "vpcId": "vpc-abc123",  // Optional: if not provided, a new VPC will be created
 *     "targetSubnets": ["subnet-12345", "subnet-67890"],  // Required when vpcId is provided: specific subnets to use
 *     "securityGroupId": "sg-1234567890abcdef0"  // Optional: security group for test endpoints
 *   },
 *   "dataplaneConfig": {
 *     "CONTAINER_URI": "awsosml/osml-model-runner:latest",
 *     "ECS_TASK_CPU": 2048,
 *     "ECS_TASK_MEMORY": 4096
 *   },
 *   "deployTestModels": true,  // Optional: whether to deploy test models stack
 *   "testEndpointsConfig": {
 *     "BUILD_FROM_SOURCE": true,  // Optional: build containers from source instead of pulling from Docker Hub
 *     "CONTAINER_URI": "awsosml/osml-models:latest"  // Optional: container image to use
 *   }
 * }
 * ```
 *
 * @packageDocumentation
 */

import * as fs from "fs";
import * as path from "path";
import { ModelRunnerDataplaneConfig } from "../../lib/constructs/model-runner/model-runner-dataplane";
import { TestEndpointsConfig } from "../../lib/constructs/test-models/test-endpoints";

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
  /** VPC configuration. If vpcId is provided, an existing VPC will be imported. Otherwise, a new VPC will be created. */
  vpcConfig?: {
    /** The ID of the VPC to import. If not provided, a new VPC will be created. */
    vpcId?: string;
    /** Target subnet IDs to use. Required when vpcId is provided. */
    targetSubnets?: string[];
    /** Security group ID to use for test endpoints. If not provided, no security group will be used. */
    securityGroupId?: string;
  };

  /** Optional Dataplane configuration. */
  dataplaneConfig?: ModelRunnerDataplaneConfig;

  /** Whether to deploy the test models stack. */
  deployTestModels?: boolean;

  /** Optional Test Endpoints configuration. */
  testEndpointsConfig?: TestEndpointsConfig;
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
  value: any,
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
  const deploymentPath = path.join(__dirname, "deployment.json");

  if (!fs.existsSync(deploymentPath)) {
    throw new DeploymentConfigError(
      `Missing deployment.json file at ${deploymentPath}. Please create it by copying deployment.json.example`
    );
  }

  let parsed: any;
  try {
    const rawContent = fs.readFileSync(deploymentPath, "utf-8");
    parsed = JSON.parse(rawContent);
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
  if (!parsed || typeof parsed !== "object") {
    throw new DeploymentConfigError(
      "deployment.json must contain a valid JSON object"
    );
  }

  // Validate project name
  const projectName = validateStringField(parsed.projectName, "projectName");
  if (projectName.length === 0) {
    throw new DeploymentConfigError("projectName cannot be empty");
  }

  // Validate account section
  if (!parsed.account || typeof parsed.account !== "object") {
    throw new DeploymentConfigError(
      "Missing or invalid account section in deployment.json",
      "account"
    );
  }

  const accountId = validateAccountId(
    validateStringField(parsed.account.id, "account.id")
  );
  const region = validateRegion(
    validateStringField(parsed.account.region, "account.region")
  );

  // Parse and validate VPC configuration
  let vpcConfig: DeploymentConfig['vpcConfig'] = undefined;
  if (parsed.vpcConfig && typeof parsed.vpcConfig === 'object') {
    vpcConfig = {};

    // Validate VPC ID if provided
    if (parsed.vpcConfig.vpcId !== undefined && parsed.vpcConfig.vpcId !== null) {
      vpcConfig.vpcId = validateVpcId(
        validateStringField(parsed.vpcConfig.vpcId, "vpcConfig.vpcId")
      );
    }

    // Validate target subnets if provided
    if (parsed.vpcConfig.targetSubnets !== undefined && parsed.vpcConfig.targetSubnets !== null) {
      if (Array.isArray(parsed.vpcConfig.targetSubnets)) {
        vpcConfig.targetSubnets = parsed.vpcConfig.targetSubnets.map((subnetId: any, index: number) =>
          validateStringField(subnetId, `vpcConfig.targetSubnets[${index}]`)
        );
      } else {
        throw new DeploymentConfigError(
          "Field 'vpcConfig.targetSubnets' must be an array",
          "vpcConfig.targetSubnets"
        );
      }
    }

    // Validate security group ID if provided
    if (parsed.vpcConfig.securityGroupId !== undefined && parsed.vpcConfig.securityGroupId !== null) {
      vpcConfig.securityGroupId = validateSecurityGroupId(
        validateStringField(parsed.vpcConfig.securityGroupId, "vpcConfig.securityGroupId")
      );
    }

    // Validate that targetSubnets is required when vpcId is provided
    if (vpcConfig.vpcId && (!vpcConfig.targetSubnets || vpcConfig.targetSubnets.length === 0)) {
      throw new DeploymentConfigError(
        "When vpcId is provided, targetSubnets must also be specified with at least one subnet ID",
        "vpcConfig.targetSubnets"
      );
    }
  }

  // Parse optional Dataplane configuration
  let dataplaneConfig: DeploymentConfig['dataplaneConfig'] = undefined;
  if (parsed.dataplaneConfig && typeof parsed.dataplaneConfig === 'object') {
    dataplaneConfig = parsed.dataplaneConfig;
  }

  // Parse optional deployTestModels flag
  let deployTestModels: boolean = false;
  if (parsed.deployTestModels !== undefined && typeof parsed.deployTestModels === 'boolean') {
    deployTestModels = parsed.deployTestModels;
  }

  // Parse optional testEndpointsConfig
  let testEndpointsConfig: TestEndpointsConfig | undefined = undefined;
  if (parsed.testEndpointsConfig && typeof parsed.testEndpointsConfig === 'object') {
    testEndpointsConfig = new TestEndpointsConfig(parsed.testEndpointsConfig);
  }

  const validatedConfig: DeploymentConfig = {
    projectName,
    account: {
      id: accountId,
      region: region,
      prodLike: parsed.account.prodLike ?? false,
      isAdc: parsed.account.isAdc ?? false
    },
    vpcConfig,
    dataplaneConfig: dataplaneConfig,
    deployTestModels: deployTestModels,
    testEndpointsConfig: testEndpointsConfig
  };

  // Only log non-sensitive configuration details (prevent duplicate logging)
  if (!(global as any).__deploymentConfigLoaded) {
    console.log(
      `🚀 Using environment from deployment.json: projectName=${validatedConfig.projectName}, region=${validatedConfig.account.region}`
    );
    (global as any).__deploymentConfigLoaded = true;
  }

  return validatedConfig;
}
