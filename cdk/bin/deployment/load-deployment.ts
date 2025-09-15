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
 *     "region": "us-west-2"
 *   },
 *   "config": {
 *     "targetVpcId": "vpc-abc123",
 *     "workspaceBucketName": "my-bucket-name"
 *   }
 * }
 * ```
 *
 * @packageDocumentation
 */

import * as fs from "fs";
import * as path from "path";

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
    /** Whether the account is prod-like. */
    prodLike: boolean;
  };
  /** The ID of the target VPC for resource placement. */
  targetVpcId: string;
  /** The name of the S3 bucket used as a workspace. */
  workspaceBucketName: string;
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
      "config.targetVpcId"
    );
  }
  return vpcId;
}

/**
 * Validates S3 bucket name format.
 *
 * @param bucketName - The bucket name to validate
 * @returns The validated bucket name
 * @throws {DeploymentConfigError} If the bucket name format is invalid
 */
function validateBucketName(bucketName: string): string {
  if (bucketName.length < 3 || bucketName.length > 63) {
    throw new DeploymentConfigError(
      `Invalid S3 bucket name length: '${bucketName}'. Must be between 3 and 63 characters.`,
      "config.workspaceBucketName"
    );
  }

  if (!/^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(bucketName)) {
    throw new DeploymentConfigError(
      `Invalid S3 bucket name format: '${bucketName}'. Must contain only lowercase letters, numbers, dots, and hyphens, and cannot start or end with a hyphen or dot.`,
      "config.workspaceBucketName"
    );
  }

  if (
    bucketName.includes("..") ||
    bucketName.includes(".-") ||
    bucketName.includes("-.")
  ) {
    throw new DeploymentConfigError(
      `Invalid S3 bucket name: '${bucketName}'. Cannot contain consecutive dots or dots adjacent to hyphens.`,
      "config.workspaceBucketName"
    );
  }

  return bucketName;
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

  // Validate config section
  if (!parsed.config || typeof parsed.config !== "object") {
    throw new DeploymentConfigError(
      "Missing or invalid config section in deployment.json",
      "config"
    );
  }

  const targetVpcId = validateVpcId(
    validateStringField(parsed.config.targetVpcId, "config.targetVpcId")
  );
  const workspaceBucketName = validateBucketName(
    validateStringField(
      parsed.config.workspaceBucketName,
      "config.workspaceBucketName"
    )
  );

  const validatedConfig: DeploymentConfig = {
    projectName,
    account: {
      id: accountId,
      region: region,
      prodLike: parsed.account.prodLike
    },
    targetVpcId,
    workspaceBucketName
  };

  // Only log non-sensitive configuration details
  console.log(
    `🚀 Using environment from deployment.json: projectName=${validatedConfig.projectName}, region=${validatedConfig.account.region}`
  );

  return validatedConfig;
}
