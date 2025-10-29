/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Test utilities for CDK unit tests.
 */

import { App, Environment, Stack } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";

import { DeploymentConfig } from "../bin/deployment/load-deployment";

/**
 * Creates a test deployment configuration.
 *
 * @param overrides - Optional properties to override defaults
 * @returns A test deployment configuration
 */
export function createTestDeploymentConfig(
  overrides?: Partial<DeploymentConfig>
): DeploymentConfig {
  return {
    projectName: "test-project",
    account: {
      id: "123456789012",
      region: "us-west-2",
      prodLike: false,
      isAdc: false,
      ...overrides?.account
    },
    networkConfig: overrides?.networkConfig,
    dataplaneConfig: overrides?.dataplaneConfig,
    deployIntegrationTests: overrides?.deployIntegrationTests ?? false,
    testModelsConfig: overrides?.testModelsConfig
  };
}

/**
 * Creates a test CDK app.
 *
 * @returns A test CDK app instance
 */
export function createTestApp(): App {
  return new App();
}

/**
 * Creates a test environment configuration.
 *
 * @param overrides - Optional properties to override defaults
 * @returns A test environment configuration
 */
export function createTestEnvironment(
  overrides?: Partial<Environment>
): Environment {
  return {
    account: "123456789012",
    region: "us-west-2",
    ...overrides
  };
}

/**
 * Creates a test VPC in a stack.
 *
 * @param stack - The stack to create the VPC in
 * @param id - The ID for the VPC construct
 * @returns The created VPC
 */
export function createTestVpc(stack: Stack, id: string = "TestVpc"): Vpc {
  return new Vpc(stack, id, {
    maxAzs: 2
  });
}
