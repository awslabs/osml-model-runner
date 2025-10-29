/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Test utilities for CDK unit tests.
 */

import { App, Environment, Stack } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { SynthesisMessage } from "aws-cdk-lib/cx-api";

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

/**
 * Interface for NAG findings.
 */
export interface NagFinding {
  resource: string;
  details: string;
  rule: string;
}

/**
 * Generates a formatted NAG compliance report for a stack.
 *
 * @param stack - The stack to generate the report for
 * @param errors - Array of error findings
 * @param warnings - Array of warning findings
 */
export function generateNagReport(
  stack: Stack,
  errors: SynthesisMessage[],
  warnings: SynthesisMessage[]
): void {
  const formatFindings = (findings: SynthesisMessage[]): NagFinding[] => {
    const regex = /(AwsSolutions-[A-Za-z0-9]+)\[([^\]]+)]:\s*(.+)/;
    return findings.map((finding) => {
      const data =
        typeof finding.entry.data === "string"
          ? finding.entry.data
          : JSON.stringify(finding.entry.data);
      const match = data.match(regex);
      if (!match) {
        return {
          rule: "",
          resource: "",
          details: ""
        };
      }
      return {
        rule: match[1],
        resource: match[2],
        details: match[3]
      };
    });
  };

  const errorFindings = formatFindings(errors);
  const warningFindings = formatFindings(warnings);

  // Generate the report
  process.stdout.write(
    "\n================== CDK-NAG Compliance Report ==================\n"
  );
  process.stdout.write(`Stack: ${stack.stackName}\n`);
  process.stdout.write(`Generated: ${new Date().toISOString()}\n`);
  process.stdout.write("\n=============== Summary ===============\n");
  process.stdout.write(`Total Errors: ${errorFindings.length}\n`);
  process.stdout.write(`Total Warnings: ${warningFindings.length}\n`);

  if (errorFindings.length > 0) {
    process.stdout.write("\n=============== Errors ===============\n");
    errorFindings.forEach((finding) => {
      process.stdout.write(`\n${finding.resource}\n`);
      process.stdout.write(`${finding.rule}\n`);
      process.stdout.write(`${finding.details}\n`);
    });
  }

  if (warningFindings.length > 0) {
    process.stdout.write("\n=============== Warnings ===============\n");
    warningFindings.forEach((finding) => {
      process.stdout.write(`\n${finding.resource}\n`);
      process.stdout.write(`${finding.rule}\n`);
      process.stdout.write(`${finding.details}\n`);
    });
  }
  process.stdout.write("\n");
}
