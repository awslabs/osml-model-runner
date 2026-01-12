/*
 * Copyright 2025-2026 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for ModelRunnerStack.
 */

import "source-map-support/register";

import { App, Aspects, Stack } from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { AwsSolutionsChecks } from "cdk-nag";

import { ConfigType } from "../lib/constructs/types";
import { ModelRunnerStack } from "../lib/model-runner-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc,
  generateNagReport
} from "./test-utils";

describe("ModelRunnerStack", () => {
  let app: App;
  let deploymentConfig: ReturnType<typeof createTestDeploymentConfig>;

  beforeEach(() => {
    app = createTestApp();
    deploymentConfig = createTestDeploymentConfig();
  });

  test("creates stack with correct properties", () => {
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    const stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    // Stack should exist and have correct termination protection
    expect(stack.terminationProtection).toBe(false);

    // VPC should be stored
    expect(stack.vpc).toBe(vpc);
  });

  test("sets termination protection when prodLike is true", () => {
    const prodDeploymentConfig = createTestDeploymentConfig({
      account: {
        id: "123456789012",
        region: "us-west-2",
        prodLike: true,
        isAdc: false
      }
    });

    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    const stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: prodDeploymentConfig,
      vpc: vpc
    });

    expect(stack.terminationProtection).toBe(true);
  });

  test("creates dataplane construct", () => {
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    const stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    // Dataplane should be created
    expect(stack.resources).toBeDefined();

    const template = Template.fromStack(stack);

    // Stack should have resources (the dataplane creates various resources)
    // Check for DynamoDB tables which are always created (4 tables total)
    template.resourceCountIs("AWS::DynamoDB::Table", 4);
  });

  test("uses provided VPC from network stack", () => {
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    const stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    // VPC should be the same instance
    expect(stack.vpc).toBe(vpc);
  });

  test("creates stack with custom dataplane config", () => {
    const dataplaneConfigPartial: Partial<ConfigType> = {
      CONTAINER_URI: "test-container:latest",
      ECS_TASK_CPU: 8192,
      ECS_CONTAINER_CPU: 4096
    };

    const deploymentWithConfig = createTestDeploymentConfig({
      dataplaneConfig: dataplaneConfigPartial
    });

    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    const stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: deploymentWithConfig,
      vpc: vpc
    });

    // Stack should be created successfully
    expect(stack).toBeDefined();
    expect(stack.resources).toBeDefined();
  });
});

describe("cdk-nag Compliance Checks - ModelRunnerStack", () => {
  let app: App;
  let stack: ModelRunnerStack;

  beforeAll(() => {
    app = createTestApp();

    const deploymentConfig = createTestDeploymentConfig();
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(networkStack);

    stack = new ModelRunnerStack(app, "TestModelRunnerStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    // Add the cdk-nag AwsSolutions Pack with extra verbose logging enabled.
    Aspects.of(stack).add(
      new AwsSolutionsChecks({
        verbose: true
      })
    );

    const errors = Annotations.fromStack(stack).findError(
      "*",
      Match.stringLikeRegexp("AwsSolutions-.*")
    );
    const warnings = Annotations.fromStack(stack).findWarning(
      "*",
      Match.stringLikeRegexp("AwsSolutions-.*")
    );
    generateNagReport(stack, errors, warnings);
  });

  test("No unsuppressed Warnings", () => {
    const warnings = Annotations.fromStack(stack).findWarning(
      "*",
      Match.stringLikeRegexp("AwsSolutions-.*")
    );
    expect(warnings).toHaveLength(0);
  });

  test("No unsuppressed Errors", () => {
    const errors = Annotations.fromStack(stack).findError(
      "*",
      Match.stringLikeRegexp("AwsSolutions-.*")
    );
    expect(errors).toHaveLength(0);
  });
});
