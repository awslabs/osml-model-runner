/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for ModelRunnerStack.
 */

import { App, Stack } from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";

import { ModelRunnerStack } from "../lib/model-runner-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc
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
    // Check for DynamoDB tables which are always created (5 tables total)
    template.resourceCountIs("AWS::DynamoDB::Table", 5);
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
    const {
      DataplaneConfig
    } = require("../lib/constructs/model-runner/dataplane");
    const dataplaneConfig = new DataplaneConfig({
      CONTAINER_URI: "test-container:latest",
      ECS_TASK_CPU: 8192,
      ECS_CONTAINER_CPU: 4096
    } as any);

    const deploymentWithConfig = createTestDeploymentConfig({
      dataplaneConfig: dataplaneConfig as any
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
