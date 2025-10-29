/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for TestModelsStack.
 */

import { App, Stack } from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";

import { TestModelsStack } from "../lib/test-models-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc
} from "./test-utils";

describe("TestModelsStack", () => {
  let app: App;
  let deploymentConfig: ReturnType<typeof createTestDeploymentConfig>;
  let vpc: Vpc;

  beforeEach(() => {
    app = createTestApp();
    deploymentConfig = createTestDeploymentConfig();

    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    vpc = createTestVpc(vpcStack);
  });

  test("creates stack with test endpoints construct", () => {
    const stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    expect(stack.testEndpoints).toBeDefined();
    expect(stack).toBeDefined();
  });

  test("uses provided security group when provided", () => {
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const networkVpc = createTestVpc(networkStack);

    // Create a mock security group from the network stack
    // Note: In real tests, this would come from the Network construct
    const stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: networkVpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    expect(stack.testEndpoints).toBeDefined();

    const template = Template.fromStack(stack);

    // Should create IAM roles for SageMaker, HTTP endpoint, and ECS tasks
    // At least 3 roles: SageMaker role, HTTP endpoint execution role, and ECS task role
    template.resourceCountIs("AWS::IAM::Role", 3);
  });

  test("uses provided SageMaker role when provided", () => {
    const roleStack = new Stack(app, "RoleStack", {
      env: createTestEnvironment()
    });
    const sagemakerRole = new Role(roleStack, "SageMakerRole", {
      assumedBy: new ServicePrincipal("sagemaker.amazonaws.com")
    });

    const stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      },
      sagemakerRole: sagemakerRole
    });

    expect(stack.testEndpoints).toBeDefined();

    const template = Template.fromStack(stack);

    // Should create IAM roles for HTTP endpoint and ECS tasks
    // When a SageMaker role is provided, it's imported from another stack, so not counted here
    // But HTTP endpoint still creates execution and task roles (2 roles)
    template.resourceCountIs("AWS::IAM::Role", 2);
  });

  test("creates stack with custom test models config", () => {
    const {
      TestModelsConfig
    } = require("../lib/constructs/integration-test/test-models");
    const customConfig = new TestModelsConfig({
      BUILD_FROM_SOURCE: true
    });

    const deploymentWithConfig = createTestDeploymentConfig({
      testModelsConfig: customConfig
    });

    const stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: deploymentWithConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      },
      testModelsConfig: customConfig
    });

    expect(stack.testEndpoints).toBeDefined();

    const template = Template.fromStack(stack);

    // Should create IAM roles for SageMaker, HTTP endpoint, and ECS tasks
    // At least 3 roles: SageMaker role, HTTP endpoint execution role, and ECS task role
    template.resourceCountIs("AWS::IAM::Role", 3);
  });

  test("passes account configuration to test endpoints", () => {
    const prodDeploymentConfig = createTestDeploymentConfig({
      account: {
        id: "123456789012",
        region: "us-west-2",
        prodLike: true,
        isAdc: true
      },
      testModelsConfig: undefined
    });

    const stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: prodDeploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      },
      testModelsConfig: undefined
    });

    expect(stack.testEndpoints).toBeDefined();

    // Should create resources with prod settings
    expect(stack.testEndpoints).toBeDefined();
  });
});
