/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for TestModelsStack.
 */

import "source-map-support/register";

import { App, Aspects, Stack } from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { AwsSolutionsChecks } from "cdk-nag";

import { TestModelsConfig } from "../lib/constructs/integration-test/test-models";
import { TestModelsStack } from "../lib/test-models-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc,
  generateNagReport
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

  test("works without provided security group", () => {
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

    // Should create IAM roles for SageMaker, HTTP endpoint, and related services
    // Roles: SageMaker role, HTTP endpoint execution role
    // Note: Previously included access logging role which was removed
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

    // Should create IAM roles for HTTP endpoint and related services
    // When a SageMaker role is provided, it's imported from another stack, so not counted here
    // HTTP endpoint creates execution role
    // Note: Previously included access logging role which was removed
    template.resourceCountIs("AWS::IAM::Role", 2);
  });

  test("creates stack with custom test models config", () => {
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

    // Should create IAM roles for SageMaker, HTTP endpoint, and related services
    // Roles: SageMaker role, HTTP endpoint execution role
    // Note: Previously included access logging role which was removed
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

describe("cdk-nag Compliance Checks - TestModelsStack", () => {
  let app: App;
  let stack: TestModelsStack;

  beforeAll(() => {
    app = createTestApp();

    const deploymentConfig = createTestDeploymentConfig();
    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(vpcStack);

    stack = new TestModelsStack(app, "TestModelsStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
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
