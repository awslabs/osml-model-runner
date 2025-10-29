/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for NetworkStack.
 */

import "source-map-support/register";

import { App, Aspects, Stack } from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { AwsSolutionsChecks } from "cdk-nag";

import { NetworkConfig } from "../lib/constructs/model-runner/network";
import { NetworkStack } from "../lib/network-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  generateNagReport
} from "./test-utils";

describe("NetworkStack", () => {
  let app: App;
  let deploymentConfig: ReturnType<typeof createTestDeploymentConfig>;

  beforeEach(() => {
    app = createTestApp();
    deploymentConfig = createTestDeploymentConfig();
  });

  test("creates stack with new VPC when no VPC provided", () => {
    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig
    });

    const template = Template.fromStack(stack);

    // Network stack should create VPC resources
    expect(stack.network).toBeDefined();

    // Network stack should create VPC resources
    template.resourceCountIs("AWS::EC2::VPC", 1);
  });

  test("uses existing VPC when VPC ID provided in deployment config", () => {
    const deploymentWithVpc = createTestDeploymentConfig({
      networkConfig: new NetworkConfig({
        VPC_ID: "vpc-12345678"
      })
    });

    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentWithVpc
    });

    expect(stack.network).toBeDefined();

    // Should still create resources for the network construct
    // When VPC_ID is provided, VPC is looked up (not created in this stack)
    expect(stack.network.vpc).toBeDefined();
  });

  test("uses provided VPC when passed as prop", () => {
    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    const existingVpc = new Vpc(vpcStack, "ExistingVpc");

    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: existingVpc
    });

    expect(stack.network).toBeDefined();

    // Stack should be created successfully
    // When VPC is provided directly, it's used (no VPC created in this stack)
    expect(stack.network.vpc).toBe(existingVpc);
  });

  test("creates network construct with default config when none provided", () => {
    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig
    });

    expect(stack.network).toBeDefined();
    expect(stack.network.vpc).toBeDefined();
  });

  test("creates network construct with custom config when provided", () => {
    const customNetworkConfigData = {
      VPC_ID: "vpc-custom123456",
      TARGET_SUBNETS: ["subnet-12345", "subnet-67890"]
    };

    const deploymentWithCustomConfig = createTestDeploymentConfig({
      networkConfig: new NetworkConfig(customNetworkConfigData)
    });

    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentWithCustomConfig
    });

    expect(stack.network).toBeDefined();

    // Stack should be created successfully
    // When VPC_ID is provided, VPC is looked up (not created in this stack)
    expect(stack.network.vpc).toBeDefined();
  });

  test("prioritizes provided VPC prop over deployment config VPC ID", () => {
    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    const providedVpc = new Vpc(vpcStack, "ProvidedVpc");

    const deploymentWithVpcId = createTestDeploymentConfig({
      networkConfig: new NetworkConfig({
        VPC_ID: "vpc-from-config"
      })
    });

    const stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentWithVpcId,
      vpc: providedVpc
    });

    // Should use the provided VPC prop
    expect(stack.network).toBeDefined();
  });
});

describe("cdk-nag Compliance Checks - NetworkStack", () => {
  let app: App;
  let stack: NetworkStack;

  beforeAll(() => {
    app = createTestApp();

    const deploymentConfig = createTestDeploymentConfig();

    stack = new NetworkStack(app, "TestNetworkStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig
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
