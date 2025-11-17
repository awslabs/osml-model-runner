/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for IntegrationTestStack.
 */

import "source-map-support/register";

import { App, Aspects, Stack } from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { AwsSolutionsChecks } from "cdk-nag";

import { TestModelsConfig } from "../lib/constructs/integration-test/test-models";
import { IntegrationTestStack } from "../lib/integration-test-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc,
  generateNagReport
} from "./test-utils";

describe("IntegrationTestStack", () => {
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

  test("creates stack with test imagery and test endpoints constructs", () => {
    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    expect(stack.testImagery).toBeDefined();
    expect(stack.testImagery.imageBucket).toBeDefined();
    expect(stack.testEndpoints).toBeDefined();
    expect(stack).toBeDefined();
  });

  test("creates S3 bucket for test imagery", () => {
    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    const template = Template.fromStack(stack);

    // Should create S3 bucket for test imagery
    template.hasResourceProperties("AWS::S3::Bucket", {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true
      }
    });
  });

  test("creates bucket with correct naming pattern", () => {
    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    const template = Template.fromStack(stack);

    // Bucket name should include account ID
    // The actual bucket name is resolved as a string, not a CloudFormation function
    template.hasResourceProperties("AWS::S3::Bucket", {
      BucketName: Match.stringLikeRegexp(
        `.*test-imagery.*${deploymentConfig.account.id}.*`
      )
    });
  });

  test("creates bucket deployment for test images", () => {
    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      }
    });

    const template = Template.fromStack(stack);

    // Should have bucket deployment
    template.hasResourceProperties("Custom::CDKBucketDeployment", {
      DestinationBucketName: {
        Ref: Match.anyValue()
      }
    });
  });

  test("works without provided security group", () => {
    const networkStack = new Stack(app, "NetworkStack", {
      env: createTestEnvironment()
    });
    const networkVpc = createTestVpc(networkStack);

    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
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
    // Plus TestImagery roles: BucketDeploymentRole and BucketDeployment handler role (2 roles)
    // Total: 3 (TestModels) + 2 (TestImagery) = 5
    template.resourceCountIs("AWS::IAM::Role", 5);
  });

  test("uses provided SageMaker role when provided", () => {
    const roleStack = new Stack(app, "RoleStack", {
      env: createTestEnvironment()
    });
    const sagemakerRole = new Role(roleStack, "SageMakerRole", {
      assumedBy: new ServicePrincipal("sagemaker.amazonaws.com")
    });

    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
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
    // Plus TestImagery roles: BucketDeploymentRole and BucketDeployment handler role (2 roles)
    // Total: 2 (TestModels) + 2 (TestImagery) = 4
    template.resourceCountIs("AWS::IAM::Role", 4);
  });

  test("creates stack with custom test models config", () => {
    const customConfig = new TestModelsConfig({
      BUILD_FROM_SOURCE: true
    });

    const deploymentWithConfig = createTestDeploymentConfig({
      testModelsConfig: customConfig
    });

    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
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
    // Plus TestImagery roles: BucketDeploymentRole and BucketDeployment handler role (2 roles)
    // Total: 3 (TestModels) + 2 (TestImagery) = 5
    template.resourceCountIs("AWS::IAM::Role", 5);
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

    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: prodDeploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      },
      testModelsConfig: undefined
    });

    expect(stack.testEndpoints).toBeDefined();
    expect(stack.testImagery).toBeDefined();
  });

  test("uses custom test imagery config when provided", () => {
    const stack = new IntegrationTestStack(app, "IntegrationTestStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      selectedSubnets: {
        subnetType: undefined
      },
      testImageryConfig: undefined // Will use defaults
    });

    expect(stack.testImagery).toBeDefined();

    const template = Template.fromStack(stack);

    // Should create 2 buckets: main bucket and access log bucket
    template.resourceCountIs("AWS::S3::Bucket", 2);
  });
});

describe("cdk-nag Compliance Checks - IntegrationTestStack", () => {
  let app: App;
  let stack: IntegrationTestStack;

  beforeAll(() => {
    app = createTestApp();

    const deploymentConfig = createTestDeploymentConfig();
    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(vpcStack);

    stack = new IntegrationTestStack(app, "IntegrationTestStack", {
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
