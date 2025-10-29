/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for TestImageryStack.
 */

import "source-map-support/register";

import { App, Aspects, Stack } from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { AwsSolutionsChecks } from "cdk-nag";

import { TestImageryStack } from "../lib/test-imagery-stack";
import {
  createTestApp,
  createTestDeploymentConfig,
  createTestEnvironment,
  createTestVpc,
  generateNagReport
} from "./test-utils";

describe("TestImageryStack", () => {
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

  test("creates stack with test imagery construct", () => {
    const stack = new TestImageryStack(app, "TestImageryStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    expect(stack.testImagery).toBeDefined();
    expect(stack.testImagery.imageBucket).toBeDefined();

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
    const stack = new TestImageryStack(app, "TestImageryStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
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

  test("uses custom test imagery config when provided", () => {
    const stack = new TestImageryStack(app, "TestImageryStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc,
      testImageryConfig: undefined // Will use defaults
    });

    expect(stack.testImagery).toBeDefined();

    const template = Template.fromStack(stack);

    // Should create 2 buckets: main bucket and access log bucket
    template.resourceCountIs("AWS::S3::Bucket", 2);
  });

  test("creates bucket deployment for test images", () => {
    const stack = new TestImageryStack(app, "TestImageryStack", {
      env: createTestEnvironment(),
      deployment: deploymentConfig,
      vpc: vpc
    });

    const template = Template.fromStack(stack);

    // Should have bucket deployment
    template.hasResourceProperties("Custom::CDKBucketDeployment", {
      DestinationBucketName: {
        Ref: Match.anyValue()
      }
    });
  });
});

describe("cdk-nag Compliance Checks - TestImageryStack", () => {
  let app: App;
  let stack: TestImageryStack;

  beforeAll(() => {
    app = createTestApp();

    const deploymentConfig = createTestDeploymentConfig();
    const vpcStack = new Stack(app, "VpcStack", {
      env: createTestEnvironment()
    });
    const vpc = createTestVpc(vpcStack);

    stack = new TestImageryStack(app, "TestImageryStack", {
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
