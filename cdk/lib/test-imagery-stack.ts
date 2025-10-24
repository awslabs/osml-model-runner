/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * @file TestImageryStack for deploying test imagery resources.
 *
 * This stack deploys the TestImagery construct which includes:
 * - S3 bucket for storing test imagery
 * - Deployment of test images from local assets
 */

import { Stack, StackProps } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";
import { TestImagery, TestImageryConfig } from "./constructs/integration-test/test-imagery";
import { DeploymentConfig } from "../bin/deployment/load-deployment";

/**
 * Properties for the TestImageryStack.
 */
export interface TestImageryStackProps extends StackProps {
  /** The deployment configuration. */
  deployment: DeploymentConfig;
  /** The VPC to use for the test imagery. */
  vpc: IVpc;
  /** Optional configuration for test imagery. */
  testImageryConfig?: TestImageryConfig;
}

/**
 * Stack for deploying test imagery resources.
 */
export class TestImageryStack extends Stack {
  /** The test imagery construct. */
  public readonly testImagery: TestImagery;

  /**
   * Creates a new TestImageryStack.
   *
   * @param scope - The scope in which to define this construct
   * @param id - The construct ID
   * @param props - The stack properties
   */
  constructor(scope: Construct, id: string, props: TestImageryStackProps) {
    super(scope, id, props);

    // Create the test imagery construct
    this.testImagery = new TestImagery(this, "TestImagery", {
      account: {
        id: props.deployment.account.id,
        region: props.deployment.account.region,
        prodLike: props.deployment.account.prodLike,
        isAdc: props.deployment.account.isAdc
      },
      vpc: props.vpc,
      config: props.testImageryConfig
    });
  }
}
