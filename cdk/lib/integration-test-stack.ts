/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * @file IntegrationTestStack for deploying integration test resources.
 *
 * This stack deploys both:
 * - TestImagery construct which includes:
 *   - S3 bucket for storing test imagery
 *   - Deployment of test images from local assets
 * - TestModels construct which includes:
 *   - SageMaker endpoints for centerpoint, flood, failure, and multi-container models
 *   - HTTP endpoints for testing
 *   - Container resources and IAM roles
 */

import { Stack, StackProps } from "aws-cdk-lib";
import { ISecurityGroup, IVpc, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { IRole } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

import { DeploymentConfig } from "../bin/deployment/load-deployment";
import {
  TestImagery,
  TestImageryConfig
} from "./constructs/integration-test/test-imagery";
import {
  TestModels,
  TestModelsConfig
} from "./constructs/integration-test/test-models";

/**
 * Properties for the IntegrationTestStack.
 */
export interface IntegrationTestStackProps extends StackProps {
  /** The deployment configuration. */
  deployment: DeploymentConfig;
  /** The VPC to use for the integration test resources. */
  vpc: IVpc;
  /** The selected subnets within the VPC for deployment. */
  selectedSubnets: SubnetSelection;
  /** Optional security group to use for the test endpoints. If not provided, one will be created. */
  securityGroup?: ISecurityGroup;
  /** Optional configuration for test imagery. */
  testImageryConfig?: TestImageryConfig;
  /** Optional configuration for test models. */
  testModelsConfig?: TestModelsConfig;
  /** Optional SageMaker role to use for the test endpoints. If not provided, one will be created. */
  sagemakerRole?: IRole;
}

/**
 * Stack for deploying integration test resources (test imagery and test models).
 */
export class IntegrationTestStack extends Stack {
  /** The test imagery construct. */
  public readonly testImagery: TestImagery;
  /** The test endpoints construct. */
  public readonly testEndpoints: TestModels;

  /**
   * Creates a new IntegrationTestStack.
   *
   * @param scope - The scope in which to define this construct
   * @param id - The construct ID
   * @param props - The stack properties
   */
  constructor(scope: Construct, id: string, props: IntegrationTestStackProps) {
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

    // Create the test endpoints construct using the provided VPC and optional security group
    this.testEndpoints = new TestModels(this, "TestEndpoints", {
      account: {
        id: props.deployment.account.id,
        region: props.deployment.account.region,
        prodLike: props.deployment.account.prodLike,
        isAdc: props.deployment.account.isAdc
      },
      vpc: props.vpc,
      selectedSubnets: props.selectedSubnets,
      securityGroup: props.securityGroup,
      config: props.testModelsConfig,
      smRole: props.sagemakerRole
    });
  }
}
