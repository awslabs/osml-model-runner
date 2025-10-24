/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * @file TestModelsStack for deploying test model endpoints.
 *
 * This stack deploys the TestEndpoints construct which includes:
 * - SageMaker endpoints for centerpoint, flood, and multi-container models
 * - HTTP endpoints for testing
 * - Container resources and IAM roles
 */

import { Stack, StackProps } from "aws-cdk-lib";
import { IVpc, ISecurityGroup, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { IRole } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { TestModels, TestModelsConfig } from "./constructs/integration-test/test-models";
import { DeploymentConfig } from "../bin/deployment/load-deployment";

/**
 * Properties for the TestModelsStack.
 */
export interface TestModelsStackProps extends StackProps {
  /** The deployment configuration. */
  deployment: DeploymentConfig;
  /** The VPC to use for the test models. */
  vpc: IVpc;
  /** The selected subnets within the VPC for deployment. */
  selectedSubnets: SubnetSelection;
  /** Optional security group to use for the test endpoints. If not provided, one will be created. */
  securityGroup?: ISecurityGroup;
  /** Optional configuration for test models. */
  testModelsConfig?: TestModelsConfig;
  /** Optional SageMaker role to use for the test endpoints. If not provided, one will be created. */
  sagemakerRole?: IRole;
}

/**
 * Stack for deploying test model endpoints.
 */
export class TestModelsStack extends Stack {
  /** The test endpoints construct. */
  public readonly testEndpoints: TestModels;

  /**
   * Creates a new TestModelsStack.
   *
   * @param scope - The scope in which to define this construct
   * @param id - The construct ID
   * @param props - The stack properties
   */
  constructor(scope: Construct, id: string, props: TestModelsStackProps) {
    super(scope, id, props);

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
