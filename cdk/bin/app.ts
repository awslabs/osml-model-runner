#!/usr/bin/env node

/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * @file Entry point for the OSML ModelRunner CDK application.
 *
 * This file bootstraps the CDK app, loads deployment configuration,
 * and instantiates the OSMLModelRunnerStack with validated parameters.
 *
 */

import { App, Stack } from "aws-cdk-lib";
import { IVpc, Vpc } from "aws-cdk-lib/aws-ec2";

import { SageMakerRole } from "../lib/constructs/integration-test/sagemaker-role";
import { IntegrationTestStack } from "../lib/integration-test-stack";
import { ModelRunnerStack } from "../lib/model-runner-stack";
import { NetworkStack } from "../lib/network-stack";
import { loadDeploymentConfig } from "./deployment/load-deployment";

// -----------------------------------------------------------------------------
// Initialize CDK Application
// -----------------------------------------------------------------------------

const app = new App();

// -----------------------------------------------------------------------------
// Load the user provided deployment configuration.
// -----------------------------------------------------------------------------

const deployment = loadDeploymentConfig();

// -----------------------------------------------------------------------------
// If we are deploying the SMEndpoints then we require this stack to correctly
// clean up the deployment. This is a workaround until SM cleans up ENI's correctly.
// Once the ticket bellow is resolved this can be removed.
// https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/1327
// -----------------------------------------------------------------------------

let sagemakerRoleStack: Stack | undefined;
let sagemakerRole: SageMakerRole | undefined;
if (deployment.deployIntegrationTests) {
  // Create a dedicated stack for the SageMaker role
  sagemakerRoleStack = new Stack(
    app,
    `${deployment.projectName}-SageMakerRole`,
    {
      env: {
        account: deployment.account.id,
        region: deployment.account.region
      }
    }
  );
  sagemakerRole = new SageMakerRole(sagemakerRoleStack, "SageMakerRole", {
    account: deployment.account,
    roleName: `${deployment.projectName}-SageMakerRole`
  });
}

// -----------------------------------------------------------------------------
// Create VPC (only if importing existing VPC)
// -----------------------------------------------------------------------------

let vpc: IVpc | undefined;
if (deployment.networkConfig?.VPC_ID) {
  // Import existing VPC
  vpc = Vpc.fromLookup(app, "SharedVPC", {
    vpcId: deployment.networkConfig.VPC_ID
  });
}

// -----------------------------------------------------------------------------
// Deploy the network stack.
// -----------------------------------------------------------------------------

const networkStack = new NetworkStack(
  app,
  `${deployment.projectName}-Network`,
  {
    env: {
      account: deployment.account.id,
      region: deployment.account.region
    },
    deployment: deployment,
    vpc: vpc
  }
);

// -----------------------------------------------------------------------------
// Add dependency on the SageMaker role stack if it exists. This is part of the workaround
// mentioned above that allows ENI's to be cleaned up correctly.
// -----------------------------------------------------------------------------

if (sagemakerRoleStack) {
  networkStack.node.addDependency(sagemakerRoleStack);
}

// -----------------------------------------------------------------------------
// Deploy the ModelRunnerStack
// -----------------------------------------------------------------------------

const modelRunnerStack = new ModelRunnerStack(
  app,
  `${deployment.projectName}-ModelRunner`,
  {
    env: {
      account: deployment.account.id,
      region: deployment.account.region
    },
    deployment: deployment,
    vpc: networkStack.network.vpc
  }
);
modelRunnerStack.node.addDependency(networkStack);

// -----------------------------------------------------------------------------
// Deploy the IntegrationTestStack (if integration tests enabled)
// -----------------------------------------------------------------------------

if (deployment.deployIntegrationTests) {
  const integrationTestStack = new IntegrationTestStack(
    app,
    `${deployment.projectName}-IntegrationTest`,
    {
      env: {
        account: deployment.account.id,
        region: deployment.account.region
      },
      deployment: deployment,
      vpc: networkStack.network.vpc,
      selectedSubnets: networkStack.network.selectedSubnets,
      securityGroup: networkStack.network.securityGroup,
      testModelsConfig: deployment.testModelsConfig,
      testImageryConfig: deployment.testImageryConfig,
      sagemakerRole: sagemakerRole?.role
    }
  );
  integrationTestStack.node.addDependency(networkStack);
  integrationTestStack.node.addDependency(sagemakerRoleStack!);
}
