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

import { App } from "aws-cdk-lib";
import { Vpc, IVpc, SubnetType, SubnetFilter } from "aws-cdk-lib/aws-ec2";
import { ModelRunnerStack } from "../lib/model-runner-stack";
import { TestModelsStack } from "../lib/test-models-stack";
import { loadDeploymentConfig } from "./deployment/load-deployment";

// -----------------------------------------------------------------------------
// Initialize CDK Application
// -----------------------------------------------------------------------------

const app = new App();

/**
 * Load and validate deployment configuration from deployment.json.
 *
 * This includes:
 * - Project name
 * - AWS account ID and region
 * - VPC and S3 workspace configuration
 */
const deployment = loadDeploymentConfig();

// -----------------------------------------------------------------------------
// Create VPC (only if importing existing VPC)
// -----------------------------------------------------------------------------

// Only import VPC if vpcId is provided - otherwise let ModelRunnerStack create it
let vpc: IVpc | undefined;

if (deployment.vpcConfig?.vpcId) {
  // Import existing VPC
  vpc = Vpc.fromLookup(app, "SharedVPC", {
    vpcId: deployment.vpcConfig.vpcId
  });
}

// -----------------------------------------------------------------------------
// Define and Deploy the ModelRunnerStack
// -----------------------------------------------------------------------------

// ModelRunnerStack will handle VPC creation and TestModelsStack creation internally
new ModelRunnerStack(app, `${deployment.projectName}-ModelRunner`, {
  env: {
    account: deployment.account.id,
    region: deployment.account.region
  },
  deployment: deployment,
  vpc: vpc
});
