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
import { OSMLModelRunnerStack } from "../lib/osml-model-runner-stack";
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
// Define and Deploy the OSMLGeoAgentStack
// -----------------------------------------------------------------------------


new OSMLModelRunnerStack(app, deployment.projectName, {
  env: {
    account: deployment.account.id,
    region: deployment.account.region
  },
  deployment: deployment,
  // Solution ID 'SO9240' should be verified to match the official AWS Solutions reference.
  description:
    "OSML GeoAgent, Guidance for Processing Overhead Imagery on AWS (SO9240)"
});
