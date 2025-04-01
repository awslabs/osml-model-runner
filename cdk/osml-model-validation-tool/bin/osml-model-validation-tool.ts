#!/usr/bin/env node

/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */


import * as cdk from 'aws-cdk-lib';
import { OsmlModelValidationToolStack } from '../lib/osml-model-validation-tool-stack';

const app = new cdk.App();
new OsmlModelValidationToolStack(app, 'OsmlModelValidationToolStack', {
  /* If you don't specify 'env', this stack will be environment-agnostic.
   * Account/Region-dependent features and context lookups will not work,
   * but a single synthesized template can be deployed anywhere. */

  /* Uncomment the next line to specialize this stack for the AWS Account
   * and Region that are implied by the current CLI configuration. */
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },

  /* For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html */

  // Optional custom properties
  // reportBucketName: 'my-custom-report-bucket',
  // testImageryBucketName: 'my-custom-test-imagery-bucket',
  // logLevel: INFO
  logLevel: 'DEBUG', // Set to DEBUG, INFO, WARNING, ERROR, or CRITICAL
});
