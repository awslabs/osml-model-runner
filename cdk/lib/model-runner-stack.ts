/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { App, Environment, Stack, StackProps } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";

import { DeploymentConfig } from "../bin/deployment/load-deployment";
import {
  Dataplane as ModelRunnerDataplane,
  DataplaneConfig
} from "./constructs/model-runner/dataplane";

export interface ModelRunnerStackProps extends StackProps {
  readonly env: Environment;
  readonly deployment: DeploymentConfig;
  readonly vpc: IVpc; // VPC is now required and provided by NetworkStack
}

export class ModelRunnerStack extends Stack {
  public resources: ModelRunnerDataplane;
  public vpc: IVpc;
  private deployment: DeploymentConfig;

  /**
   * Constructor for the model runner dataplane cdk stack
   * @param parent the parent cdk app object
   * @param name the name of the stack to be created in the parent app object.
   * @param props the properties required to create the stack.
   * @returns the created OSMLModelRunnerStack object
   */
  constructor(parent: App, name: string, props: ModelRunnerStackProps) {
    super(parent, name, {
      terminationProtection: props.deployment.account.prodLike,
      ...props
    });

    // Store deployment config for use in other methods
    this.deployment = props.deployment;

    // Use the provided VPC from NetworkStack
    this.vpc = props.vpc;

    // Create the model runner application dataplane using the VPC
    const dataplaneConfig = props.deployment.dataplaneConfig
      ? new DataplaneConfig(props.deployment.dataplaneConfig)
      : undefined;
    this.resources = new ModelRunnerDataplane(this, "Dataplane", {
      account: props.deployment.account,
      vpc: this.vpc,
      config: dataplaneConfig
    });
  }
}
