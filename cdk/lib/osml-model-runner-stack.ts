/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { App, Environment, Stack, StackProps } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { DeploymentConfig } from "../bin/deployment/load-deployment";
import { ModelRunnerDataplane } from "./constructs/model-runner-dataplane";

export interface ModelRunnerStackProps extends StackProps {
  readonly env: Environment;
  readonly deployment: DeploymentConfig;
}

export class OSMLModelRunnerStack extends Stack {
  public resources: ModelRunnerDataplane;

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

    // Look up the existing VPC
    const vpc = Vpc.fromLookup(this, `${props.deployment.projectName}-VpcImport`, {
      vpcId: props.deployment.targetVpcId
    });

    // Create the model runner application dataplane
    this.resources = new ModelRunnerDataplane(this, "MRDataplane", {
      account: props.deployment.account,
      vpc: vpc,
      config: props.deployment.dataplaneConfig
    });
  }
}
