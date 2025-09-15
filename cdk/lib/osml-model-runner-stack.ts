/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { App, Environment, Stack, StackProps } from "aws-cdk-lib";
import { MRDataplane, OSMLVpc } from "osml-cdk-constructs";
import { DeploymentConfig } from "../bin/deployment/load-deployment";

export interface ModelRunnerStackProps extends StackProps {
  readonly env: Environment;
  readonly deployment: DeploymentConfig;
}

export class OSMLModelRunnerStack extends Stack {
  public resources: MRDataplane;

  /**
   * Constructor for the model runner dataplane cdk stack
   * @param parent the parent cdk app object
   * @param name the name of the stack to be created in the parent app object.
   * @param props the properties required to create the stack.
   * @returns the created MRDataplaneStack object
   */
  constructor(parent: App, name: string, props: ModelRunnerStackProps) {
    super(parent, name, {
      terminationProtection: props.deployment.account.prodLike,
      ...props
    });

    const osmlVpc = new OSMLVpc(this, `${props.deployment.projectName}-VpcImport`, {
      account: props.deployment.account,
      config: {
        VPC_ID: props.deployment.targetVpcId,
        VPC_NAME: "MRVpcImport"
      }
    });

    // Create the model runner application dataplane
    this.resources = new MRDataplane(this, "MRDataplane", {
      account: props.deployment.account,
      osmlVpc: osmlVpc
    });
  }
}
