/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * @file NetworkStack for deploying VPC and networking infrastructure.
 *
 * This stack deploys the Network construct which includes:
 * - VPC with public and private subnets
 * - Security groups
 * - VPC flow logs (for production environments)
 * - NAT Gateway for private subnet egress
 */

import { Stack, StackProps } from "aws-cdk-lib";
import { IVpc } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";
import { Network, NetworkConfig } from "./constructs/model-runner/network";
import { DeploymentConfig } from "../bin/deployment/load-deployment";

/**
 * Properties for the NetworkStack.
 */
export interface NetworkStackProps extends StackProps {
  /** The deployment configuration. */
  deployment: DeploymentConfig;
  /** Optional existing VPC to import instead of creating a new one. */
  vpc?: IVpc;
}

/**
 * Stack for deploying networking infrastructure.
 */
export class NetworkStack extends Stack {
  /** The network construct containing VPC and security groups. */
  public readonly network: Network;

  /**
   * Creates a new NetworkStack.
   *
   * @param scope - The scope in which to define this construct
   * @param id - The construct ID
   * @param props - The stack properties
   */
  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    // If an existing VPC is provided, we don't need to create a new Network construct
    // Instead, we'll create a minimal Network construct that wraps the existing VPC
    if (props.vpc) {
      // For imported VPCs, we'll create a Network construct that uses the existing VPC
      const networkConfig = new NetworkConfig({
        VPC_ID: props.vpc.vpcId
      });
      this.network = new Network(this, "Network", {
        account: props.deployment.account,
        config: networkConfig
      });
    } else {
      // Create new VPC using Network construct
      const networkConfig = props.deployment.networkConfig
        ? new NetworkConfig(props.deployment.networkConfig)
        : new NetworkConfig();
      this.network = new Network(this, "Network", {
        account: props.deployment.account,
        config: networkConfig
      });
    }
  }
}
