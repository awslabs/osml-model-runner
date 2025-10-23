/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { App, Environment, Stack, StackProps } from "aws-cdk-lib";
import { IVpc, SecurityGroup, SubnetType, SubnetFilter, ISecurityGroup, SubnetSelection } from "aws-cdk-lib/aws-ec2";
import { DeploymentConfig } from "../bin/deployment/load-deployment";
import { ModelRunnerDataplane } from "./constructs/model-runner/model-runner-dataplane";
import { ModelRunnerVpc } from "./constructs/model-runner/model-runner-vpc";
import { TestModelsStack } from "./test-models-stack";

export interface ModelRunnerStackProps extends StackProps {
  readonly env: Environment;
  readonly deployment: DeploymentConfig;
  readonly vpc?: IVpc; // Make VPC optional since we might create it in the stack
}

export class ModelRunnerStack extends Stack {
  public resources: ModelRunnerDataplane;
  public vpc: IVpc;
  public modelRunnerVpc?: ModelRunnerVpc;
  public testModelsStack?: TestModelsStack;
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

    // Create or use the provided VPC
    if (props.vpc) {
      this.vpc = props.vpc;
    } else {
      // Create new VPC using ModelRunnerVpc with its defaults
      this.modelRunnerVpc = new ModelRunnerVpc(this, "ModelRunnerVpc", {
        account: props.deployment.account
      });
      this.vpc = this.modelRunnerVpc.vpc;
    }

    // Create the model runner application dataplane using the VPC
    this.resources = new ModelRunnerDataplane(this, "ModelRunnerDataplane", {
      account: props.deployment.account,
      vpc: this.vpc,
      config: props.deployment.dataplaneConfig
    });

    // Conditionally create TestModelsStack if requested
    if (props.deployment.deployTestModels) {
      this.createTestModelsStack();
    }
  }

  /**
   * Creates the TestModelsStack with appropriate VPC configuration
   */
  private createTestModelsStack(): void {
    const { selectedSubnets, securityGroup } = this.getTestModelsVpcConfiguration();

    this.testModelsStack = new TestModelsStack(this, "TestModels", {
      stackName: `${this.deployment.projectName}-TestModels`,
      env: {
        account: this.account,
        region: this.region
      },
      deployment: this.deployment,
      vpc: this.vpc,
      selectedSubnets,
      securityGroup,
      testEndpointsConfig: this.deployment.testEndpointsConfig
    });
  }

  /**
   * Determines the VPC configuration for test models based on whether we created or imported the VPC
   */
  private getTestModelsVpcConfiguration(): {
    selectedSubnets: SubnetSelection;
    securityGroup?: ISecurityGroup;
  } {
    // If we created a new VPC, use its pre-configured properties
    if (this.modelRunnerVpc) {
      return {
        selectedSubnets: this.modelRunnerVpc.selectedSubnets,
        securityGroup: SecurityGroup.fromSecurityGroupId(
          this,
          "TestModelsSecurityGroup",
          this.modelRunnerVpc.defaultSecurityGroup
        )
      };
    }

    // For imported VPCs, configure based on deployment settings
    const selectedSubnets = this.vpc.selectSubnets({
      subnetType: SubnetType.PRIVATE_WITH_EGRESS,
      subnetFilters: this.deployment.vpcConfig?.targetSubnets
        ? [SubnetFilter.byIds(this.deployment.vpcConfig.targetSubnets)]
        : undefined
    });

    const securityGroup = this.deployment.vpcConfig?.securityGroupId
      ? SecurityGroup.fromSecurityGroupId(
          this,
          "ImportedTestModelsSecurityGroup",
          this.deployment.vpcConfig.securityGroupId
        )
      : undefined;

    return { selectedSubnets, securityGroup };
  }
}
