/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { region_info } from "aws-cdk-lib";
import {
  Effect,
  IRole,
  ManagedPolicy,
  PolicyStatement,
  Role,
  ServicePrincipal
} from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

import { OSMLAccount, BaseConfig, ConfigType } from "../types";

/**
 * Configuration class for SageMakerRole Construct.
 */
export class SageMakerRoleConfig extends BaseConfig {
  /**
   * The name of the SageMaker execution role.
   * @default undefined
   */
  public SM_ROLE_NAME?: string | undefined;

  /**
   * Constructor for SageMakerRoleConfig.
   * @param config - The configuration object for SageMakerRole
   */
  constructor(config: ConfigType = {}) {
    super({
      ...config
    });
  }
}

/**
 * Properties for creating SageMaker roles.
 */
export interface SageMakerRoleProps {
  /** The OSML deployment account. */
  readonly account: OSMLAccount;
  /** The name for the SageMaker execution role. */
  readonly roleName: string;
  /** Custom configuration for the SageMakerRole Construct (optional). */
  readonly config?: SageMakerRoleConfig;
  /** Optional existing SageMaker role to use instead of creating one. */
  readonly existingRole?: IRole;
}

/**
 * Represents a SageMakerRole construct responsible for managing the SageMaker
 * execution role for test model endpoints.
 *
 * This construct encapsulates the creation and configuration of the SageMaker
 * execution role required by test model endpoints, providing a unified interface
 * for role management with support for using existing roles or creating new ones.
 */
export class SageMakerRole extends Construct {
  /** The configuration for the SageMakerRole. */
  public readonly config: SageMakerRoleConfig;
  /** The SageMaker execution role. */
  public readonly role: IRole;
  /** The AWS partition in which the role will operate. */
  public readonly partition: string;

  /**
   * Constructs an instance of SageMakerRole.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties of this construct
   */
  constructor(scope: Construct, id: string, props: SageMakerRoleProps) {
    super(scope, id);

    // Initialize configuration
    this.config = props.config ?? new SageMakerRoleConfig();

    // Determine the AWS partition based on the provided AWS region
    this.partition = region_info.Fact.find(
      props.account.region,
      region_info.FactName.PARTITION
    )!;

    // Determine which role to use
    if (this.config.SM_ROLE_NAME) {
      // Use existing role by name
      this.role = Role.fromRoleName(
        this,
        "ImportedSageMakerRole",
        this.config.SM_ROLE_NAME,
        {
          mutable: false
        }
      );
    } else if (props.existingRole) {
      // Use provided existing role
      this.role = props.existingRole;
    } else {
      // Create a new role
      this.role = this.createSageMakerRole(props);
    }
  }

  /**
   * Creates the SageMaker execution role.
   *
   * @param props - The SageMaker role properties
   * @returns The created SageMaker execution role
   */
  private createSageMakerRole(props: SageMakerRoleProps): IRole {
    const role = new Role(this, "SageMakerExecutionRole", {
      roleName: props.roleName,
      assumedBy: new ServicePrincipal("sagemaker.amazonaws.com"),
      description:
        "Allows SageMaker to access necessary AWS services (S3, SQS, DynamoDB, ...)"
    });

    const smExecutionPolicy = new ManagedPolicy(
      this,
      "SageMakerExecutionPolicy",
      {
        managedPolicyName: "SageMakerExecutionPolicy"
      }
    );

    // Add permissions to describe EC2 instance types
    const ec2NetworkPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "ec2:DescribeInstanceTypes",
        "ec2:DescribeVpcEndpoints",
        "ec2:DescribeDhcpOptions",
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterfacePermission",
        "ec2:DeleteNetworkInterface",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:CreateNetworkInterface"
      ],
      resources: ["*"]
    });

    // Add permissions for ECR permissions
    const ecrAuthPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["ecr:GetAuthorizationToken"],
      resources: ["*"]
    });

    const ecrPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:DescribeImages",
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:DescribeRepositories"
      ],
      resources: [
        `arn:${this.partition}:ecr:${props.account.region}:${props.account.id}:repository/*`
      ]
    });

    // Add permissions for cloudwatch permissions
    const cwLogsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "logs:CreateLogDelivery",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:DeleteLogDelivery",
        "logs:Describe*",
        "logs:GetLogEvents",
        "logs:GetLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutLogEvents",
        "logs:PutResourcePolicy",
        "logs:UpdateLogDelivery"
      ],
      resources: [
        `arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
      ]
    });

    // Add permissions to assume roles
    const stsPolicyStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["sts:AssumeRole"],
      resources: ["*"]
    });

    smExecutionPolicy.addStatements(
      cwLogsPolicyStatement,
      ecrAuthPolicyStatement,
      ecrPolicyStatement,
      ec2NetworkPolicyStatement,
      stsPolicyStatement
    );

    role.addManagedPolicy(smExecutionPolicy);

    return role;
  }
}
