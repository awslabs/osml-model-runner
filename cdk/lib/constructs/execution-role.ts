/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { region_info } from "aws-cdk-lib";
import {
  CompositePrincipal,
  Effect,
  IRole,
  ManagedPolicy,
  PolicyStatement,
  Role,
  ServicePrincipal
} from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

import { OSMLAccount } from "osml-cdk-constructs";

/**
 * Defines the properties required for creating an `ExecutionRole`.
 * @interface ExecutionRoleProps
 * @property {OSMLAccount} account - The OSML (OversightML) deployment account associated with this role.
 * @property {string} roleName - The name to assign to the IAM role.
 */
export interface ExecutionRoleProps {
  account: OSMLAccount;
  roleName: string;
}

/**
 * Constructs a new IAM role designed for ECS tasks execution within AWS,
 * providing necessary permissions predefined for model runner operations.
 *
 * @class ExecutionRole
 * @extends {Construct}
 * @property {IRole} role - The AWS IAM role associated with this construct.
 * @property {string} partition - The AWS partition in which the role will operate.
 */
export class ExecutionRole extends Construct {
  /**
   * The AWS IAM role associated with this construct.
   */
  public role: IRole;

  /**
   * The AWS partition in which the role will operate.
   */
  public partition: string;

  /**
   * Initializes a new instance of the `ExecutionRole` class.
   *
   * @constructor
   * @param {Construct} scope - The scope in which to define this construct, typically a CDK `Stack`.
   * @param {string} id - A unique identifier for this construct within the scope.
   * @param {ExecutionRoleProps} props - The properties for configuring this role.
   */
  constructor(scope: Construct, id: string, props: ExecutionRoleProps) {
    super(scope, id);

    this.partition = region_info.Fact.find(
      props.account.region,
      region_info.FactName.PARTITION
    )!;

    const role = new Role(this, "ExecutionRole", {
      roleName: props.roleName,
      assumedBy: new CompositePrincipal(
        new ServicePrincipal("ecs-tasks.amazonaws.com")
      ),
      description: "Allows ECS tasks to access necessary AWS services."
    });

    const policy = new ManagedPolicy(this, "MRExecutionPolicy", {
      managedPolicyName: "MRExecutionPolicy"
    });

    policy.addStatements(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["ecr:GetAuthorizationToken"],
        resources: ["*"]
      }),
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "logs:CreateLogStream",
          "logs:CreateLogGroup"
        ],
        resources: [
          `arn:${this.partition}:logs:${props.account.region}:${props.account.id}:log-group:*`
        ]
      })
    );

    role.addManagedPolicy(policy);
    this.role = role;
  }
}
