/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Construct } from "constructs";
import { RemovalPolicy } from "aws-cdk-lib";
import {
  FlowLog,
  FlowLogDestination,
  FlowLogResourceType,
  IVpc,
  SecurityGroup,
  SubnetFilter,
  SubnetSelection,
  SubnetType,
  Vpc,
} from "aws-cdk-lib/aws-ec2";
import { IRole, Role } from "aws-cdk-lib/aws-iam";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { OSMLAccount } from "../types";
import { BaseConfig, ConfigType, RegionalConfig } from "../types";

export class ModelRunnerVpcConfig extends BaseConfig {
  /**
   * The name to assign the creation of the VPC.
   * @default "ModelRunner-VPC"
   */
  public VPC_NAME: string;

  /**
   * Unique identifier to import/use an existing VPC instead of creating a new one.
   */
  public VPC_ID?: string;

  /**
   * Define the maximum number of AZs for the VPC.
   */
  public MAX_AZS?: number;

  /**
   * Specify whether to enable creating VPC endpoints on the VPC.
   * @default false
   */
  public ENABLE_VPC_ENDPOINTS?: boolean;

  /**
   * Specifies an optional list of subnet IDs to specifically target within the VPC.
   */
  public TARGET_SUBNETS?: string[];

  /**
   * Specify role to provide when creating CW flow logs.
   */
  public IAM_FLOW_LOG_ROLE_NAME?: string;

  /**
   * Constructor for ModelRunnerVpcConfig.
   * @param config - The configuration object for the VPC.
   */
  constructor(config: ConfigType = {}) {
    super({
      // Set default values here
      VPC_NAME: "ModelRunner-VPC",
      ENABLE_VPC_ENDPOINTS: false,
      ...config
    });
  }
}

/**
 * Properties for creating a model runner VPC.
 */
export interface ModelRunnerVpcProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The custom configuration to be used when deploying this VPC. */
  readonly config?: ModelRunnerVpcConfig;
}

/**
 * Model Runner VPC construct that can either import an existing VPC or create a new one.
 *
 * When creating a new VPC, it includes:
 * - Public subnets with Internet Gateway
 * - Private subnets with NAT Gateway
 * - Default security group (or custom security group if provided)
 */
export class ModelRunnerVpc extends Construct {
  /** The VPC instance. */
  public readonly vpc: IVpc;
  /** The selected subnets based on configuration. */
  public readonly selectedSubnets: SubnetSelection;
  /** The default security group for the VPC. */
  public readonly defaultSecurityGroup: string;
  /** Flow log instance to monitor and capture IP traffic related to the VPC. */
  public flowLog?: FlowLog;
  /** Determines the lifecycle of VPC resources upon stack deletion. */
  public readonly removalPolicy: RemovalPolicy;
  /** The configuration of this construct. */
  public readonly config: ModelRunnerVpcConfig;
  /** The flow log Role used for the VPC. */
  public flowLogRole?: IRole;

  /**
   * Creates a new ModelRunnerVpc construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: ModelRunnerVpcProps) {
    super(scope, id);

    // Check if a custom configuration was provided
    if (props.config) {
      // Import existing passed-in configuration
      this.config = props.config;
    } else {
      // Create a new default configuration
      this.config = new ModelRunnerVpcConfig();
    }

    const regionConfig = RegionalConfig.getConfig(props.account.region);

    this.removalPolicy = props.account.prodLike
      ? RemovalPolicy.RETAIN
      : RemovalPolicy.DESTROY;

    if (this.config.VPC_ID) {
      // Import existing VPC
      this.vpc = Vpc.fromLookup(this, "ImportedVpc", {
        vpcId: this.config.VPC_ID,
        isDefault: false
      });
    } else {
      this.vpc = new Vpc(this, "ModelRunnerVpc", {
        vpcName: this.config.VPC_NAME,
        maxAzs: this.config.MAX_AZS ?? regionConfig.maxVpcAzs,
        subnetConfiguration: [
          {
            cidrMask: 24,
            name: `${this.config.VPC_NAME}-Public`,
            subnetType: SubnetType.PUBLIC
          },
          {
            cidrMask: 24,
            name: `${this.config.VPC_NAME}-Private`,
            subnetType: SubnetType.PRIVATE_WITH_EGRESS
          }
        ]
      });

      const customSecurityGroup = new SecurityGroup(this, "ModelRunnerSecurityGroup", {
        vpc: this.vpc,
        description: "Security group for OSML Model Runner with outbound access",
        allowAllOutbound: true // This ensures outbound access for general traffic
      });

      this.defaultSecurityGroup = customSecurityGroup.securityGroupId;

    }

    // Select subnets based on configuration
    this.selectedSubnets = this.selectSubnets();

    if (props.account.prodLike) {
      this.setupFlowLogs();
    }
  }


  /**
   * Sets up the VPC flow logs for monitoring and auditing network traffic.
   * The logs are stored in CloudWatch with a specified retention period and removal policy.
   */
  private setupFlowLogs(): void {
    const flowLogGroup = new LogGroup(this, "ModelRunnerVpcFlowLogsLogGroup", {
      logGroupName: `${this.config.VPC_NAME}-FlowLogs`,
      retention: RetentionDays.TEN_YEARS,
      removalPolicy: this.removalPolicy
    });

    // Check if a custom flow log role was provided
    if (this.config.IAM_FLOW_LOG_ROLE_NAME) {
      this.flowLogRole = Role.fromRoleName(
        this,
        "ImportFlowLog",
        this.config.IAM_FLOW_LOG_ROLE_NAME,
        {
          mutable: false
        }
      );
    }

    // Create the Flow Logs for the VPC
    this.flowLog = new FlowLog(this, "ModelRunnerVpcFlowLogs", {
      resourceType: FlowLogResourceType.fromVpc(this.vpc),
      destination: FlowLogDestination.toCloudWatchLogs(
        flowLogGroup,
        this.flowLogRole
      )
    });
  }

  /**
   * Selects subnets within the VPC based on user specifications.
   * If target subnets are provided, those are selected; otherwise,
   * it defaults to selecting all private subnets with egress.
   *
   * @returns The selected subnet selection
   */
  private selectSubnets(): SubnetSelection {
    // If specified subnets are provided, use them
    if (this.config.TARGET_SUBNETS) {
      return this.vpc.selectSubnets({
        subnetFilters: [SubnetFilter.byIds(this.config.TARGET_SUBNETS)]
      });
    } else {
      // Otherwise, select all private subnets
      return this.vpc.selectSubnets({
        subnetType: SubnetType.PRIVATE_WITH_EGRESS
      });
    }
  }
}
