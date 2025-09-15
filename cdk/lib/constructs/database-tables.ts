/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { BackupPlan, BackupPlanRule, BackupResource, BackupVault } from "aws-cdk-lib/aws-backup";
import { Construct } from "constructs";

import { OSMLAccount, OSMLTable } from "osml-cdk-constructs";
import { ModelRunnerDataplaneConfig } from "./model-runner-dataplane";
import { AttributeType } from "aws-cdk-lib/aws-dynamodb";

/**
 * Properties for creating database tables.
 */
export interface DatabaseTablesProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The MR dataplane configuration. */
  readonly config: ModelRunnerDataplaneConfig;
  /** The removal policy for resources. */
  readonly removalPolicy: RemovalPolicy;
}

/**
 * Construct that manages all DynamoDB tables and backup configuration for the Model Runner.
 *
 * This construct encapsulates the creation and configuration of all DynamoDB tables
 * required by the Model Runner, including backup policies for production environments.
 */
export class DatabaseTables extends Construct {
  /** The DynamoDB table for outstanding image processing jobs. */
  public readonly outstandingImageJobsTable: OSMLTable;

  /** The DynamoDB table for job status. */
  public readonly jobStatusTable: OSMLTable;

  /** The DynamoDB table for feature data. */
  public readonly featureTable: OSMLTable;

  /** The DynamoDB table for endpoint statistics. */
  public readonly endpointStatisticsTable: OSMLTable;

  /** The DynamoDB table for region request status. */
  public readonly regionRequestTable: OSMLTable;

  /**
   * Creates a new DatabaseTables construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: DatabaseTablesProps) {
    super(scope, id);

    // Create all DynamoDB tables
    this.outstandingImageJobsTable = this.createOutstandingImageJobsTable(props);
    this.jobStatusTable = this.createJobStatusTable(props);
    this.featureTable = this.createFeatureTable(props);
    this.endpointStatisticsTable = this.createEndpointStatisticsTable(props);
    this.regionRequestTable = this.createRegionRequestTable(props);

    // Create backup configuration for production environments
    if (props.account.prodLike && !props.account.isAdc) {
      this.createBackupConfiguration(props);
    }
  }

  /**
   * Creates the outstanding image jobs table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createOutstandingImageJobsTable(props: DatabaseTablesProps): OSMLTable {
    return new OSMLTable(this, "MROutstandingImageJobsTable", {
      tableName: props.config.DDB_OUTSTANDING_IMAGE_JOBS_TABLE,
      partitionKey: {
        name: "endpoint_id",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "job_id",
        type: AttributeType.STRING
      },
      removalPolicy: props.removalPolicy
    });
  }

  /**
   * Creates the job status table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createJobStatusTable(props: DatabaseTablesProps): OSMLTable {
    return new OSMLTable(this, "MRJobStatusTable", {
      tableName: props.config.DDB_JOB_STATUS_TABLE,
      partitionKey: {
        name: "image_id",
        type: AttributeType.STRING
      },
      removalPolicy: props.removalPolicy,
      ttlAttribute: props.config.DDB_TTL_ATTRIBUTE
    });
  }

  /**
   * Creates the feature table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createFeatureTable(props: DatabaseTablesProps): OSMLTable {
    return new OSMLTable(this, "MRFeaturesTable", {
      tableName: props.config.DDB_FEATURES_TABLE,
      partitionKey: {
        name: "hash_key",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "range_key",
        type: AttributeType.STRING
      },
      removalPolicy: props.removalPolicy,
      ttlAttribute: props.config.DDB_TTL_ATTRIBUTE
    });
  }

  /**
   * Creates the endpoint statistics table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createEndpointStatisticsTable(props: DatabaseTablesProps): OSMLTable {
    return new OSMLTable(this, "MREndpointProcessingTable", {
      tableName: props.config.DDB_ENDPOINT_PROCESSING_TABLE,
      partitionKey: {
        name: "hash_key",
        type: AttributeType.STRING
      },
      removalPolicy: props.removalPolicy,
      ttlAttribute: props.config.DDB_TTL_ATTRIBUTE
    });
  }

  /**
   * Creates the region request table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createRegionRequestTable(props: DatabaseTablesProps): OSMLTable {
    return new OSMLTable(this, "MRRegionRequestTable", {
      tableName: props.config.DDB_REGION_REQUEST_TABLE,
      partitionKey: {
        name: "image_id",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "region_id",
        type: AttributeType.STRING
      },
      removalPolicy: props.removalPolicy,
      ttlAttribute: props.config.DDB_TTL_ATTRIBUTE
    });
  }

  /**
   * Creates backup configuration for production environments.
   *
   * @param props - The database tables properties
   */
  private createBackupConfiguration(props: DatabaseTablesProps): void {
    const backupVault = new BackupVault(this, "MRBackupVault", {
      backupVaultName: "MRBackupVault"
    });

    const backupPlan = new BackupPlan(this, "MRBackupPlan");
    backupPlan.addRule(BackupPlanRule.weekly(backupVault));
    backupPlan.addRule(BackupPlanRule.monthly5Year(backupVault));

    backupPlan.addSelection("MRBackupSelection", {
      resources: [
        BackupResource.fromDynamoDbTable(this.featureTable.table),
        BackupResource.fromDynamoDbTable(this.regionRequestTable.table),
        BackupResource.fromDynamoDbTable(this.endpointStatisticsTable.table),
        BackupResource.fromDynamoDbTable(this.jobStatusTable.table)
      ]
    });
  }
}
