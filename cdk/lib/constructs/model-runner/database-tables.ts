/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { RemovalPolicy } from "aws-cdk-lib";
import { BackupPlan, BackupPlanRule, BackupResource, BackupVault } from "aws-cdk-lib/aws-backup";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";
import { ModelRunnerDataplaneConfig } from "./model-runner-dataplane";
import { AttributeType, BillingMode, Table, TableEncryption } from "aws-cdk-lib/aws-dynamodb";

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
  /** The DynamoDB table for outstanding image processing requests. */
  public readonly outstandingImageRequestsTable: Table;

  /** The DynamoDB table for image request status. */
  public readonly imageRequestTable: Table;

  /** The DynamoDB table for feature data. */
  public readonly featureTable: Table;

  /** The DynamoDB table for endpoint statistics. */
  public readonly endpointStatisticsTable: Table;

  /** The DynamoDB table for region request status. */
  public readonly regionRequestTable: Table;

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
    this.outstandingImageRequestsTable = this.createOutstandingImageRequestsTable(props);
    this.imageRequestTable = this.createImageRequestTable(props);
    this.featureTable = this.createFeatureTable(props);
    this.endpointStatisticsTable = this.createEndpointStatisticsTable(props);
    this.regionRequestTable = this.createRegionRequestTable(props);

    // Create backup configuration for production environments
    if (props.account.prodLike && !props.account.isAdc) {
      this.createBackupConfiguration(props);
    }
  }

  /**
   * Creates the outstanding image requests table.
   *
   * @param props - The database tables properties
   * @returns The created Table
   */
  private createOutstandingImageRequestsTable(props: DatabaseTablesProps): Table {
    return new Table(this, "OutstandingImageRequestsTable", {
      tableName: props.config.DDB_OUTSTANDING_IMAGE_REQUESTS_TABLE,
      partitionKey: {
        name: "endpoint_id",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "job_id",
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: props.removalPolicy || RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      encryption: TableEncryption.AWS_MANAGED
    });
  }

  /**
   * Creates the image request table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createImageRequestTable(props: DatabaseTablesProps): Table {
    return new Table(this, "ImageRequestTable", {
      tableName: props.config.DDB_IMAGE_REQUEST_TABLE,
      partitionKey: {
        name: "image_id",
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: props.removalPolicy || RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      encryption: TableEncryption.AWS_MANAGED
    });
  }

  /**
   * Creates the feature table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createFeatureTable(props: DatabaseTablesProps): Table {
    const table = new Table(this, "FeaturesTable", {
      tableName: props.config.DDB_FEATURES_TABLE,
      partitionKey: {
        name: "hash_key",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "range_key",
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: props.removalPolicy || RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      encryption: TableEncryption.AWS_MANAGED
    });

    // Add TTL attribute
    table.addGlobalSecondaryIndex({
      indexName: "ttl-gsi",
      partitionKey: { name: props.config.DDB_TTL_ATTRIBUTE, type: AttributeType.STRING }
    });

    return table;
  }

  /**
   * Creates the endpoint statistics table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createEndpointStatisticsTable(props: DatabaseTablesProps): Table {
    const table = new Table(this, "EndpointProcessingTable", {
      tableName: props.config.DDB_ENDPOINT_PROCESSING_TABLE,
      partitionKey: {
        name: "hash_key",
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: props.removalPolicy || RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      encryption: TableEncryption.AWS_MANAGED
    });

    // Add TTL attribute
    table.addGlobalSecondaryIndex({
      indexName: "ttl-gsi",
      partitionKey: { name: props.config.DDB_TTL_ATTRIBUTE, type: AttributeType.STRING }
    });

    return table;
  }

  /**
   * Creates the region request table.
   *
   * @param props - The database tables properties
   * @returns The created OSMLTable
   */
  private createRegionRequestTable(props: DatabaseTablesProps): Table {
    const table = new Table(this, "RegionRequestTable", {
      tableName: props.config.DDB_REGION_REQUEST_TABLE,
      partitionKey: {
        name: "image_id",
        type: AttributeType.STRING
      },
      sortKey: {
        name: "region_id",
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: props.removalPolicy || RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      encryption: TableEncryption.AWS_MANAGED
    });

    // Add TTL attribute
    table.addGlobalSecondaryIndex({
      indexName: "ttl-gsi",
      partitionKey: { name: props.config.DDB_TTL_ATTRIBUTE, type: AttributeType.STRING }
    });

    return table;
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
        BackupResource.fromDynamoDbTable(this.featureTable),
        BackupResource.fromDynamoDbTable(this.regionRequestTable),
        BackupResource.fromDynamoDbTable(this.endpointStatisticsTable),
        BackupResource.fromDynamoDbTable(this.imageRequestTable)
      ]
    });
  }
}
