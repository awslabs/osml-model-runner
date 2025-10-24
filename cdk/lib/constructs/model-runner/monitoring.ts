/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import {
  Dashboard,
  GraphWidget,
  GraphWidgetView,
  LogQueryVisualizationType,
  LogQueryWidget,
  MathExpression,
  SingleValueWidget
} from "aws-cdk-lib/aws-cloudwatch";
import { FargateService } from "aws-cdk-lib/aws-ecs";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";
import { DataplaneConfig } from "./dataplane";

/**
 * Interface for MR Monitoring Props.
 *
 * @interface MonitoringProps
 */
export interface MonitoringProps {
  /**
   * The OSML account interface.
   *
   * @type {OSMLAccount}
   */
  account: OSMLAccount;

  /**
   * The model runner region request queue.
   *
   * @type {Queue}
   */
  regionRequestQueue: Queue;

  /**
   * The model runner region request DLQ (Dead Letter Queue).
   *
   * @type {Queue}
   */
  regionRequestDlQueue: Queue;

  /**
   * The model runner image request queue.
   *
   * @type {Queue}
   */
  imageRequestQueue: Queue;

  /**
   * The model runner image request DLQ (Dead Letter Queue).
   *
   * @type {Queue}
   */
  imageRequestDlQueue: Queue;

  /**
   * The model runner Fargate service.
   *
   * @type {FargateService}
   */
  service: FargateService;

  /**
   * The model runner dataplane configuration.
   *
   * @type {DataplaneConfig}
   */
  mrDataplaneConfig: DataplaneConfig;
}

/**
 * Represents an Monitoring construct for monitoring various metrics and statistics.
 */
export class Monitoring extends Construct {
  /**
   * Creates a new Monitoring construct.
   * @param {Construct} scope - The scope/stack in which to define this construct.
   * @param {string} id - The id of this construct within the current scope.
   * @param {MonitoringProps} props - The properties of this construct.
   * @returns {Monitoring} - The Monitoring construct.
   */
  public mrDashboard: Dashboard;
  constructor(scope: Construct, id: string, props: MonitoringProps) {
    super(scope, id);

    // Create a dashboard for monitoring
    this.mrDashboard = new Dashboard(this, "Dashboard", {
      dashboardName: "ModelRunner"
    });

    const topRowWidgets = [
      new SingleValueWidget({
        region: props.account.region,
        title: "Completed",
        width: 3,
        height: 5,
        metrics: [
          new MathExpression({
            label: "Images",
            expression:
              "SUM(SEARCH('{OSML/ModelRunner, InputFormat, ModelName, Operation} ImageProcessing Invocations', 'Sum'))"
          }),
          new MathExpression({
            label: "Tiles",
            expression:
              "SUM(SEARCH('{OSML/ModelRunner, ModelName, Operation} TileProcessing Invocations', 'Sum'))"
          })
        ],
        sparkline: false,
        setPeriodToTimeRange: true
      }),
      new GraphWidget({
        region: props.account.region,
        title: "Average Image Processing Time (Seconds)",
        view: GraphWidgetView.BAR,
        height: 5,
        width: 5,
        left: [
          new MathExpression({
            label: "",
            expression:
              "SEARCH('{OSML/ModelRunner, InputFormat, ModelName, Operation} ImageProcessing Duration', 'Average')"
          })
        ],
        statistic: "Average",
        leftAnnotations: [
          { color: "#2ca02c", label: "5m", value: 300 },
          { color: "#d62728", label: "15m", value: 900 }
        ]
      }),
      new GraphWidget({
        region: props.account.region,
        title: "Request Volume",
        height: 5,
        width: 11,
        view: GraphWidgetView.TIME_SERIES,
        stacked: false,
        statistic: "Sum",
        left: [
          props.imageRequestQueue.metricNumberOfMessagesReceived({
            label: "Image Requests Received",
            statistic: "Sum"
          }),
          props.imageRequestQueue.metricApproximateNumberOfMessagesVisible({
            label: "Image Requests Waiting",
            statistic: "Sum"
          })
        ]
      }),
      new GraphWidget({
        region: props.account.region,
        title: "Input Formats",
        height: 5,
        width: 5,
        view: GraphWidgetView.PIE,
        statistic: "Average",
        setPeriodToTimeRange: true,
        left: [
          new MathExpression({
            expression:
              "SEARCH('{OSML/ModelRunner, InputFormat, Operation}  TileGeneration Invocations', 'Sum')",
            label: ""
          })
        ]
      })
    ];

    this.mrDashboard.addWidgets(...topRowWidgets);

    this.mrDashboard.addWidgets(
      new GraphWidget({
        region: props.account.region,
        title: "Model Utilization",
        width: 24,
        height: 6,
        view: GraphWidgetView.TIME_SERIES,
        stacked: false,
        statistic: "Average",
        left: [
          new MathExpression({
            expression:
              "SEARCH('{OSML/ModelRunner, ModelName, Operation} ModelInvocation Duration', 'Average', 60)",
            label: ""
          })
        ],
        right: [
          new MathExpression({
            expression:
              "SEARCH('{OSML/ModelRunner, ModelName, Operation} ModelInvocation Invocations', 'Sum', 60)",
            label: ""
          }),
          new MathExpression({
            expression:
              "SEARCH('{OSML/ModelRunner, ModelName, Operation} ModelInvocation Errors', 'Sum', 60)",
            label: ""
          }),
          new MathExpression({
            expression:
              "SEARCH('{OSML/ModelRunner, ModelName, Operation} ModelInvocation Retries', 'Sum', 60)",
            label: ""
          })
        ]
      })
    );

    this.mrDashboard.addWidgets(
      new LogQueryWidget({
        title: "Image Status",
        width: 24,
        height: 11,
        region: props.account.region,
        logGroupNames: ["/aws/OSML/MRService"],
        view: LogQueryVisualizationType.TABLE,
        queryLines: [
          "fields job_id as Job, request.image_url as Image, request.model_name as Model, status as Status, ",
          '  concat(request.region_success, "/", request.region_count) as Regions, request.region_error as Failures,',
          "  fromMillis(request.start_time) as Start, fromMillis(request.end_time) as End",
          'filter message = "ImageStatusMonitorUpdate" and status in ["SUCCESS", "FAILED"]',
          "sort @timestamp desc",
          "limit 50"
        ]
      })
    );
  }
}
