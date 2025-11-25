/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { EcsIsoServiceAutoscaler } from "@cdklabs/cdk-enterprise-iac";
import { Duration } from "aws-cdk-lib";
import { Alarm } from "aws-cdk-lib/aws-cloudwatch";
import { Cluster, FargateService } from "aws-cdk-lib/aws-ecs";
import { IRole } from "aws-cdk-lib/aws-iam";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";
import { DataplaneConfig } from "./dataplane";

/**
 * Properties for creating autoscaling resources.
 */
export interface AutoscalingProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The MR dataplane configuration. */
  readonly config: DataplaneConfig;
  /** The Fargate service to autoscale. */
  readonly fargateService: FargateService;
  /** The ECS task role. */
  readonly taskRole: IRole;
  /** The ECS cluster. */
  readonly cluster: Cluster;
  /** The image request queue. */
  readonly imageRequestQueue: Queue;
  /** The region request queue. */
  readonly regionRequestQueue: Queue;
}

/**
 * Construct that manages autoscaling resources for the Model Runner.
 *
 * This construct encapsulates the creation and configuration of autoscaling
 * policies for the Model Runner Fargate service.
 */
export class Autoscaling extends Construct {
  /** The service autoscaler. */
  public readonly serviceAutoscaler?: EcsIsoServiceAutoscaler;

  /**
   * Creates a new Autoscaling construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: AutoscalingProps) {
    super(scope, id);

    // Create autoscaling configuration
    this.serviceAutoscaler = this.createAutoscaling(props);
  }

  /**
   * Creates the autoscaling configuration.
   *
   * @param props - The autoscaling properties
   * @returns The created EcsIsoServiceAutoscaler or undefined
   */
  private createAutoscaling(
    props: AutoscalingProps
  ): EcsIsoServiceAutoscaler | undefined {
    if (props.account.isAdc) {
      return this.createAdcAutoscaling(props);
    } else {
      this.createStandardAutoscaling(props);
      return undefined;
    }
  }

  /**
   * Creates autoscaling configuration for ADC environments.
   *
   * @param props - The autoscaling properties
   * @returns The created EcsIsoServiceAutoscaler
   */
  private createAdcAutoscaling(
    props: AutoscalingProps
  ): EcsIsoServiceAutoscaler {
    // Scale out when region queue has backlog
    // Using a higher threshold and multiple evaluation periods to prevent rapid scaling
    const regionQueueScalingAlarm = new Alarm(this, "RegionQueueScalingAlarm", {
      metric:
        props.regionRequestQueue.metricApproximateNumberOfMessagesVisible(),
      evaluationPeriods: 3,
      threshold: 10,
      datapointsToAlarm: 2
    });

    return new EcsIsoServiceAutoscaler(this, "MRServiceAutoscaling", {
      role: props.taskRole,
      ecsCluster: props.cluster,
      ecsService: props.fargateService,
      minimumTaskCount: props.config.ECS_AUTOSCALING_TASK_MIN_COUNT,
      maximumTaskCount: props.config.ECS_AUTOSCALING_TASK_MAX_COUNT,
      scaleAlarm: regionQueueScalingAlarm,
      scaleOutIncrement: props.config.ECS_AUTOSCALING_TASK_OUT_INCREMENT,
      scaleInIncrement: props.config.ECS_AUTOSCALING_TASK_IN_INCREMENT,
      scaleOutCooldown: Duration.minutes(
        props.config.ECS_AUTOSCALING_TASK_OUT_COOLDOWN
      ),
      scaleInCooldown: Duration.minutes(
        props.config.ECS_AUTOSCALING_TASK_IN_COOLDOWN
      )
    });
  }

  /**
   * Creates standard autoscaling configuration for non-ADC environments.
   *
   * @param props - The autoscaling properties
   */
  private createStandardAutoscaling(props: AutoscalingProps): void {
    const mrServiceScaling = props.fargateService.autoScaleTaskCount({
      maxCapacity: props.config.ECS_AUTOSCALING_TASK_MAX_COUNT,
      minCapacity: props.config.ECS_AUTOSCALING_TASK_MIN_COUNT
    });

    // Scale based on region queue messages
    mrServiceScaling.scaleOnMetric("RegionQueueScaling", {
      metric:
        props.regionRequestQueue.metricApproximateNumberOfMessagesVisible(),
      scalingSteps: [
        { change: +3, lower: 1 },
        { change: +5, lower: 5 },
        { change: +8, lower: 20 },
        { change: +15, lower: 100 }
      ]
    });

    // Scale based on image queue depth (messages waiting to be processed)
    // This helps ensure capacity is available when new image requests arrive
    mrServiceScaling.scaleOnMetric("ImageQueueScaling", {
      metric:
        props.imageRequestQueue.metricApproximateNumberOfMessagesVisible(),
      scalingSteps: [
        { change: +2, lower: 5 },
        { change: +5, lower: 20 },
        { change: +10, lower: 50 }
      ],
      cooldown: Duration.minutes(2),
      evaluationPeriods: 2
    });
  }
}
