/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { ITopic, Topic } from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Construct } from "constructs";

import { OSMLAccount, OSMLQueue, OSMLTopic } from "osml-cdk-constructs";
import { ModelRunnerDataplaneConfig } from "./model-runner-dataplane";

/**
 * Properties for creating messaging resources.
 */
export interface MessagingProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The MR dataplane configuration. */
  readonly config: ModelRunnerDataplaneConfig;
}

/**
 * Construct that manages all SQS queues and SNS topics for the Model Runner.
 *
 * This construct encapsulates the creation and configuration of all messaging
 * resources required by the Model Runner, including SQS queues for processing
 * requests and SNS topics for status notifications.
 */
export class Messaging extends Construct {
  /** The SQS queue for image processing requests. */
  public readonly imageRequestQueue: OSMLQueue;

  /** The SQS queue for region processing requests. */
  public readonly regionRequestQueue: OSMLQueue;

  /** The SNS topic for image status notifications. */
  public readonly imageStatusTopic?: ITopic;

  /** The SNS topic for region status notifications. */
  public readonly regionStatusTopic?: ITopic;

  /** The SQS queue for image status updates. */
  public readonly imageStatusQueue?: OSMLQueue;

  /** The SQS queue for region status updates. */
  public readonly regionStatusQueue?: OSMLQueue;

  /**
   * Creates a new Messaging construct.
   *
   * @param scope - The scope/stack in which to define this construct
   * @param id - The id of this construct within the current scope
   * @param props - The properties for configuring this construct
   */
  constructor(scope: Construct, id: string, props: MessagingProps) {
    super(scope, id);

    // Create core processing queues
    this.imageRequestQueue = this.createImageRequestQueue(props);
    this.regionRequestQueue = this.createRegionRequestQueue(props);

    // Create status notification resources if enabled
    if (props.config.MR_ENABLE_IMAGE_STATUS) {
      this.imageStatusTopic = this.createImageStatusTopic(props);
      this.imageStatusQueue = this.createImageStatusQueue(props);
      this.subscribeImageStatusTopic();
    }

    if (props.config.MR_ENABLE_REGION_STATUS) {
      this.regionStatusTopic = this.createRegionStatusTopic(props);
      this.regionStatusQueue = this.createRegionStatusQueue(props);
      this.subscribeRegionStatusTopic();
    }
  }

  /**
   * Creates the image request queue.
   *
   * @param props - The messaging properties
   * @returns The created OSMLQueue
   */
  private createImageRequestQueue(props: MessagingProps): OSMLQueue {
    return new OSMLQueue(this, "MRImageRequestQueue", {
      queueName: props.config.SQS_IMAGE_REQUEST_QUEUE
    });
  }

  /**
   * Creates the region request queue.
   *
   * @param props - The messaging properties
   * @returns The created OSMLQueue
   */
  private createRegionRequestQueue(props: MessagingProps): OSMLQueue {
    return new OSMLQueue(this, "MRRegionRequestQueue", {
      queueName: props.config.SQS_REGION_REQUEST_QUEUE
    });
  }

  /**
   * Creates the image status topic.
   *
   * @param props - The messaging properties
   * @returns The created or imported ITopic
   */
  private createImageStatusTopic(props: MessagingProps): ITopic {
    if (props.config.SNS_IMAGE_STATUS_TOPIC_ARN) {
      return Topic.fromTopicArn(
        this,
        "ImportedMRImageStatusTopic",
        props.config.SNS_IMAGE_STATUS_TOPIC_ARN
      );
    }

    return new OSMLTopic(this, "MRImageStatusTopic", {
      topicName: props.config.SNS_IMAGE_STATUS_TOPIC
    }).topic;
  }

  /**
   * Creates the region status topic.
   *
   * @param props - The messaging properties
   * @returns The created or imported ITopic
   */
  private createRegionStatusTopic(props: MessagingProps): ITopic {
    if (props.config.SNS_REGION_STATUS_TOPIC_ARN) {
      return Topic.fromTopicArn(
        this,
        "ImportedMRRegionStatusTopic",
        props.config.SNS_REGION_STATUS_TOPIC_ARN
      );
    }

    return new OSMLTopic(this, "MRRegionStatusTopic", {
      topicName: props.config.SNS_REGION_STATUS_TOPIC
    }).topic;
  }

  /**
   * Creates the image status queue.
   *
   * @param props - The messaging properties
   * @returns The created OSMLQueue
   */
  private createImageStatusQueue(props: MessagingProps): OSMLQueue {
    return new OSMLQueue(this, "MRImageStatusQueue", {
      queueName: props.config.SQS_IMAGE_STATUS_QUEUE
    });
  }

  /**
   * Creates the region status queue.
   *
   * @param props - The messaging properties
   * @returns The created OSMLQueue
   */
  private createRegionStatusQueue(props: MessagingProps): OSMLQueue {
    return new OSMLQueue(this, "MRRegionStatusQueue", {
      queueName: props.config.SQS_REGION_STATUS_QUEUE
    });
  }

  /**
   * Subscribes the image status topic to the image status queue.
   */
  private subscribeImageStatusTopic(): void {
    if (this.imageStatusTopic && this.imageStatusQueue) {
      this.imageStatusTopic.addSubscription(
        new SqsSubscription(this.imageStatusQueue.queue)
      );
    }
  }

  /**
   * Subscribes the region status topic to the region status queue.
   */
  private subscribeRegionStatusTopic(): void {
    if (this.regionStatusTopic && this.regionStatusQueue) {
      this.regionStatusTopic.addSubscription(
        new SqsSubscription(this.regionStatusQueue.queue)
      );
    }
  }
}
