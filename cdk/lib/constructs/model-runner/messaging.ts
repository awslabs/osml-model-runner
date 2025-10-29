/*
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

import { Duration, RemovalPolicy } from "aws-cdk-lib";
import { ITopic, Topic } from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

import { OSMLAccount } from "../types";
import { DataplaneConfig } from "./dataplane";

/**
 * Properties for creating messaging resources.
 */
export interface MessagingProps {
  /** The OSML account configuration. */
  readonly account: OSMLAccount;
  /** The MR dataplane configuration. */
  readonly config: DataplaneConfig;
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
  public readonly imageRequestQueue: Queue;

  /** The SQS queue for region processing requests. */
  public readonly regionRequestQueue: Queue;

  /** The SNS topic for image status notifications. */
  public readonly imageStatusTopic?: ITopic;

  /** The SNS topic for region status notifications. */
  public readonly regionStatusTopic?: ITopic;

  /** The SQS queue for image status updates. */
  public readonly imageStatusQueue?: Queue;

  /** The SQS queue for region status updates. */
  public readonly regionStatusQueue?: Queue;

  /** The dead letter queue for image request queue. */
  public imageRequestDlQueue: Queue;

  /** The dead letter queue for region request queue. */
  public regionRequestDlQueue: Queue;

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
   * @returns The created Queue
   */
  private createImageRequestQueue(props: MessagingProps): Queue {
    this.imageRequestDlQueue = new Queue(this, "ImageRequestQueueDLQ", {
      queueName: `${props.config.SQS_IMAGE_REQUEST_QUEUE}-dlq`,
      retentionPeriod: Duration.days(14)
    });

    return new Queue(this, "ImageRequestQueue", {
      queueName: props.config.SQS_IMAGE_REQUEST_QUEUE,
      visibilityTimeout: Duration.seconds(300),
      retentionPeriod: Duration.days(14),
      removalPolicy: RemovalPolicy.DESTROY,
      deadLetterQueue: {
        maxReceiveCount: 3,
        queue: this.imageRequestDlQueue
      }
    });
  }

  /**
   * Creates the region request queue.
   *
   * @param props - The messaging properties
   * @returns The created Queue
   */
  private createRegionRequestQueue(props: MessagingProps): Queue {
    this.regionRequestDlQueue = new Queue(this, "RegionRequestQueueDLQ", {
      queueName: `${props.config.SQS_REGION_REQUEST_QUEUE}-dlq`,
      retentionPeriod: Duration.days(14)
    });

    return new Queue(this, "RegionRequestQueue", {
      queueName: props.config.SQS_REGION_REQUEST_QUEUE,
      visibilityTimeout: Duration.seconds(300),
      retentionPeriod: Duration.days(14),
      removalPolicy: RemovalPolicy.DESTROY,
      deadLetterQueue: {
        maxReceiveCount: 3,
        queue: this.regionRequestDlQueue
      }
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
        "ImportedImageStatusTopic",
        props.config.SNS_IMAGE_STATUS_TOPIC_ARN
      );
    }

    return new Topic(this, "ImageStatusTopic", {
      topicName: props.config.SNS_IMAGE_STATUS_TOPIC
    });
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
        "ImportedRegionStatusTopic",
        props.config.SNS_REGION_STATUS_TOPIC_ARN
      );
    }

    return new Topic(this, "RegionStatusTopic", {
      topicName: props.config.SNS_REGION_STATUS_TOPIC
    });
  }

  /**
   * Creates the image status queue.
   *
   * @param props - The messaging properties
   * @returns The created Queue
   */
  private createImageStatusQueue(props: MessagingProps): Queue {
    return new Queue(this, "ImageStatusQueue", {
      queueName: props.config.SQS_IMAGE_STATUS_QUEUE,
      visibilityTimeout: Duration.seconds(300),
      retentionPeriod: Duration.days(14),
      removalPolicy: RemovalPolicy.DESTROY
    });
  }

  /**
   * Creates the region status queue.
   *
   * @param props - The messaging properties
   * @returns The created Queue
   */
  private createRegionStatusQueue(props: MessagingProps): Queue {
    return new Queue(this, "RegionStatusQueue", {
      queueName: props.config.SQS_REGION_STATUS_QUEUE,
      visibilityTimeout: Duration.seconds(300),
      retentionPeriod: Duration.days(14),
      removalPolicy: RemovalPolicy.DESTROY
    });
  }

  /**
   * Subscribes the image status topic to the image status queue.
   */
  private subscribeImageStatusTopic(): void {
    if (this.imageStatusTopic && this.imageStatusQueue) {
      this.imageStatusTopic.addSubscription(
        new SqsSubscription(this.imageStatusQueue)
      );
    }
  }

  /**
   * Subscribes the region status topic to the region status queue.
   */
  private subscribeRegionStatusTopic(): void {
    if (this.regionStatusTopic && this.regionStatusQueue) {
      this.regionStatusTopic.addSubscription(
        new SqsSubscription(this.regionStatusQueue)
      );
    }
  }
}
