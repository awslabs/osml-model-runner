/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

// TODO: Is this supported in classified regions?
// @see https://docs.aws.amazon.com/guardduty/latest/ug/runtime-monitoring-ecr-repository-gdu-agent.html
export const AMZN_GUARD_DUTY_ECR_ARNS_BY_REGION: Map<string, string> = new Map([
    ["us-west-2", "arn:aws:ecr:us-west-2:733349766148:repository/aws-guardduty-agent-fargate"],
    ["us-west-1", "arn:aws:ecr:us-west-1:684579721401:repository/aws-guardduty-agent-fargate"],
    ["us-east-1", "arn:aws:ecr:us-east-1:593207742271:repository/aws-guardduty-agent-fargate"],
    ["us-east-2", "arn:aws:ecr:us-east-2:307168627858:repository/aws-guardduty-agent-fargate"]
])
