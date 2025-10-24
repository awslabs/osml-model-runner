#!/bin/bash
# This script runs a locally built model runner container and hooks it up to AWS resources (DDB Tables, SQS Queues, etc.)
# running in the specified account. Note that it attempts to figure out which docker images was built latest. If you
# want to run a specific image you need to set the image ID yourself.
export DEVELOPER_ACCOUNT_ID=${5:-$(aws sts get-caller-identity --query Account --output text)}
export REGION=us-west-2

# This command relies on the fact that the docker images command lists images in most recent order so the most
# recent image built is the one that will be used.
export OVERSIGHTML_MR_IMAGE_ID=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{ print $1 ":" $2 }')

echo "Found Docker Image: $OVERSIGHTML_MR_IMAGE_ID"
echo "Running with resources in account: $DEVELOPER_ACCOUNT_ID"
echo "Region is set to: $REGION"

#
docker run -i --rm \
    --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
    --env AWS_DEFAULT_REGION=${REGION} \
    --env WORKERS_PER_CPU=1 \
    --env JOB_TABLE=ImageProcessingJobFeatures \
    --env FEATURE_TABLE=ImageProcessingFeatures \
    --env REGION_REQUEST_TABLE=RegionProcessingJobStatus \
    --env ENDPOINT_TABLE=EndpointProcessingStatistics \
    --env WORKERS=4 \
    --env IMAGE_QUEUE=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/ImageRequestQueue \
    --env REGION_QUEUE=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/RegionRequestQueue \
    --cpus 1 \
    -a stdin -a stdout -a stderr \
    ${OVERSIGHTML_MR_IMAGE_ID}
