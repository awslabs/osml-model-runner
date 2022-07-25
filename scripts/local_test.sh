#!/bin/bash
# This script runs a locally built model runner container and hooks it up to AWS resources (DDB Tables, SQS Queues, etc.)
# running in the specified account. Note that it attempts to figure out which docker images was built latest. If you
# want to run a specific image you need to set the image ID yourself.
# This command relies on the fact that the docker images command lists images in most recent order so the most
# recent image built is the one that will be used.
export MR_IMAGE_ID=$(docker images | grep mr-container | awk 'NR==1{ print $1 ":" $2 }')

echo "Found Docker Image: $MR_IMAGE_ID"
echo "Running with resources in account: $AWS_DEFAULT_ACCOUNT"

#
docker run -i --rm \
    --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
    --env AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    --env WORKERS_PER_CPU=1 \
    --env JOB_TABLE=overwatch-mr-jobs \
    --env FEATURE_TABLE=overwatch-mr-features \
    --env IMAGE_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AWS_DEFAULT_ACCOUNT}/ImageQueue \
    --env REGION_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AWS_DEFAULT_ACCOUNT}/RegionQueue \
    --cpus 1 \
    -a stdin -a stdout -a stderr \
    ${OVERSIGHTML_MR_IMAGE_ID}