#!/bin/bash
# This script runs the model runner container as an interactive shell that can be used to debug.
export DEVELOPER_ACCOUNT_ID=${5:-$(aws sts get-caller-identity --query Account --output text)}
export REGION=us-west-2

# This command relies on the fact that the docker images command lists images in most recent order so the most
# recent image built is the one that will be used.
export OVERSIGHTML_MR_IMAGE_ID=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{ print $1 ":" $2 }')

echo "Found Docker Image: $OVERSIGHTML_MR_IMAGE_ID"
echo "Running with resources in account: $DEVELOPER_ACCOUNT_ID"
echo "Region is set to: $REGION"

#
docker run -it --rm \
    --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
    --env AWS_DEFAULT_REGION=${REGION} \
    --env WORKERS_PER_CPU=1 \
    --env JOB_TABLE=overwatch-mr-jobs \
    --env FEATURE_TABLE=overwatch-mr-features \
    --env IMAGE_QUEUE=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/ImageRequestQueue \
    --env REGION_QUEUE=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/RegionRequestQueue\
    --cpus 1 \
    --user root \
    --entrypoint /bin/bash \
    ${OVERSIGHTML_MR_IMAGE_ID}