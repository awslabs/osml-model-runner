#!/bin/bash
# This script runs the model runner container as an interactive shell that can be used to debug.
# This command relies on the fact that the docker images command lists images in most recent order so the most
# recent image built is the one that will be used.
export MR_IMAGE_ID=$(docker images | grep mr-container | awk 'NR==1{ print $1 ":" $2 }')

echo "Found Docker Image: $MR_IMAGE_ID"
echo "Running with resources in account: ${AWS_DEFAULT_ACCOUNT}"

#
docker run -it --rm \
    --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
    --env AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    --env WORKERS_PER_CPU=1 \
    --env JOB_TABLE=ImageProcessingJobStatus \
    --env FEATURE_TABLE=ImageProcessingFeatures \
    --env IMAGE_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AWS_DEFAULT_ACCOUNT}/ImageQueue \
    --env REGION_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AWS_DEFAULT_ACCOUNT}/RegionQueue \
    --cpus 1 \
    --user root \
    --entrypoint /bin/bash \
    ${MR_IMAGE_ID}