#!/bin/bash
export DEVELOPER_ACCOUNT_ID=010321660603

# This command relies on the fact that the docker images command lists images in most recent order so the most
# recent image built is the one that will be used.
export OVERSIGHTML_MR_IMAGE_ID=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{ print $1 ":" $2 }')

echo "Found Docker Image: $OVERSIGHTML_MR_IMAGE_ID"
echo "Running with resources in account: $DEVELOPER_ACCOUNT_ID"

#
docker run -i --rm \
    --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
    --env AWS_DEFAULT_REGION=us-east-1 \
    --env WORKERS_PER_CPU=1 \
    --env JOB_TABLE=overwatch-mr-jobs \
    --env FEATURE_TABLE=overwatch-mr-features \
    --env IMAGE_QUEUE=https://sqs.us-east-1.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/Oversight-ImageQueue \
    --env REGION_QUEUE=https://sqs.us-east-1.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/Oversight-RegionQueue \
    --cpus 1 \
    -a stdin -a stdout -a stderr \
    ${OVERSIGHTML_MR_IMAGE_ID}