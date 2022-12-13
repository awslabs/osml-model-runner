#!/bin/bash

export DEVELOPER_ACCOUNT_ID=${5:-$(aws sts get-caller-identity --query Account --output text)}
export REGION=us-west-2

LATEST_IMAGE=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{print $1":"$2}')

docker run -it \
           --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_SESSION_TOKEN \
           --env "AWS_DEFAULT_REGION=${REGION}" \
           --env "image_queue=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/ImageRequestQueue" \
           --env "region_queue=https://sqs.${REGION}.amazonaws.com/${DEVELOPER_ACCOUNT_ID}/RegionRequestQueue" \
           --env "feature_table=ImageProcessingFeatures" \
           --env "job_table=ImageProcessingJobStatus" \
           --env "workers_per_cpu=8" \
           --entrypoint /bin/bash \
           ${LATEST_IMAGE}
