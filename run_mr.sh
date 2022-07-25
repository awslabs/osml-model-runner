#!/bin/bash
LATEST_IMAGE=$(docker images | grep mr-container | awk 'NR==1{print $1":"$2}')

docker run -it \
            --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
            --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
            --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
            --env AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
            --env IMAGE_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AWS_DEFAULT_ACCOUNT}/ImageQueue \
            --env REGION_QUEUE=https://sqs.${AWS_DEFAULT_REGION}.com/${AWS_DEFAULT_ACCOUNT}/RegionQueue \
            --env "feature_table=overwatch-mr-features" \
            --env "job_table=ImageProcessingJobStatus" \
            --env "workers_per_cpu=8" \
            --entrypoint /bin/bash \
            ${LATEST_IMAGE}
