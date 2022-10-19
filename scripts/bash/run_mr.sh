#!/bin/bash
LATEST_IMAGE=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{print $1":"$2}')

docker run -it \
           --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_SESSION_TOKEN \
           --env "AWS_DEFAULT_REGION=us-east-1" \
           --env "image_queue=https://sqs.us-east-1.amazonaws.com/010321660603/Oversight-ImageQueue" \
           --env "region_queue=https://sqs.us-east-1.amazonaws.com/010321660603/Oversight-RegionQueue" \
           --env "feature_table=overwatch-mr-features" \
           --env "job_table=overwatch-mr-jobs" \
           --env "workers_per_cpu=8" \
           --entrypoint /bin/bash \
           ${LATEST_IMAGE}
