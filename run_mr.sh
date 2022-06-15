#!/bin/bash
LATEST_IMAGE=$(docker images | grep AWSOversightMLModelRunner | awk 'NR==1{print $1":"$2}')

docker run -it \
           --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_SESSION_TOKEN \
           --env "AWS_DEFAULT_REGION=us-west-2" \
           --env "image_queue=https://sqs.us-west-2.amazonaws.com/572096393728/ImageQueue" \
           --env "region_queue=https://sqs.us-west-2.amazonaws.com/572096393728/RegionQueue" \
           --env "feature_table=overwatch-mr-features" \
           --env "job_table=ImageProcessingJobStatus" \
           --env "workers_per_cpu=8" \
           --entrypoint /bin/bash \
           ${LATEST_IMAGE}
