#!/bin/bash

docker run --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_SESSION_TOKEN \
           --env "AWS_DEFAULT_REGION=us-east-1" \
           --env "image_queue=https://sqs.us-east-1.amazonaws.com/010321660603/Oversight-ImageQueue" \
           --env "feature_table=overwatch-mr-features" \
           --env "job_table=overwatch-mr-jobs" \
           --env "workers_per_cpu=8" \
           oversight-mr:latest
