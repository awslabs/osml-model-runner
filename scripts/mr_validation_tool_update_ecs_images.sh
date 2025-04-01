#!/bin/bash
#
# Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
#

# ./scripts/mr_validation_tool_update_ecs_images.sh [AWS account] [region name] benchmarking
# ./scripts/mr_validation_tool_update_ecs_images.sh [AWS account] [region name] inference_recommender

if [ $# -ne 3 ]; then
    echo "Usage: $0 [AWS account] [region name] <(benchmarking|inference_recommender)>"
    echo
    echo "Example: $0 123456789123 us-east-2 inference_recommender"
    exit 1
fi

account=$1
region=$2
docker_name=$3
docker_script=$3.py
docker_name_prefix="osml-validation-tool"

ecs_image="${account}.dkr.ecr.${region}.amazonaws.com/${docker_name_prefix}/${docker_name}"

aws --profile osml ecr get-login-password --region ${region} | \
docker login --username AWS --password-stdin ${account}.dkr.ecr.${region}.amazonaws.com

# Bundle docker artifacts including common python code and build
./src/aws/osml/model_runner_validation_tool/docker/build-docker.sh ${docker_name}

# Tag uploaded image and upload
ecr_repo="${account}.dkr.ecr.${region}.amazonaws.com"
docker tag "${docker_name_prefix}-${docker_name}:latest" "${ecr_repo}/${docker_name_prefix}/$docker_name:latest"
docker push "${ecr_repo}/${docker_name_prefix}/${docker_name}:latest"
