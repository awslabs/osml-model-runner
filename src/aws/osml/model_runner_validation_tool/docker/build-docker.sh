#!/bin/bash
# Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

# For local development:
# account=123456789123
# region=us-east-2
# ./scripts/mr_validation_tool_update_ecs_images.sh $account us-east-2 benchmarking
# docker run -e AWS_REGION=$region -e LOG_LEVEL='debug' \
#   $account.dkr.ecr.$region.amazonaws.com/osml-validation-tool/benchmarking

# Exit on error
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <docker name>"
  echo
  echo "Example: $0 benchmarking"
  exit 1
fi
docker_name=$1
docker_script=$1.py

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../../../" && pwd )"
SRC_DIR="$PROJECT_ROOT/src"
COMMON_DIR="$SRC_DIR/aws/osml/model_runner_validation_tool/common"
DOCKER_DIR="$SRC_DIR/aws/osml/model_runner_validation_tool/docker"
BUILD_DIR="$DOCKER_DIR/build"

echo "Preparing Docker build directory..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/aws/osml/model_runner_validation_tool/common"

# Copy main handler
cp "$DOCKER_DIR/$docker_script" "$BUILD_DIR/"

# Copy common utilities
cp -r "$COMMON_DIR"/* "$BUILD_DIR/aws/osml/model_runner_validation_tool/common/"

# Create __init__.py files
touch "$BUILD_DIR/aws/__init__.py"
touch "$BUILD_DIR/aws/osml/__init__.py"
touch "$BUILD_DIR/aws/osml/model_runner_validation_tool/__init__.py"
touch "$BUILD_DIR/aws/osml/model_runner_validation_tool/common/__init__.py"

# Create a simplified Dockerfile in the build directory
cat > "$BUILD_DIR/Dockerfile" << EOF
# Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir boto3 numpy pandas matplotlib

# Copy all files from build directory
COPY . /app/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the docker $docker_name script
ENTRYPOINT ["python", "$docker_script"]
EOF

image_prefix="osml-validation-tool"
echo "Building Docker image..."
docker build -t "${image_prefix}-$docker_name" "$BUILD_DIR"

echo "Docker ${image_prefix}-${docker_name} build complete!"
