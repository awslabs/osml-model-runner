# SageMaker Async Endpoint Integration Guide

This guide provides comprehensive documentation for using the SageMaker Async Endpoint integration with the OSML Model Runner extensions.

## Table of Contents

1. [Overview](#overview)
2. [Configuration](#configuration)
3. [Basic Usage](#basic-usage)
4. [Worker Pool Optimization](#worker-pool-optimization)
5. [Resource Management](#resource-management)
6. [Error Handling](#error-handling)
7. [Performance Tuning](#performance-tuning)
8. [Troubleshooting](#troubleshooting)
9. [IAM Permissions](#iam-permissions)

## Overview

The SageMaker Async Endpoint integration enables processing of large-scale inference workloads using Amazon SageMaker's asynchronous inference capabilities. This integration provides:

- **True Asynchronous Processing**: Submit inference requests without blocking, allowing for better resource utilization
- **S3-Based Input/Output**: Automatic handling of large payloads through S3 storage
- **Worker Pool Optimization**: Separate submission and polling workers for maximum throughput
- **Comprehensive Resource Management**: Automatic cleanup of temporary resources
- **Robust Error Handling**: Retry logic and graceful failure handling
- **Detailed Metrics**: Comprehensive timing and performance metrics

## Configuration

### AsyncEndpointConfig

The `AsyncEndpointConfig` class provides comprehensive configuration options for async endpoint operations:

```python
from osml_extensions.config import AsyncEndpointConfig

# Basic configuration
config = AsyncEndpointConfig(
    input_bucket="my-async-input-bucket",
    max_wait_time=3600,  # 1 hour timeout
    polling_interval=30,  # Poll every 30 seconds initially
    cleanup_enabled=True
)

# Advanced configuration with worker pool optimization
config = AsyncEndpointConfig(
    input_bucket="my-async-input-bucket",

    # Polling configuration
    max_wait_time=7200,  # 2 hour timeout
    polling_interval=15,  # Start polling every 15 seconds
    max_polling_interval=300,  # Max 5 minutes between polls
    exponential_backoff_multiplier=1.5,

    # S3 operation configuration
    max_retries=5,
    cleanup_enabled=True,
    cleanup_policy="delayed",  # immediate, delayed, disabled
    cleanup_delay_seconds=600,  # 10 minutes delay

    # Worker pool optimization
    enable_worker_optimization=True,
    submission_workers=8,  # Number of submission workers
    polling_workers=4,     # Number of polling workers
    max_concurrent_jobs=200,
    job_queue_timeout=300
)
```

### Environment Variables

All configuration options can be set via environment variables:

```bash
# S3 Configuration
export ASYNC_SM_INPUT_BUCKET="my-async-input-bucket"
export ASYNC_SM_INPUT_PREFIX="async-inference/input/"
export ASYNC_SM_OUTPUT_PREFIX="async-inference/output/"

# Polling Configuration
export ASYNC_SM_MAX_WAIT_TIME="3600"
export ASYNC_SM_POLLING_INTERVAL="30"
export ASYNC_SM_MAX_POLLING_INTERVAL="300"
export ASYNC_SM_BACKOFF_MULTIPLIER="1.5"

# S3 Operations
export ASYNC_SM_MAX_RETRIES="3"
export ASYNC_SM_CLEANUP_ENABLED="true"
export ASYNC_SM_CLEANUP_POLICY="immediate"
export ASYNC_SM_CLEANUP_DELAY_SECONDS="300"

# Worker Pool Optimization
export ASYNC_SM_WORKER_OPTIMIZATION="true"
export ASYNC_SM_SUBMISSION_WORKERS="4"
export ASYNC_SM_POLLING_WORKERS="2"
export ASYNC_SM_MAX_CONCURRENT_JOBS="100"
export ASYNC_SM_JOB_QUEUE_TIMEOUT="300"
```

## Worker Pool Optimization

The worker pool optimization separates submission and polling operations for maximum throughput:

### Architecture

- **Submission Workers**: Quickly submit tiles to async endpoints without waiting
- **Polling Workers**: Independently monitor job completion and process results
- **Job Queue**: Coordinates between submission and polling workers
- **Result Queue**: Collects completed results for final processing

### Configuration

```python
from osml_extensions.workers import AsyncTileWorkerPool
from osml_extensions.metrics import AsyncMetricsTracker

# Create optimized configuration
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    enable_worker_optimization=True,
    submission_workers=8,  # Optimize based on endpoint capacity
    polling_workers=4,     # Fewer polling workers needed
    max_concurrent_jobs=200
)

# Create detector and worker pool
detector = AsyncSMDetector(endpoint="my-endpoint", async_config=config)
metrics_tracker = AsyncMetricsTracker()

worker_pool = AsyncTileWorkerPool(
    async_detector=detector,
    config=config,
    metrics_tracker=metrics_tracker
)

# Process tiles with optimization
tile_queue = Queue()
# Add tiles to queue...

total_processed, total_failed = worker_pool.process_tiles_async(tile_queue)
print(f"Processed: {total_processed}, Failed: {total_failed}")
```

### Performance Benefits

Worker pool optimization provides significant performance improvements:

- **Increased Throughput**: Up to 3-5x improvement in tile processing speed
- **Better Resource Utilization**: Separate workers prevent blocking operations
- **Scalable Architecture**: Easy to tune worker counts based on workload
- **Reduced Latency**: Immediate submission without waiting for completion

## Resource Management

The integration includes comprehensive resource management with configurable cleanup policies:

### Cleanup Policies

```python
from osml_extensions.utils import CleanupPolicy

# Immediate cleanup (default)
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    cleanup_policy="immediate"
)

# Delayed cleanup (useful for debugging)
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    cleanup_policy="delayed",
    cleanup_delay_seconds=1800  # 30 minutes
)

# Disabled cleanup (for development/debugging)
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    cleanup_policy="disabled"
)
```

### Manual Resource Management

```python
# Get resource statistics
stats = detector.get_resource_stats()
print(f"Total resources: {stats['total_resources']}")
print(f"S3 objects: {stats['by_type']['s3_object']['total']}")

# Manual cleanup
cleaned_count = detector.cleanup_resources(force=True)
print(f"Cleaned up {cleaned_count} resources")
```

### Resource Types Managed

- **S3 Objects**: Input and output data files
- **Temporary Files**: Local temporary files created during processing
- **Inference Jobs**: SageMaker async inference job metadata
- **Worker Threads**: Background worker threads for processing

## Error Handling

The integration provides robust error handling for various failure scenarios:

### Common Error Types

```python
from osml_extensions.errors import (
    ExtensionRuntimeError,
    AsyncInferenceTimeoutError
)
from osml_extensions.s3 import S3OperationError
from osml_extensions.polling import AsyncInferenceTimeoutError

try:
    result = detector.find_features(payload)
except S3OperationError as e:
    print(f"S3 operation failed: {e}")
    # Handle S3 permission or connectivity issues

except AsyncInferenceTimeoutError as e:
    print(f"Inference timed out: {e}")
    # Handle long-running inference jobs

except ExtensionRuntimeError as e:
    print(f"Runtime error: {e}")
    # Handle general runtime issues
```

### Retry Configuration

```python
# Configure retry behavior
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    max_retries=5,  # Retry S3 operations up to 5 times
    max_wait_time=7200  # Wait up to 2 hours for completion
)
```

### Error Recovery

The system automatically handles:

- **Network Failures**: Automatic retry with exponential backoff
- **S3 Permission Issues**: Clear error messages and retry logic
- **Endpoint Unavailability**: Graceful failure with proper cleanup
- **Worker Failures**: Automatic worker restart and job redistribution

## Performance Tuning

### Optimal Configuration Guidelines

#### For High-Throughput Workloads

```python
config = AsyncEndpointConfig(
    # Aggressive polling for faster completion detection
    polling_interval=10,
    max_polling_interval=60,
    exponential_backoff_multiplier=1.2,

    # More workers for parallel processing
    submission_workers=12,
    polling_workers=6,
    max_concurrent_jobs=300,

    # Immediate cleanup to free resources quickly
    cleanup_policy="immediate"
)
```

#### For Cost-Optimized Workloads

```python
config = AsyncEndpointConfig(
    # Conservative polling to reduce API calls
    polling_interval=60,
    max_polling_interval=600,
    exponential_backoff_multiplier=2.0,

    # Fewer workers to reduce resource usage
    submission_workers=4,
    polling_workers=2,
    max_concurrent_jobs=50,

    # Delayed cleanup to batch operations
    cleanup_policy="delayed",
    cleanup_delay_seconds=3600
)
```

### Monitoring Performance

```python
from osml_extensions.metrics import AsyncMetricsTracker

# Create metrics tracker
metrics = AsyncMetricsTracker()

# Use with detector
detector = AsyncSMDetector(endpoint="my-endpoint", async_config=config)
result = detector.find_features(payload, metrics=metrics)

# Get performance metrics
print(f"S3 Upload Time: {metrics.get_metric('S3UploadDuration')}")
print(f"Total Processing Time: {metrics.get_metric('TotalAsyncDuration')}")
print(f"Queue Time: {metrics.get_metric('QueueTime')}")
```
