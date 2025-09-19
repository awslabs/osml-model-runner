# SageMaker Async Endpoint Troubleshooting Guide

This guide provides comprehensive troubleshooting information for common issues encountered when using the SageMaker Async Endpoint integration.

## Table of Contents

1. [Common Issues](#common-issues)
2. [S3 Related Issues](#s3-related-issues)
3. [SageMaker Endpoint Issues](#sagemaker-endpoint-issues)
4. [Performance Issues](#performance-issues)
5. [Resource Management Issues](#resource-management-issues)
6. [Worker Pool Issues](#worker-pool-issues)
7. [Configuration Issues](#configuration-issues)
8. [Debugging Tools](#debugging-tools)
9. [Monitoring and Logging](#monitoring-and-logging)

## Common Issues

### Issue: AsyncInferenceTimeoutError

**Symptoms:**
- `AsyncInferenceTimeoutError: Job timed out after X seconds`
- Long-running inference jobs that never complete

**Causes:**
- Insufficient timeout configuration
- SageMaker endpoint capacity issues
- Large or complex input data
- Endpoint scaling delays

**Solutions:**

1. **Increase timeout configuration:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    max_wait_time=7200,  # Increase to 2 hours
    polling_interval=60   # Reduce polling frequency
)
```

2. **Check SageMaker endpoint metrics:**
```bash
# Check endpoint utilization
aws cloudwatch get-metric-statistics \
    --namespace AWS/SageMaker \
    --metric-name ModelLatency \
    --dimensions Name=EndpointName,Value=my-async-endpoint \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-01T23:59:59Z \
    --period 300 \
    --statistics Average
```

3. **Verify endpoint configuration:**
```bash
# Check endpoint status
aws sagemaker describe-endpoint --endpoint-name my-async-endpoint

# Check endpoint configuration
aws sagemaker describe-endpoint-config --endpoint-config-name my-endpoint-config
```

### Issue: S3OperationError - Access Denied

**Symptoms:**
- `S3OperationError: Access denied`
- `ClientError: An error occurred (AccessDenied) when calling the PutObject operation`

**Causes:**
- Insufficient IAM permissions
- Incorrect bucket names
- Bucket policies blocking access
- Cross-account access issues

**Solutions:**

1. **Verify IAM permissions:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket/*",
                "arn:aws:s3:::my-async-output-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket",
                "arn:aws:s3:::my-async-output-bucket"
            ]
        }
    ]
}
```

2. **Test bucket access:**
```python
from osml_extensions.detectors import AsyncSMDetector
from osml_extensions.config import AsyncEndpointConfig

config = AsyncEndpointConfig(
    input_bucket="my-input-bucket",
    output_bucket="my-output-bucket"
)

detector = AsyncSMDetector("my-endpoint", async_config=config)

try:
    detector.s3_manager.validate_bucket_access()
    print("S3 buckets are accessible")
except Exception as e:
    print(f"S3 access error: {e}")
```

3. **Check bucket policies:**
```bash
# Get bucket policy
aws s3api get-bucket-policy --bucket my-async-input-bucket

# Check bucket location
aws s3api get-bucket-location --bucket my-async-input-bucket
```

## S3 Related Issues

### Issue: NoSuchBucket Error

**Symptoms:**
- `ClientError: The specified bucket does not exist`
- Bucket not found errors during upload/download

**Solutions:**

1. **Verify bucket exists:**
```bash
aws s3 ls s3://my-async-input-bucket
```

2. **Create bucket if needed:**
```bash
aws s3 mb s3://my-async-input-bucket --region us-west-2
```

3. **Check bucket region:**
```python
import boto3

s3_client = boto3.client('s3')
response = s3_client.get_bucket_location(Bucket='my-async-input-bucket')
print(f"Bucket region: {response['LocationConstraint']}")
```

### Issue: S3 Upload/Download Failures

**Symptoms:**
- Intermittent upload/download failures
- Network timeout errors
- Connection reset errors

**Solutions:**

1. **Increase retry configuration:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    max_retries=5  # Increase retries
)
```

2. **Check network connectivity:**
```bash
# Test S3 connectivity
aws s3 ls s3://my-async-input-bucket --region us-west-2
```

3. **Monitor S3 request metrics:**
```bash
# Check S3 error rates
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name 4xxErrors \
    --dimensions Name=BucketName,Value=my-async-input-bucket \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-01T23:59:59Z \
    --period 300 \
    --statistics Sum
```

## SageMaker Endpoint Issues

### Issue: Endpoint Not Found

**Symptoms:**
- `ValidationException: Could not find endpoint`
- Endpoint does not exist errors

**Solutions:**

1. **Verify endpoint exists:**
```bash
aws sagemaker describe-endpoint --endpoint-name my-async-endpoint
```

2. **Check endpoint status:**
```bash
aws sagemaker list-endpoints --name-contains async
```

3. **Ensure endpoint supports async inference:**
```bash
# Check endpoint configuration
aws sagemaker describe-endpoint-config --endpoint-config-name my-endpoint-config

# Look for AsyncInferenceConfig in the output
```

### Issue: Endpoint Throttling

**Symptoms:**
- `TooManyRequestsException`
- High latency for inference requests
- Requests timing out frequently

**Solutions:**

1. **Reduce concurrent requests:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    max_concurrent_jobs=50,  # Reduce from default
    submission_workers=2,    # Fewer workers
    polling_workers=1
)
```

2. **Implement exponential backoff:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    polling_interval=60,  # Start with longer intervals
    exponential_backoff_multiplier=2.0
)
```

3. **Monitor endpoint metrics:**
```bash
# Check invocation metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/SageMaker \
    --metric-name Invocations \
    --dimensions Name=EndpointName,Value=my-async-endpoint \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-01T23:59:59Z \
    --period 300 \
    --statistics Sum
```

## Performance Issues

### Issue: Slow Processing Speed

**Symptoms:**
- Low throughput compared to expectations
- Long processing times per tile
- High queue times

**Solutions:**

1. **Optimize worker configuration:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    enable_worker_optimization=True,
    submission_workers=8,  # Increase based on endpoint capacity
    polling_workers=4,
    max_concurrent_jobs=200
)
```

2. **Tune polling intervals:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    polling_interval=10,  # More frequent polling
    max_polling_interval=60,
    exponential_backoff_multiplier=1.2  # Slower backoff
)
```

3. **Monitor performance metrics:**
```python
from osml_extensions.metrics import AsyncMetricsTracker

metrics = AsyncMetricsTracker()
# Use metrics with detector
result = detector.find_features(payload, metrics=metrics)

# Check metrics
print(f"S3 Upload Time: {metrics.get_metric('S3UploadDuration')}")
print(f"Total Processing Time: {metrics.get_metric('TotalAsyncDuration')}")
```

### Issue: High Memory Usage

**Symptoms:**
- Memory usage grows over time
- Out of memory errors
- System becomes unresponsive

**Solutions:**

1. **Enable immediate cleanup:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    cleanup_policy="immediate"
)
```

2. **Reduce concurrent jobs:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    max_concurrent_jobs=25,  # Reduce memory usage
    submission_workers=2,
    polling_workers=1
)
```

3. **Monitor resource usage:**
```python
# Check resource statistics
stats = detector.get_resource_stats()
print(f"Total resources: {stats['total_resources']}")

# Force cleanup if needed
if stats['total_resources'] > 1000:
    detector.cleanup_resources(force=True)
```

## Resource Management Issues

### Issue: Resource Leaks

**Symptoms:**
- S3 objects not being cleaned up
- Temporary files accumulating
- Growing resource counts

**Solutions:**

1. **Verify cleanup configuration:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    cleanup_enabled=True,
    cleanup_policy="immediate"
)
```

2. **Manual cleanup:**
```python
# Force cleanup of all resources
cleaned_count = detector.cleanup_resources(force=True)
print(f"Cleaned up {cleaned_count} resources")
```

3. **Use context managers:**
```python
with AsyncSMDetector(endpoint="my-endpoint", async_config=config) as detector:
    result = detector.find_features(payload)
    # Resources automatically cleaned up
```

### Issue: Cleanup Failures

**Symptoms:**
- Cleanup operations failing silently
- Resources not being deleted
- Error messages during cleanup

**Solutions:**

1. **Check cleanup permissions:**
```json
{
    "Effect": "Allow",
    "Action": [
        "s3:DeleteObject"
    ],
    "Resource": [
        "arn:aws:s3:::my-async-input-bucket/*",
        "arn:aws:s3:::my-async-output-bucket/*"
    ]
}
```

2. **Enable debug logging:**
```python
import logging
logging.getLogger('osml_extensions').setLevel(logging.DEBUG)
```

3. **Monitor cleanup statistics:**
```python
stats = detector.get_resource_stats()
cleanup_stats = stats['cleanup_stats']
print(f"Cleanup attempted: {cleanup_stats['attempted']}")
print(f"Cleanup successful: {cleanup_stats['successful']}")
print(f"Cleanup failed: {cleanup_stats['failed']}")
```

## Worker Pool Issues

### Issue: Workers Not Starting

**Symptoms:**
- Worker pool initialization fails
- No workers created
- Processing doesn't begin

**Solutions:**

1. **Check worker configuration:**
```python
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    enable_worker_optimization=True,
    submission_workers=4,  # Must be > 0
    polling_workers=2      # Must be > 0
)
```

2. **Verify resource limits:**
```python
import threading
print(f"Active threads: {threading.active_count()}")
```

3. **Check for exceptions during startup:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
logger = logging.getLogger('osml_extensions.workers')
logger.setLevel(logging.DEBUG)
```

### Issue: Workers Not Stopping

**Symptoms:**
- Worker threads remain active after processing
- Application doesn't exit cleanly
- Resource cleanup incomplete

**Solutions:**

1. **Proper shutdown sequence:**
```python
try:
    total_processed, total_failed = worker_pool.process_tiles_async(tile_queue)
finally:
    # Ensure proper cleanup
    worker_pool._stop_workers()
```

2. **Use timeout for worker shutdown:**
```python
# Workers should stop within timeout
# If not, they may be logged as warnings
```

3. **Check for blocking operations:**
```python
# Ensure tile queue has shutdown signals
for _ in range(config.submission_workers):
    tile_queue.put(None)  # Shutdown signal
```

## Configuration Issues

### Issue: Invalid Configuration

**Symptoms:**
- `ExtensionConfigurationError` during initialization
- Configuration validation failures
- Unexpected behavior due to wrong settings

**Solutions:**

1. **Validate configuration:**
```python
try:
    config = AsyncEndpointConfig(
        input_bucket="my-bucket",
        output_bucket="my-bucket"
    )
    print("Configuration is valid")
except ExtensionConfigurationError as e:
    print(f"Configuration error: {e}")
```

2. **Check required parameters:**
```python
# Required parameters
config = AsyncEndpointConfig(
    input_bucket="my-input-bucket",    # Required
    output_bucket="my-output-bucket"   # Required
)
```

3. **Verify environment variables:**
```bash
# Check environment variables
echo $ASYNC_SM_INPUT_BUCKET
echo $ASYNC_SM_OUTPUT_BUCKET
echo $ASYNC_SM_MAX_WAIT_TIME
```

### Issue: Environment Variable Loading

**Symptoms:**
- Environment variables not being loaded
- Default values used instead of environment settings
- Configuration not matching expectations

**Solutions:**

1. **Use from_environment method:**
```python
config = AsyncEndpointConfig.from_environment()
```

2. **Verify environment variable names:**
```bash
# Correct environment variable names
export ASYNC_SM_INPUT_BUCKET="my-input-bucket"
export ASYNC_SM_OUTPUT_BUCKET="my-output-bucket"
export ASYNC_SM_MAX_WAIT_TIME="3600"
export ASYNC_SM_CLEANUP_ENABLED="true"
```

3. **Check variable precedence:**
```python
# Constructor parameters override environment variables
config = AsyncEndpointConfig(
    input_bucket="override-bucket",  # This overrides env var
    max_wait_time=1800              # This overrides env var
)
```

## Debugging Tools

### Enable Debug Logging

```python
import logging

# Enable debug logging for all extensions
logging.basicConfig(level=logging.DEBUG)

# Or enable for specific modules
logging.getLogger('osml_extensions.detectors').setLevel(logging.DEBUG)
logging.getLogger('osml_extensions.s3').setLevel(logging.DEBUG)
logging.getLogger('osml_extensions.polling').setLevel(logging.DEBUG)
logging.getLogger('osml_extensions.workers').setLevel(logging.DEBUG)
```

### Resource Statistics Monitoring

```python
def monitor_resources(detector, interval=30):
    """Monitor resource statistics periodically."""
    import time
    import threading
    
    def monitor():
        while True:
            stats = detector.get_resource_stats()
            print(f"Resources: {stats['total_resources']}")
            print(f"Cleanup success rate: {stats['cleanup_stats']['successful']}/{stats['cleanup_stats']['attempted']}")
            time.sleep(interval)
    
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    return monitor_thread
```

### Performance Profiling

```python
import time
from contextlib import contextmanager

@contextmanager
def profile_operation(operation_name):
    """Profile operation timing."""
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        print(f"{operation_name}: {end_time - start_time:.2f} seconds")

# Usage
with profile_operation("Async Inference"):
    result = detector.find_features(payload)
```

### Configuration Validation

```python
def validate_async_setup(config, endpoint_name):
    """Validate complete async setup."""
    print("Validating async endpoint setup...")
    
    # 1. Validate configuration
    try:
        print(f"✓ Configuration valid")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return False
    
    # 2. Test S3 access
    detector = AsyncSMDetector(endpoint_name, async_config=config)
    try:
        detector.s3_manager.validate_bucket_access()
        print("✓ S3 buckets accessible")
    except Exception as e:
        print(f"✗ S3 access error: {e}")
        return False
    
    # 3. Test endpoint access (would need actual endpoint)
    print("✓ Setup validation complete")
    return True
```

## Monitoring and Logging

### CloudWatch Metrics to Monitor

1. **SageMaker Metrics:**
   - `ModelLatency`
   - `Invocations`
   - `InvocationErrors`
   - `ModelSetupTime`

2. **S3 Metrics:**
   - `BucketRequests`
   - `4xxErrors`
   - `5xxErrors`

3. **Custom Application Metrics:**
   - Processing throughput
   - Queue times
   - Resource cleanup rates

### Log Analysis

```bash
# Search for common error patterns
grep -i "error\|exception\|failed" application.log

# Monitor S3 operations
grep "S3" application.log | grep -i "error\|retry"

# Check inference timeouts
grep "AsyncInferenceTimeoutError" application.log

# Monitor resource cleanup
grep "cleanup" application.log
```

### Health Check Implementation

```python
def health_check(detector):
    """Perform health check on async detector."""
    health_status = {
        "s3_access": False,
        "resource_stats": {},
        "configuration": {},
        "timestamp": time.time()
    }
    
    try:
        # Check S3 access
        detector.s3_manager.validate_bucket_access()
        health_status["s3_access"] = True
    except Exception as e:
        health_status["s3_error"] = str(e)
    
    # Get resource statistics
    health_status["resource_stats"] = detector.get_resource_stats()
    
    # Get configuration info
    config = detector.async_config
    health_status["configuration"] = {
        "input_bucket": config.input_bucket,
        "output_bucket": config.output_bucket,
        "max_wait_time": config.max_wait_time,
        "cleanup_enabled": config.cleanup_enabled
    }
    
    return health_status
```

## Getting Help

If you continue to experience issues:

1. **Check the logs** with debug logging enabled
2. **Verify IAM permissions** for all required services
3. **Test with minimal configuration** to isolate issues
4. **Monitor CloudWatch metrics** for the services involved
5. **Check AWS service health** in the AWS Health Dashboard
6. **Review the examples** in the documentation for working configurations

For additional support, provide:
- Complete error messages and stack traces
- Configuration details (sanitized)
- AWS region and service versions
- Steps to reproduce the issue
- Resource statistics and metrics