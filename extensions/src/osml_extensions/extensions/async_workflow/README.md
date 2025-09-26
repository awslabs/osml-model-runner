# OSML Model Runner Extensions - SageMaker Async Endpoint Integration

This extension provides comprehensive support for Amazon SageMaker Async Inference endpoints in the OSML Model Runner, enabling high-throughput, scalable machine learning inference processing.

## Features

- **True Asynchronous Processing**: Submit inference requests without blocking, allowing for better resource utilization
- **S3-Based Input/Output**: Automatic handling of large payloads through S3 storage with configurable cleanup policies
- **Worker Pool Optimization**: Separate submission and polling workers for maximum throughput (3-5x performance improvement)
- **Comprehensive Resource Management**: Automatic cleanup of temporary resources with configurable policies
- **Robust Error Handling**: Retry logic, exponential backoff, and graceful failure handling
- **Detailed Metrics**: Comprehensive timing and performance metrics with CloudWatch integration
- **Flexible Configuration**: Environment variable support and multiple deployment configurations
- **Cross-Account Support**: Full support for cross-account SageMaker and S3 access

## Quick Start

### Installation

```bash
# Install the extensions package
pip install osml-extensions

# Or install from source
git clone <repository-url>
cd model-runner-extensions
pip install -e .
```

### Basic Usage

```python
from osml_extensions.config import AsyncEndpointConfig
from osml_extensions.detectors import AsyncSMDetector
from io import BytesIO
import json

# Configure async endpoint
config = AsyncEndpointConfig(
    input_bucket="my-async-input-bucket",
    output_bucket="my-async-output-bucket",
    max_wait_time=3600,  # 1 hour timeout
    cleanup_enabled=True
)

# Create async detector
detector = AsyncSMDetector(
    endpoint="my-async-sagemaker-endpoint",
    async_config=config
)

# Process inference request
payload = BytesIO(json.dumps({"image_data": "base64_encoded_image"}).encode())
feature_collection = detector.find_features(payload)

print(f"Found {len(feature_collection['features'])} features")
```

### Environment Configuration

```bash
# S3 Configuration
export ARTIFACT_BUCKET="my-async-input-bucket"
export ASYNC_SM_OUTPUT_BUCKET="my-async-output-bucket"

# Performance Configuration
export ASYNC_SM_SUBMISSION_WORKERS="8"
export ASYNC_SM_POLLING_WORKERS="4"
export ASYNC_SM_MAX_CONCURRENT_JOBS="200"

# Cleanup Configuration
export ASYNC_SM_CLEANUP_POLICY="immediate"
export ASYNC_SM_CLEANUP_ENABLED="true"
```

## Architecture

The async endpoint integration consists of several key components:

### Core Components

- **AsyncSMDetector**: Main detector class that handles async inference workflow
- **AsyncEndpointConfig**: Comprehensive configuration management
- **S3Manager**: Handles S3 upload, download, and cleanup operations
- **AsyncInferencePoller**: Manages job polling with exponential backoff
- **ResourceManager**: Comprehensive resource cleanup and management

### Worker Pool Architecture

- **AsyncSubmissionWorker**: Quickly submits tiles to async endpoints
- **AsyncResultsWorker**: Independently monitors job completion
- **AsyncTileWorkerPool**: Coordinates workers for optimal throughput

## Performance Benefits

The worker pool optimization provides significant performance improvements:

- **3-5x Throughput Improvement**: Compared to sequential processing
- **Better Resource Utilization**: Separate workers prevent blocking operations
- **Scalable Architecture**: Easy to tune worker counts based on workload
- **Reduced Latency**: Immediate submission without waiting for completion

## Configuration Options

### Basic Configuration

```python
config = AsyncEndpointConfig(
    input_bucket="my-input-bucket",
    output_bucket="my-output-bucket",
    max_wait_time=3600,
    polling_interval=30,
    cleanup_enabled=True
)
```

### High-Performance Configuration

```python
config = AsyncEndpointConfig(
    input_bucket="my-input-bucket",
    output_bucket="my-output-bucket",

    # Optimized polling
    polling_interval=10,
    max_polling_interval=60,
    exponential_backoff_multiplier=1.2,

    # High concurrency
    submission_workers=12,
    polling_workers=6,
    max_concurrent_jobs=300,

    # Immediate cleanup
    cleanup_policy="immediate",

    enable_worker_optimization=True
)
```

## Resource Management

The integration includes comprehensive resource management:

### Cleanup Policies

- **Immediate**: Clean up resources immediately after use (default)
- **Delayed**: Clean up resources after a configurable delay
- **Disabled**: No automatic cleanup (manual cleanup required)

### Resource Types Managed

- S3 objects (input and output data)
- Temporary files
- Inference job metadata
- Worker threads

### Usage Example

```python
# Get resource statistics
stats = detector.get_resource_stats()
print(f"Total resources: {stats['total_resources']}")

# Manual cleanup
cleaned_count = detector.cleanup_resources(force=True)
print(f"Cleaned up {cleaned_count} resources")

# Context manager for automatic cleanup
with AsyncSMDetector(endpoint="my-endpoint", async_config=config) as detector:
    result = detector.find_features(payload)
    # Resources automatically cleaned up
```

## Error Handling

Comprehensive error handling for various scenarios:

```python
from osml_extensions.errors import ExtensionRuntimeError
from osml_extensions.s3 import S3OperationError

try:
    result = detector.find_features(payload)
except S3OperationError as e:
    print(f"S3 operation failed: {e}")
    # Handle S3 permission or connectivity issues

except ExtensionRuntimeError as e:
    print(f"Runtime error: {e}")
    # Handle general runtime issues
```

## Monitoring and Metrics

Built-in metrics tracking for performance monitoring:

```python
from osml_extensions.metrics import AsyncMetricsTracker

metrics = AsyncMetricsTracker()
result = detector.find_features(payload, metrics=metrics)

# Get performance metrics
print(f"S3 Upload Time: {metrics.get_metric('S3UploadDuration')}")
print(f"Total Processing Time: {metrics.get_metric('TotalAsyncDuration')}")
print(f"Queue Time: {metrics.get_metric('QueueTime')}")
```

## Documentation

Comprehensive documentation is available:

- **[Async Endpoint Guide](docs/async_endpoint_guide.md)**: Complete usage guide
- **[Troubleshooting Guide](docs/troubleshooting_guide.md)**: Common issues and solutions
- **[Performance Tuning Guide](docs/performance_tuning_guide.md)**: Optimization strategies
- **[IAM Permissions Guide](docs/iam_permissions_guide.md)**: Required AWS permissions

## Examples

Working examples are provided in the `examples/` directory:

- **[Basic Usage](examples/basic_async_usage.py)**: Simple async endpoint usage
- **[Worker Pool Optimization](examples/worker_pool_optimization.py)**: High-throughput processing
- **[Resource Management](examples/resource_management.py)**: Resource cleanup and monitoring
- **[Error Handling](examples/error_handling_and_recovery.py)**: Comprehensive error handling

## Requirements

### AWS Services

- Amazon SageMaker (with async inference endpoints)
- Amazon S3 (for input/output storage)
- AWS IAM (for permissions)

### Python Dependencies

- boto3 >= 1.26.0
- botocore >= 1.29.0
- geojson >= 2.5.0
- aws-embedded-metrics >= 2.0.0

### IAM Permissions

Required IAM permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket",
                "arn:aws:s3:::my-async-input-bucket/*",
                "arn:aws:s3:::my-async-output-bucket",
                "arn:aws:s3:::my-async-output-bucket/*"
            ]
        }
    ]
}
```

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run specific test modules
python -m pytest tests/test_async_sm_detector.py -v
python -m pytest tests/test_resource_manager.py -v
python -m pytest tests/test_worker_pool.py -v

# Run with coverage
python -m pytest tests/ --cov=osml_extensions --cov-report=html
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd model-runner-extensions

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Run tests
python -m pytest tests/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

1. Check the [Troubleshooting Guide](docs/troubleshooting_guide.md)
2. Review the [documentation](docs/)
3. Search existing [issues](../../issues)
4. Create a new [issue](../../issues/new) if needed

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a list of changes and version history.

## Acknowledgments

- AWS SageMaker team for async inference capabilities
- OSML Model Runner team for the base framework
- Contributors and maintainers
