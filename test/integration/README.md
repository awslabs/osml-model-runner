# OSML Model Runner Integration Tests

This directory contains a unified integration test suite for OSML Model Runner with a clean, simple interface.

## Prerequisites

Install the required dependencies:

```bash
pip install -r test/integration/requirements.txt
```

## Quick Start

The simplest way to run an integration test:

```bash
# Test with your image and model
python test/integration/integ_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint

# Test with expected output validation
python test/integration/integ_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

# Test HTTP endpoint
python test/integration/integ_runner.py s3://my-bucket/image.tif my-model expected.json --http

# Test with SageMaker model variant
python test/integration/integ_runner.py s3://my-bucket/image.tif flood expected.json --model-variant flood-50

# Test with multi-container endpoint
python test/integration/integ_runner.py s3://my-bucket/image.tif multi-container expected.json --target-container centerpoint-container

# Run with verbose logging
python test/integration/integ_runner.py s3://my-bucket/image.tif centerpoint --verbose

# Save results to JSON file
python test/integration/integ_runner.py s3://my-bucket/image.tif centerpoint --output results.json
```

**Note**: For test suites, you can use the `${ACCOUNT}` placeholder in JSON configuration files to automatically use your current AWS account ID.

## Test Suite Execution

Run multiple tests from a JSON configuration:

```bash
# Run full test suite
python test/integration/integ_runner.py --suite test_suites/model_runner_full.json

# Run with custom timeout and delay
python test/integration/integ_runner.py --suite test_suites/model_runner_full.json --timeout 45 --delay 10

# Run with verbose logging and save results
python test/integration/integ_runner.py --suite test_suites/model_runner_full.json --verbose --output test_results.json
```

## File Structure

```text
test/integration/
├── integ_runner.py                 # Unified test runner (supports single tests and test suites)
├── integ_types.py                  # Local type definitions (no dependency on model runner)
├── integ_config.py                 # Configuration management
├── feature_validator.py            # GeoJSON feature validation utilities
├── requirements.txt                # Python dependencies for integration tests
├── __init__.py                     # Package initialization
└── test_suites/                    # JSON test suite definitions
    ├── model_runner_full.json      # Full test suite with multiple test cases
    └── results/                    # Expected output files for validation
```

## Test Suite Format

Test suites are defined in JSON format as an array of test case objects:

```json
[
  {
    "name": "Centerpoint Basic Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "endpoint_type": "SM_ENDPOINT",
    "expected_output": "test_suites/results/sample_centerpoint_model_output.geojson",
    "timeout_minutes": 30
  },
  {
    "name": "Centerpoint HTTP Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "endpoint_type": "HTTP_ENDPOINT",
    "expected_output": "test_suites/results/sample_centerpoint_http_model_small_output.geojson",
    "timeout_minutes": 10
  },
  {
    "name": "Flood Detection Test (flood-50 variant)",
    "image_uri": "s3://mr-test-imagery-975050113711/large.tif",
    "model_name": "flood",
    "endpoint_type": "SM_ENDPOINT",
    "model_variant": "flood-50",
    "timeout_minutes": 15
  },
  {
    "name": "Multi-Container Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "multi-container",
    "endpoint_type": "SM_ENDPOINT",
    "target_container": "centerpoint-container",
    "expected_output": "test_suites/results/sample_centerpoint_model_small_output.geojson",
    "timeout_minutes": 10
  }
]
```

### Test Case Fields

- **`name`** (required): Descriptive name for the test case
- **`image_uri`** (required): S3 URI to the test image (supports `${ACCOUNT}` placeholder)
- **`model_name`** (required): Name of the model to test
- **`endpoint_type`** (required): Either `"SM_ENDPOINT"` or `"HTTP_ENDPOINT"` (default: `"SM_ENDPOINT"`)
- **`expected_output`** (optional): Path to expected GeoJSON output file for validation
  - Can be absolute path, relative to current directory, or relative to `test/integration/` directory
  - Not required for models that use count-based validation (e.g., flood model)
- **`timeout_minutes`** (optional): Maximum time to wait for test completion (default: 30)
- **`model_variant`** (optional): SageMaker model variant name (e.g., `"flood-50"`, `"flood-100"`, `"AllTraffic"`)
- **`target_container`** (optional): Target container hostname for multi-container endpoints (e.g., `"centerpoint-container"`, `"flood-container"`)

### Placeholder Support

**Account ID Placeholders**: Test suite configurations support placeholders that are automatically replaced with values from your AWS environment. This allows test configurations to work across different AWS accounts without modification.

Supported placeholders:

- `${ACCOUNT}`: Current AWS account ID (detected from your AWS credentials/environment)

Example in `model_runner_full.json`:

```json
{
  "image_uri": "s3://mr-test-imagery-${ACCOUNT}/tile.tif"
}
```

When you run the test suite, `${ACCOUNT}` will be automatically replaced with your current AWS account ID. The test runner will log when placeholders are replaced.

### Expected Output Path Resolution

The `expected_output` field supports flexible path resolution:

1. **Absolute paths**: Used as-is (e.g., `/absolute/path/to/file.geojson`)
2. **Relative paths**: First checked relative to current working directory
3. **Fallback**: If not found, checked relative to `test/integration/` directory

This allows test suite JSON files to work regardless of where the test runner is invoked from.

### Validation Types

The integration test framework supports two types of validation:

1. **Feature-based validation**: Used when `expected_output` is provided
   - Compares actual GeoJSON features against expected features
   - Validates features in both S3 and Kinesis outputs
   - Used for models like `centerpoint` that produce structured detection features

2. **Count-based validation**: Used automatically for certain models (e.g., `flood`)
   - Validates feature counts and region request counts
   - Does not require `expected_output` file
   - Expected counts are determined based on image type and model variant
   - Models using this approach: `flood` (and models with `target_container` set to `flood-container`)

For flood model tests, validation is automatically performed using count-based validation regardless of whether `expected_output` is specified.

## Command-Line Options

### Single Test Options

- `image_uri` (positional): S3 URI to the test image
- `model_name` (positional): Name of the model to test
- `expected_output` (positional, optional): Path to expected output file for validation
- `--http`: Use HTTP endpoint instead of SageMaker endpoint
- `--timeout MINUTES`: Maximum time to wait for test completion (default: 30)
- `--verbose`: Enable verbose logging (debug level)
- `--output FILE`: Save test results to JSON file
- `--model-variant VARIANT`: SageMaker model variant (e.g., `flood-50`, `AllTraffic`)
- `--target-container CONTAINER`: Target container hostname for multi-container endpoints

### Test Suite Options

- `--suite FILE`: Path to test suite JSON file
- `--timeout MINUTES`: Default timeout for all tests (default: 30)
- `--delay SECONDS`: Delay between tests in seconds (default: 5)
- `--verbose`: Enable verbose logging
- `--output FILE`: Save test results to JSON file

**Note**: Cannot specify both `--suite` and individual test parameters.

## Benefits

✅ **Simple**: Just 2-3 parameters instead of complex configuration
✅ **Clear**: No hidden environment variables or configuration files
✅ **Flexible**: Works with any image URI and model name, supports multiple endpoint types
✅ **Fast**: No complex setup or configuration management
✅ **Reliable**: Direct parameter passing eliminates configuration errors
✅ **Independent**: No dependency on the main model runner package
✅ **Comprehensive**: Supports both feature-based and count-based validation

## Dependencies

The integration tests have minimal dependencies to ensure they can run independently:

- `boto3` & `botocore`: AWS SDK for interacting with AWS services
- `geojson`: GeoJSON feature validation and comparison
- `requests`: HTTP requests for EC2 metadata discovery

See `requirements.txt` for specific versions.
