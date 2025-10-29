# OSML Model Runner Integration Tests

This directory contains a unified integration test suite for OSML Model Runner with a clean, simple interface.

## Prerequisites

Install the required dependencies:

```bash
pip install -r test/integration/requirements.txt
```

Alternatively, if you're using the project's conda environment:

```bash
conda env update -f ../../conda/test-models-py310.yml
```

## Quick Start

The simplest way to run an integration test:

```bash
# Test with your image and model
python test/integration/integration_test_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint

# Test with expected output validation
python test/integration/integration_test_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

# Test HTTP endpoint
python test/integration/integration_test_runner.py s3://my-bucket/image.tif my-model expected.json --http
```

**Note**: For test suites, you can use the `${ACCOUNT}` placeholder in JSON configuration files to automatically use your current AWS account ID.

## Test Suite Execution

Run multiple tests from a JSON configuration:

```bash
# Run centerpoint test suite
python test/integration/integration_test_runner.py --suite test_suites/centerpoint_tests.json

# Run with custom timeout and delay
python test/integration/integration_test_runner.py --suite test_suites/centerpoint_tests.json --timeout 45 --delay 10
```

## File Structure

```text
test/integration/
├── integration_test_runner.py     # Unified test runner (supports single tests and test suites)
├── integration_types.py            # Local type definitions (no dependency on model runner)
├── config.py                       # Configuration management
├── feature_validator.py            # GeoJSON feature validation utilities
├── requirements.txt                # Python dependencies for integration tests
├── __init__.py                     # Package initialization
└── test_suites/                    # JSON test suite definitions
    ├── centerpoint_tests.json
```

## Test Suite Format

Test suites are defined in JSON format:

```json
[
  {
    "name": "Centerpoint Basic Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "endpoint_type": "SM_ENDPOINT",s
    "expected_output": "/path/to/expected_results.json",
    "timeout_minutes": 30
  },
  {
    "name": "Quick Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "timeout_minutes": 15
  }
]
```

### Placeholder Support

**Account ID Placeholders**: Test suite configurations support placeholders that are automatically replaced with values from your AWS environment. This allows test configurations to work across different AWS accounts without modification.

Supported placeholders:

- `${ACCOUNT}`: Current AWS account ID (detected from your environment)

Example in `centerpoint_tests.json`:

```json
{
  "image_uri": "s3://mr-test-imagery-${ACCOUNT}/tile.tif"
}
```

When you run the test suite, `${ACCOUNT}` will be automatically replaced with your current AWS account ID. The test runner will log when placeholders are replaced.

## Benefits

✅ **Simple**: Just 2-3 parameters instead of complex configuration
✅ **Clear**: No hidden environment variables or configuration files
✅ **Flexible**: Works with any image URI and model name
✅ **Fast**: No complex setup or configuration management
✅ **Reliable**: Direct parameter passing eliminates configuration errors
✅ **Independent**: No dependency on the main model runner package

## Dependencies

The integration tests have minimal dependencies to ensure they can run independently:

- `boto3` & `botocore`: AWS SDK for interacting with AWS services
- `geojson`: GeoJSON feature validation and comparison
- `requests`: HTTP requests for EC2 metadata discovery

See `requirements.txt` for specific versions.
