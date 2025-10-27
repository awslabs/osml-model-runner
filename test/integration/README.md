# OSML Model Runner Integration Tests

This directory contains a unified integration test suite for OSML Model Runner with a clean, simple interface.

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
├── test_suites/                   # JSON test suite definitions
│   ├── centerpoint_tests.json
│   └── osml-model-runner.code-workspace
└── utils/                         # Utility functions
    ├── __init__.py
    ├── config.py
    └── integ_utils.py
```

## Test Suite Format

Test suites are defined in JSON format:

```json
[
  {
    "name": "Centerpoint Basic Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "endpoint_type": "SM_ENDPOINT",
    "expected_output": "/path/to/expected_results.json",
    "validate_results": true,
    "timeout_minutes": 30
  },
  {
    "name": "Quick Test",
    "image_uri": "s3://mr-test-imagery-975050113711/small.tif",
    "model_name": "centerpoint",
    "validate_results": false,
    "timeout_minutes": 15
  }
]
```

## Benefits

✅ **Simple**: Just 2-3 parameters instead of complex configuration
✅ **Clear**: No hidden environment variables or configuration files
✅ **Flexible**: Works with any image URI and model name
✅ **Fast**: No complex setup or configuration management
✅ **Reliable**: Direct parameter passing eliminates configuration errors

## Migration from Old System

**Old way (complex):**

```bash
export TARGET_IMAGE="s3://mr-test-imagery-975050113711/small.tif"
export TARGET_MODEL="centerpoint"
export TILE_SIZE="1024"
export TILE_OVERLAP="0.1"
# ... many more environment variables
python3 process_image.py --image small --model centerpoint
```

**New way (simple):**

```bash
python test/integration/integration_test_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint
```

Much simpler! 🎉
