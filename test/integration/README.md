# OSML Model Runner Integration Tests

This directory contains a simplified integration test suite for OSML Model Runner, built around the working `test.py` approach.

## Quick Start

The simplest way to run an integration test:

```bash
# Test with your image and model
python3 scripts/integration/test.py s3://mr-test-imagery-975050113711/small.tif centerpoint

# Test with expected output validation
python3 scripts/integration/test.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

# Test HTTP endpoint
python3 scripts/integration/test.py s3://my-bucket/image.tif my-model expected.json --http
```

## Test Suite Execution

Run multiple tests from a JSON configuration:

```bash
# Run centerpoint test suite
python3 test/integration/run_test.py --suite test_suites/centerpoint_tests.json

# Run quick tests
python3 test/integration/run_test.py --suite test_suites/quick_tests.json --timeout 20
```

## File Structure

```
test/integration/
├── run_test.py                    # Main test runner (supports single tests and test suites)
├── test_suites/                   # JSON test suite definitions
│   ├── centerpoint_tests.json
│   ├── centerpoint_with_variants.json
│   └── quick_tests.json
└── utils/                         # Utility functions
    ├── __init__.py
    ├── clients.py
    ├── config.py
    └── integ_utils.py

scripts/integration/
└── test.py                        # Ultra-simple test script
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
python3 scripts/integration/test.py s3://mr-test-imagery-975050113711/small.tif centerpoint
```

Much simpler! 🎉
