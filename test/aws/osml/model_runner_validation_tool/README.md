# Model Validation Tool Tests

This directory contains tests for the OSML Model Validation Tool components:

- `test_oversight_ml_compatibility.py`: Tests for the OversightML compatibility lambda
- `test_sagemaker_compatibility.py`: Tests for the SageMaker compatibility lambda

## Running Tests

These tests are not included in the default pytest configuration. To run these tests specifically, use:

```bash
pytest test/aws/osml/model_runner_validation_tool/
```

## Test Configuration

To include these tests in your CI/CD pipeline, you'll need to explicitly configure your test runner to include this directory.

These tests were moved from the CDK project directory to this location as part of a code reorganization.
