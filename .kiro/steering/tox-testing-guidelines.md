# Testing Guidelines

## Running Tests

This project uses **tox** for running tests across multiple Python versions and environments.

### Test Commands

- **Run all tests**: `tox`
- **Run tests for specific Python version**: `tox -e py310-prod` (or py311, py312, py313)
- **Run specific test**: `tox -- path/to/test_file.py::test_name`
- **Run tests with verbose output**: `tox -- -v`
- **Run tests and show print statements**: `tox -- -s`
- **Run integration tests** (requires deployed infrastructure): `tox -- -m integration`

### Important Notes

- Always use `tox` to run tests, not `pytest` directly
- Tests run in isolated conda environments defined in `conda/model-runner.yml`
- The default test command excludes integration tests (use `-m integration` to include them)
- Test coverage reports are generated automatically in `htmlcov/` directory
- Multiple Python versions (3.10, 3.11, 3.12, 3.13) are tested

### Other Tox Environments

- **Linting**: `tox -e lint` - runs pre-commit hooks
- **Documentation**: `tox -e docs` - builds Sphinx documentation
- **Package checks**: `tox -e twine` - validates distribution package

## Test Environment Variables

Tests use mocked AWS services with the following environment variables (automatically set by tox):
- `AWS_DEFAULT_REGION=us-west-2`
- `IMAGE_REQUEST_TABLE=TEST-IMAGE-REQUEST-TABLE`
- `FEATURE_TABLE=TEST-FEATURE-TABLE`
- And other test-specific AWS resource names

## Writing Tests

- Use `pytest` framework for writing tests
- Place tests in the `test/` directory
- Use `pytest-asyncio` for async tests
- Use `moto` for mocking AWS services
- Mark integration tests with `@pytest.mark.integration` decorator
