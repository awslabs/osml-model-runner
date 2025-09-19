# OSML Model Runner Extensions

This directory contains modular extensions for the OSML Model Runner, organized by functionality to enable easy maintenance and development of different types of extensions.

## Extension Modules

### async_workflow/

The **async_workflow** module provides comprehensive support for Amazon SageMaker Async Inference endpoints, enabling high-throughput, scalable machine learning inference processing.

**Key Features:**
- True asynchronous processing with SageMaker async endpoints
- S3-based input/output handling with configurable cleanup policies
- Worker pool optimization for maximum throughput (3-5x performance improvement)
- Comprehensive resource management and cleanup
- Robust error handling with retry logic and exponential backoff
- Detailed metrics and performance monitoring

**Quick Start:**
```python
from async_workflow.src.osml_extensions.config import AsyncEndpointConfig
from async_workflow.src.osml_extensions.detectors import AsyncSMDetector

config = AsyncEndpointConfig(
    input_bucket="my-async-input-bucket",
    output_bucket="my-async-output-bucket"
)

detector = AsyncSMDetector(
    endpoint="my-async-sagemaker-endpoint",
    async_config=config
)

result = detector.find_features(payload)
```

**Documentation:** See [async_workflow/README.md](async_workflow/README.md) for complete documentation.

## Extension Structure

Each extension module follows a consistent structure:

```
extension_name/
├── README.md                    # Extension-specific documentation
├── __init__.py                  # Module initialization and exports
├── src/                         # Source code
│   └── osml_extensions/         # Extension package
│       ├── __init__.py          # Package initialization
│       ├── config/              # Configuration management
│       ├── detectors/           # Detector implementations
│       ├── errors/              # Error classes
│       ├── handlers/            # Request handlers
│       ├── metrics/             # Metrics and monitoring
│       ├── polling/             # Polling mechanisms
│       ├── s3/                  # S3 operations
│       ├── utils/               # Utility classes
│       └── workers/             # Worker pool implementations
├── tests/                       # Unit tests
├── examples/                    # Usage examples
├── docs/                        # Detailed documentation
└── test_implementation.py       # Implementation validation script
```

## Adding New Extensions

To add a new extension module:

1. **Create the module directory:**
   ```bash
   mkdir -p extensions/new_extension_name/{src/osml_extensions,tests,examples,docs}
   ```

2. **Implement the extension following the standard structure**

3. **Create module __init__.py with appropriate exports**

4. **Add comprehensive tests and documentation**

5. **Update this README to include the new extension**

## Development Guidelines

### Code Organization
- Keep extension-specific code within the extension directory
- Use consistent naming conventions across extensions
- Follow the established package structure

### Testing
- Each extension should have comprehensive unit tests
- Include integration tests where appropriate
- Provide a test_implementation.py script for validation

### Documentation
- Include detailed README for each extension
- Provide usage examples and API documentation
- Document configuration options and best practices

### Dependencies
- Minimize cross-extension dependencies
- Clearly document any shared dependencies
- Use relative imports within extensions

## Available Extensions

| Extension | Description | Status |
|-----------|-------------|--------|
| [async_workflow](async_workflow/) | SageMaker Async Endpoint Integration | ✅ Complete |

## Future Extensions

Potential future extensions could include:
- **batch_processing**: Batch inference processing capabilities
- **streaming_workflow**: Real-time streaming inference
- **model_optimization**: Model optimization and quantization tools
- **monitoring_dashboard**: Real-time monitoring and alerting
- **data_pipeline**: Data preprocessing and postprocessing pipelines

## Contributing

When contributing to extensions:

1. Follow the established directory structure
2. Include comprehensive tests and documentation
3. Ensure backward compatibility
4. Update this main README with new extensions
5. Follow the existing code style and conventions

## Support

For extension-specific support, refer to the individual extension documentation. For general questions about the extensions framework, create an issue in the main repository.
