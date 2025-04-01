# OSML Model Runner Validation Tool

**⚠️ IMPORTANT: This tool is currently under development and not ready for production use. ⚠️**

## Overview

The OSML Model Runner Validation Tool is designed to validate machine learning models for compatibility with the OSML Model Runner 
ecosystem. It provides a comprehensive suite of tests to ensure that models can be successfully integrated and executed within the OSML framework.

## Planned Functionality

This validation tool will provide the following capabilities:

1. **Model Compatibility Testing**
   - Verify model compatibility with SageMaker endpoints
   - Validate model input/output formats against OSML requirements
   - Test model behavior with various image formats and sizes

2. **Performance Benchmarking**
   - Measure inference latency across different input sizes
   - Evaluate throughput under various load conditions
   - Analyze resource utilization patterns

3. **Integration Testing**
   - Validate end-to-end workflow with the OSML Model Runner
   - Test model behavior with tiled imagery processing
   - Verify correct handling of geospatial metadata

4. **Reporting**
   - Generate comprehensive validation reports
   - Provide actionable recommendations for model optimization
   - Document compatibility status and any identified issues

## Infrastructure Components

The validation tool leverages AWS infrastructure components deployed via CDK, including:

- **SNS Topics** for event-driven communication
  - Model validation requests
  - Success/failure notifications

- **Step Functions Workflow** for orchestrating the validation process
  - SageMaker compatibility testing
  - OSML compatibility testing
  - Performance benchmarking
  - Report generation

- **Lambda Functions** for validation tasks
  - SageMaker compatibility checks
  - OSML compatibility verification
  - Report compilation

- **ECS Fargate Tasks** for resource-intensive operations
  - Performance benchmarking
  - SageMaker Inference Recommender integration

- **S3 Buckets** for storing validation artifacts and reports

## Development Status

This tool is currently in early development. Key components that are being implemented include:

- Core validation framework
- Model compatibility test suite
- Integration with OSML Model Runner
- Performance benchmarking modules
- Reporting engine

## Future Work

Planned enhancements include:

- Support for additional model types and formats
- Extended benchmarking capabilities
- User interface for validation configuration and report viewing

## License

MIT No Attribution Licensed. See LICENSE file in the project root.
