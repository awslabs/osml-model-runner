# Model Runner Extensions: Simple Guide

## What Are Extensions?

Extensions add enhanced functionality to the Model Runner by creating improved versions of core components. These enhanced components inherit all the original functionality while adding new capabilities like async processing and better monitoring.

## How It Works

The system uses **inheritance** - enhanced classes extend base classes:

```
Base Components          Enhanced Components
┌─────────────────┐     ┌─────────────────────┐
│ ModelRunner     │────▶│ EnhancedModelRunner │
│ Factory         │     │ + Dependency inject │
│ Detector        │     │ + Better config     │
└─────────────────┘     └─────────────────────┘
                        
                        ┌─────────────────────┐
                        │ EnhancedFactory     │
                        │ + Async detectors   │
                        │ + Extended modes    │
                        └─────────────────────┘
                        
                        ┌─────────────────────┐
                        │ AsyncSMDetector     │
                        │ + Async processing  │
                        │ + Better monitoring │
                        └─────────────────────┘
```

## Key Components

### 1. EnhancedModelRunner
- Inherits from base ModelRunner
- Adds dependency injection support
- Uses enhanced components when configured

### 2. EnhancedFeatureDetectorFactory  
- Inherits from base FeatureDetectorFactory
- Supports extended modes (like async processing)
- Falls back to base functionality when needed

### 3. AsyncSMDetector
- New detector type for async SageMaker processing
- Better performance for large workloads
- Enhanced monitoring and error handling

## Configuration

Extensions are controlled by environment variables:

```bash
# Enable extensions
export USE_EXTENSIONS=true

# Disable extensions (use base functionality)
export USE_EXTENSIONS=false
```

When enabled, the system automatically uses enhanced components. When disabled, it uses the original base components.

## Usage Examples

### Basic Usage
```python
from osml_extensions import EnhancedModelRunner

# Create enhanced model runner
runner = EnhancedModelRunner()
runner.run()
```

### Using Enhanced Factory
```python
from osml_extensions import EnhancedFeatureDetectorFactory, ExtendedModelInvokeMode

# Create factory with async support
factory = EnhancedFeatureDetectorFactory(
    endpoint="my-endpoint",
    endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC
)

detector = factory.build()  # Gets AsyncSMDetector
```

## Safety Features

The system has built-in safety mechanisms:

1. **Configuration Control**: Extensions can be disabled via `USE_EXTENSIONS=false`
2. **Graceful Fallback**: If enhanced components fail, system uses base functionality  
3. **Inheritance Safety**: Enhanced classes inherit all base functionality automatically
4. **Error Handling**: Failed operations retry with base components

## Requirements and Architecture Goals

Based on the extension refactor requirements, the system provides:

### Core Requirements
1. **Clean Separation**: Extensions are completely separate from base package
2. **Easy Maintenance**: Upstream updates integrate without conflicts  
3. **Backward Compatibility**: Existing applications work unchanged
4. **Environment Control**: Extensions enabled/disabled via configuration
5. **Graceful Degradation**: Automatic fallback when extensions fail

### Architecture Principles
- **Inheritance-Based**: Enhanced components extend base classes
- **Dependency Injection**: Components can be swapped at runtime
- **Configuration-Driven**: Behavior controlled by environment variables
- **Fail-Safe Design**: Multiple fallback layers prevent system failures
- **Performance Monitoring**: Built-in metrics and logging for enhanced components

### Extension Capabilities
- **Async Processing**: AsyncSMDetector for improved performance
- **Enhanced Monitoring**: Better metrics and logging
- **Extended Modes**: Support for new processing modes
- **Retry Logic**: Configurable retry behavior for failed operations
- **Debug Support**: Enhanced debugging and troubleshooting tools

## Installation and Setup

### Install Extensions
```bash
# Install from source
pip install -e .

# Or install with development dependencies  
pip install -e ".[dev]"
```

### Environment Configuration
```bash
# Enable extensions
export USE_EXTENSIONS=true
export ASYNC_DETECTOR_ENABLED=true
export ENHANCED_MONITORING_ENABLED=true

# Configure retry behavior
export EXTENSION_MAX_RETRY_ATTEMPTS=3
export EXTENSION_RETRY_DELAY_SECONDS=1.0

# Enable debug logging
export EXTENSION_DEBUG_LOGGING_ENABLED=true
```

## Troubleshooting

### Common Issues

**Extensions not loading:**
- Check `USE_EXTENSIONS=true` is set
- Verify extensions package is installed: `pip list | grep osml-extensions`
- Check logs for import errors

**Configuration problems:**
```python
# Validate configuration
from osml_extensions.config import validate_environment_variables
issues = validate_environment_variables()
if issues:
    print("Issues:", issues)

# Get configuration summary  
from osml_extensions.config import get_config_summary
print(get_config_summary())
```

**Enable debug logging:**
```bash
export EXTENSION_DEBUG_LOGGING_ENABLED=true
```

## Summary

The Model Runner Extensions provide enhanced functionality through clean inheritance:

- **Enhanced components** extend base classes with new capabilities
- **Same interface** as original components - no code changes needed
- **Configuration controlled** - enable/disable via environment variables  
- **Safe fallback** - automatically uses base functionality if extensions fail
- **Easy maintenance** - upstream updates integrate smoothly

The system adds powerful features like async processing and enhanced monitoring while maintaining full compatibility with existing applications.