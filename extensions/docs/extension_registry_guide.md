# Extension Registry System Guide

## Overview

The Extension Registry System provides a flexible, environment-driven approach to loading and managing handler extensions in the OSML Model Runner. This system enables automatic registration of both base handlers and extension handlers, with dynamic selection based on environment configuration and endpoint requirements.

## Key Concepts

### Request Types

The system organizes handlers by **request types**, which determine the processing approach:

- **`http`**: Standard HTTP-based processing (base handlers)
- **`sm_endpoint`**: SageMaker endpoint processing
- **`async_sm_endpoint`**: Asynchronous SageMaker endpoint processing

Each request type has exactly one region handler and one image handler.

### Handler Types

- **`REGION_REQUEST_HANDLER`**: Processes individual image regions
- **`IMAGE_REQUEST_HANDLER`**: Orchestrates overall image processing

### Handler Selection Priority

1. Explicit request type parameter
2. `REQUEST_TYPE` environment variable
3. Inference from endpoint configuration
4. Extensions disabled → use `http`
5. Default fallback to `http`

## Creating Extensions

### Basic Extension Structure

```python
from osml_extensions.registry import register_handler, HandlerType

@register_handler(
    request_type="my_custom_type",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="my_region_handler",
    supported_endpoints=["custom_endpoint"],
    dependencies=["config", "table", "monitor"],
    version="1.0.0",
    description="Custom region handler for special processing"
)
class MyRegionHandler(BaseRegionHandler):
    def __init__(self, config, table, monitor):
        super().__init__(config, table, monitor)
        # Custom initialization
    
    def process_region_request(self, region_request, region_request_item, raster_dataset, sensor_model=None, metrics=None):
        # Custom processing logic
        return super().process_region_request(region_request, region_request_item, raster_dataset, sensor_model, metrics)
```

### Registration Parameters

- **`request_type`** (required): The request type this handler supports
- **`handler_type`** (required): `HandlerType.REGION_REQUEST_HANDLER` or `HandlerType.IMAGE_REQUEST_HANDLER`
- **`name`** (required): Unique identifier for the handler
- **`supported_endpoints`** (optional): List of endpoint types this handler supports
- **`dependencies`** (optional): List of required dependencies for instantiation
- **`version`** (optional): Handler version (default: "1.0.0")
- **`description`** (optional): Human-readable description

### Complete Handler Pair Example

```python
from osml_extensions.registry import register_handler, HandlerType
from aws.osml.model_runner.region_request_handler import RegionRequestHandler
from aws.osml.model_runner.image_request_handler import ImageRequestHandler

@register_handler(
    request_type="custom_processing",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="custom_region_handler",
    supported_endpoints=["custom_api", "special_endpoint"],
    dependencies=[
        "region_request_table",
        "job_table",
        "region_status_monitor",
        "endpoint_statistics_table",
        "tiling_strategy",
        "endpoint_utils",
        "config"
    ],
    version="2.0.0",
    description="Custom region handler with enhanced processing"
)
class CustomRegionHandler(RegionRequestHandler):
    def __init__(self, region_request_table, job_table, region_status_monitor, 
                 endpoint_statistics_table, tiling_strategy, endpoint_utils, config):
        super().__init__(
            region_request_table, job_table, region_status_monitor,
            endpoint_statistics_table, tiling_strategy, endpoint_utils, config
        )
        # Custom initialization

@register_handler(
    request_type="custom_processing",
    handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
    name="custom_image_handler",
    supported_endpoints=["custom_api", "special_endpoint"],
    dependencies=[
        "job_table",
        "image_status_monitor",
        "endpoint_statistics_table",
        "tiling_strategy",
        "region_request_queue",
        "region_request_table",
        "endpoint_utils",
        "config",
        "region_request_handler"
    ],
    version="2.0.0",
    description="Custom image handler with enhanced processing"
)
class CustomImageHandler(ImageRequestHandler):
    def __init__(self, job_table, image_status_monitor, endpoint_statistics_table,
                 tiling_strategy, region_request_queue, region_request_table,
                 endpoint_utils, config, region_request_handler):
        super().__init__(
            job_table, image_status_monitor, endpoint_statistics_table,
            tiling_strategy, region_request_queue, region_request_table,
            endpoint_utils, config, region_request_handler
        )
        # Custom initialization
```

## Configuration

### Environment Variables

- **`REQUEST_TYPE`**: Specify the request type to use
  ```bash
  export REQUEST_TYPE=async_sm_endpoint
  ```

- **`USE_EXTENSIONS`**: Enable/disable extension system
  ```bash
  export USE_EXTENSIONS=true
  ```

- **`EXTENSION_FALLBACK_ENABLED`**: Enable fallback to base handlers on failure
  ```bash
  export EXTENSION_FALLBACK_ENABLED=true
  ```

### Endpoint Configuration

Configure endpoints using environment variables:

```bash
export ENDPOINT_ASYNC_CONFIG='{"type": "async", "url": "https://async-endpoint.com"}'
export ENDPOINT_SYNC_CONFIG='{"type": "sagemaker", "url": "https://sm-endpoint.com"}'
```

The system will automatically infer request types from endpoint configurations:
- `"async"` in endpoint type → `async_sm_endpoint`
- `"sagemaker"` or `"sm"` in endpoint type → `sm_endpoint`
- Other types → `http`

## Extension Module Structure

### Recommended Directory Structure

```
my_extension/
├── __init__.py              # Import handlers to trigger registration
├── region_handler.py        # Region handler implementation
├── image_handler.py         # Image handler implementation
├── config.py               # Extension-specific configuration
└── utils.py                # Extension utilities
```

### Module Initialization

Ensure handlers are imported in `__init__.py` to trigger registration:

```python
# my_extension/__init__.py
from .region_handler import MyRegionHandler
from .image_handler import MyImageHandler

__all__ = ["MyRegionHandler", "MyImageHandler"]
```

## Testing Extensions

### Unit Testing

```python
import unittest
from unittest.mock import Mock
from osml_extensions.registry import get_registry, reset_registry
from my_extension import MyRegionHandler

class TestMyExtension(unittest.TestCase):
    def setUp(self):
        reset_registry()
        self.registry = get_registry()
    
    def tearDown(self):
        reset_registry()
    
    def test_handler_registration(self):
        # Import triggers registration
        from my_extension import MyRegionHandler
        
        # Verify registration
        self.assertTrue(
            self.registry.is_registered("my_custom_type", HandlerType.REGION_REQUEST_HANDLER)
        )
    
    def test_handler_functionality(self):
        # Test handler logic
        handler = MyRegionHandler(Mock(), Mock(), Mock())
        # Test methods...
```

### Integration Testing

```python
from osml_extensions.registry import HandlerSelector, DependencyInjector

def test_end_to_end_selection():
    selector = HandlerSelector()
    injector = DependencyInjector()
    
    # Select handlers
    region_metadata, image_metadata = selector.select_handlers(
        request_type="my_custom_type"
    )
    
    # Create handlers with dependencies
    dependencies = create_mock_dependencies()
    region_handler = injector.create_handler(region_metadata, dependencies)
    
    # Test functionality
    assert isinstance(region_handler, MyRegionHandler)
```

## Troubleshooting

### Common Issues

#### Handler Not Found
```
HandlerSelectionError: No region request handler found for request_type='my_type'
```

**Solutions:**
1. Verify handler is registered by importing the module
2. Check request type spelling
3. Verify handler registration parameters

#### Missing Dependencies
```
DependencyInjectionError: Missing required dependencies for handler 'my_handler': {'missing_dep'}
```

**Solutions:**
1. Add missing dependency to the dependencies context
2. Update handler's `dependencies` list in registration
3. Check dependency names match constructor parameters

#### Registration Errors
```
HandlerRegistrationError: handler_type must be a HandlerType enum
```

**Solutions:**
1. Use `HandlerType.REGION_REQUEST_HANDLER` or `HandlerType.IMAGE_REQUEST_HANDLER`
2. Import `HandlerType` from `osml_extensions.registry`
3. Check all required registration parameters are provided

### Debugging Tips

#### Check Registry State
```python
from osml_extensions.registry import get_registry

registry = get_registry()
print("Supported request types:", registry.get_supported_request_types())
print("Registry stats:", registry.get_registry_stats())

# Check specific handler
metadata = registry.get_handler("my_type", HandlerType.REGION_REQUEST_HANDLER)
if metadata:
    print(f"Handler: {metadata.name}, Class: {metadata.handler_class}")
else:
    print("Handler not found")
```

#### Enable Debug Logging
```python
import logging
logging.getLogger('osml_extensions.registry').setLevel(logging.DEBUG)
```

#### Test Handler Selection
```python
from osml_extensions.registry import HandlerSelector

selector = HandlerSelector()
try:
    region_metadata, image_metadata = selector.select_handlers(
        request_type="my_type"
    )
    print(f"Selected: {region_metadata.name}, {image_metadata.name}")
except Exception as e:
    print(f"Selection failed: {e}")
```

## Best Practices

### Handler Implementation
1. **Inherit from base handlers** to maintain compatibility
2. **Call parent methods** when overriding to preserve base functionality
3. **Handle errors gracefully** and provide meaningful error messages
4. **Document dependencies** clearly in the registration

### Extension Organization
1. **Group related handlers** in the same module
2. **Use descriptive names** for handlers and request types
3. **Version your extensions** appropriately
4. **Provide comprehensive descriptions** in registration

### Testing
1. **Test handler registration** in unit tests
2. **Test handler functionality** independently
3. **Test integration** with the registry system
4. **Use mock dependencies** for isolated testing
5. **Clean up registry** between tests

### Performance
1. **Minimize initialization overhead** in handler constructors
2. **Cache expensive computations** when possible
3. **Use appropriate logging levels** to avoid performance impact
4. **Consider thread safety** if handlers will be used concurrently

## Migration Guide

### From Hardcoded Handlers

If you have existing hardcoded handler instantiation:

**Before:**
```python
self.region_request_handler = EnhancedRegionRequestHandler(
    region_request_table=self.region_request_table,
    # ... other dependencies
)
```

**After:**
```python
from osml_extensions.registry import HandlerSelector, DependencyInjector

selector = HandlerSelector()
injector = DependencyInjector()

region_metadata, image_metadata = selector.select_handlers(
    config=self.config
)

dependencies = {
    "region_request_table": self.region_request_table,
    # ... other dependencies
}

self.region_request_handler = injector.create_handler(
    region_metadata, dependencies
)
```

### Adding Registration to Existing Handlers

**Before:**
```python
class MyHandler(BaseHandler):
    pass
```

**After:**
```python
from osml_extensions.registry import register_handler, HandlerType

@register_handler(
    request_type="my_type",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="my_handler",
    dependencies=["dep1", "dep2"]
)
class MyHandler(BaseHandler):
    pass
```

## Examples

See the `async_workflow` extension for a complete example of:
- Handler registration with decorators
- Dependency injection
- Environment-driven configuration
- Integration with the base model runner

The extension is located at:
`InferenceEngine/model-runner/extensions/src/osml_extensions/extensions/async_workflow/`