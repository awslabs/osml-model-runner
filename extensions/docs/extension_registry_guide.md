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

## Creating Extensions

### Basic Extension Structure

```python
from osml_extensions.registry import register_handler, HandlerType

@register_handler(
    request_type="my_custom_type",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="my_region_handler",
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

### Module Initialization

Ensure handlers are imported in `__init__.py` to trigger registration:

```python
# my_extension/__init__.py
from .region_handler import MyRegionHandler
from .image_handler import MyImageHandler

__all__ = ["MyRegionHandler", "MyImageHandler"]
```
