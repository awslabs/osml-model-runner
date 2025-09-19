#!/usr/bin/env python3
"""
Basic Async Endpoint Usage Example

This example demonstrates the basic usage of the SageMaker Async Endpoint integration
with the OSML Model Runner extensions.
"""

import json
import logging
import os
from io import BytesIO

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.detectors import AsyncSMDetector
from ..src.osml_extensions.errors import ExtensionRuntimeError
from ..src.osml_extensions.s3 import S3OperationError
from ..src.osml_extensions.polling import AsyncInferenceTimeoutError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function demonstrating basic async endpoint usage."""
    
    # Configuration
    endpoint_name = os.getenv("SAGEMAKER_ASYNC_ENDPOINT", "my-async-endpoint")
    input_bucket = os.getenv("ASYNC_INPUT_BUCKET", "my-async-input-bucket")
    output_bucket = os.getenv("ASYNC_OUTPUT_BUCKET", "my-async-output-bucket")
    
    logger.info("Starting basic async endpoint example")
    logger.info(f"Endpoint: {endpoint_name}")
    logger.info(f"Input bucket: {input_bucket}")
    logger.info(f"Output bucket: {output_bucket}")
    
    # Create async endpoint configuration
    config = AsyncEndpointConfig(
        input_bucket=input_bucket,
        output_bucket=output_bucket,
        max_wait_time=1800,  # 30 minutes
        polling_interval=30,  # Poll every 30 seconds
        cleanup_enabled=True
    )
    
    # Create async detector
    detector = AsyncSMDetector(
        endpoint=endpoint_name,
        async_config=config
    )
    
    try:
        # Validate S3 bucket access
        logger.info("Validating S3 bucket access...")
        detector.s3_manager.validate_bucket_access()
        logger.info("S3 buckets are accessible")
        
        # Create sample payload
        sample_data = {
            "image": "base64_encoded_image_data_here",
            "parameters": {
                "confidence_threshold": 0.5,
                "max_detections": 100
            }
        }
        
        payload = BytesIO(json.dumps(sample_data).encode('utf-8'))
        logger.info(f"Created payload with {len(payload.getvalue())} bytes")
        
        # Process inference request
        logger.info("Submitting async inference request...")
        feature_collection = detector.find_features(payload)
        
        # Process results
        features = feature_collection.get('features', [])
        logger.info(f"Inference completed successfully!")
        logger.info(f"Found {len(features)} features")
        
        # Display sample results
        if features:
            logger.info("Sample feature:")
            sample_feature = features[0]
            logger.info(f"  Type: {sample_feature.get('type')}")
            logger.info(f"  Geometry: {sample_feature.get('geometry', {}).get('type')}")
            logger.info(f"  Properties: {list(sample_feature.get('properties', {}).keys())}")
        
        # Get resource statistics
        stats = detector.get_resource_stats()
        logger.info(f"Resource statistics: {stats}")
        
    except S3OperationError as e:
        logger.error(f"S3 operation failed: {e}")
        logger.error("Please check:")
        logger.error("1. S3 bucket names are correct")
        logger.error("2. IAM permissions for S3 access")
        logger.error("3. S3 buckets exist and are accessible")
        return 1
        
    except AsyncInferenceTimeoutError as e:
        logger.error(f"Async inference timed out: {e}")
        logger.error("Consider:")
        logger.error("1. Increasing max_wait_time in configuration")
        logger.error("2. Checking SageMaker endpoint capacity")
        logger.error("3. Verifying input data size and complexity")
        return 1
        
    except ExtensionRuntimeError as e:
        logger.error(f"Runtime error: {e}")
        logger.error("Please check the logs for more details")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.exception("Full traceback:")
        return 1
    
    finally:
        # Clean up resources
        logger.info("Cleaning up resources...")
        try:
            cleaned_count = detector.cleanup_resources(force=True)
            logger.info(f"Cleaned up {cleaned_count} resources")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    logger.info("Basic async endpoint example completed successfully")
    return 0


if __name__ == "__main__":
    exit(main())