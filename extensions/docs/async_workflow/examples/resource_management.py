#!/usr/bin/env python3
"""
Resource Management Example

This example demonstrates the comprehensive resource management capabilities
of the async endpoint integration, including cleanup policies and monitoring.
"""

import json
import logging
import os
import tempfile
import time
from io import BytesIO

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.detectors import AsyncSMDetector
from ..src.osml_extensions.utils import ResourceManager, CleanupPolicy, ResourceType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demonstrate_cleanup_policies():
    """Demonstrate different cleanup policies."""
    logger.info("=" * 60)
    logger.info("CLEANUP POLICY DEMONSTRATION")
    logger.info("=" * 60)
    
    # 1. Immediate cleanup (default)
    logger.info("1. Immediate Cleanup Policy")
    config_immediate = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="immediate"
    )
    logger.info("   - Resources cleaned up immediately after use")
    logger.info("   - Best for production environments")
    logger.info("   - Minimizes resource usage and costs")
    
    # 2. Delayed cleanup
    logger.info("\n2. Delayed Cleanup Policy")
    config_delayed = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="delayed",
        cleanup_delay_seconds=1800  # 30 minutes
    )
    logger.info("   - Resources cleaned up after a delay")
    logger.info("   - Useful for debugging and troubleshooting")
    logger.info("   - Allows inspection of intermediate files")
    
    # 3. Disabled cleanup
    logger.info("\n3. Disabled Cleanup Policy")
    config_disabled = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="disabled"
    )
    logger.info("   - No automatic cleanup")
    logger.info("   - Manual cleanup required")
    logger.info("   - Best for development and debugging")
    
    return config_immediate, config_delayed, config_disabled


def demonstrate_resource_monitoring():
    """Demonstrate resource monitoring capabilities."""
    logger.info("=" * 60)
    logger.info("RESOURCE MONITORING DEMONSTRATION")
    logger.info("=" * 60)
    
    # Create configuration with immediate cleanup disabled for monitoring
    config = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="disabled"  # Disable for monitoring demo
    )
    
    # Create resource manager directly for demonstration
    resource_manager = ResourceManager(config)
    
    try:
        # Register various types of resources
        logger.info("Registering sample resources...")
        
        # Register S3 objects
        s3_resource_1 = resource_manager.register_s3_object(
            "s3://demo-bucket/input-file-1.json"
        )
        s3_resource_2 = resource_manager.register_s3_object(
            "s3://demo-bucket/output-file-1.json"
        )
        
        # Register temporary files
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"sample data")
            temp_path = temp_file.name
        
        temp_resource = resource_manager.register_temp_file(temp_path)
        
        # Register inference job
        job_data = {
            "input_s3_uri": "s3://demo-bucket/input-file-1.json",
            "output_s3_uri": "s3://demo-bucket/output-file-1.json",
            "temp_files": [temp_path]
        }
        job_resource = resource_manager.register_inference_job(
            "demo-inference-123", job_data
        )
        
        logger.info(f"Registered resources:")
        logger.info(f"  S3 objects: 2")
        logger.info(f"  Temp files: 1")
        logger.info(f"  Inference jobs: 1")
        
        # Get resource statistics
        stats = resource_manager.get_resource_stats()
        
        logger.info("\nResource Statistics:")
        logger.info(f"Total resources: {stats['total_resources']}")
        
        for resource_type, type_stats in stats['by_type'].items():
            logger.info(f"{resource_type}:")
            logger.info(f"  Total: {type_stats['total']}")
            logger.info(f"  Cleanup attempted: {type_stats['cleanup_attempted']}")
            logger.info(f"  Cleanup successful: {type_stats['cleanup_successful']}")
            logger.info(f"  Cleanup failed: {type_stats['cleanup_failed']}")
        
        # Demonstrate selective cleanup
        logger.info("\nDemonstrating selective cleanup...")
        
        # Clean up only S3 objects
        s3_cleaned = resource_manager.cleanup_all_resources(
            ResourceType.S3_OBJECT, force=True
        )
        logger.info(f"Cleaned up {s3_cleaned} S3 objects")
        
        # Clean up temp files
        temp_cleaned = resource_manager.cleanup_all_resources(
            ResourceType.TEMP_FILE, force=True
        )
        logger.info(f"Cleaned up {temp_cleaned} temp files")
        
        # Get updated statistics
        updated_stats = resource_manager.get_resource_stats()
        logger.info(f"\nUpdated statistics:")
        logger.info(f"Cleanup attempted: {updated_stats['cleanup_stats']['attempted']}")
        logger.info(f"Cleanup successful: {updated_stats['cleanup_stats']['successful']}")
        logger.info(f"Cleanup failed: {updated_stats['cleanup_stats']['failed']}")
        
    finally:
        # Clean up remaining resources
        resource_manager.cleanup_all_resources(force=True)
        resource_manager.stop_cleanup_worker()
        
        # Clean up temp file if it still exists
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def demonstrate_context_manager():
    """Demonstrate using AsyncSMDetector as a context manager for automatic cleanup."""
    logger.info("=" * 60)
    logger.info("CONTEXT MANAGER DEMONSTRATION")
    logger.info("=" * 60)
    
    endpoint_name = "demo-async-endpoint"
    config = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="immediate"
    )
    
    logger.info("Using AsyncSMDetector as context manager...")
    logger.info("Resources will be automatically cleaned up when exiting context")
    
    try:
        with AsyncSMDetector(endpoint=endpoint_name, async_config=config) as detector:
            logger.info("Inside context manager - detector is active")
            
            # Get initial resource stats
            initial_stats = detector.get_resource_stats()
            logger.info(f"Initial resources: {initial_stats['total_resources']}")
            
            # Simulate some resource usage
            # In a real scenario, you would call detector.find_features() here
            logger.info("Simulating resource usage...")
            
            # Register some resources manually for demonstration
            detector.resource_manager.register_s3_object(
                "s3://demo-bucket/context-demo.json"
            )
            
            # Get updated stats
            updated_stats = detector.get_resource_stats()
            logger.info(f"Resources after usage: {updated_stats['total_resources']}")
            
        # Context manager automatically cleans up resources here
        logger.info("Exited context manager - resources automatically cleaned up")
        
    except Exception as e:
        logger.error(f"Error in context manager demo: {e}")


def demonstrate_failed_job_cleanup():
    """Demonstrate cleanup of failed job resources."""
    logger.info("=" * 60)
    logger.info("FAILED JOB CLEANUP DEMONSTRATION")
    logger.info("=" * 60)
    
    config = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        cleanup_policy="disabled"  # Disable for demo
    )
    
    resource_manager = ResourceManager(config)
    
    try:
        # Simulate a failed inference job
        inference_id = "failed-job-123"
        job_data = {
            "input_s3_uri": "s3://demo-bucket/failed-input.json",
            "output_s3_uri": "s3://demo-bucket/failed-output.json",
            "temp_files": ["/tmp/failed-temp-file.txt"]
        }
        
        logger.info(f"Registering failed job: {inference_id}")
        resource_manager.register_inference_job(inference_id, job_data)
        
        # Also register individual S3 objects
        resource_manager.register_s3_object(job_data["input_s3_uri"])
        resource_manager.register_s3_object(job_data["output_s3_uri"])
        
        # Get stats before cleanup
        stats_before = resource_manager.get_resource_stats()
        logger.info(f"Resources before cleanup: {stats_before['total_resources']}")
        
        # Clean up failed job resources
        logger.info("Cleaning up failed job resources...")
        success = resource_manager.cleanup_failed_job_resources(inference_id)
        
        if success:
            logger.info("Failed job cleanup completed successfully")
        else:
            logger.warning("Failed job cleanup encountered issues")
        
        # Get stats after cleanup
        stats_after = resource_manager.get_resource_stats()
        logger.info(f"Resources after cleanup: {stats_after['total_resources']}")
        logger.info(f"Cleanup operations: {stats_after['cleanup_stats']['attempted']}")
        
    finally:
        resource_manager.stop_cleanup_worker()


def main():
    """Main function demonstrating resource management features."""
    logger.info("Starting resource management demonstration")
    
    try:
        # Demonstrate cleanup policies
        demonstrate_cleanup_policies()
        
        # Demonstrate resource monitoring
        demonstrate_resource_monitoring()
        
        # Demonstrate context manager usage
        demonstrate_context_manager()
        
        # Demonstrate failed job cleanup
        demonstrate_failed_job_cleanup()
        
        # Best practices summary
        logger.info("=" * 60)
        logger.info("RESOURCE MANAGEMENT BEST PRACTICES")
        logger.info("=" * 60)
        logger.info("1. Use immediate cleanup in production for efficiency")
        logger.info("2. Use delayed cleanup for debugging and troubleshooting")
        logger.info("3. Monitor resource statistics regularly")
        logger.info("4. Use context managers for automatic cleanup")
        logger.info("5. Handle failed job cleanup explicitly")
        logger.info("6. Configure appropriate cleanup delays based on workload")
        logger.info("7. Test cleanup policies in development environments")
        
    except Exception as e:
        logger.error(f"Error in resource management demo: {e}")
        logger.exception("Full traceback:")
        return 1
    
    logger.info("Resource management demonstration completed successfully")
    return 0


if __name__ == "__main__":
    exit(main())