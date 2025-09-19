#!/usr/bin/env python3
"""
Error Handling and Recovery Example

This example demonstrates comprehensive error handling and recovery mechanisms
for the async endpoint integration, including retry logic and graceful failure handling.
"""

import json
import logging
import os
import time
from io import BytesIO
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError, NoCredentialsError

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


def demonstrate_s3_error_handling():
    """Demonstrate S3 error handling and recovery."""
    logger.info("=" * 60)
    logger.info("S3 ERROR HANDLING DEMONSTRATION")
    logger.info("=" * 60)
    
    config = AsyncEndpointConfig(
        input_bucket="demo-input-bucket",
        output_bucket="demo-output-bucket",
        max_retries=3  # Enable retries
    )
    
    detector = AsyncSMDetector(
        endpoint="demo-endpoint",
        async_config=config
    )
    
    # Test S3 bucket validation
    logger.info("1. Testing S3 bucket access validation...")
    try:
        detector.s3_manager.validate_bucket_access()
        logger.info("   ✓ S3 buckets are accessible")
    except S3OperationError as e:
        logger.error(f"   ✗ S3 access error: {e}")
        logger.info("   Recovery actions:")
        logger.info("   - Check bucket names in configuration")
        logger.info("   - Verify IAM permissions")
        logger.info("   - Ensure buckets exist in the correct region")
    
    # Test S3 upload with simulated errors
    logger.info("\n2. Testing S3 upload error handling...")
    
    # Simulate different S3 errors
    s3_errors = [
        {
            "name": "Access Denied",
            "error": ClientError(
                error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                operation_name='PutObject'
            ),
            "recovery": [
                "Check IAM permissions for s3:PutObject",
                "Verify bucket policy allows uploads",
                "Ensure correct AWS credentials are configured"
            ]
        },
        {
            "name": "No Credentials",
            "error": NoCredentialsError(),
            "recovery": [
                "Configure AWS credentials",
                "Check AWS_PROFILE environment variable",
                "Verify IAM role permissions if using EC2/Lambda"
            ]
        },
        {
            "name": "Bucket Not Found",
            "error": ClientError(
                error_response={'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket does not exist'}},
                operation_name='PutObject'
            ),
            "recovery": [
                "Create the S3 bucket",
                "Check bucket name spelling",
                "Verify bucket region matches configuration"
            ]
        }
    ]
    
    for error_case in s3_errors:
        logger.info(f"\n   Simulating: {error_case['name']}")
        logger.info(f"   Error type: {type(error_case['error']).__name__}")
        logger.info("   Recovery actions:")
        for action in error_case['recovery']:
            logger.info(f"   - {action}")


def demonstrate_inference_timeout_handling():
    """Demonstrate inference timeout handling and recovery."""
    logger.info("=" * 60)
    logger.info("INFERENCE TIMEOUT HANDLING DEMONSTRATION")
    logger.info("=" * 60)
    
    # Configuration with different timeout strategies
    timeout_configs = [
        {
            "name": "Short Timeout (Development)",
            "config": AsyncEndpointConfig(
                input_bucket="demo-bucket",
                output_bucket="demo-bucket",
                max_wait_time=300,  # 5 minutes
                polling_interval=10
            ),
            "use_case": "Quick feedback during development"
        },
        {
            "name": "Medium Timeout (Production)",
            "config": AsyncEndpointConfig(
                input_bucket="demo-bucket",
                output_bucket="demo-bucket",
                max_wait_time=1800,  # 30 minutes
                polling_interval=30
            ),
            "use_case": "Standard production workloads"
        },
        {
            "name": "Long Timeout (Complex Models)",
            "config": AsyncEndpointConfig(
                input_bucket="demo-bucket",
                output_bucket="demo-bucket",
                max_wait_time=7200,  # 2 hours
                polling_interval=60,
                max_polling_interval=600  # 10 minutes max
            ),
            "use_case": "Complex models or large datasets"
        }
    ]
    
    for timeout_config in timeout_configs:
        logger.info(f"\n{timeout_config['name']}:")
        logger.info(f"  Max wait time: {timeout_config['config'].max_wait_time} seconds")
        logger.info(f"  Polling interval: {timeout_config['config'].polling_interval} seconds")
        logger.info(f"  Use case: {timeout_config['use_case']}")
    
    # Demonstrate timeout error handling
    logger.info("\nTimeout Error Handling:")
    logger.info("When AsyncInferenceTimeoutError occurs:")
    logger.info("1. Check SageMaker endpoint capacity and scaling")
    logger.info("2. Verify input data size and complexity")
    logger.info("3. Consider increasing max_wait_time")
    logger.info("4. Check for endpoint throttling or limits")
    logger.info("5. Monitor CloudWatch metrics for the endpoint")


def demonstrate_retry_mechanisms():
    """Demonstrate retry mechanisms and exponential backoff."""
    logger.info("=" * 60)
    logger.info("RETRY MECHANISMS DEMONSTRATION")
    logger.info("=" * 60)
    
    # Configuration with aggressive retry settings
    config = AsyncEndpointConfig(
        input_bucket="demo-bucket",
        output_bucket="demo-bucket",
        max_retries=5,  # Retry up to 5 times
        polling_interval=5,  # Start with 5 seconds
        max_polling_interval=300,  # Max 5 minutes
        exponential_backoff_multiplier=2.0  # Double each time
    )
    
    logger.info("Retry Configuration:")
    logger.info(f"  Max retries: {config.max_retries}")
    logger.info(f"  Initial polling interval: {config.polling_interval} seconds")
    logger.info(f"  Max polling interval: {config.max_polling_interval} seconds")
    logger.info(f"  Backoff multiplier: {config.exponential_backoff_multiplier}")
    
    # Simulate exponential backoff calculation
    logger.info("\nExponential Backoff Progression:")
    base_interval = config.polling_interval
    multiplier = config.exponential_backoff_multiplier
    
    for attempt in range(6):
        interval = min(base_interval * (multiplier ** attempt), config.max_polling_interval)
        logger.info(f"  Attempt {attempt + 1}: {interval:.1f} seconds")
    
    logger.info("\nRetry Scenarios:")
    logger.info("• S3 operations: Automatic retry with exponential backoff")
    logger.info("• Network failures: Transparent retry handling")
    logger.info("• Temporary endpoint issues: Polling continues with backoff")
    logger.info("• Rate limiting: Automatic backoff prevents further throttling")


def demonstrate_graceful_failure_handling():
    """Demonstrate graceful failure handling and cleanup."""
    logger.info("=" * 60)
    logger.info("GRACEFUL FAILURE HANDLING DEMONSTRATION")
    logger.info("=" * 60)
    
    config = AsyncEndpointConfig(
        input_bucket="demo-bucket",
        output_bucket="demo-bucket",
        cleanup_enabled=True  # Ensure cleanup on failure
    )
    
    detector = AsyncSMDetector(
        endpoint="demo-endpoint",
        async_config=config
    )
    
    # Simulate different failure scenarios
    failure_scenarios = [
        {
            "name": "S3 Upload Failure",
            "description": "Input payload upload to S3 fails",
            "cleanup_actions": [
                "No S3 objects created yet",
                "Local resources cleaned up",
                "Error logged with details"
            ]
        },
        {
            "name": "Endpoint Invocation Failure",
            "description": "SageMaker endpoint invocation fails",
            "cleanup_actions": [
                "Input S3 object cleaned up",
                "No inference job created",
                "Detailed error information provided"
            ]
        },
        {
            "name": "Inference Job Failure",
            "description": "Inference job fails during processing",
            "cleanup_actions": [
                "Input S3 object cleaned up",
                "Failed job resources cleaned up",
                "Error details from SageMaker logged"
            ]
        },
        {
            "name": "Result Download Failure",
            "description": "Output download from S3 fails",
            "cleanup_actions": [
                "Input S3 object cleaned up",
                "Output S3 object cleaned up (if exists)",
                "Inference job marked as failed"
            ]
        }
    ]
    
    for scenario in failure_scenarios:
        logger.info(f"\n{scenario['name']}:")
        logger.info(f"  Scenario: {scenario['description']}")
        logger.info("  Cleanup actions:")
        for action in scenario['cleanup_actions']:
            logger.info(f"    - {action}")
    
    logger.info("\nFailure Handling Best Practices:")
    logger.info("1. Always use try-catch blocks for async operations")
    logger.info("2. Enable cleanup to prevent resource leaks")
    logger.info("3. Log detailed error information for debugging")
    logger.info("4. Implement appropriate retry strategies")
    logger.info("5. Monitor resource usage and cleanup statistics")
    logger.info("6. Use context managers for automatic cleanup")


def demonstrate_error_recovery_patterns():
    """Demonstrate common error recovery patterns."""
    logger.info("=" * 60)
    logger.info("ERROR RECOVERY PATTERNS")
    logger.info("=" * 60)
    
    config = AsyncEndpointConfig(
        input_bucket="demo-bucket",
        output_bucket="demo-bucket"
    )
    
    # Pattern 1: Retry with exponential backoff
    logger.info("1. Retry with Exponential Backoff Pattern:")
    logger.info("""
    def robust_async_inference(detector, payload, max_attempts=3):
        for attempt in range(max_attempts):
            try:
                return detector.find_features(payload)
            except S3OperationError as e:
                if attempt == max_attempts - 1:
                    raise
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                logger.info(f"Retrying after {wait_time}s (attempt {attempt + 1})")
    """)
    
    # Pattern 2: Fallback to alternative configuration
    logger.info("\n2. Fallback Configuration Pattern:")
    logger.info("""
    def inference_with_fallback(endpoint, payload):
        # Try with optimized configuration first
        try:
            config = AsyncEndpointConfig(
                input_bucket="primary-bucket",
                output_bucket="primary-bucket",
                max_wait_time=1800
            )
            detector = AsyncSMDetector(endpoint, async_config=config)
            return detector.find_features(payload)
        except Exception:
            # Fallback to conservative configuration
            config = AsyncEndpointConfig(
                input_bucket="fallback-bucket",
                output_bucket="fallback-bucket",
                max_wait_time=3600,
                max_retries=5
            )
            detector = AsyncSMDetector(endpoint, async_config=config)
            return detector.find_features(payload)
    """)
    
    # Pattern 3: Circuit breaker pattern
    logger.info("\n3. Circuit Breaker Pattern:")
    logger.info("""
    class AsyncInferenceCircuitBreaker:
        def __init__(self, failure_threshold=5, recovery_timeout=300):
            self.failure_count = 0
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.last_failure_time = None
            self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
        def call(self, detector, payload):
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = detector.find_features(payload)
                self.failure_count = 0
                self.state = 'CLOSED'
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                raise
    """)


def main():
    """Main function demonstrating error handling and recovery."""
    logger.info("Starting error handling and recovery demonstration")
    
    try:
        # Demonstrate S3 error handling
        demonstrate_s3_error_handling()
        
        # Demonstrate inference timeout handling
        demonstrate_inference_timeout_handling()
        
        # Demonstrate retry mechanisms
        demonstrate_retry_mechanisms()
        
        # Demonstrate graceful failure handling
        demonstrate_graceful_failure_handling()
        
        # Demonstrate error recovery patterns
        demonstrate_error_recovery_patterns()
        
        # Summary of best practices
        logger.info("=" * 60)
        logger.info("ERROR HANDLING BEST PRACTICES SUMMARY")
        logger.info("=" * 60)
        logger.info("1. Implement comprehensive exception handling")
        logger.info("2. Use appropriate retry strategies with exponential backoff")
        logger.info("3. Configure timeouts based on workload characteristics")
        logger.info("4. Enable resource cleanup to prevent leaks")
        logger.info("5. Log detailed error information for debugging")
        logger.info("6. Implement fallback mechanisms for critical operations")
        logger.info("7. Monitor error rates and adjust configurations accordingly")
        logger.info("8. Use circuit breaker patterns for external dependencies")
        logger.info("9. Test error scenarios in development environments")
        logger.info("10. Document error recovery procedures for operations teams")
        
    except Exception as e:
        logger.error(f"Error in demonstration: {e}")
        logger.exception("Full traceback:")
        return 1
    
    logger.info("Error handling and recovery demonstration completed successfully")
    return 0


if __name__ == "__main__":
    exit(main())