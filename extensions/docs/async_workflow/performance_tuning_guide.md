# Performance Tuning Guide for SageMaker Async Endpoints

This guide provides comprehensive guidance for optimizing the performance of the SageMaker Async Endpoint integration based on different workload patterns and requirements.

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Configuration Optimization](#configuration-optimization)
3. [Workload-Specific Tuning](#workload-specific-tuning)
4. [Worker Pool Optimization](#worker-pool-optimization)
5. [Resource Management Tuning](#resource-management-tuning)
6. [Monitoring and Metrics](#monitoring-and-metrics)
7. [Benchmarking](#benchmarking)
8. [Cost Optimization](#cost-optimization)

## Performance Overview

The async endpoint integration provides several optimization opportunities:

### Key Performance Factors

1. **Worker Pool Configuration**: Number and type of workers
2. **Polling Strategy**: Frequency and backoff patterns
3. **Resource Management**: Cleanup policies and timing
4. **S3 Operations**: Upload/download optimization
5. **Concurrency Limits**: Maximum concurrent jobs
6. **Endpoint Capacity**: SageMaker endpoint scaling

### Performance Metrics

- **Throughput**: Tiles processed per second
- **Latency**: Time from submission to completion
- **Queue Time**: Time waiting for inference completion
- **Resource Utilization**: CPU, memory, and network usage
- **Cost Efficiency**: Cost per processed tile

## Configuration Optimization

### High-Throughput Configuration

Optimized for maximum processing speed:

```python
from osml_extensions.config import AsyncEndpointConfig

high_throughput_config = AsyncEndpointConfig(
    input_bucket="high-perf-input-bucket",
    output_bucket="high-perf-output-bucket",
    
    # Aggressive polling for fast completion detection
    polling_interval=5,           # Poll every 5 seconds
    max_polling_interval=30,      # Max 30 seconds between polls
    exponential_backoff_multiplier=1.2,  # Slow backoff growth
    
    # High concurrency settings
    max_concurrent_jobs=500,      # Allow many concurrent jobs
    submission_workers=16,        # Many submission workers
    polling_workers=8,            # Adequate polling workers
    job_queue_timeout=60,         # Quick queue operations
    
    # Optimized S3 operations
    max_retries=2,                # Fewer retries for speed
    
    # Immediate cleanup for memory efficiency
    cleanup_policy="immediate",
    
    # Extended timeout for complex workloads
    max_wait_time=7200,           # 2 hours
    
    # Enable all optimizations
    enable_worker_optimization=True
)
```

### Low-Latency Configuration

Optimized for fastest individual request processing:

```python
low_latency_config = AsyncEndpointConfig(
    input_bucket="low-latency-input-bucket",
    output_bucket="low-latency-output-bucket",
    
    # Very frequent polling
    polling_interval=2,           # Poll every 2 seconds
    max_polling_interval=10,      # Max 10 seconds
    exponential_backoff_multiplier=1.1,  # Minimal backoff
    
    # Moderate concurrency to avoid overwhelming
    max_concurrent_jobs=100,
    submission_workers=8,
    polling_workers=6,            # More polling workers for responsiveness
    
    # Quick operations
    max_retries=1,                # Minimal retries
    job_queue_timeout=30,
    
    # Immediate cleanup
    cleanup_policy="immediate",
    
    # Shorter timeout for quick feedback
    max_wait_time=1800,           # 30 minutes
    
    enable_worker_optimization=True
)
```

### Cost-Optimized Configuration

Optimized for minimal AWS service costs:

```python
cost_optimized_config = AsyncEndpointConfig(
    input_bucket="cost-opt-input-bucket",
    output_bucket="cost-opt-output-bucket",
    
    # Conservative polling to reduce API calls
    polling_interval=60,          # Poll every minute
    max_polling_interval=600,     # Max 10 minutes
    exponential_backoff_multiplier=2.0,  # Aggressive backoff
    
    # Lower concurrency to reduce resource usage
    max_concurrent_jobs=50,
    submission_workers=4,
    polling_workers=2,
    
    # More retries to handle transient issues
    max_retries=5,
    job_queue_timeout=300,
    
    # Delayed cleanup to batch S3 operations
    cleanup_policy="delayed",
    cleanup_delay_seconds=3600,   # 1 hour delay
    
    # Longer timeout to avoid premature failures
    max_wait_time=10800,          # 3 hours
    
    enable_worker_optimization=True
)
```

## Workload-Specific Tuning

### Small Tiles, High Volume

For processing many small tiles quickly:

```python
small_tiles_config = AsyncEndpointConfig(
    # Fast processing settings
    polling_interval=10,
    max_polling_interval=60,
    exponential_backoff_multiplier=1.3,
    
    # High concurrency for small payloads
    max_concurrent_jobs=300,
    submission_workers=12,
    polling_workers=6,
    
    # Quick cleanup
    cleanup_policy="immediate",
    
    # Shorter timeout for small tiles
    max_wait_time=1800,
    
    enable_worker_optimization=True
)

# Usage example
detector = AsyncSMDetector(
    endpoint="small-tile-endpoint",
    async_config=small_tiles_config
)

# Process with optimized worker pool
worker_pool = AsyncTileWorkerPool(
    async_detector=detector,
    config=small_tiles_config
)
```

### Large Tiles, Complex Models

For processing large, complex tiles:

```python
large_tiles_config = AsyncEndpointConfig(
    # Conservative polling for long-running jobs
    polling_interval=30,
    max_polling_interval=300,
    exponential_backoff_multiplier=1.5,
    
    # Lower concurrency for large payloads
    max_concurrent_jobs=50,
    submission_workers=6,
    polling_workers=4,
    
    # More retries for complex operations
    max_retries=5,
    
    # Delayed cleanup to avoid interfering with long jobs
    cleanup_policy="delayed",
    cleanup_delay_seconds=1800,
    
    # Extended timeout for complex processing
    max_wait_time=14400,  # 4 hours
    
    enable_worker_optimization=True
)
```

### Mixed Workload

For handling diverse tile sizes and complexities:

```python
mixed_workload_config = AsyncEndpointConfig(
    # Balanced polling strategy
    polling_interval=20,
    max_polling_interval=180,
    exponential_backoff_multiplier=1.4,
    
    # Moderate concurrency
    max_concurrent_jobs=150,
    submission_workers=8,
    polling_workers=5,
    
    # Standard retry policy
    max_retries=3,
    
    # Immediate cleanup with some tolerance
    cleanup_policy="immediate",
    
    # Reasonable timeout for mixed workloads
    max_wait_time=3600,  # 1 hour
    
    enable_worker_optimization=True
)
```

## Worker Pool Optimization

### Determining Optimal Worker Counts

#### Submission Workers

The optimal number of submission workers depends on:
- SageMaker endpoint capacity
- S3 upload bandwidth
- CPU cores available

```python
def calculate_submission_workers(endpoint_capacity, cpu_cores):
    """Calculate optimal submission worker count."""
    # Rule of thumb: 2x endpoint capacity, capped by CPU cores
    optimal_workers = min(endpoint_capacity * 2, cpu_cores)
    return max(optimal_workers, 2)  # Minimum 2 workers

# Example calculation
endpoint_capacity = 10  # Concurrent requests endpoint can handle
cpu_cores = 8
submission_workers = calculate_submission_workers(endpoint_capacity, cpu_cores)
print(f"Recommended submission workers: {submission_workers}")
```

#### Polling Workers

Polling workers need fewer resources but should be sufficient to handle completion rates:

```python
def calculate_polling_workers(submission_workers, avg_job_duration):
    """Calculate optimal polling worker count."""
    # Rule of thumb: 1 polling worker per 2-4 submission workers
    # Adjust based on job duration
    if avg_job_duration < 300:  # < 5 minutes
        ratio = 4  # 1 polling worker per 4 submission workers
    elif avg_job_duration < 1800:  # < 30 minutes
        ratio = 3  # 1 polling worker per 3 submission workers
    else:
        ratio = 2  # 1 polling worker per 2 submission workers
    
    optimal_workers = max(submission_workers // ratio, 1)
    return optimal_workers

# Example calculation
avg_job_duration = 600  # 10 minutes average
polling_workers = calculate_polling_workers(submission_workers, avg_job_duration)
print(f"Recommended polling workers: {polling_workers}")
```

### Dynamic Worker Scaling

For varying workloads, consider implementing dynamic scaling:

```python
class DynamicWorkerPool:
    """Worker pool with dynamic scaling capabilities."""
    
    def __init__(self, base_config):
        self.base_config = base_config
        self.current_load = 0
        self.performance_history = []
    
    def adjust_workers_based_on_load(self, queue_size, completion_rate):
        """Adjust worker counts based on current load."""
        config = self.base_config
        
        # Scale submission workers based on queue size
        if queue_size > 100:
            config.submission_workers = min(config.submission_workers * 2, 20)
        elif queue_size < 20:
            config.submission_workers = max(config.submission_workers // 2, 2)
        
        # Scale polling workers based on completion rate
        if completion_rate < 0.5:  # Low completion rate
            config.polling_workers = min(config.polling_workers + 2, 10)
        elif completion_rate > 2.0:  # High completion rate
            config.polling_workers = max(config.polling_workers - 1, 1)
        
        return config
```

## Resource Management Tuning

### Cleanup Policy Selection

Choose cleanup policy based on workload characteristics:

```python
def select_cleanup_policy(workload_type, debug_mode=False):
    """Select optimal cleanup policy based on workload."""
    
    if debug_mode:
        return {
            "cleanup_policy": "disabled",
            "cleanup_delay_seconds": 0
        }
    
    if workload_type == "high_throughput":
        return {
            "cleanup_policy": "immediate",
            "cleanup_delay_seconds": 0
        }
    elif workload_type == "cost_optimized":
        return {
            "cleanup_policy": "delayed",
            "cleanup_delay_seconds": 3600  # 1 hour
        }
    elif workload_type == "development":
        return {
            "cleanup_policy": "delayed",
            "cleanup_delay_seconds": 1800  # 30 minutes
        }
    else:
        return {
            "cleanup_policy": "immediate",
            "cleanup_delay_seconds": 0
        }

# Usage
cleanup_config = select_cleanup_policy("high_throughput")
config = AsyncEndpointConfig(
    input_bucket="my-bucket",
    output_bucket="my-bucket",
    **cleanup_config
)
```

### Memory Management

Monitor and manage memory usage:

```python
def monitor_memory_usage(detector, threshold_mb=1000):
    """Monitor memory usage and trigger cleanup if needed."""
    import psutil
    import gc
    
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    if memory_mb > threshold_mb:
        print(f"High memory usage: {memory_mb:.1f} MB")
        
        # Force resource cleanup
        cleaned = detector.cleanup_resources(force=True)
        print(f"Cleaned up {cleaned} resources")
        
        # Force garbage collection
        gc.collect()
        
        # Check memory after cleanup
        new_memory_mb = process.memory_info().rss / 1024 / 1024
        print(f"Memory after cleanup: {new_memory_mb:.1f} MB")
        
        return new_memory_mb < memory_mb * 0.8  # 20% reduction
    
    return True

# Usage in processing loop
while processing:
    # Process tiles...
    
    # Periodic memory check
    if not monitor_memory_usage(detector):
        print("Warning: Memory usage remains high after cleanup")
```

## Monitoring and Metrics

### Performance Metrics Collection

```python
from osml_extensions.metrics import AsyncMetricsTracker
import time

class PerformanceMonitor:
    """Comprehensive performance monitoring."""
    
    def __init__(self):
        self.metrics = AsyncMetricsTracker()
        self.start_time = time.time()
        self.processed_count = 0
        self.failed_count = 0
    
    def record_tile_processed(self, processing_time):
        """Record successful tile processing."""
        self.processed_count += 1
        self.metrics.set_counter("ProcessingTime", int(processing_time))
        self.metrics.increment_counter("TilesProcessed")
    
    def record_tile_failed(self):
        """Record failed tile processing."""
        self.failed_count += 1
        self.metrics.increment_counter("TilesFailed")
    
    def get_performance_summary(self):
        """Get comprehensive performance summary."""
        elapsed_time = time.time() - self.start_time
        total_tiles = self.processed_count + self.failed_count
        
        return {
            "elapsed_time": elapsed_time,
            "total_tiles": total_tiles,
            "processed_tiles": self.processed_count,
            "failed_tiles": self.failed_count,
            "success_rate": self.processed_count / total_tiles if total_tiles > 0 else 0,
            "throughput": self.processed_count / elapsed_time if elapsed_time > 0 else 0,
            "avg_processing_time": self.metrics.get_counter("ProcessingTime") / self.processed_count if self.processed_count > 0 else 0
        }

# Usage
monitor = PerformanceMonitor()

# In processing loop
start_time = time.time()
try:
    result = detector.find_features(payload)
    processing_time = time.time() - start_time
    monitor.record_tile_processed(processing_time)
except Exception:
    monitor.record_tile_failed()

# Get summary
summary = monitor.get_performance_summary()
print(f"Throughput: {summary['throughput']:.2f} tiles/second")
print(f"Success rate: {summary['success_rate']:.1%}")
```

### Real-time Performance Dashboard

```python
def create_performance_dashboard(detector, worker_pool):
    """Create real-time performance dashboard."""
    import threading
    import time
    
    def dashboard_loop():
        while True:
            # Get worker statistics
            worker_stats = worker_pool.get_worker_stats()
            
            # Get resource statistics
            resource_stats = detector.get_resource_stats()
            
            # Display dashboard
            print("\n" + "="*60)
            print("PERFORMANCE DASHBOARD")
            print("="*60)
            
            # Worker statistics
            print(f"Submission Workers: {worker_stats['submission_workers']['workers']}")
            print(f"  Processed: {worker_stats['submission_workers']['total_processed']}")
            print(f"  Failed: {worker_stats['submission_workers']['total_failed']}")
            
            print(f"Polling Workers: {worker_stats['polling_workers']['workers']}")
            print(f"  Completed: {worker_stats['polling_workers']['total_completed']}")
            print(f"  Active Jobs: {worker_stats['polling_workers']['active_jobs']}")
            
            # Queue statistics
            print(f"Job Queue Size: {worker_stats['job_queue_size']}")
            print(f"Result Queue Size: {worker_stats['result_queue_size']}")
            
            # Resource statistics
            print(f"Total Resources: {resource_stats['total_resources']}")
            print(f"Cleanup Success Rate: {resource_stats['cleanup_stats']['successful']}/{resource_stats['cleanup_stats']['attempted']}")
            
            time.sleep(10)  # Update every 10 seconds
    
    dashboard_thread = threading.Thread(target=dashboard_loop, daemon=True)
    dashboard_thread.start()
    return dashboard_thread
```

## Benchmarking

### Performance Benchmarking Suite

```python
import time
import statistics
from concurrent.futures import ThreadPoolExecutor

class AsyncEndpointBenchmark:
    """Comprehensive benchmarking suite for async endpoints."""
    
    def __init__(self, detector, test_payloads):
        self.detector = detector
        self.test_payloads = test_payloads
        self.results = []
    
    def benchmark_single_request(self, payload):
        """Benchmark a single request."""
        start_time = time.time()
        try:
            result = self.detector.find_features(payload)
            end_time = time.time()
            
            return {
                "success": True,
                "duration": end_time - start_time,
                "features_found": len(result.get('features', [])),
                "error": None
            }
        except Exception as e:
            end_time = time.time()
            return {
                "success": False,
                "duration": end_time - start_time,
                "features_found": 0,
                "error": str(e)
            }
    
    def benchmark_throughput(self, num_requests=100, max_workers=10):
        """Benchmark throughput with concurrent requests."""
        print(f"Running throughput benchmark with {num_requests} requests...")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all requests
            futures = []
            for i in range(num_requests):
                payload = self.test_payloads[i % len(self.test_payloads)]
                future = executor.submit(self.benchmark_single_request, payload)
                futures.append(future)
            
            # Collect results
            results = [future.result() for future in futures]
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Analyze results
        successful_requests = [r for r in results if r['success']]
        failed_requests = [r for r in results if not r['success']]
        
        durations = [r['duration'] for r in successful_requests]
        
        return {
            "total_requests": num_requests,
            "successful_requests": len(successful_requests),
            "failed_requests": len(failed_requests),
            "success_rate": len(successful_requests) / num_requests,
            "total_duration": total_duration,
            "throughput": len(successful_requests) / total_duration,
            "avg_duration": statistics.mean(durations) if durations else 0,
            "median_duration": statistics.median(durations) if durations else 0,
            "p95_duration": statistics.quantiles(durations, n=20)[18] if len(durations) > 20 else 0,
            "p99_duration": statistics.quantiles(durations, n=100)[98] if len(durations) > 100 else 0
        }
    
    def benchmark_configurations(self, configs):
        """Benchmark multiple configurations."""
        results = {}
        
        for config_name, config in configs.items():
            print(f"\nBenchmarking configuration: {config_name}")
            
            # Create detector with this configuration
            test_detector = AsyncSMDetector(
                endpoint=self.detector.endpoint,
                async_config=config
            )
            
            # Run benchmark
            benchmark = AsyncEndpointBenchmark(test_detector, self.test_payloads)
            result = benchmark.benchmark_throughput(num_requests=50)
            results[config_name] = result
            
            print(f"  Throughput: {result['throughput']:.2f} req/sec")
            print(f"  Success rate: {result['success_rate']:.1%}")
            print(f"  Avg duration: {result['avg_duration']:.2f}s")
        
        return results

# Usage example
def run_performance_benchmark():
    """Run comprehensive performance benchmark."""
    
    # Create test configurations
    configs = {
        "high_throughput": high_throughput_config,
        "low_latency": low_latency_config,
        "cost_optimized": cost_optimized_config
    }
    
    # Create test payloads
    test_payloads = [
        BytesIO(json.dumps({"test": f"payload_{i}"}).encode())
        for i in range(10)
    ]
    
    # Create benchmark
    detector = AsyncSMDetector("benchmark-endpoint", async_config=configs["high_throughput"])
    benchmark = AsyncEndpointBenchmark(detector, test_payloads)
    
    # Run configuration comparison
    results = benchmark.benchmark_configurations(configs)
    
    # Display comparison
    print("\n" + "="*80)
    print("CONFIGURATION COMPARISON")
    print("="*80)
    
    for config_name, result in results.items():
        print(f"\n{config_name.upper()}:")
        print(f"  Throughput: {result['throughput']:.2f} requests/second")
        print(f"  Success Rate: {result['success_rate']:.1%}")
        print(f"  Average Duration: {result['avg_duration']:.2f} seconds")
        print(f"  95th Percentile: {result['p95_duration']:.2f} seconds")
        print(f"  99th Percentile: {result['p99_duration']:.2f} seconds")
```

## Cost Optimization

### Cost Analysis

```python
def calculate_processing_costs(
    num_tiles,
    avg_processing_time_seconds,
    endpoint_cost_per_hour,
    s3_requests_cost_per_1000,
    s3_storage_cost_per_gb_month,
    avg_payload_size_mb
):
    """Calculate estimated processing costs."""
    
    # SageMaker endpoint costs
    total_processing_hours = (num_tiles * avg_processing_time_seconds) / 3600
    endpoint_cost = total_processing_hours * endpoint_cost_per_hour
    
    # S3 request costs (upload + download per tile)
    s3_requests = num_tiles * 2  # Upload and download
    s3_request_cost = (s3_requests / 1000) * s3_requests_cost_per_1000
    
    # S3 storage costs (temporary storage)
    total_storage_gb = (num_tiles * avg_payload_size_mb) / 1024
    # Assume data stored for average processing time
    storage_hours = avg_processing_time_seconds / 3600
    s3_storage_cost = total_storage_gb * (storage_hours / (24 * 30)) * s3_storage_cost_per_gb_month
    
    total_cost = endpoint_cost + s3_request_cost + s3_storage_cost
    cost_per_tile = total_cost / num_tiles
    
    return {
        "total_cost": total_cost,
        "cost_per_tile": cost_per_tile,
        "endpoint_cost": endpoint_cost,
        "s3_request_cost": s3_request_cost,
        "s3_storage_cost": s3_storage_cost,
        "breakdown": {
            "endpoint_percentage": (endpoint_cost / total_cost) * 100,
            "s3_request_percentage": (s3_request_cost / total_cost) * 100,
            "s3_storage_percentage": (s3_storage_cost / total_cost) * 100
        }
    }

# Example cost calculation
costs = calculate_processing_costs(
    num_tiles=10000,
    avg_processing_time_seconds=120,  # 2 minutes
    endpoint_cost_per_hour=2.50,      # ml.m5.large
    s3_requests_cost_per_1000=0.0004, # PUT/GET requests
    s3_storage_cost_per_gb_month=0.023, # Standard storage
    avg_payload_size_mb=1.5
)

print(f"Total cost: ${costs['total_cost']:.2f}")
print(f"Cost per tile: ${costs['cost_per_tile']:.4f}")
```

### Cost Optimization Strategies

1. **Optimize Polling Frequency**:
   - Reduce API calls with longer polling intervals
   - Use exponential backoff to minimize unnecessary polls

2. **Batch S3 Operations**:
   - Use delayed cleanup to batch delete operations
   - Optimize payload sizes to reduce request counts

3. **Right-size Endpoint Instances**:
   - Monitor utilization and scale appropriately
   - Use auto-scaling when available

4. **Optimize Worker Configuration**:
   - Balance worker counts to avoid over-provisioning
   - Monitor resource utilization

## Best Practices Summary

1. **Start with Conservative Settings**: Begin with moderate configurations and tune based on observed performance
2. **Monitor Key Metrics**: Track throughput, latency, success rates, and resource usage
3. **Test Different Configurations**: Use benchmarking to compare configuration options
4. **Consider Workload Characteristics**: Tune based on tile sizes, model complexity, and volume
5. **Implement Gradual Scaling**: Increase worker counts and concurrency gradually
6. **Monitor Costs**: Track AWS service costs and optimize based on budget constraints
7. **Use Appropriate Cleanup Policies**: Balance performance, debugging needs, and costs
8. **Implement Health Checks**: Monitor system health and adjust configurations as needed
9. **Document Optimal Settings**: Record successful configurations for different workload types
10. **Regular Performance Reviews**: Periodically review and adjust configurations as workloads evolve