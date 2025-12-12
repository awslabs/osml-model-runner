# Implementation Plan

## Phase 1: Add New Components

- [x] 1. Implement configuration enhancements
- [x] 1.1 Add new environment variables to ServiceConfig
  - Add SCHEDULER_THROTTLING_ENABLED (default "True")
  - Add DEFAULT_INSTANCE_CONCURRENCY (default "2")
  - Add DEFAULT_HTTP_ENDPOINT_CONCURRENCY (default "10")
  - Add TILE_WORKERS_PER_INSTANCE (default "4")
  - Add CAPACITY_TARGET_PERCENTAGE (default "1.0")
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.5_

- [x] 1.2 Add validation logic in ServiceConfig.__post_init__
  - Validate capacity_target_percentage > 0.0, default to 1.0 with warning if invalid
  - Validate default_instance_concurrency >= 1, default to 2 with warning if invalid
  - Validate tile_workers_per_instance >= 1, default to 4 with warning if invalid
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.5_

- [x] 1.3 Write unit tests for configuration validation
  - Test valid capacity_target_percentage values (0.8, 1.0, 1.2) are accepted
  - Test invalid capacity_target_percentage (0.0, -0.5) defaults to 1.0 with warning
  - Test invalid default_instance_concurrency (0, -1) defaults to 2 with warning
  - Test invalid tile_workers_per_instance (0, -1) defaults to 4 with warning
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.5_

- [ ] 2. Implement RegionCalculator interface and implementation
- [x] 2.1 Create RegionCalculator abstract base class
  - Create new file: src/aws/osml/model_runner/tile_worker/region_calculator.py
  - Define RegionCalculator as ABC with @abstractmethod calculate_regions()
  - Add proper type hints (ImageDimensions, ImageRegion, Optional[shapely.geometry.base.BaseGeometry])
  - Add comprehensive docstrings following Sphinx format (no :type: or :rtype:)
  - Document that LoadImageException is raised for inaccessible images
  - _Requirements: 3.1, 3.2, 3.5, 7.1_

- [ ]* 2.2 Write unit tests for RegionCalculator interface
  - Test that RegionCalculator is abstract and cannot be instantiated
  - Test that calculate_regions() must be implemented by subclasses
  - _Requirements: 7.1, 7.4_

- [x] 2.3 Implement ToolkitRegionCalculator class
  - Create new file: src/aws/osml/model_runner/tile_worker/toolkit_region_calculator.py
  - Implement __init__ accepting TilingStrategy and region_size
  - Implement calculate_regions() main method
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 7.1_

- [x] 2.4 Implement ToolkitRegionCalculator helper methods
  - Implement _load_image_and_calculate_bounds() to load GDAL dataset and sensor model
  - Implement _compute_regions() to use TilingStrategy
  - Reuse get_credentials_for_assumed_role for IAM role assumption
  - Reuse GDALConfigEnv for GDAL configuration with credentials
  - Reuse load_gdal_dataset for loading images
  - Reuse calculate_processing_bounds for ROI handling
  - Raise LoadImageException for inaccessible images (fail-fast)
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 7.1_

- [x] 2.5 Update tile_worker/__init__.py to export RegionCalculator
  - Add RegionCalculator to imports
  - Add ToolkitRegionCalculator to imports
  - _Requirements: 7.1_

- [x] 2.6 Write unit tests for ToolkitRegionCalculator
  - Test small image (1024×1024) returns 1 region
  - Test large image (20480×20480) returns 4 regions
  - Test image with ROI returns fewer regions than without ROI
  - Test inaccessible image raises LoadImageException
  - Test IAM role assumption works correctly
  - Test GDAL configuration is set up properly
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ]* 2.7 Write property test for image load calculation
  - Set up Hypothesis testing framework
  - **Property 6: Image load calculation**
  - **Validates: Requirements 3.3**
  - Generate random num_regions (1-100) and workers_per_instance (1-16)
  - Verify estimated_load = num_regions × workers_per_instance
  - Run minimum 100 iterations

- [ ]* 2.8 Write property test for ROI effect on region count
  - **Property 11: ROI affects region count**
  - **Validates: Requirements 3.5**
  - Generate random image dimensions (1024-20480)
  - Generate random ROI ratios (0.1-1.0)
  - Verify region count with ROI <= region count without ROI
  - Run minimum 100 iterations

- [ ] 3. Implement EndpointCapacityEstimator
- [ ] 3.1 Create EndpointCapacityEstimator class skeleton
  - Create new file: src/aws/osml/model_runner/scheduler/endpoint_capacity_estimator.py
  - Implement __init__ accepting sm_client, default_instance_concurrency, default_http_concurrency, cache_ttl_seconds
  - Set up instance variables for configuration and caching
  - Add comprehensive docstrings following Sphinx format
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ] 3.2 Implement EndpointCapacityEstimator.estimate_capacity()
  - Implement main estimate_capacity(endpoint_name, variant_name) method
  - Check if HTTP endpoint using _is_http_endpoint()
  - For HTTP: return default_http_concurrency
  - For SageMaker: call _get_sagemaker_capacity()
  - Add docstring explaining variant_name parameter (None = all variants, specific = that variant only)
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ] 3.3 Implement EndpointCapacityEstimator helper methods
  - Implement _is_http_endpoint() to check for http:// or https:// prefix
  - Implement _get_sagemaker_capacity() to query SageMaker DescribeEndpoint with caching
  - Implement _get_variant_capacity() to calculate capacity for single variant
  - Handle serverless variants (use MaxConcurrency)
  - Handle instance-backed variants (check osml:instance-concurrency tag, multiply by CurrentInstanceCount)
  - Use default_instance_concurrency if tag not present
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ] 3.4 Update scheduler/__init__.py to export EndpointCapacityEstimator
  - Add EndpointCapacityEstimator to imports
  - _Requirements: 2.6_

- [ ]* 3.5 Write unit tests for EndpointCapacityEstimator
  - Test HTTP endpoint (http://example.com) returns DEFAULT_HTTP_ENDPOINT_CONCURRENCY
  - Test HTTPS endpoint (https://example.com) returns DEFAULT_HTTP_ENDPOINT_CONCURRENCY
  - Test serverless endpoint with MaxConcurrency=100 returns 100
  - Test instance-backed endpoint with tag (3 instances × 5 concurrency = 15)
  - Test instance-backed endpoint without tag (2 instances × 2 default = 4)
  - Test multi-variant endpoint with variant_name=None returns sum of all variants
  - Test multi-variant endpoint with variant_name="variant-1" returns only that variant's capacity
  - Test capacity caching reduces SageMaker API calls
  - Test cache expiration after TTL
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ]* 3.6 Write property test for serverless capacity calculation
  - **Property 2: Serverless capacity calculation**
  - **Validates: Requirements 2.1**
  - Generate random MaxConcurrency values (1-10000)
  - Verify calculated capacity equals MaxConcurrency
  - Run minimum 100 iterations

- [ ]* 3.7 Write property test for instance-backed capacity with tag
  - **Property 3: Instance-backed capacity calculation with tag**
  - **Validates: Requirements 2.2**
  - Generate random instance_count (1-100) and instance_concurrency (1-100)
  - Verify calculated capacity = instance_count × instance_concurrency
  - Run minimum 100 iterations

- [ ]* 3.8 Write property test for instance-backed capacity without tag
  - **Property 4: Instance-backed capacity calculation without tag**
  - **Validates: Requirements 2.3**
  - Generate random instance_count (1-100) and default_concurrency (1-10)
  - Verify calculated capacity = instance_count × default_concurrency
  - Run minimum 100 iterations

- [ ]* 3.9 Write property test for HTTP endpoint capacity
  - **Property 5: HTTP endpoint capacity**
  - **Validates: Requirements 2.5**
  - Generate random HTTP URLs and default_http_concurrency (1-100)
  - Verify calculated capacity = default_http_concurrency
  - Run minimum 100 iterations

- [ ] 4. Implement EndpointVariantSelector
- [ ] 4.1 Create EndpointVariantSelector class skeleton
  - Create new file: src/aws/osml/model_runner/scheduler/endpoint_variant_selector.py
  - Implement __init__ accepting sm_client and cache_ttl_seconds
  - Set up instance variables for caching
  - Add comprehensive docstrings following Sphinx format
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 4.2 Implement EndpointVariantSelector.select_variant()
  - Implement main select_variant(image_request) method
  - Check if TargetVariant already set (if so, return unchanged - always honor explicit variant)
  - Check if SageMaker endpoint using _is_sagemaker_endpoint()
  - For HTTP endpoints: return unchanged (no variants)
  - For SageMaker without TargetVariant: select variant and update request
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 4.3 Implement EndpointVariantSelector helper methods
  - Implement _is_sagemaker_endpoint() to check if request uses SageMaker
  - Implement _needs_variant_selection() to check if TargetVariant is not set
  - Implement _get_endpoint_variants() to query SageMaker with caching
  - Implement _select_weighted_variant() using random.choices() with CurrentWeight
  - Extract logic from ImageRequestHandler.set_default_model_endpoint_variant()
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 4.4 Update scheduler/__init__.py to export EndpointVariantSelector
  - Add EndpointVariantSelector to imports
  - _Requirements: 4.1_

- [ ]* 4.5 Write unit tests for EndpointVariantSelector
  - Test single variant endpoint returns that variant
  - Test multi-variant with equal weights (50/50 split) over many selections
  - Test explicit TargetVariant is honored (never overridden)
  - Test HTTP endpoints return request unchanged
  - Test variant caching reduces SageMaker API calls
  - Test cache expiration after TTL
  - Test weighted random selection uses CurrentWeight correctly
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 4.6 Write property test for variant selection distribution
  - **Property 7: Variant selection honors weights**
  - **Validates: Requirements 4.3**
  - Generate random variant weights (0.1-1.0, 2-5 variants)
  - Perform 1000 selections
  - Verify distribution approximates configured weights using chi-squared test
  - Run minimum 100 iterations (each with 1000 selections)

- [ ] 5. Enhance RequestedJobsTable
- [ ] 5.1 Update RequestedJobsTable.add_new_request() signature
  - Modify method signature to include region_count: Optional[int] = None parameter
  - Update ImageRequestStatusRecord.new_from_request() to accept region_count
  - Store region_count in DynamoDB item when provided
  - Update docstring to document new parameter
  - _Requirements: 1.5, 3.3_

- [ ]* 5.2 Write unit tests for RequestedJobsTable enhancements
  - Test add_new_request() with region_count=10 stores value correctly in DDB
  - Test add_new_request() without region_count stores None in DDB
  - Test get_outstanding_requests() returns records with region_count field
  - Test region_count persists across start_next_attempt() calls
  - _Requirements: 1.5, 3.3_

## Phase 2: Integrate with Scheduler

- [ ] 6. Enhance BufferedImageRequestQueue
- [ ] 6.1 Update BufferedImageRequestQueue.__init__() signature
  - Add optional region_calculator: Optional[RegionCalculator] = None parameter
  - Add optional variant_selector: Optional[EndpointVariantSelector] = None parameter
  - Store as instance variables
  - Update docstring to document new parameters
  - _Requirements: 3.1, 3.4, 4.1_

- [ ] 6.2 Enhance BufferedImageRequestQueue._fetch_new_requests() for region calculation
  - After creating valid ImageRequest, check if region_calculator is provided
  - If provided: call region_calculator.calculate_regions() to get regions list
  - Calculate region_count = len(regions)
  - Pass region_count to requested_jobs_table.add_new_request()
  - If LoadImageException raised: move message to DLQ immediately (fail-fast)
  - If region_calculator not provided: pass region_count=None (backward compatible)
  - Log warning when region_calculator is not provided
  - _Requirements: 1.5, 3.1, 3.2, 3.3, 3.4_

- [ ] 6.3 Enhance BufferedImageRequestQueue._fetch_new_requests() for variant selection
  - After creating valid ImageRequest, check if variant_selector is provided
  - If provided: call variant_selector.select_variant() to select variant early
  - Update image_request with selected TargetVariant
  - If variant_selector not provided: leave TargetVariant as-is
  - Always honor explicit TargetVariant (never override)
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 6.4 Write unit tests for BufferedImageRequestQueue region calculation
  - Test _fetch_new_requests() with region_calculator calculates and stores region_count
  - Test _fetch_new_requests() moves inaccessible images to DLQ (fail-fast)
  - Test _fetch_new_requests() without region_calculator stores region_count=None
  - Test _fetch_new_requests() logs warning when region_calculator not provided
  - Test LoadImageException handling moves message to DLQ
  - _Requirements: 1.5, 3.1, 3.2, 3.3, 3.4_

- [ ]* 6.5 Write unit tests for BufferedImageRequestQueue variant selection
  - Test _fetch_new_requests() with variant_selector selects variant early
  - Test _fetch_new_requests() without variant_selector leaves TargetVariant unchanged
  - Test _fetch_new_requests() honors explicit TargetVariant (never overrides)
  - Test _fetch_new_requests() works for HTTP endpoints (no variant selection)
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 7. Enhance EndpointLoadImageScheduler with capacity-based throttling
- [ ] 7.1 Update EndpointLoadImageScheduler.__init__() signature
  - Add optional capacity_estimator: Optional[EndpointCapacityEstimator] = None parameter
  - Add optional variant_selector: Optional[EndpointVariantSelector] = None parameter
  - Add throttling_enabled: bool = True parameter
  - Add capacity_target_percentage: float = 1.0 parameter
  - Store as instance variables
  - Update docstring to document new parameters
  - _Requirements: 1.1, 1.2, 1.3, 4.1, 8.1, 8.2, 8.3, 8.4_

- [ ] 7.2 Implement EndpointLoadImageScheduler._ensure_variant_selected()
  - Check if request.request_payload has TargetVariant set
  - If set: return request unchanged (always honor explicit variant)
  - If not set and variant_selector provided: call variant_selector.select_variant()
  - Update request with selected TargetVariant
  - Return modified request
  - Add comprehensive docstring
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 7.3 Implement EndpointLoadImageScheduler._estimate_image_load()
  - Accept ImageRequestStatusRecord parameter
  - If region_count is not None: return region_count × TILE_WORKERS_PER_INSTANCE
  - If region_count is None: return default estimate (20 × TILE_WORKERS_PER_INSTANCE)
  - Return estimated load as integer
  - Add comprehensive docstring
  - _Requirements: 1.1, 3.3_

- [ ] 7.4 Implement EndpointLoadImageScheduler._calculate_available_capacity()
  - Accept endpoint_name, variant_name, and outstanding_requests parameters
  - Call capacity_estimator.estimate_capacity(endpoint_name, variant_name) to get max capacity
  - Calculate target_capacity = max_capacity × capacity_target_percentage
  - Filter outstanding_requests to same endpoint_id and target_variant
  - For each matching request: calculate estimated_load using _estimate_image_load()
  - Sum all estimated_loads
  - Return target_capacity - sum_of_loads
  - Add comprehensive docstring explaining capacity_target_percentage
  - _Requirements: 1.2, 1.3, 2.1, 2.2, 2.3, 2.4, 2.5, 8.1, 8.2, 8.3, 8.4_

- [ ] 7.5 Implement EndpointLoadImageScheduler._check_capacity_available()
  - Accept request and available_capacity parameters
  - Calculate image_load using _estimate_image_load(request)
  - If available_capacity >= image_load: return True
  - Check if this is the only job for this endpoint (single image exception)
  - If only job: return True (prevents deadlock)
  - Otherwise: return False
  - Add comprehensive docstring explaining single image exception
  - _Requirements: 1.3, 1.4_

- [ ] 7.6 Enhance EndpointLoadImageScheduler.get_next_scheduled_request() with throttling
  - After getting outstanding_requests, check if throttling_enabled
  - If throttling_enabled is False: use existing logic (no capacity checks)
  - If throttling_enabled is True and capacity_estimator provided:
    - For each candidate request: call _ensure_variant_selected()
    - Calculate available_capacity using _calculate_available_capacity()
    - Check capacity using _check_capacity_available()
    - Only proceed with start_next_attempt() if capacity available
    - Log INFO for scheduling decisions with capacity details
    - Log WARN when images are throttled due to insufficient capacity
  - If capacity_estimator not provided: use existing logic
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 5.1, 5.2_

- [ ]* 7.7 Write unit tests for EndpointLoadImageScheduler._ensure_variant_selected()
  - Test explicit TargetVariant is honored (not overridden)
  - Test variant selection when TargetVariant not set
  - Test no variant_selector provided returns request unchanged
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 7.8 Write unit tests for EndpointLoadImageScheduler._estimate_image_load()
  - Test with region_count=10 and TILE_WORKERS=4 returns 40
  - Test with region_count=None returns default (20 × TILE_WORKERS)
  - _Requirements: 1.1, 3.3_

- [ ]* 7.9 Write unit tests for EndpointLoadImageScheduler._calculate_available_capacity()
  - Test with max_capacity=100, target=0.8, current_load=50 returns 30
  - Test with max_capacity=50, target=1.0, current_load=30 returns 20
  - Test with max_capacity=200, target=1.2, current_load=100 returns 140
  - Test filters requests by endpoint and variant correctly
  - _Requirements: 1.2, 1.3, 2.1, 2.2, 2.3, 2.4, 2.5, 8.1, 8.2, 8.3, 8.4_

- [ ]* 7.10 Write unit tests for EndpointLoadImageScheduler._check_capacity_available()
  - Test sufficient capacity returns True
  - Test insufficient capacity returns False
  - Test single image exception returns True when no other jobs
  - _Requirements: 1.3, 1.4_

- [ ]* 7.11 Write unit tests for EndpointLoadImageScheduler.get_next_scheduled_request() throttling
  - Test throttling_enabled=False schedules without capacity checks
  - Test throttling_enabled=True checks capacity before scheduling
  - Test variant selection happens before capacity calculation
  - Test capacity calculation for specific variant (not all variants)
  - Test no capacity_estimator provided uses existing logic
  - Test logging of throttling decisions
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 5.1, 5.2_

- [ ]* 7.12 Write property test for capacity never over-committed
  - **Property 1: Capacity never over-committed**
  - **Validates: Requirements 1.3, 1.5**
  - Generate random max_capacity (1-1000) and reservation_requests (1-50 requests, 1-100 load each)
  - Simulate scheduling with capacity checks
  - Verify sum of successful reservations never exceeds max_capacity
  - Run minimum 100 iterations

- [ ]* 7.13 Write property test for job start atomicity
  - **Property 9: Job start is atomic**
  - **Validates: Requirements 1.5**
  - Generate random num_threads (2-10) and num_jobs (5-20)
  - Simulate concurrent start_next_attempt() calls
  - Verify only one thread succeeds per job due to conditional update
  - Run minimum 100 iterations

- [ ]* 7.14 Write property test for capacity target percentage
  - **Property 12: Capacity target percentage applied correctly**
  - **Validates: Requirements 8.1**
  - Generate random max_capacity (1-1000) and target_percentage (0.1-1.5)
  - Verify target_capacity = max_capacity × target_percentage
  - Run minimum 100 iterations

- [ ]* 7.15 Write property test for throttling respects configuration flag
  - **Property 8: Throttling respects configuration flag**
  - **Validates: Requirements 5.2**
  - Generate random throttling_enabled (True/False), image_load (1-1000), available_capacity (0-100)
  - When throttling_enabled=False, verify image scheduled regardless of capacity
  - When throttling_enabled=True, verify capacity checks enforced
  - Run minimum 100 iterations

- [ ] 8. Refactor ImageRequestHandler to use RegionCalculator
- [ ] 8.1 Update ImageRequestHandler.__init__() to require RegionCalculator
  - Add region_calculator: RegionCalculator parameter (required, not optional)
  - Store as instance variable
  - Update docstring to document new parameter
  - _Requirements: 7.1, 7.2_

- [ ] 8.2 Refactor ImageRequestHandler.load_image_request() to use RegionCalculator
  - Locate existing inline region calculation logic
  - Replace with call to region_calculator.calculate_regions()
  - Pass image_url, tile_size, tile_overlap, roi, and image_read_role
  - Maintain existing return signature: (extension, dataset, sensor_model, regions)
  - Ensure consistent region calculation with BufferedImageRequestQueue
  - _Requirements: 3.1, 3.2, 7.1, 7.2_

- [ ] 8.3 Remove ImageRequestHandler.set_default_model_endpoint_variant()
  - Delete the static method from ImageRequestHandler class
  - Remove call to this method in process_image_request()
  - Variant selection now happens earlier in BufferedImageRequestQueue or EndpointLoadImageScheduler
  - Update any imports or references to this method
  - _Requirements: 4.1, 7.2_

- [ ]* 8.4 Write unit tests for ImageRequestHandler.load_image_request() refactoring
  - Test load_image_request() calls region_calculator.calculate_regions()
  - Test load_image_request() returns same structure as before (extension, dataset, sensor_model, regions)
  - Test load_image_request() passes correct parameters to region_calculator
  - Test load_image_request() handles LoadImageException from region_calculator
  - _Requirements: 3.1, 3.2, 7.1, 7.2_

- [ ]* 8.5 Write unit tests for ImageRequestHandler.process_image_request() refactoring
  - Test process_image_request() works without calling set_default_model_endpoint_variant()
  - Test process_image_request() uses TargetVariant from request (already selected earlier)
  - Test process_image_request() handles images with pre-selected variants
  - _Requirements: 4.1, 7.2_

- [ ] 9. Add monitoring and metrics
- [ ] 9.1 Add CloudWatch metrics to EndpointLoadImageScheduler
  - Use @metric_scope decorator for get_next_scheduled_request()
  - Emit scheduler.images_throttled counter when image is delayed due to capacity
  - Emit scheduler.capacity_utilization gauge showing current utilization percentage
  - Emit scheduler.scheduling_decision_time histogram for decision latency
  - Add endpoint_name as dimension for all metrics
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 9.2 Add CloudWatch metrics to EndpointCapacityEstimator
  - Use @metric_scope decorator for estimate_capacity()
  - Emit scheduler.endpoint_api_errors counter when SageMaker API fails
  - Add endpoint_name as dimension
  - _Requirements: 2.6_

- [ ] 9.3 Add CloudWatch metrics to BufferedImageRequestQueue
  - Use @metric_scope decorator for _fetch_new_requests()
  - Emit scheduler.image_access_errors counter when LoadImageException raised
  - Add endpoint_name as dimension
  - _Requirements: 3.4_

- [ ] 9.4 Add comprehensive logging to EndpointLoadImageScheduler
  - Log INFO when scheduling image with capacity details (available, required, target percentage)
  - Log WARN when image is throttled due to insufficient capacity
  - Log WARN when capacity_estimator is not provided but throttling is enabled
  - Log ERROR for unexpected exceptions during scheduling
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 9.5 Add comprehensive logging to EndpointCapacityEstimator
  - Log DEBUG for capacity calculations with breakdown (instances, concurrency, variants)
  - Log WARN when SageMaker API fails and using cached capacity
  - Log ERROR when all retries fail for SageMaker API
  - _Requirements: 2.6_

- [ ] 9.6 Add comprehensive logging to BufferedImageRequestQueue
  - Log INFO when region calculation succeeds with region count
  - Log WARN when region_calculator is not provided
  - Log ERROR when LoadImageException raised (image inaccessible)
  - Log INFO when moving inaccessible image to DLQ
  - _Requirements: 3.4_

- [ ]* 9.7 Write unit tests for metrics emission
  - Test scheduler.images_throttled counter increments when throttling occurs
  - Test scheduler.capacity_utilization gauge shows correct percentage
  - Test scheduler.scheduling_decision_time histogram records latency
  - Test scheduler.endpoint_api_errors counter increments on API failures
  - Test scheduler.image_access_errors counter increments on LoadImageException
  - Test metrics include correct dimensions (endpoint_name)
  - _Requirements: 1.1, 1.2, 1.3, 2.6, 3.4_

- [ ]* 9.8 Write unit tests for logging
  - Test INFO logs contain capacity details (available, required, target)
  - Test WARN logs for throttled images include reason
  - Test WARN logs for API failures include error details
  - Test ERROR logs for image access failures include image URL
  - Test log levels are appropriate for each scenario
  - _Requirements: 1.1, 1.2, 1.3, 2.6, 3.4_

- [ ] 10. Update dependency wiring in model_runner.py
- [ ] 10.1 Create and wire ToolkitRegionCalculator
  - Import ToolkitRegionCalculator and RegionCalculator
  - Instantiate ToolkitRegionCalculator with existing TilingStrategy and config.region_size
  - Inject into BufferedImageRequestQueue as optional parameter
  - Inject into ImageRequestHandler as required parameter
  - _Requirements: 7.1, 7.3_

- [ ] 10.2 Create and wire EndpointCapacityEstimator
  - Import EndpointCapacityEstimator
  - Instantiate with SageMaker client (boto3.client("sagemaker"))
  - Pass config.default_instance_concurrency
  - Pass config.default_http_endpoint_concurrency
  - Pass cache_ttl_seconds=300
  - Inject into EndpointLoadImageScheduler as optional parameter
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ] 10.3 Create and wire EndpointVariantSelector
  - Import EndpointVariantSelector
  - Instantiate with SageMaker client (boto3.client("sagemaker"))
  - Pass cache_ttl_seconds=300
  - Inject into BufferedImageRequestQueue as optional parameter
  - Inject into EndpointLoadImageScheduler as optional parameter
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 10.4 Update EndpointLoadImageScheduler instantiation
  - Pass capacity_estimator parameter
  - Pass variant_selector parameter
  - Pass throttling_enabled=config.scheduler_throttling_enabled
  - Pass capacity_target_percentage=config.capacity_target_percentage
  - _Requirements: 1.1, 1.2, 1.3, 5.1, 5.2, 8.1, 8.2, 8.3, 8.4, 8.5_

- [ ]* 10.5 Write integration tests for full scheduling flow
  - Test end-to-end scheduling with mocked SageMaker API
  - Test image request flows from SQS → BufferedImageRequestQueue → EndpointLoadImageScheduler
  - Test region calculation happens during buffering
  - Test variant selection happens during buffering or scheduling
  - Test capacity checks prevent over-commitment
  - Test throttling can be disabled via configuration
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 4.1, 4.2, 4.3_

- [ ]* 10.6 Write integration tests for capacity tracking
  - Test capacity tracking across multiple concurrent images
  - Test capacity is reserved when job starts
  - Test capacity is released when job completes
  - Test atomic job start prevents race conditions
  - _Requirements: 1.3, 1.5_

- [ ]* 10.7 Write integration tests for configuration changes
  - Test changing SCHEDULER_THROTTLING_ENABLED applies to new scheduling decisions
  - Test changing CAPACITY_TARGET_PERCENTAGE applies to new scheduling decisions
  - Test changing DEFAULT_INSTANCE_CONCURRENCY affects capacity calculations
  - Test changing TILE_WORKERS_PER_INSTANCE affects load estimates
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.5_

## Phase 3: Remove Old Implementation

- [ ] 11. Remove old throttling implementation
- [ ] 11.1 Remove deprecated configuration variables
  - Remove self_throttling field (SM_SELF_THROTTLING)
  - Remove workers_per_cpu field (WORKERS_PER_CPU)
  - Remove throttling_vcpu_scale_factor field (THROTTLING_SCALE_FACTOR)
  - Remove throttling_retry_timeout field (THROTTLING_RETRY_TIMEOUT)
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 11.2 Remove EndpointStatisticsTable usage from ImageRequestHandler
  - Remove endpoint_statistics_table parameter from ImageRequestHandler.__init__()
  - Remove self_throttling checks in process_image_request()
  - Remove calls to endpoint_statistics_table.upsert_endpoint()
  - Remove endpoint_utils.calculate_max_regions() calls
  - Update docstring to remove references to endpoint statistics
  - _Requirements: 6.5, 6.7_

- [ ] 11.3 Remove EndpointStatisticsTable class file
  - Delete src/aws/osml/model_runner/database/endpoint_statistics_table.py
  - Remove EndpointStatisticsTable import from database/__init__.py
  - Remove any other imports of EndpointStatisticsTable throughout codebase
  - _Requirements: 6.5, 6.7_

- [ ] 11.4 Remove SelfThrottledRegionException
  - Delete SelfThrottledRegionException class from exceptions.py
  - Remove any handling of this exception in region_request_handler.py
  - Remove any imports of this exception
  - _Requirements: 6.6_

- [ ] 11.5 Update model_runner.py to remove old throttling wiring
  - Remove EndpointStatisticsTable instantiation
  - Remove endpoint_statistics_table parameter from ImageRequestHandler
  - Remove ENDPOINT_TABLE environment variable usage
  - _Requirements: 6.5, 6.7_

- [ ]* 11.6 Write tests to verify old throttling is removed
  - Test that SM_SELF_THROTTLING environment variable is not used
  - Test that THROTTLING_SCALE_FACTOR environment variable is not used
  - Test that WORKERS_PER_CPU environment variable is not used
  - Test that THROTTLING_RETRY_TIMEOUT environment variable is not used
  - Test that EndpointStatisticsTable is not instantiated
  - Test that SelfThrottledRegionException is not raised during region processing
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

- [ ] 12. Add monitoring and metrics
- [ ] 12.1 Add CloudWatch metrics to EndpointLoadImageScheduler
  - Use @metric_scope decorator for get_next_scheduled_request()
  - Emit scheduler.images_throttled counter when image is delayed due to capacity
  - Emit scheduler.capacity_utilization gauge showing current utilization percentage
  - Emit scheduler.scheduling_decision_time histogram for decision latency
  - Add endpoint_name as dimension for all metrics
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 12.2 Add CloudWatch metrics to EndpointCapacityEstimator
  - Use @metric_scope decorator for estimate_capacity()
  - Emit scheduler.endpoint_api_errors counter when SageMaker API fails
  - Add endpoint_name as dimension
  - _Requirements: 2.6_

- [ ] 12.3 Add CloudWatch metrics to BufferedImageRequestQueue
  - Use @metric_scope decorator for _fetch_new_requests()
  - Emit scheduler.image_access_errors counter when LoadImageException raised
  - Add endpoint_name as dimension
  - _Requirements: 3.4_

- [ ] 12.4 Add comprehensive logging to EndpointLoadImageScheduler
  - Log INFO when scheduling image with capacity details (available, required, target percentage)
  - Log WARN when image is throttled due to insufficient capacity
  - Log WARN when capacity_estimator is not provided but throttling is enabled
  - Log ERROR for unexpected exceptions during scheduling
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 12.5 Add comprehensive logging to EndpointCapacityEstimator
  - Log DEBUG for capacity calculations with breakdown (instances, concurrency, variants)
  - Log WARN when SageMaker API fails and using cached capacity
  - Log ERROR when all retries fail for SageMaker API
  - _Requirements: 2.6_

- [ ] 12.6 Add comprehensive logging to BufferedImageRequestQueue
  - Log INFO when region calculation succeeds with region count
  - Log WARN when region_calculator is not provided
  - Log ERROR when LoadImageException raised (image inaccessible)
  - Log INFO when moving inaccessible image to DLQ
  - _Requirements: 3.4_

- [ ]* 12.7 Write unit tests for metrics emission
  - Test scheduler.images_throttled counter increments when throttling occurs
  - Test scheduler.capacity_utilization gauge shows correct percentage
  - Test scheduler.scheduling_decision_time histogram records latency
  - Test scheduler.endpoint_api_errors counter increments on API failures
  - Test scheduler.image_access_errors counter increments on LoadImageException
  - Test metrics include correct dimensions (endpoint_name)
  - _Requirements: 1.1, 1.2, 1.3, 2.6, 3.4_

- [ ]* 12.8 Write unit tests for logging
  - Test INFO logs contain capacity details (available, required, target)
  - Test WARN logs for throttled images include reason
  - Test WARN logs for API failures include error details
  - Test ERROR logs for image access failures include image URL
  - Test log levels are appropriate for each scenario
  - _Requirements: 1.1, 1.2, 1.3, 2.6, 3.4_

- [ ] 13. Final checkpoint - Ensure all tests pass
  - Run full test suite with tox
  - Verify all unit tests pass
  - Verify all property-based tests pass (minimum 100 iterations each)
  - Verify integration tests pass
  - Fix any failing tests
  - Ensure code coverage meets project standards
  - Run linting with tox -e lint
  - Fix any linting issues
  - Ensure all tests pass, ask the user if questions arise
  - _Requirements: All_
