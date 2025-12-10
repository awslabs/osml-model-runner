# Requirements Document

## Introduction

This specification defines capacity-based throttling for the ModelRunner image scheduler to prevent endpoint overload in deployments where multiple ModelRunner instances process images through under-provisioned SageMaker endpoints. The system will proactively throttle image requests at scheduling time based on endpoint capacity rather than reactively during region processing.

## Glossary

- **ModelRunner**: The system that processes overhead imagery by decomposing images into regions and tiles for ML model inference
- **Image Scheduler**: Component added in Q1-2025 that manages when image processing jobs begin
- **SageMaker Endpoint**: AWS service endpoint hosting ML models for inference
- **Region**: Logical subdivision of an image (default 10240x10240 pixels) processed by a single ModelRunner instance
- **Tile**: Smaller subdivision of a region sized for model input (typically 1024x1024 to 4096x4096 pixels)
- **Endpoint Capacity**: Maximum number of concurrent inference requests an endpoint can handle
- **Image Load**: Estimated number of concurrent tile requests an image will generate
- **Variant**: A specific model configuration within a multi-variant endpoint
- **Serverless Endpoint**: SageMaker endpoint with auto-scaling capacity defined by MaxConcurrency
- **Instance-backed Endpoint**: SageMaker endpoint with fixed instance count and per-instance concurrency

## Requirements

### Requirement 1

**User Story:** As a system operator, I want the scheduler to check endpoint capacity before starting image jobs, so that jobs only start when they can be completed without overloading endpoints.

#### Acceptance Criteria

1. WHEN the scheduler evaluates an image for processing THEN the system SHALL calculate the estimated image load in concurrent tile requests
2. WHEN the scheduler evaluates an image for processing THEN the system SHALL query the current endpoint capacity
3. WHEN the estimated image load exceeds available endpoint capacity THEN the system SHALL delay scheduling the image unless the image would be the only one running on the endpoint
4. WHEN sufficient endpoint capacity becomes available THEN the system SHALL schedule waiting images for processing
5. WHEN an image is scheduled for processing THEN the system SHALL reserve the estimated capacity to prevent over-commitment

### Requirement 2

**User Story:** As a system operator, I want endpoint capacity calculated accurately for different endpoint types, so that throttling works correctly across serverless and instance-backed endpoints.

#### Acceptance Criteria

1. WHEN an endpoint has a CurrentServerlessConfig THEN the system SHALL use the MaxConcurrency value as the endpoint capacity
2. WHEN an endpoint is instance-backed and has an "osml:instance-concurrency" tag THEN the system SHALL multiply the tag value by CurrentInstanceCount to calculate capacity
3. WHEN an endpoint is instance-backed without an "osml:instance-concurrency" tag THEN the system SHALL multiply DEFAULT_INSTANCE_CONCURRENCY by CurrentInstanceCount to calculate capacity
4. WHEN an endpoint has multiple variants THEN the system SHALL calculate capacity separately for each variant
5. WHEN an endpoint is not SageMaker (ex. HTTP) THEN the system SHALL use DEFAULT_HTTP_ENDPOINT_CONCURRENCY
6. WHEN calculating capacity THEN the system SHALL express the result in units of concurrent inference requests

### Requirement 3

**User Story:** As a system operator, I want image load estimated based on regions and tile workers, so that capacity reservations accurately reflect actual system behavior.

#### Acceptance Criteria

1. WHEN estimating image load THEN the system SHALL read the image header to determine dimensions
2. WHEN estimating image load THEN the system SHALL use the TilingStrategy to calculate the number of regions
3. WHEN estimating image load THEN the system SHALL multiply the number of regions by TILE_WORKERS_PER_INSTANCE to get maximum concurrent tiles
4. WHEN the image cannot be accessed THEN the system SHALL fail the job immediately during scheduling
5. WHEN a region of interest boundary is specified THEN the system SHALL include it in the region count calculation

### Requirement 4

**User Story:** As a system operator, I want variant selection to happen during scheduling, so that capacity calculations can account for variant-specific configurations.

#### Acceptance Criteria

1. WHEN an image request does not specify a variant THEN the scheduler SHALL select one before capacity evaluation
2. WHEN selecting a variant THEN the system SHALL honor the routing weights configured on the endpoint
3. WHEN selecting a variant THEN the system SHALL use random selection weighted by the variant routing configuration
4. WHEN a variant is selected THEN the system SHALL use that variant's capacity for throttling calculations
5. WHEN a variant is already specified in the request THEN the system SHALL use the specified variant

### Requirement 5

**User Story:** As a system operator, I want to configure throttling behavior through environment variables, so that I can tune the system for different deployment scenarios.

#### Acceptance Criteria

1. WHEN SCHEDULER_THROTTLING_ENABLED is True THEN the system SHALL enforce capacity-based throttling
2. WHEN SCHEDULER_THROTTLING_ENABLED is False THEN the system SHALL schedule images without capacity checks
3. WHEN DEFAULT_INSTANCE_CONCURRENCY is set THEN the system SHALL use this value for instance-backed endpoints without tags
4. WHEN TILE_WORKERS_PER_INSTANCE is set THEN the system SHALL use this value in image load calculations
5. WHEN DEFAULT_HTTP_ENDPOINT_CONCURRENCY is set THEN the system SHALL use this value for HTTP endpoint concurrency
5. WHEN configuration values are updated THEN the system SHALL apply them to new scheduling decisions without restart

### Requirement 6

**User Story:** As a system operator, I want the old region-based throttling removed, so that the system has reduced complexity and no conflicting throttling mechanisms.

#### Acceptance Criteria

1. WHEN the system starts THEN the SM_SELF_THROTTLING configuration SHALL not be recognized
2. WHEN the system starts THEN the THROTTLING_SCALE_FACTOR configuration SHALL not be recognized
3. WHEN the system starts THEN the WORKERS_PER_CPU configuration SHALL not be recognized
4. WHEN the system starts THEN the THROTTLING_RETRY_TIMEOUT configuration SHALL not be recognized
5. WHEN processing regions THEN the system SHALL not check the EndpointStatisticsTable
6. WHEN processing regions THEN the system SHALL not raise SelfThrottledRegionException
7. WHEN the system is deployed THEN the EndpointStatisticsTable DynamoDB table SHALL not be required

### Requirement 7

**User Story:** As a developer, I want clear separation between scheduling logic and processing logic, so that the system is maintainable and testable.

#### Acceptance Criteria

1. WHEN capacity calculations are performed THEN the logic SHALL be encapsulated in dedicated capacity estimation components
2. WHEN the scheduler makes throttling decisions THEN the logic SHALL not be duplicated in region or tile handlers
3. WHEN endpoint metadata is queried THEN the system SHALL cache results to minimize API calls
4. WHEN testing capacity logic THEN the components SHALL be testable independently of the full scheduler
5. WHEN variant selection occurs THEN the logic SHALL be reusable across scheduling and processing contexts

### Requirement 8

**User Story:** As a system operator, I want to configure a target capacity utilization percentage, so that I can maintain headroom for autoscaling and burst traffic without hitting hard endpoint limits.

#### Acceptance Criteria

1. WHEN CAPACITY_TARGET_PERCENTAGE is set THEN the system SHALL multiply the maximum endpoint capacity by this percentage to determine the target capacity for scheduling decisions
2. WHEN CAPACITY_TARGET_PERCENTAGE is less than 1.0 THEN the system SHALL reserve headroom between the target capacity and maximum capacity
3. WHEN CAPACITY_TARGET_PERCENTAGE is 1.0 THEN the system SHALL use the full maximum capacity for scheduling decisions
4. WHEN calculating available capacity THEN the system SHALL use target capacity (max capacity × target percentage) minus current utilization
5. WHEN CAPACITY_TARGET_PERCENTAGE is not set THEN the system SHALL default to 1.0 (100% utilization)
