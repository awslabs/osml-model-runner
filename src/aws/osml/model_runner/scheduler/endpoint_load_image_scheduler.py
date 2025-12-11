#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import logging
import time
from dataclasses import dataclass
from itertools import groupby
from operator import attrgetter
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.app_config import BotoConfig
from aws.osml.model_runner.database import ImageRequestStatusRecord
from aws.osml.model_runner.scheduler.buffered_image_request_queue import BufferedImageRequestQueue
from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator
from aws.osml.model_runner.scheduler.image_scheduler import ImageScheduler

logger = logging.getLogger(__name__)


@dataclass
class EndpointUtilizationSummary:
    """
    Tracks the load information for a SageMaker endpoint.

    :param endpoint_id: The identifier of the endpoint
    :param instance_count: Number of instances backing the endpoint
    :param current_load: Number of images currently being processed
    :param requests: List of pending requests for this endpoint
    """

    endpoint_id: str
    instance_count: int
    current_load: int
    requests: List[ImageRequestStatusRecord]

    @property
    def load_factor(self) -> float:
        """
        Calculate the load factor for this endpoint which is just the ratio of load per endpoint instance.

        :return: The load factor (current_load / instance_count)
        :rtype: float
        """
        return self.current_load / max(1, self.instance_count)


class EndpointLoadImageScheduler(ImageScheduler):
    """
    This class prioritizes image jobs that will make requests against the least utilized model endpoint.

    It does this by using a buffered request queue that will allow us to look ahead some number of requests
    and then pick the oldest request for the endpoint currently processing the fewest number of regions.

    The scheduler supports optional capacity-based throttling to prevent endpoint overload by checking
    available capacity before starting new image jobs.
    """

    def __init__(
        self,
        image_request_queue: BufferedImageRequestQueue,
        capacity_estimator: Optional[EndpointCapacityEstimator] = None,
        throttling_enabled: bool = True,
        capacity_target_percentage: float = 1.0,
    ):
        """
        Initialize the load based image scheduler.

        :param image_request_queue: A request queue that buffers messages to enable lookahead
        :param capacity_estimator: Optional estimator for calculating endpoint capacity.
                                   When provided with throttling_enabled=True, enables capacity-based
                                   throttling to prevent endpoint overload. If None, scheduler operates
                                   without capacity checks.
        :param throttling_enabled: Whether to enforce capacity-based throttling. When True and
                                  capacity_estimator is provided, the scheduler checks available capacity
                                  before starting new images. When False, images are scheduled without
                                  capacity checks. Default is True.
        :param capacity_target_percentage: Target utilization percentage for capacity planning (0.0-inf).
                                          Values < 1.0 reserve headroom for autoscaling and burst traffic.
                                          Values = 1.0 use full endpoint capacity (default).
                                          Values > 1.0 allow overprovisioning for aggressive scaling.
                                          For example, 0.8 maintains 20% headroom, 1.2 allows 120% utilization.

        Note: Variant selection is NOT needed in the scheduler - it already happened in BufferedImageRequestQueue
        during request buffering. By the time the scheduler sees a request, TargetVariant is already
        set and the scheduler simply uses that variant for capacity calculations.
        """
        self.image_request_queue = image_request_queue
        self.sm_client = boto3.client("sagemaker", config=BotoConfig.default)
        self.capacity_estimator = capacity_estimator
        self.throttling_enabled = throttling_enabled
        self.capacity_target_percentage = capacity_target_percentage

    def get_next_scheduled_request(self) -> Optional[ImageRequest]:
        """
        Get the image request for the endpoint with the lowest load.

        When throttling is enabled and a capacity estimator is configured, this method checks
        available endpoint capacity before starting new images. This prevents endpoint overload
        by ensuring sufficient capacity exists to handle the estimated load of each image.

        The scheduling process:
        1. Retrieve all outstanding requests from the buffered queue
        2. Group requests by endpoint and calculate current utilization
        3. Select the next eligible request based on load balancing
        4. If throttling is enabled:
           - Extract the TargetVariant from the request (already set by BufferedImageRequestQueue)
           - Calculate available capacity for the specific endpoint variant
           - Check if sufficient capacity exists for this image
           - Only start the image if capacity is available or single image exception applies
        5. Atomically start the next attempt via conditional DynamoDB update

        :return: The next image request to process, if any
        """
        # We want a consolidated log message that captures the result of the scheduling run
        # at the end of each cycle.
        schedule_cycle_start_time = time.time()
        schedule_cycle_log_message = None

        try:
            logger.debug("Starting image processing request selection process")
            outstanding_requests = self.image_request_queue.get_outstanding_requests()
            if not outstanding_requests:
                schedule_cycle_log_message = None
                return None
            logger.debug(f"Retrieved {len(outstanding_requests)} image processing requests from the buffered queue")

            # Check if throttling is enabled and capacity estimator is configured
            throttling_active = self.throttling_enabled and self.capacity_estimator is not None

            if throttling_active:
                logger.debug("Capacity-based throttling is enabled. Checking capacity before scheduling.")
            elif self.throttling_enabled and self.capacity_estimator is None:
                logger.warning(
                    "Throttling is enabled but capacity_estimator is not configured. "
                    "Scheduling will proceed without capacity checks. "
                    "To enable capacity-based throttling, provide a capacity_estimator during initialization."
                )
            else:
                logger.debug("Capacity-based throttling is disabled. Scheduling without capacity checks.")

            # Group requests by endpoint and calculate loads
            grouped_requests = self._group_requests_by_endpoint(outstanding_requests)
            endpoint_utilization = self._calculate_endpoint_utilization(grouped_requests)

            # Find next eligible request
            next_request = self._select_next_eligible_request(endpoint_utilization)
            if not next_request:
                logger.debug("No outstanding requests are eligible to start.")
                schedule_cycle_log_message = None
                return None
            logger.debug(f"Selected job {next_request.job_id} requested at {next_request.request_time} for processing.")

            # If throttling is active, check capacity before starting the request
            if throttling_active:
                # Extract TargetVariant from the request (already set by BufferedImageRequestQueue)
                endpoint_name = next_request.endpoint_id
                variant_name = (
                    next_request.request_payload.model_endpoint_parameters.get("TargetVariant")
                    if next_request.request_payload.model_endpoint_parameters
                    else None
                )

                # Calculate available capacity for the specific variant
                # Pass current job_id to exclude it from utilization calculation
                available_capacity = self._calculate_available_capacity(
                    endpoint_name, variant_name, outstanding_requests, current_job_id=next_request.job_id
                )

                # Check if sufficient capacity exists for this image
                capacity_available = self._check_capacity_available(next_request, available_capacity, outstanding_requests)

                if not capacity_available:
                    # Insufficient capacity - throttle this image
                    image_load = self._estimate_image_load(next_request)
                    schedule_cycle_log_message = (
                        f"Throttling job {next_request.job_id} due to insufficient capacity. "
                        f"Endpoint: {endpoint_name}, Variant: {variant_name}, "
                        f"Required load: {image_load} tiles, Available capacity: {available_capacity} tiles. "
                        f"Image will be delayed until capacity becomes available."
                    )
                    return None

                # Capacity is available - log scheduling decision with details
                image_load = self._estimate_image_load(next_request)
                logger.info(
                    f"Scheduling job {next_request.job_id} with sufficient capacity. "
                    f"Endpoint: {endpoint_name}, Variant: {variant_name}, "
                    f"Required load: {image_load} tiles, Available capacity: {available_capacity} tiles, "
                    f"Target percentage: {self.capacity_target_percentage:.1%}"
                )

            # Try to start the next attempt. If the attempt can't be started that usually means the conditional
            # update of the record failed because another worker started the same request. In that case we
            # do not return an image processing request because we want this worker to go check the region
            # queue before starting a new image.
            if self.image_request_queue.requested_jobs_table.start_next_attempt(next_request):
                schedule_cycle_log_message = (
                    f"Started selected job {next_request.job_id}. Attempt # {next_request.num_attempts + 1}"
                )
                return next_request.request_payload

            schedule_cycle_log_message = (
                f"Unable to start selected job {next_request.job_id}. " "Request was likely started by another worker."
            )
            return None

        except Exception as e:
            logger.error(f"Error getting next scheduled request: {e}", exc_info=True)
            schedule_cycle_log_message = f"Error getting next scheduled request: {e}"
            return None
        finally:
            elapsed_ms = (time.time() - schedule_cycle_start_time) * 1000
            if schedule_cycle_log_message:
                logger.info(f"{schedule_cycle_log_message}, elapsed_ms={elapsed_ms:.2f}", extra={"tag": "SCHEDULER EVENT"})

    def finish_request(self, image_request: ImageRequest, should_retry: bool = False) -> None:
        """
        Complete processing of an image request.

        :param image_request: The completed image request
        :param should_retry: Whether the request should be retried
        """
        # Nothing to do here. The requests are fully managed by the buffered queue and do not need manual cleanup.
        # This is just a noop placeholder for the abstract method defined on the base class.
        pass

    def _estimate_image_load(self, request: ImageRequestStatusRecord) -> int:
        """
        Calculate the estimated load for an image request in concurrent tile requests.

        The load is calculated as the number of regions remaining to be processed multiplied
        by the number of tile workers per instance. This represents the maximum number of
        concurrent inference requests the image can generate at a time.

        When region_count is not available (None), a default estimate is used based on typical
        image sizes. This can occur for requests that were added before region calculation was
        implemented or when region calculation is disabled.

        :param request: The image request status record containing region count information
        :return: Estimated load in concurrent tile requests (regions × workers per instance)
        """
        # Import here to avoid circular dependency
        from aws.osml.model_runner.app_config import ServiceConfig

        config = ServiceConfig()

        if request.region_count is not None:
            # Use actual region count minus the number of complete regions when available
            estimated_load = request.region_count * config.tile_workers_per_instance
        else:
            # Use default estimate when region count is not available
            # Default assumes 20 regions per image (typical for large images)
            default_region_count = 20
            estimated_load = default_region_count * config.tile_workers_per_instance

        return estimated_load

    def _get_running_jobs_for_endpoint_variant(
        self,
        endpoint_name: str,
        variant_name: Optional[str],
        outstanding_requests: List[ImageRequestStatusRecord],
        exclude_job_id: Optional[str] = None,
    ) -> List[ImageRequestStatusRecord]:
        """
        Get currently running jobs for a specific endpoint and variant.

        A job is considered "currently running" if it has been started (last_attempt > 0)
        and has not exceeded the retry timeout (last_attempt + retry_time >= current_time).

        :param endpoint_name: Name of the endpoint (SageMaker endpoint name or HTTP URL)
        :param variant_name: Specific variant to filter by. For SageMaker endpoints with
                           multiple variants, this should be the selected variant. For HTTP
                           endpoints, this is typically None.
        :param outstanding_requests: All outstanding requests to filter
        :param exclude_job_id: Optional job ID to exclude from the results. Used to exclude
                              the current job being evaluated from the running jobs list.
        :return: List of currently running jobs for the specified endpoint and variant
        """
        current_time = int(time.time())
        retry_time = self.image_request_queue.retry_time

        return [
            req
            for req in outstanding_requests
            if req.endpoint_id == endpoint_name
            and (
                req.request_payload.model_endpoint_parameters.get("TargetVariant")
                if req.request_payload.model_endpoint_parameters
                else None
            )
            == variant_name
            and req.last_attempt > 0  # Request has been started
            and req.last_attempt + retry_time >= current_time  # Request is currently running
            and req.job_id != exclude_job_id  # Exclude the specified job if provided
        ]

    def _calculate_available_capacity(
        self,
        endpoint_name: str,
        variant_name: Optional[str],
        outstanding_requests: List[ImageRequestStatusRecord],
        current_job_id: Optional[str] = None,
    ) -> int:
        """
        Calculate available capacity for a specific endpoint variant.

        This method determines how much capacity is available for scheduling new images by:
        1. Querying the maximum capacity for the specific endpoint variant
        2. Applying the capacity_target_percentage to determine the target capacity
        3. Calculating current utilization by summing estimated loads for all in-progress jobs
           targeting the same endpoint and variant
        4. Returning the difference: target_capacity - current_utilization

        The capacity_target_percentage allows operators to maintain headroom for autoscaling
        and burst traffic. For example:
        - 0.8 (80%) reserves 20% headroom for other requests or autoscaling triggers
        - 1.0 (100%) uses full endpoint capacity (default)
        - 1.2 (120%) allows overprovisioning for systems with aggressive autoscaling

        Capacity is expressed in units of concurrent inference requests (tiles). Each image's
        load is estimated as region_count × TILE_WORKERS_PER_INSTANCE, representing the maximum
        number of concurrent tile requests the image will generate.

        :param endpoint_name: Name of the endpoint (SageMaker endpoint name or HTTP URL)
        :param variant_name: Specific variant to calculate capacity for. For SageMaker endpoints
                           with multiple variants, this should be the selected variant. For HTTP
                           endpoints, this is typically None.
        :param outstanding_requests: All outstanding requests to filter by endpoint and variant
        :param current_job_id: Optional job ID to exclude from utilization calculation. This prevents
                              counting the current job's capacity against itself when checking if it
                              can be scheduled.
        :return: Available capacity in concurrent inference requests. Returns 0 if capacity_estimator
                is not configured or if current utilization exceeds target capacity.
        :raises: No exceptions raised - returns 0 on errors to fail gracefully
        """
        # If no capacity estimator is configured, we can't calculate capacity
        if self.capacity_estimator is None:
            logger.debug(f"Capacity estimator not configured. Cannot calculate available capacity for {endpoint_name}.")
            return 0

        try:
            # Get maximum capacity for the specific variant
            max_capacity = self.capacity_estimator.estimate_capacity(endpoint_name, variant_name)

            # Apply target percentage to get the target capacity for scheduling
            target_capacity = int(max_capacity * self.capacity_target_percentage)

            # Get currently running jobs for this endpoint and variant
            matching_requests = self._get_running_jobs_for_endpoint_variant(
                endpoint_name, variant_name, outstanding_requests, exclude_job_id=current_job_id
            )

            # Calculate current utilization by summing estimated loads
            current_utilization = sum(self._estimate_image_load(req) for req in matching_requests)

            # Calculate available capacity
            available_capacity = target_capacity - current_utilization

            logger.info(
                f"Capacity calculation for {endpoint_name} (variant={variant_name}): "
                f"max={max_capacity}, target={target_capacity} ({self.capacity_target_percentage:.1%}), "
                f"current={current_utilization}, available={available_capacity}"
            )

            # Return available capacity (minimum of 0 to avoid negative values)
            return max(0, available_capacity)

        except Exception as e:
            logger.error(
                f"Error calculating available capacity for {endpoint_name} (variant={variant_name}): {e}",
                exc_info=True,
            )
            # Return 0 on error to fail gracefully - scheduler will delay the image
            return 0

    def _check_capacity_available(
        self,
        request: ImageRequestStatusRecord,
        available_capacity: int,
        outstanding_requests: List[ImageRequestStatusRecord],
    ) -> bool:
        """
        Check if sufficient capacity exists for this image request.

        This method determines whether an image should be scheduled based on available endpoint
        capacity. It implements a "single image exception" to prevent deadlock scenarios where
        a large image would never be scheduled because its load exceeds total endpoint capacity.

        The single image exception allows an image to be scheduled even if its load exceeds
        available capacity, but ONLY if it would be the only job running on that endpoint.
        This prevents the system from getting stuck when:
        - A single image requires more capacity than the endpoint has
        - The endpoint would otherwise sit idle waiting for capacity that will never be available

        Without this exception, large images could cause deadlock where:
        1. Image requires 100 tiles of capacity
        2. Endpoint has only 80 tiles of capacity
        3. Image is never scheduled because 100 > 80
        4. Endpoint sits idle because no other work can start

        With the exception:
        1. If no other jobs are running on this endpoint, allow the image to start
        2. The endpoint will be overloaded, but the system makes progress
        3. Once the image completes, normal capacity checks resume

        :param request: The image request to check
        :param available_capacity: Available capacity in concurrent inference requests (tiles)
        :param outstanding_requests: All outstanding requests to check for other jobs on this endpoint
        :return: True if the image should be scheduled, False if it should be delayed
        """
        # Calculate the estimated load for this image
        image_load = self._estimate_image_load(request)

        # If sufficient capacity is available, schedule the image
        if available_capacity >= image_load:
            logger.debug(
                f"Sufficient capacity available for job {request.job_id}: "
                f"required={image_load}, available={available_capacity}"
            )
            return True

        # Check for single image exception: is this the only job for this endpoint?
        # Get currently running jobs for the same endpoint and variant
        endpoint_name = request.endpoint_id
        variant_name = (
            request.request_payload.model_endpoint_parameters.get("TargetVariant")
            if request.request_payload.model_endpoint_parameters
            else None
        )

        other_jobs = self._get_running_jobs_for_endpoint_variant(
            endpoint_name, variant_name, outstanding_requests, exclude_job_id=request.job_id
        )

        # If no other jobs are running on this endpoint/variant, allow this image to start
        # This prevents deadlock when a single image exceeds total endpoint capacity
        if not other_jobs:
            logger.info(
                f"Single image exception: Allowing job {request.job_id} to start despite insufficient capacity. "
                f"This is the only job for endpoint {endpoint_name} (variant={variant_name}). "
                f"Required={image_load}, available={available_capacity}. "
                f"Endpoint may be temporarily overloaded."
            )
            return True

        # Insufficient capacity and other jobs are running - delay this image
        logger.debug(
            f"Insufficient capacity for job {request.job_id}: "
            f"required={image_load}, available={available_capacity}. "
            f"{len(other_jobs)} other job(s) running on endpoint {endpoint_name} (variant={variant_name}). "
            f"Image will be delayed."
        )
        return False

    def _is_http_endpoint(self, endpoint_name: str) -> bool:
        """
        Check if the endpoint name is an HTTP endpoint URL.

        :param endpoint_name: The endpoint identifier (name or URL)
        :return: True if this is an HTTP endpoint URL, False otherwise
        """
        return endpoint_name.startswith("http://") or endpoint_name.startswith("https://")

    def _get_endpoint_instance_count(self, endpoint_name: str) -> int:
        """
        Get the number of instances backing a SageMaker endpoint.

        For HTTP endpoints, returns a default value since instance counts
        cannot be determined via SageMaker APIs.

        :param endpoint_name: Name of the SageMaker endpoint or HTTP endpoint URL
        :return: Number of instances backing the endpoint
        """
        # HTTP endpoints are not SageMaker endpoints, so we can't query instance counts
        if self._is_http_endpoint(endpoint_name):
            logger.debug(f"HTTP endpoint detected: {endpoint_name}. Using default instance count of 1.")
            return 1  # Default to 1 instance for HTTP endpoints

        try:
            response = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
            total_instances = 0
            for production_variant in response["ProductionVariants"]:
                total_instances += production_variant.get("CurrentInstanceCount", 1)
            return total_instances
        except ClientError as e:
            logger.error(f"Error describing endpoint {endpoint_name}: {e}")
            return 1  # Default to 1 instance if we can't get the count

    def _group_requests_by_endpoint(
        self, requests: List[ImageRequestStatusRecord]
    ) -> Dict[str, List[ImageRequestStatusRecord]]:
        """
        Group requests by their endpoint ID.

        :param requests: List of request records to group
        :return: Dictionary mapping endpoint IDs to lists of requests
        """
        sorted_requests = sorted(requests, key=attrgetter("endpoint_id"))
        return {endpoint_id: list(group) for endpoint_id, group in groupby(sorted_requests, key=attrgetter("endpoint_id"))}

    def _calculate_endpoint_utilization(
        self, grouped_requests: Dict[str, List[ImageRequestStatusRecord]]
    ) -> List[EndpointUtilizationSummary]:
        """
        Calculate load information for each endpoint.

        The load of an endpoint is estimated by counting up the total number of regions that still need to be
        processed for running requests against that endpoint. This is approximate because there is still some
        variance in size for regions but overall this heuristic recognizes that the whole image sizes will vary
        widely and larger images will place a more substantial load on the endpoints.

        :param grouped_requests: Requests grouped by endpoint ID
        :return: List of endpoint load information
        """
        endpoint_loads = []
        for endpoint_id, requests in grouped_requests.items():
            instance_count = self._get_endpoint_instance_count(endpoint_id)

            current_load = 0
            for r in requests:
                if r.region_count is None:
                    if r.last_attempt > 0:
                        # Attempt has started but we don't have a count of regions yet. Assume it is 1
                        current_load += 1
                else:
                    current_load += r.region_count - len(r.regions_complete)

            logger.debug(f"ENDPOINT UTILIZATION:  {endpoint_id} {instance_count} {len(requests)} {current_load}")
            endpoint_loads.append(
                EndpointUtilizationSummary(
                    endpoint_id=endpoint_id, instance_count=instance_count, current_load=current_load, requests=requests
                )
            )
        return endpoint_loads

    def _select_next_eligible_request(
        self, endpoint_loads: List[EndpointUtilizationSummary]
    ) -> Optional[ImageRequestStatusRecord]:
        """
        Find the next eligible request to process.

        :param endpoint_loads: List of endpoint load information
        :return: The next request to process, if any
        """
        oldest_request = None
        last_load = None
        for endpoint_load in sorted(endpoint_loads, key=lambda x: x.load_factor):
            if last_load is not None and endpoint_load.load_factor > last_load:
                break
            if endpoint_load.requests:
                current_time = time.time()
                visible_requests = [
                    request
                    for request in endpoint_load.requests
                    if request.last_attempt + self.image_request_queue.retry_time < current_time
                ]
                if visible_requests:
                    current_oldest_request = min(visible_requests, key=attrgetter("request_time"))
                    if oldest_request is None or oldest_request.request_time > current_oldest_request.request_time:
                        oldest_request = current_oldest_request
                        last_load = endpoint_load.load_factor

        return oldest_request
