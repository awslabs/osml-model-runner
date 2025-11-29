#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Generate cost report for load test runs using AWS Cost Explorer.

This script reads a load test summary JSON file (job_summary.json or log_summary.json)
produced by the load test and generates a cost report using AWS Cost Explorer API for the time period
covered by the load test.

Requirements:
- AWS credentials configured with Cost Explorer API access
- IAM permissions: ce:GetCostAndUsage
- Note: Cost Explorer API is only available in us-east-1 region
- Note: Cost data may have a 24-48 hour delay before being available
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CostReportGenerator:
    """Generate cost reports from load test summaries using AWS Cost Explorer."""

    # Common AWS services used by the model runner
    RELEVANT_SERVICES = [
        "Amazon SageMaker",
        "Amazon Elastic Compute Cloud - Compute",
        "Amazon Simple Storage Service",
        "Amazon Elastic Container Service",
        "Amazon Simple Queue Service",
        "AWS Lambda",
        "Amazon CloudWatch",
        "Amazon Elastic Container Registry",
    ]

    def __init__(self, region: Optional[str] = None):
        """
        Initialize the cost report generator.

        :param region: AWS region (defaults to us-east-1 for Cost Explorer)
        """
        # Cost Explorer API is only available in us-east-1
        self.ce_client = boto3.client("ce", region_name="us-east-1")
        self.region = region

    def load_summary_file(self, file_path: str) -> Dict:
        """
        Load and parse the summary JSON file.

        :param file_path: Path to the summary JSON file
        :return: Parsed JSON dictionary
        :raises FileNotFoundError: If file doesn't exist
        :raises json.JSONDecodeError: If file is not valid JSON
        """
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            logger.info(f"Loaded summary file: {file_path}")
            return data
        except FileNotFoundError:
            logger.error(f"Summary file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in summary file: {e}")
            raise

    def parse_time_range(self, summary_data: Dict) -> tuple[datetime, datetime]:
        """
        Extract start and stop times from summary data.

        :param summary_data: Summary data dictionary
        :return: Tuple of (start_time, stop_time) as datetime objects
        :raises ValueError: If required time fields are missing
        """
        start_time_str = summary_data.get("start_time")
        stop_time_str = summary_data.get("stop_time")

        if not start_time_str:
            raise ValueError("start_time not found in summary data")
        if not stop_time_str:
            raise ValueError("stop_time not found in summary data")

        try:
            # Parse ISO format datetime strings
            start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            stop_time = datetime.fromisoformat(stop_time_str.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"Failed to parse datetime: {e}")
            raise ValueError(f"Invalid datetime format: {e}") from e

        # Cost Explorer requires dates in YYYY-MM-DD format
        logger.info(f"Time range: {start_time} to {stop_time}")
        return start_time, stop_time

    def get_cost_and_usage(
        self,
        start_date: datetime,
        end_date: datetime,
        granularity: str = "HOURLY",
        group_by: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Query AWS Cost Explorer for cost and usage data.

        :param start_date: Start date for the query
        :param end_date: End date for the query
        :param granularity: Granularity of the data (DAILY, MONTHLY, HOURLY)
        :param group_by: Optional list of group_by dimensions
        :return: Cost Explorer API response
        """
        # Cost Explorer API requires different formats based on granularity:
        # - HOURLY: yyyy-MM-ddThh:mm:ssZ (ISO 8601 with timezone in UTC)
        # - DAILY/MONTHLY: YYYY-MM-DD
        if granularity == "HOURLY":
            # For HOURLY, we need full datetime with timezone in UTC
            # Convert to UTC if timezone-aware, otherwise assume UTC
            if start_date.tzinfo is None:
                start_date_utc = start_date.replace(tzinfo=timezone.utc)
            else:
                start_date_utc = start_date.astimezone(timezone.utc)

            if end_date.tzinfo is None:
                end_date_utc = end_date.replace(tzinfo=timezone.utc)
            else:
                end_date_utc = end_date.astimezone(timezone.utc)

            start_date_str = start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            # Add a small buffer to end_date to ensure we capture the full hour
            end_date_str = (end_date_utc + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            # For DAILY/MONTHLY, use date format
            start_date_str = start_date.strftime("%Y-%m-%d")
            # Add one day to end_date to include the full day
            end_date_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

        params = {
            "TimePeriod": {"Start": start_date_str, "End": end_date_str},
            "Granularity": granularity,
            "Metrics": ["BlendedCost", "UnblendedCost", "UsageQuantity"],
        }

        if group_by:
            params["GroupBy"] = group_by

        try:
            logger.info(f"Querying Cost Explorer from {start_date_str} to {end_date_str}")
            response = self.ce_client.get_cost_and_usage(**params)
            return response
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_message = e.response.get("Error", {}).get("Message", str(e))
            if error_code == "AccessDeniedException":
                logger.error(
                    "Access denied to Cost Explorer API. " "Ensure your IAM role/user has 'ce:GetCostAndUsage' permission."
                )
            elif error_code == "DataUnavailableException":
                logger.error(
                    f"Cost data unavailable for the specified time period. "
                    f"Cost Explorer data may have a 24-48 hour delay. Error: {error_message}"
                )
            else:
                logger.error(f"Error querying Cost Explorer ({error_code}): {error_message}")
            raise

    def get_cost_by_service(self, start_date: datetime, end_date: datetime) -> Dict[str, float]:
        """
        Get costs grouped by AWS service.

        :param start_date: Start date for the query
        :param end_date: End date for the query
        :return: Dictionary mapping service names to costs
        """
        group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}]
        response = self.get_cost_and_usage(start_date, end_date, group_by=group_by)

        costs_by_service = {}
        for result in response.get("ResultsByTime", []):
            for group in result.get("Groups", []):
                service_name = group.get("Keys", [""])[0]
                cost = float(group.get("Metrics", {}).get("BlendedCost", {}).get("Amount", "0"))
                if service_name:
                    costs_by_service[service_name] = costs_by_service.get(service_name, 0) + cost

        return costs_by_service

    def get_total_cost(self, start_date: datetime, end_date: datetime) -> float:
        """
        Get total cost for the time period.

        :param start_date: Start date for the query
        :param end_date: End date for the query
        :return: Total cost as a float
        """
        response = self.get_cost_and_usage(start_date, end_date)
        total_cost = 0.0

        for result in response.get("ResultsByTime", []):
            cost = float(result.get("Total", {}).get("BlendedCost", {}).get("Amount", "0"))
            total_cost += cost

        return total_cost

    def filter_relevant_services(self, costs_by_service: Dict[str, float]) -> Dict[str, float]:
        """
        Filter costs to only include relevant services.

        :param costs_by_service: Dictionary of all service costs
        :return: Filtered dictionary with only relevant services
        """
        relevant_costs = {}
        other_costs = 0.0

        for service, cost in costs_by_service.items():
            if any(relevant in service for relevant in self.RELEVANT_SERVICES):
                relevant_costs[service] = cost
            else:
                other_costs += cost

        if other_costs > 0.01:  # Only include if significant (> $0.01)
            relevant_costs["Other Services"] = other_costs

        return relevant_costs

    def generate_report(
        self,
        summary_data: Dict,
        output_file: Optional[str] = None,
    ) -> Dict:
        """
        Generate a comprehensive cost report.

        :param summary_data: Summary data from load test
        :param output_file: Optional path to write JSON report
        :return: Dictionary containing the cost report
        """
        # Parse time range
        start_time, stop_time = self.parse_time_range(summary_data)

        # Get costs
        logger.info("Fetching cost data from AWS Cost Explorer...")
        total_cost = self.get_total_cost(start_time, stop_time)
        costs_by_service = self.get_cost_by_service(start_time, stop_time)

        # Calculate duration
        duration = stop_time - start_time
        duration_hours = duration.total_seconds() / 3600

        # Calculate pixel metrics (1 gigapixel = 10^9 pixels)
        total_pixels = summary_data.get("total_pixels_processed", 0)
        gigapixels = total_pixels / 1e9 if total_pixels > 0 else 0

        # Build report
        report = {
            "load_test_summary": {
                "start_time": summary_data.get("start_time"),
                "stop_time": summary_data.get("stop_time"),
                "duration_hours": round(duration_hours, 2),
                "total_images_sent": summary_data.get("total_image_sent", 0),
                "total_images_processed": summary_data.get("total_image_processed", 0),
                "total_images_succeeded": summary_data.get("total_image_succeeded", 0),
                "total_gb_processed": summary_data.get("total_gb_processed", 0),
                "total_pixels_processed": total_pixels,
                "gigapixels_processed": round(gigapixels, 3),
            },
            "cost_summary": {
                "total_cost_usd": round(total_cost, 2),
                "cost_per_hour": round(total_cost / duration_hours if duration_hours > 0 else 0, 2),
                "cost_per_image": round(
                    (
                        total_cost / summary_data.get("total_image_processed", 1)
                        if summary_data.get("total_image_processed", 0) > 0
                        else 0
                    ),
                    4,
                ),
                "cost_per_gb": round(
                    (
                        total_cost / summary_data.get("total_gb_processed", 1)
                        if summary_data.get("total_gb_processed", 0) > 0
                        else 0
                    ),
                    2,
                ),
                "cost_per_gigapixel": round(
                    total_cost / gigapixels if gigapixels > 0 else 0,
                    4,
                ),
            },
            "costs_by_service": {
                service: round(cost, 2)
                for service, cost in sorted(costs_by_service.items(), key=lambda x: x[1], reverse=True)
            },
            "time_period": {
                "start": start_time.isoformat(),
                "end": stop_time.isoformat(),
            },
        }

        # Write to file if specified
        if output_file:
            with open(output_file, "w") as f:
                json.dump(report, f, indent=2)
            logger.info(f"Cost report written to: {output_file}")

        return report

    def print_report(self, report: Dict) -> None:
        """
        Print a formatted cost report to stdout.

        :param report: Cost report dictionary
        """
        summary = report["load_test_summary"]
        cost_summary = report["cost_summary"]
        costs_by_service = report["costs_by_service"]

        print("\n" + "=" * 80)
        print("LOAD TEST COST REPORT")
        print("=" * 80)
        print("\nTest Period:")
        print(f"  Start Time: {summary['start_time']}")
        print(f"  Stop Time:  {summary['stop_time']}")
        print(f"  Duration:   {summary['duration_hours']:.2f} hours")
        print("\nTest Metrics:")
        print(f"  {'Images Sent:':<22} {summary['total_images_sent']}")
        print(f"  {'Images Processed:':<22} {summary['total_images_processed']}")
        print(f"  {'Images Succeeded:':<22} {summary['total_images_succeeded']}")
        print(f"  {'GB Processed:':<22} {summary['total_gb_processed']:.2f}")
        if "gigapixels_processed" in summary:
            print(f"  {'Gigapixels Processed:':<22} {summary['gigapixels_processed']:.3f}")
        print("\nCost Summary:")
        print(f"  {'Total Cost:':<22} ${cost_summary['total_cost_usd']:.2f}")
        print(f"  {'Cost per Hour:':<22} ${cost_summary['cost_per_hour']:.2f}")
        print(f"  {'Cost per Image:':<22} ${cost_summary['cost_per_image']:.4f}")
        print(f"  {'Cost per GB:':<22} ${cost_summary['cost_per_gb']:.2f}")
        if "cost_per_gigapixel" in cost_summary:
            print(f"  {'Cost per Gigapixel:':<22} ${cost_summary['cost_per_gigapixel']:.4f}")
        print("\nCosts by Service:")
        for service, cost in costs_by_service.items():
            percentage = (cost / cost_summary["total_cost_usd"] * 100) if cost_summary["total_cost_usd"] > 0 else 0
            # Skip services with 0% cost
            if percentage > 0 or cost > 0.01:
                print(f"  {service:<45} ${cost:.2f} ({percentage:5.1f}%)")
        print("=" * 80 + "\n")


def main():
    """Main entry point for the cost report generator."""
    parser = argparse.ArgumentParser(
        description="Generate cost report for load test runs using AWS Cost Explorer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate cost report from job_summary.json
  python generate_cost_report.py logs/job_summary.json

  # Save report to JSON file
  python generate_cost_report.py logs/job_summary.json --output cost_report.json

  # Use log_summary.json instead
  python generate_cost_report.py logs/log_summary.json
        """,
    )
    parser.add_argument(
        "summary_file",
        type=str,
        help="Path to load test summary JSON file (job_summary.json or log_summary.json)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional path to write JSON cost report",
    )
    parser.add_argument(
        "--region",
        type=str,
        help="AWS region (Cost Explorer API uses us-east-1 regardless)",
    )

    args = parser.parse_args()

    try:
        generator = CostReportGenerator(region=args.region)
        summary_data = generator.load_summary_file(args.summary_file)
        report = generator.generate_report(
            summary_data,
            output_file=args.output,
        )
        generator.print_report(report)
        return 0
    except Exception as e:
        logger.error(f"Failed to generate cost report: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
