# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Generate a cost breakdown for a load test window using AWS Cost Explorer.

This is a lightweight replacement for the older `osml-model-runner-test/bin/get_cost_report.py`
implementation, but without external dependencies (pandas/tabulate).

Examples:
  # Daily granularity (recommended)
  python test/load/generate_cost_report.py --start 2026-01-01 --end 2026-01-08

  # Hourly granularity (use timestamps; Cost Explorer availability/latency applies)
  python test/load/generate_cost_report.py --granularity HOURLY \
    --start 2026-01-01T00:00:00Z --end 2026-01-01T06:00:00Z
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import boto3


def _parse_time(value: str) -> datetime:
    """
    Parse either YYYY-MM-DD or an ISO-8601-ish timestamp like YYYY-MM-DDTHH:MM:SSZ.

    :param value: Time string to parse.
    :returns: Parsed datetime in UTC.
    """
    v = value.strip()
    if "T" not in v:
        # Date only
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Timestamp (accept trailing Z)
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_for_ce(dt: datetime, granularity: str) -> str:
    """
    Format a datetime for the Cost Explorer API.

    :param dt: Datetime to format.
    :param granularity: Cost Explorer granularity.
    :returns: Formatted time string.
    """
    if granularity == "HOURLY":
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Cost Explorer commonly expects YYYY-MM-DD for DAILY/MONTHLY
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


@dataclass(frozen=True)
class CostRow:
    service: str
    amount: float
    unit: str


def _iter_cost_and_usage(
    ce,
    start: str,
    end: str,
    granularity: str,
    metrics: List[str],
    group_by: List[Dict[str, str]],
) -> Iterable[dict]:
    """
    Iterate over Cost Explorer results, handling pagination.

    :param ce: Boto3 Cost Explorer client.
    :param start: Start time formatted for Cost Explorer.
    :param end: End time formatted for Cost Explorer.
    :param granularity: Cost Explorer granularity.
    :param metrics: Metrics to request.
    :param group_by: GroupBy configuration.
    :returns: Iterator over ResultsByTime items.
    """
    token: Optional[str] = None
    while True:
        kwargs = {
            "TimePeriod": {"Start": start, "End": end},
            "Granularity": granularity,
            "Metrics": metrics,
            "GroupBy": group_by,
        }
        if token:
            kwargs["NextPageToken"] = token

        resp = ce.get_cost_and_usage(**kwargs)
        for rbt in resp.get("ResultsByTime", []):
            yield rbt

        token = resp.get("NextPageToken")
        if not token:
            break


def get_cost_breakdown(
    *,
    start: datetime,
    end: datetime,
    granularity: str,
    metric: str,
    region: str,
    services_filter: Optional[List[str]] = None,
) -> Tuple[List[CostRow], float, str]:
    """
    Return (rows, total, unit) for the given window.

    :param start: Start time in UTC.
    :param end: End time in UTC.
    :param granularity: Cost Explorer granularity.
    :param metric: Cost metric name.
    :param region: AWS region to use for the Cost Explorer client.
    :param services_filter: Optional list of service names to include.
    :returns: Tuple of `(rows, total, unit)`.
    """
    session = boto3.Session(region_name=region)
    ce = session.client("ce")

    start_str = _format_for_ce(start, granularity)
    end_str = _format_for_ce(end, granularity)

    # Most useful default: per-service breakdown
    group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}]

    totals: Dict[str, Tuple[float, str]] = {}
    for period in _iter_cost_and_usage(
        ce,
        start=start_str,
        end=end_str,
        granularity=granularity,
        metrics=[metric],
        group_by=group_by,
    ):
        for group in period.get("Groups", []):
            service = group["Keys"][0]
            m = group["Metrics"][metric]
            amount = float(m["Amount"])
            unit = m.get("Unit", "USD")
            prev_amount, _prev_unit = totals.get(service, (0.0, unit))
            totals[service] = (prev_amount + amount, unit)

    # Optional whitelist
    if services_filter:
        wanted = {s.lower() for s in services_filter}
        totals = {k: v for k, v in totals.items() if k.lower() in wanted}

    rows = [CostRow(service=k, amount=v[0], unit=v[1]) for k, v in totals.items()]
    rows.sort(key=lambda r: r.amount, reverse=True)

    total = sum(r.amount for r in rows)
    unit = rows[0].unit if rows else "USD"
    return rows, total, unit


def _print_table(rows: List[CostRow], total: float, unit: str) -> None:
    """
    Print a simple table for cost rows.

    :param rows: Cost rows to print.
    :param total: Total cost.
    :param unit: Cost unit (usually USD).
    :returns: None
    """
    if not rows:
        print("No cost data returned for the given window (Cost Explorer can lag).")
        return

    name_width = max(len("Service"), *(len(r.service) for r in rows))
    cost_width = max(len("Cost"), *(len(f"{r.amount:.6f}") for r in rows))

    print(f"{'Service'.ljust(name_width)}  {'Cost'.rjust(cost_width)}  Unit")
    print(f"{'-' * name_width}  {'-' * cost_width}  ----")
    for r in rows:
        print(f"{r.service.ljust(name_width)}  {f'{r.amount:.6f}'.rjust(cost_width)}  {r.unit}")
    print()
    print(f"Total: {total:.6f} {unit}")


def main() -> int:
    """
    CLI entry point.

    :returns: Process exit code.
    """
    parser = argparse.ArgumentParser(description="Generate an AWS Cost Explorer breakdown for a time window.")
    parser.add_argument("--start", help="Start time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument("--end", help="End time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument(
        "--job-summary",
        help="Path to a job_summary.json produced by the load tests (contains start_time/stop_time).",
    )
    parser.add_argument(
        "--granularity",
        choices=["DAILY", "HOURLY"],
        default="DAILY",
        help="Cost Explorer granularity (default: DAILY). HOURLY may have additional latency/limits.",
    )
    parser.add_argument(
        "--metric",
        choices=["UnblendedCost", "BlendedCost", "NetUnblendedCost", "AmortizedCost"],
        default="UnblendedCost",
        help="Cost metric to report (default: UnblendedCost).",
    )
    parser.add_argument(
        "--aws-region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="Region for the Cost Explorer API client (default: AWS_DEFAULT_REGION or us-east-1).",
    )
    parser.add_argument(
        "--service",
        action="append",
        default=None,
        help="Optional service filter (repeatable), e.g. --service 'Amazon SageMaker' --service 'Amazon S3'",
    )

    args = parser.parse_args()

    # Allow using a load-test summary file as input
    if args.job_summary:
        with open(args.job_summary, "r") as f:
            summary = json.load(f)
        start_dt = _parse_time(summary["start_time"])
        end_dt = _parse_time(summary["stop_time"])
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --start and --end, or provide --job-summary")
        start_dt = _parse_time(args.start)
        end_dt = _parse_time(args.end)
    if end_dt <= start_dt:
        raise SystemExit("--end must be after --start")

    rows, total, unit = get_cost_breakdown(
        start=start_dt,
        end=end_dt,
        granularity=args.granularity,
        metric=args.metric,
        region=args.aws_region,
        services_filter=args.service,
    )
    _print_table(rows, total, unit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
