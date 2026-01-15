# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Job tracking and summary output for Locust load tests.

This is intentionally lightweight: it tracks submitted Model Runner jobs (by job_id)
and writes `logs/job_status.json` and `logs/job_summary-<run_id>.json` on test stop.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, Optional


@dataclass
class JobRecord:
    """
    In-memory record for a single job submission.
    """

    job_id: str
    image_url: str
    submitted_at: str  # ISO-8601 UTC
    status: str = "SUBMITTED"
    completed_at: Optional[str] = None  # ISO-8601 UTC
    processing_duration_s: Optional[float] = None


@dataclass
class JobSummary:
    """
    Summary metadata for a single load test run.
    """

    start_time: str  # ISO-8601 UTC
    stop_time: str  # ISO-8601 UTC
    aws_account: str
    aws_region: str
    mr_input_queue: str
    mr_status_queue: str
    test_imagery_location: str
    test_results_location: str
    total_submitted: int
    total_completed: int
    total_success: int
    total_failed: int
    total_partial: int
    total_other_terminal: int


class JobTracker:
    """
    Thread-safe job tracker for a Locust run.

    This tracks job submissions by `job_id` and writes JSON artifacts on test stop.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._jobs: Dict[str, JobRecord] = {}
        self._start_time: Optional[datetime] = None
        self._stop_time: Optional[datetime] = None
        self._run_id: Optional[str] = None
        self._written: bool = False

    def mark_start(self) -> None:
        """
        Mark the start of a load test window.

        :returns: None
        """
        with self._lock:
            # Only initialize once per run.
            if self._start_time is None:
                self._start_time = datetime.now(timezone.utc)
                self._run_id = self._start_time.strftime("%Y%m%dT%H%M%SZ")
            self._stop_time = None
            self._written = False

    def mark_stop(self) -> None:
        """
        Mark the stop of a load test window.

        :returns: None
        """
        with self._lock:
            if self._stop_time is None:
                self._stop_time = datetime.now(timezone.utc)

    def register_job(self, job_id: str, image_url: str) -> None:
        """
        Register a new job submission.

        This is idempotent: if the job is already registered, the call is ignored.

        :param job_id: Job identifier.
        :param image_url: Image URL associated with the job.
        :returns: None
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            # Idempotent: keep first submission timestamp.
            if job_id in self._jobs:
                return
            self._jobs[job_id] = JobRecord(job_id=job_id, image_url=image_url, submitted_at=now)

    def complete_job(self, job_id: str, status: str, processing_duration_s: Optional[float] = None) -> None:
        """
        Mark a job as completed and record its terminal status.

        If the job wasn't previously registered, a minimal record is created.

        :param job_id: Job identifier.
        :param status: Terminal (or final observed) status string.
        :param processing_duration_s: Optional job processing duration.
        :returns: None
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                # If we somehow missed register, create a minimal record.
                rec = JobRecord(job_id=job_id, image_url="", submitted_at=now)
                self._jobs[job_id] = rec
            rec.status = status
            rec.completed_at = now
            if processing_duration_s is not None:
                rec.processing_duration_s = float(processing_duration_s)

    def snapshot(self) -> Dict[str, JobRecord]:
        """
        Return a snapshot copy of all job records.

        :returns: Mapping of job_id to `JobRecord`.
        """
        with self._lock:
            return dict(self._jobs)

    def build_summary(self, environment) -> JobSummary:
        """
        Build a run-level summary from tracked jobs and Locust CLI options.

        :param environment: Locust `Environment` instance.
        :returns: `JobSummary` for the run window.
        """
        with self._lock:
            start = self._start_time or datetime.now(timezone.utc)
            stop = self._stop_time or datetime.now(timezone.utc)

            jobs = list(self._jobs.values())
            terminal = [j for j in jobs if j.completed_at is not None]

            def _count(status: str) -> int:
                return sum(1 for j in terminal if j.status == status)

            total_success = _count("SUCCESS")
            total_failed = _count("FAILED")
            total_partial = _count("PARTIAL")
            total_other_terminal = len(terminal) - (total_success + total_failed + total_partial)

            opts = environment.parsed_options
            return JobSummary(
                start_time=start.isoformat(),
                stop_time=stop.isoformat(),
                aws_account=getattr(opts, "aws_account", ""),
                aws_region=getattr(opts, "aws_region", ""),
                mr_input_queue=getattr(opts, "mr_input_queue", ""),
                mr_status_queue=getattr(opts, "mr_status_queue", ""),
                test_imagery_location=getattr(opts, "test_imagery_location", ""),
                test_results_location=getattr(opts, "test_results_location", ""),
                total_submitted=len(jobs),
                total_completed=len(terminal),
                total_success=total_success,
                total_failed=total_failed,
                total_partial=total_partial,
                total_other_terminal=total_other_terminal,
            )

    def write_outputs(self, environment, output_dir: Optional[str] = None) -> None:
        """
        Write job tracking artifacts to disk.

        Defaults to `test/load/logs/` (relative to this file) so the artifacts live
        alongside the load test implementation regardless of current working directory.

        :param environment: Locust `Environment` instance.
        :param output_dir: Optional directory path to write outputs.
        :returns: None
        """
        if output_dir is None:
            output_dir_path = Path(__file__).parent / "logs"
        else:
            output_dir_path = Path(output_dir)

        output_dir_path.mkdir(parents=True, exist_ok=True)

        with self._lock:
            # Make writing idempotent: Locust may fire test_stop more than once.
            if self._written:
                return
            self._written = True
            run_id = self._run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        status_payload = {jid: asdict(rec) for jid, rec in self.snapshot().items()}

        # job_status.json (latest)
        status_path = str(output_dir_path / "job_status.json")
        with open(status_path, "w") as f:
            json.dump(status_payload, f, indent=2)

        # job_status-<run_id>.json (timestamped, matches job_summary naming)
        status_ts_path = str(output_dir_path / f"job_status-{run_id}.json")
        with open(status_ts_path, "w") as f:
            json.dump(status_payload, f, indent=2)

        # job_summary.json
        summary = self.build_summary(environment)
        summary_payload = asdict(summary)

        # Write a single timestamped summary for this run.
        summary_ts_path = str(output_dir_path / f"job_summary-{run_id}.json")
        with open(summary_ts_path, "w") as f:
            json.dump(summary_payload, f, indent=2)


_shared_tracker: Optional[JobTracker] = None
_shared_lock = Lock()


def get_job_tracker() -> JobTracker:
    """
    Get the shared `JobTracker` instance.

    :returns: Singleton `JobTracker`.
    """
    global _shared_tracker
    with _shared_lock:
        if _shared_tracker is None:
            _shared_tracker = JobTracker()
        return _shared_tracker
