# OSML Model Runner Load Tests (Locust)

This directory contains a self-contained Locust load test suite for OSML Model Runner.

## Quick start

Install dependencies:

```bash
pip install -r test/load/requirements.txt
```

Run Locust with the built-in class picker UI:

```bash
python -m locust -f ./test/load --class-picker \
  --aws-account 409719124294 \
  --test-imagery-location "s3://your-imagery-bucket" \
  --test-results-location "s3://your-output-bucket"
```

Then open the UI at `http://localhost:8089` and pick a **User class** (e.g. `RandomRequestUser`).

## User classes

- **`RandomRequestUser`**: picks random images from `--test-imagery-location` (filters to `.ntf`, `.nitf`, `.tif`, `.tiff`) and random SageMaker endpoints.
- **`PredefinedRequestsUser`**: runs requests from `--request-file` (default: `./test/load/sample-requests.json`).

## Notes

- **Host field in the UI**: these tests do not load test HTTP, but Locust may require a host value. `ModelRunnerUser` sets `host = "http://localhost"` by default.
- **What shows up in the UI**:
  - `Process Image` fires when the job reaches a terminal status (SUCCESS/FAILED/PARTIAL).

## Cost report (AWS Cost Explorer)

After running a load test, you can generate a cost breakdown for a time window:

```bash
python test/load/generate_cost_report.py --start 2026-01-01 --end 2026-01-08
```

Or use the timestamped job summary produced at the end of a load test run (in `test/load/logs/`):

```bash
python test/load/generate_cost_report.py --job-summary test/load/logs/job_summary-YYYYMMDDTHHMMSSZ.json
```

The load tests also write a timestamped job status file (`job_status-YYYYMMDDTHHMMSSZ.json`) alongside the latest `job_status.json`.

You can also request hourly granularity (Cost Explorer latency/limits apply):

```bash
python test/load/generate_cost_report.py --granularity HOURLY \
  --start 2026-01-01T00:00:00Z --end 2026-01-01T06:00:00Z
```
