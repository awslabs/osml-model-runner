# OSML Model Runner Load Tests

This directory contains a unified load test suite for OSML Model Runner with a clean, simple interface.

## Overview

Load tests submit multiple images to the Model Runner over a specified time window and monitor their processing status. This is useful for:

- Testing system capacity and throughput
- Validating performance under sustained load
- Identifying bottlenecks and scaling issues
- Measuring processing statistics (GB processed, pixels processed, success rates)

The load tests use [Locust](https://locust.io/) for concurrent load generation, providing scalability and real-time monitoring capabilities.

## Prerequisites

1. **Install dependencies:**

   ```bash
   pip install -r test/load/requirements.txt
   ```

2. **Deployed Model Runner**: Load tests require a deployed OSML Model Runner with an ECS task definition. The tests automatically import environment variables (SQS queues, DynamoDB tables, etc.) from the task definition, ensuring tests run against the actual deployed configuration.

3. **AWS Credentials**: Configure AWS credentials with access to:
   - ECS (to read task definitions)
   - SQS (to send/receive messages)
   - S3 (to read test images and write results)
   - SageMaker (to query endpoint configurations)

4. **S3 Buckets**: You need:
   - A source bucket containing test images (`.ntf`, `.nitf`, `.tif`, `.tiff`, `.png`, `.jpg`, `.jpeg`)
   - A result bucket for storing processing outputs

5. **Task Definition Pattern**: By default, tests look for a task definition containing `"ModelRunnerDataplane"`. To use a different pattern:

   ```bash
   export TASK_DEFINITION_PATTERN="YourPattern"
   ```

## Quick Start

The load tests use Locust for concurrent load generation. You can run them in two modes:

**Headless Mode (CLI):**

```bash
# Run load test with concurrent users
python bin/run-load-tests.py \
  --source-bucket s3://my-source-bucket \
  --result-bucket s3://my-result-bucket \
  --model-name centerpoint \
  --users 5 \
  --spawn-rate 1 \
  --processing-window-min 10 \
  --headless
```

**Interactive Mode (Web UI):**

```bash
# Start Locust with web UI
python bin/run-load-tests.py \
  --source-bucket s3://my-source-bucket \
  --result-bucket s3://my-result-bucket \
  --model-name centerpoint

# Then open http://localhost:8089 in your browser
# Use the web UI to configure users, spawn rate, and start the test
```

**Using Locust Directly:**

```bash
# For advanced options, use Locust directly
locust -f test/load/locustfile.py \
  --headless \
  --users 5 \
  --spawn-rate 1 \
  --source-bucket s3://my-bucket \
  --result-bucket s3://my-results \
  --model-name centerpoint \
  --processing-window-min 10
```

### Using Environment Variables

```bash
export S3_LOAD_TEST_SOURCE_IMAGE_BUCKET=s3://my-source-bucket
export S3_LOAD_TEST_RESULT_BUCKET=s3://my-result-bucket
export SM_LOAD_TEST_MODEL=centerpoint

python bin/run-load-tests.py --users 5 --spawn-rate 1 --headless
```

## Configuration

### Environment Variables

Load tests can be configured via environment variables:

- `S3_LOAD_TEST_SOURCE_IMAGE_BUCKET` or `S3_SOURCE_IMAGE_BUCKET`: S3 bucket containing source images
- `S3_LOAD_TEST_RESULT_BUCKET` or `S3_RESULT_BUCKET`: S3 bucket for storing results
- `SM_LOAD_TEST_MODEL`: SageMaker model name (default: `centerpoint`)
- `PROCESSING_WINDOW_MIN`: Processing window duration in minutes (default: `1`)

### Command-Line Arguments

**Main CLI (`bin/run-load-tests.py`):**

- `--source-bucket`: S3 bucket containing source images (required)
- `--result-bucket`: S3 bucket for storing results (required)
- `--model-name`: SageMaker model name (default: `centerpoint`)
- `--users`: Number of concurrent users (default: `1`)
- `--spawn-rate`: Rate to spawn users per second (default: `1.0`)
- `--processing-window-min`: Processing window duration in minutes (default: `1`)
- `--max-queue-depth`: Maximum queue depth before throttling (default: `3`)
- `--headless`: Run in headless mode (no web UI)
- `--host`: Host URL for Locust (default: `http://localhost`)
- `--web-host`: Web UI host (default: `0.0.0.0`)
- `--web-port`: Web UI port (default: `8089`)
- `--verbose`: Enable verbose logging
- `--output`: Output file prefix for CSV results (requires `--headless`)
- `--stats-interval`: Interval in seconds for displaying statistics (default: `30`)
- `--log-dir`: Directory for writing log files (default: `logs`)
- `--wait-for-completion`: Wait for all jobs to complete after time window expires

**Locust-specific arguments** (when using Locust directly via `locust -f test/load/locustfile.py`):

- `--aws-account`: AWS Account ID (auto-detected if not provided)
- `--aws-region`: AWS Region (auto-detected if not provided)

All standard Locust arguments (`--users`, `--spawn-rate`, `--run-time`, etc.) are also supported when using Locust directly.

## How It Works

The Locust-based load test works as follows:

1. **Image Discovery**: Each Locust user scans the source S3 bucket for image files and creates a list.

2. **Concurrent Load Generation**: Multiple Locust users run concurrently, each submitting image processing requests independently.

3. **Queue Management**: Before submitting each image, users check the SQS queue depth. If the queue depth exceeds the threshold, they wait before submitting more.

4. **Background Status Monitoring**: A singleton background thread monitors the SQS status queue for all job completion messages and maintains a cache of job statuses.

5. **Time Window Control**: A custom load shape (`TimeWindowLoadShape`) ensures the test runs for the specified time window and then stops.

6. **Real-time Metrics**: Locust provides real-time metrics through:
   - **Web UI**: Interactive dashboard showing requests/sec, response times, failures, etc.
   - **Console Output**: Real-time statistics in the terminal
   - **CSV Export**: Detailed metrics exported to CSV files

7. **Job Tracking**: All submitted jobs are tracked with detailed information:
   - Image size and pixel count
   - Start time and processing duration
   - Status updates (STARTED, IN_PROGRESS, SUCCESS, FAILED, PARTIAL)
   - Comprehensive statistics (GB processed, pixels processed, success/failure counts)

8. **File Logging**: Job status and summary statistics are written to JSON files:
   - `logs/job_status.json`: Detailed status of all jobs
   - `logs/job_summary.json`: Summary statistics

9. **Completion**: After the time window expires, Locust stops spawning new users. If `--wait-for-completion` is specified, the test waits for all submitted jobs to complete before finishing.

## File Structure

```text
test/load/
├── locustfile.py          # Main Locust entry point
├── locust_user.py         # Locust User class for submitting requests
├── locust_setup.py        # Locust event handlers and CLI arguments
├── locust_status_monitor.py  # Background status monitoring thread
├── locust_load_shape.py   # Custom load shape for time window control
├── locust_job_tracker.py  # Job tracking and statistics calculation
├── types.py               # Local type definitions (no dependency on model runner)
├── config.py              # Configuration management
├── requirements.txt        # Python dependencies for load tests
├── __init__.py            # Package initialization
└── README.md              # This file
```

## Output

### Web UI (Interactive Mode)

When running without `--headless`, Locust provides a web UI at `http://localhost:8089` with:

- Real-time request statistics (RPS, response times, failures)
- Charts showing request rate and response times over time
- Ability to change user count and spawn rate dynamically
- Downloadable statistics and charts

### Console Output (Headless Mode)

When running with `--headless`, you'll see real-time statistics in the console:

```text
[2024-01-01 10:00:00,000] INFO/locust.main: Starting web interface at http://0.0.0.0:8089
[2024-01-01 10:00:00,000] INFO/locust.main: Starting Locust 2.32.0
 Name                                                          # reqs      # fails  |     Avg     Min     Max  |  Median   req/s
--------------------------------------------------------------------------------------------------------------------------------------------
 Submit Image:image1.tif:centerpoint                              50          0  |     245     120     450  |     230    5.00
 Submit Image:image2.tif:centerpoint                              45          0  |     230     110     420  |     220    4.50
--------------------------------------------------------------------------------------------------------------------------------------------
 Aggregated                                                        95          0  |     238     110     450  |     225    9.50
```

### CSV Export

With `--output` flag, Locust exports detailed statistics to CSV files:

- `{output}_stats.csv`: Request statistics
- `{output}_failures.csv`: Failure details
- `{output}_stats_history.csv`: Time-series statistics

### File Logging

The load test writes detailed job information to JSON files in the `logs/` directory (configurable via `--log-dir`):

**`logs/job_status.json`**: Detailed status of all submitted jobs:

```json
{
  "job_id:image_url": {
    "job_id": "...",
    "image_url": "s3://...",
    "message_id": "...",
    "status": "SUCCESS",
    "completed": true,
    "size": 1048576,
    "pixels": 1048576,
    "start_time": "01/15/2024/10:30:00",
    "processing_duration": 45.2
  }
}
```

**`logs/job_summary.json`**: Summary statistics:

```json
{
  "total_image_sent": 100,
  "total_image_in_progress": 5,
  "total_image_processed": 95,
  "total_image_succeeded": 90,
  "total_image_failed": 5,
  "total_gb_processed": 2.45,
  "total_pixels_processed": 1048576000
}
```

These files are updated periodically during execution (every `--stats-interval` seconds) and written once more at the end.

### Periodic Statistics Display

During test execution, comprehensive statistics are displayed periodically (every `--stats-interval` seconds, default: 30):

```text
            Total Images Sent: 100
            Total Images In-Progress: 5
            Total Images Processed: 95
            Total Images Succeeded: 90
            Total Images Failed: 5
            Total GB Processed: 2.45
            Total Pixels Processed: 1048576000
```

## Benefits

✅ **Concurrent Load Generation**: Locust enables true concurrent load testing with multiple users
✅ **Real-time Monitoring**: Web UI provides live metrics and charts
✅ **Scalable**: Easily scale from 1 to hundreds of concurrent users
✅ **Simple**: Just specify source bucket, result bucket, and model name
✅ **Automatic**: Configuration imported from deployed task definition
✅ **Flexible**: Configurable time windows, user counts, and spawn rates
✅ **Comprehensive**: Tracks detailed statistics (requests/sec, response times, failures, GB processed, pixels processed)
✅ **Job Tracking**: Maintains detailed information about every submitted job (size, pixels, duration, status)
✅ **File Logging**: Writes job status and summary statistics to JSON files
✅ **Independent**: No dependency on the main model runner package
✅ **Background Monitoring**: Shared status monitor tracks all job completions
✅ **Completion Waiting**: Optional wait for all jobs to complete after time window expires

## Dependencies

The load tests have minimal dependencies to ensure they can run independently:

- `boto3` & `botocore`: AWS SDK for interacting with AWS services
- `GDAL`: For reading image metadata (dimensions, pixels)
- `locust`: Load testing framework for concurrent request generation

See `requirements.txt` for specific versions.
