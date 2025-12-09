# OSML Model Runner Load Tests

This directory contains a unified load test suite for OSML Model Runner with a clean, simple interface.

## Overview

Load tests submit multiple images to the Model Runner over a specified time window and monitor their processing status. This is useful for:

- Testing system capacity and throughput
- Validating performance under sustained load
- Identifying bottlenecks and scaling issues
- Measuring processing statistics (GB processed, pixels processed, success rates)

The load tests use [Locust](https://locust.io/) for concurrent load generation, providing scalability and real-time monitoring capabilities.

### User Classes

The load test suite provides multiple user classes for different testing scenarios:

1. **`ModelRunnerLoadTestUser`** (default): Submits image processing jobs without waiting for completion. Best for high-throughput load testing where you want to stress the system.

2. **`PredefinedRequestsUser`**: Reads requests from a JSON file and executes them sequentially, waiting for each job to complete. Useful for testing specific request patterns.

3. **`RandomRequestUser`**: Selects random images and SageMaker endpoints, waiting for each job to complete. Useful for testing with varied configurations.

4. **`ModelRunnerUser`**: Base class that provides synchronous processing (waits for completion). Used by `PredefinedRequestsUser` and `RandomRequestUser`.

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
# ModelRunnerLoadTestUser (default - submits jobs without waiting)
locust -f test/load/locustfile.py \
  --headless \
  --users 5 \
  --spawn-rate 1 \
  --source-bucket s3://my-bucket \
  --result-bucket s3://my-results \
  --model-name centerpoint \
  --processing-window-min 10

# PredefinedRequestsUser (reads from JSON file, waits for completion)
locust -f test/load/locustfile.py \
  --headless \
  --users 5 \
  --spawn-rate 1 \
  --user-class test.load.predefined_requests_user.PredefinedRequestsUser \
  --test-imagery-location s3://my-images \
  --test-results-location s3://my-results \
  --request-file ./sample-requests.json

# RandomRequestUser (random images/endpoints, waits for completion)
locust -f test/load/locustfile.py \
  --headless \
  --users 5 \
  --spawn-rate 1 \
  --user-class test.load.random_requests_user.RandomRequestUser \
  --test-imagery-location s3://my-images \
  --test-results-location s3://my-results
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
- `--user-class`: User class to use (default: `test.load.locust_user.ModelRunnerLoadTestUser`)
  - `test.load.locust_user.ModelRunnerLoadTestUser`: Submit jobs without waiting (default)
  - `test.load.predefined_requests_user.PredefinedRequestsUser`: Read from JSON file, wait for completion
  - `test.load.random_requests_user.RandomRequestUser`: Random images/endpoints, wait for completion
- `--test-imagery-location`: S3 location of test images (for PredefinedRequestsUser and RandomRequestUser)
- `--test-results-location`: S3 location for results (for PredefinedRequestsUser and RandomRequestUser)
- `--request-file`: Path to JSON file with predefined requests (for PredefinedRequestsUser)
- `--mr-input-queue`: Name of ModelRunner input queue (overrides config)

All standard Locust arguments (`--users`, `--spawn-rate`, `--run-time`, etc.) are also supported when using Locust directly.

## How It Works

The Locust-based load test works as follows:

1. **Image Discovery**: Each Locust user scans the source S3 bucket for image files and creates a list.

2. **Concurrent Load Generation**: Multiple Locust users run concurrently, each submitting image processing requests independently.

3. **Load Generation**: Multiple Locust users run concurrently, each submitting image processing requests independently. The load test does not throttle based on queue depth, allowing you to stress the system to its limits.

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
├── locustfile.py              # Main Locust entry point
├── locust_user.py             # ModelRunnerLoadTestUser - submits jobs without waiting
├── model_runner_user.py       # ModelRunnerUser base class - waits for completion
├── predefined_requests_user.py # PredefinedRequestsUser - reads from JSON file
├── random_requests_user.py    # RandomRequestUser - random images/endpoints
├── locust_setup.py            # Locust event handlers and CLI arguments
├── locust_status_monitor.py   # Background status monitoring thread
├── locust_load_shape.py       # Custom load shape for time window control
├── locust_job_tracker.py      # Job tracking and statistics calculation
├── generate_cost_report.py    # Cost report generator using AWS Cost Explorer
├── _test_utils.py             # Utility functions (S3 path parsing, etc.)
├── types.py                   # Local type definitions (no dependency on model runner)
├── config.py                  # Configuration management
├── requirements.txt           # Python dependencies for load tests
├── __init__.py                # Package initialization
└── README.md                  # This file
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

## Cost Reporting

After running a load test, you can generate a cost report using AWS Cost Explorer to analyze the AWS costs incurred during the test period.

### Requirements

- AWS credentials configured with Cost Explorer API access
- IAM permissions: `ce:GetCostAndUsage`
- **Note**: Cost Explorer API is only available in `us-east-1` region
- **Note**: Cost data may have a 24-48 hour delay before being available

### Usage

The `generate_cost_report.py` script reads a load test summary JSON file (typically `logs/job_summary.json`) and generates a comprehensive cost report:

```bash
# Generate cost report from job_summary.json
python test/load/generate_cost_report.py logs/job_summary.json

# Save report to JSON file
python test/load/generate_cost_report.py logs/job_summary.json --output cost_report.json
```

### Report Contents

The cost report includes:

- **Load Test Summary**: Test duration, images processed, GB processed, pixels processed
- **Cost Summary**:
  - Total cost (USD)
  - Cost per hour
  - Cost per image
  - Cost per GB
  - Cost per gigapixel
- **Costs by Service**: Breakdown of costs by AWS service (SageMaker, EC2, S3, ECS, SQS, Lambda, CloudWatch, ECR, etc.)

The script extracts the time range from the summary file (`start_time` and `stop_time` fields) and queries AWS Cost Explorer for costs during that period. The report is displayed in a formatted table and can optionally be saved to a JSON file for further analysis.

**Note**: The cost report captures costs for all AWS services running in the account during the test period, not just services provisioned by Model Runner. For accurate cost attribution, this tool should be used in an AWS account that only has Model Runner deployed.

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
✅ **Multiple User Classes**: Choose between fire-and-forget or wait-for-completion patterns
✅ **True Load Testing**: No queue depth throttling - stress the system to its limits

## Dependencies

The load tests have minimal dependencies to ensure they can run independently:

- `boto3` & `botocore`: AWS SDK for interacting with AWS services
- `GDAL`: For reading image metadata (dimensions, pixels)
- `locust`: Load testing framework for concurrent request generation

See `requirements.txt` for specific versions.
