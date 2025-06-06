# OSML Model Runner

This package contains an application used to orchestrate the execution of ML models on large satellite images. The
application monitors an input queue for processing requests, decomposes the image into a set of smaller regions and
tiles, invokes an ML model endpoint with each tile, and finally aggregates all the results into a single output. The
application itself has been containerized and is designed to run on a distributed cluster of machines collaborating
across instances to process images as quickly as possible.

### Table of Contents
* [Getting Started](#getting-started)
  * [Key Design Concepts](#key-design-concepts)
    * [Load Balancing](#load-balancing)
    * [Image Tiling](#image-tiling)
    * [Geolocation](#geolocation)
    * [Merging Results from Overlap Regions](#merging-results-from-overlap-regions)
  * [Package Layout](#package-layout)
  * [Prerequisites](prerequisites)
  * [Development Environment](#development-environment)
  * [Running ModelRunner](#running-modelrunner)
  * [Infrastructure](#infrastructure)
    * [S3](#s3)
  * [Documentation](#documentation)
* [Support & Feedback](#support--feedback)
* [Security](#security)
* [License](#license)


## Getting Started

### Key Design Concepts

The [Guidance for Model Developers](./GUIDE_FOR_MODEL_DEVELOPERS.md) document contains details of how the
OversightML ModelRunner applications interacts with containerized computer vision (CV) models and examples of the
GeoJSON formatted inputs it expects and generates. At a high level this application provides the following functions:

#### Load Balancing

The OversightML ModelRunner receives processing requests from an input queue and internally balances them across the
tasked model endpoints to improve throughput and utilization. Requests to endpoints with unused capacity are pulled
ahead of requests that would otherwise have been delayed waiting for previously tasked images to complete. Requests
are still handled in a first-in, first-out (FIFO) sequencing for each model endpoint. We estimate the load on each
endpoint as the ratio of the number of in-progress regions to the compute instances backing the endpoint. This helps
the load balancing algorithm adjust for images of varying size and endpoints with dynamic capacity that scales up
and down.

#### Image Tiling

The images to be processed by this application are expected to range anywhere from 500MB to 500GB in size. The upper
bound is consistently growing as sensors become increasingly capable of collecting larger swaths of high resolution
data. To handle these images the application applies two levels of tiling. The first is region based tiling in which the
application breaks the full image up into pieces that are small enough for a single machine to handle. All regions after
the first are placed on a second queue so other model runners can start processing those regions in parallel. The second
tiling phase is to break each region up into individual chunks that will be sent to the ML models. Many ML model
containers are configured to process images that are between 512 and 2048 pixels in size so the full processing of a
large 200,000 x 200,000 satellite image can result in >10,000 requests to those model endpoints.

The images themselves are assumed to reside in S3 and are assumed to be compressed and encoded in such a way as to
facilitate piecewise access to tiles without downloading the entire image. The GDAL library, a frequently used open
source implementation of GIS data tools, has the ability to read images directly from S3 making use of partial range
reads to only download the part of the overall image necessary to process the region.

#### Inference
OversightML Model Runner is optimized to utilize models hosted on
[SageMaker Endpoints](https://docs.aws.amazon.com/sagemaker/latest/dg/realtime-endpoints-manage.html) and supports
single-model endpoints, multi-container endpoints, and endpoints with multiple variants deployed.  Endpoint parameters
can be specified in the image request to Model Runner using the `imageProcessor` and `imageProcessorParameters`
options detailed in the table below.  Model Runner can also be configured to use HTTP endpoints for increased
compatibility.

#### Geolocation

Most ML models do not contain the photogrammetry libraries needed to geolocate objects detected in an
image. ModelRunner will convert these detections into geospatial features by using sensor models described
in an image metadata. The details of the photogrammetry operations are in the
[osml-imagery-toolkit](https://github.com/aws-solutions-library-samples/osml-imagery-toolkit) library.

#### Merging Results from Overlap Regions

Many of the ML algorithms we expect to run will involve object detection or feature extraction. It is possible that
features of interest would fall on the tile boundaries and therefore be missed by the ML models because they are only
seeing a fractional object. This application mitigates that by allowing requests to specify an overlap region size that
should be tuned to the expected size of the objects. Each tile sent to the ML model will be cut from the full image
overlapping the previous by the specified amount. Then the results from each tile are aggregated with the aid of a
Non-Maximal Suppression algorithm used to eliminate duplicates in cases where an object in an overlap region was picked
up by multiple model runs.

### Metrics and Logs

As the application runs key performance metrics and detailed logging information are output to [CloudWatch](https://aws.amazon.com/cloudwatch/).
A detailed description of what information is tracked along with example dashboards can be found in
[METRICS_AND_DASHBOARDS.md](./METRICS_AND_DASHBOARDS.md).

CloudWatch Logs Insights can be leveraged to provide anything from an overview of processing activity to in depth
diagnostics.  For example, querying the `/aws/OSML/MRService` log group using the following query will provide a
"Timeline" view of a specific job.
```
fields @timestamp, message, job_id, region_id, @logStream
| filter job_id like /<job_id>/
| filter tag like "TIMELINE EVENT"
| sort @timestamp desc
```

Alternatively, the job_id filter can be omitted to see events from all jobs for a given time window.

Note: Timeline Event logs like the example above are written at an INFO level.

### Package Layout

* **/src**: This is the Python implementation of this application.
* **/test**: Unit tests have been implemented using [pytest](https://docs.pytest.org).
* **/bin**: The entry point for the containerized application.
* **/scripts**: Utility scripts that are not part of the main application frequently used in development / testing.

### Prerequisites

First, ensure you have installed the following tools locally

- [docker](https://nodejs.org/en)
- [tox](https://tox.wiki/en/latest/installation.html)
- [osml cdk](https://github.com/aws-solutions-library-samples/osml-cdk-constructs) deployed into your aws account

### Development Environment

To run the container in a build/test mode and work inside it.

```shell
docker run -it -v `pwd`/:/home/ --entrypoint /bin/bash .
```

### Running ModelRunner

To start a job, place an ImageRequest on the ImageRequestQueue.

Sample ImageRequest:
```json
{
    "jobName": "<job_name>",
    "jobId": "<job_id>",
    "imageUrls": ["<image_url>"],
    "outputs": [
        {"type": "S3", "bucket": "<result_bucket_arn>", "prefix": "<job_name>/"},
        {"type": "Kinesis", "stream": "<result_stream_arn>", "batchSize": 1000}
    ],
    "imageProcessor": {"name": "<sagemaker_endpoint>", "type": "SM_ENDPOINT"},
    "imageProcessorParameters": {"TargetVariant": "AllTraffic"},
    "imageProcessorTileSize": 2048,
    "imageProcessorTileOverlap": 50,
    "imageProcessorTileFormat": "< NITF | JPEG | PNG | GTIFF >",
    "imageProcessorTileCompression": "< NONE | JPEG | J2K | LZW >"
}
```
Below are additional details about each key-value pair supported by the image request:

| key                           | value                                                                                                                                                                 | type                 | details                                                                                                                                                                                                                                                                                      |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| jobId                         | `<job_id>`                                                                                                                                                            | string               | Unique id for a job, ex: `testId1`                                                                                                                                                                                                                                                           |
| jobName                       | `<job_name>`                                                                                                                                                          | string               | Name of the job, ex: `jobtest-testId1`                                                                                                                                                                                                                                                       |
| imageUrls                     | `["<image_url>"]`                                                                                                                                                     | list[string]         | List of S3 image path, which can be found by going to your S3 bucket, ex: `s3://test-images-0123456789/tile.tif`                                                                                                                                                                             |
| outputs                       | ```{"type": "S3", "bucket": "<result_bucket_name>", "prefix": "<job_name>/"},```</br> ```{"type": "Kinesis", "stream": "<result_stream_name>", "batchSize": 1000}```  | dict[string, string] | Once the OSML has processed an image request, it will output its GeoJson files into two services, Kinesis and S3. The Kinesis and S3 are defined in `osml-cdk-constructs` package which can be found there. ex: `"bucket":"test-results-0123456789"` and `"stream":"test-stream-0123456789"` |
| imageProcessor                | ```{"name": "<endpoint_name>", "type": "<SM_ENDPOINT \| HTTP_ENDPOINT>"}```                                                                                           | dict[string, string] | Select the endpoint that you want to process your image. You can find the list of endpoints by going to AWS Console > SageMaker Console > Click `Inference` (left sidebar) > Click `Endpoints` > Copy the name of any endpoint. ex: `aircraft`                                               |
| imageProcessorParameters      | ```{"ContentType": "<string>", "CustomAttributes": "<string>", "TargetContainerHostname": "<string>", "TargetModel": "<string>", "TargetVariant": "<string>", ...}``` | dict[string: string] | Additional parameters to pass to the model.  For SageMaker endpoints, the supported parameters can be found in the [API docs](https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_runtime_InvokeEndpoint.html#API_runtime_InvokeEndpoint_RequestParameters)                        |
| imageProcessorTileSize        | 512                                                                                                                                                                   | integer              | Tile size represents width x height pixels and split the images into it. ex: `512`                                                                                                                                                                                                           |
| imageProcessorTileOverlap     | 32                                                                                                                                                                    | integer              | Tile overlap represents the width x height pixels and how much to overlap the existing tile, ex: `32`                                                                                                                                                                                        |
| imageProcessorTileFormat      | `NTIF / JPEF / PNG / GTIFF`                                                                                                                                           | string               | Tile format to use for tiling. I comes with 4 formats, ex: `GTIFF`                                                                                                                                                                                                                           |
| imageProcessorTileCompression | `NONE / JPEG / J2K / LZW`                                                                                                                                             | string               | The compression used for the target image. It comes with 4 formats, ex: `NONE`                                                                                                                                                                                                               |


### Infrastructure

#### S3
When configuring S3 buckets for images and results, be sure to follow [S3 Security Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html).

### Code Documentation

You can find documentation for this library in the `./doc` directory. Sphinx is used to construct a searchable HTML
version of the API documents.

```shell
tox -e docs
```

## Support & Feedback

To post feedback, submit feature ideas, or report bugs, please use the [Issues](https://github.com/aws-solutions-library-samples/osml-model-runner/issues) section of this GitHub repo.

If you are interested in contributing to OversightML Model Runner, see the [CONTRIBUTING](CONTRIBUTING.md) guide.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

MIT No Attribution Licensed. See [LICENSE](LICENSE).
