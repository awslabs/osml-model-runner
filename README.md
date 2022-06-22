# AIP Model Runner Container

This package contains an application used to orchestrate the execution of ML models on large satellite images. The
application monitors an input queue for processing requests, decomposes the image into a set of smaller regions and
tiles, invokes a ML model endpoint with each tile, and finally aggregates all of the results into a single output. The
application itself has been containerized and is designed to run on a distributed cluster of machines collaborating
across instances to process images as quickly as possible.

This application has been hardened and built on top of an IronBank container located:
* https://repo1.dso.mil/dsop/opensource/python/python38/-/blob/development/Dockerfile
* OS=RHEL 8
* Python=Python 3.8 (python38@sha256:7ec293f50da6961131a7d42f40dc8078dd13fbbe0f4eb7a9b37b427c360f2797)
* OPENJPEG_VERSION=2.3.1
* PROJ=8.2.1
* GDAL=3.4.2

## Using Iron Bank Registry 
1. Log into: `https://registry1.dso.mil/`
2. Click on your `username` in the upper right.
3. In drop down menu select `User Profile`
4. In the menu that appears:
    1. docker user name: `Username`
    2. password: `CLI Secret`
5. In the CLI:
    1. `docker login registry1.dso.mil -u {Username} -p {CLI Secret}`
6. To build locally you can use: 
```bash
docker build -f docker/Dockerfile.mr_container --build-arg BASE_REGISTRY=registry1.dso.mil --build-arg BASE_IMAGE=ironbank/opensource/python/python38 -t mr-container:latest .
```
7. 

## How to install certs.
1. First follow these steps to make sure you have httpd installed: 
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/SSL-on-amazon-linux-2.html
2. Either go here and download: https://public.cyber.mil/pki-pke/ or 
```bash
curl -Ok https://dl.dod.cyber.mil/wp-content/uploads/pki-pke/zip/certificates_pkcs7_DoD.zip 
```
2. Install
```bash
unzip certificates_pkcs7_DoD.zip
openssl pkcs7 -print_certs -in ./Certificates_PKCS7_v5.7_DoD/Certificates_PKCS7_v5.7_DoD.pem.p7b -out /etc/pki/ca-trust/source/anchors/ca-bundle.pem
chmod 644 /etc/pki/ca-trust/source/anchors/ca-bundle.pem
update-ca-trust
update-ca-trust extract
```
3. Restart docker
```bash
systemctl restart docker
```


## Key Design Concepts

### Image Tiling

The images to be processed by this application are expected to range anywhere from 500MB to 500GB in size. The upper
bound is consistently growing as sensors become increasingly capable of collecting larger swaths of high resolution
data. To handle these images the application applies two levels of tiling. The first is region based tiling in which the
application breaks the full image up into pieces that are small enough for a single machine to handle. All regions after
the first are placed on a second queue so other model runners can start processing those regions in parallel. The second
tiling phase is to break each region up into individual chunks that will be sent to the ML models. Many ML model
containers are configured to process images that are between 512 and 2048 pixels in size so the full processing of a
large 200,000 x 200,000 satellite image can result in >10,000 requests.

### Lazy IO & Encoding Formats with Internal Tiles

The images themselves are assumed to reside in S3 and are assumed to be compressed and encoded in such a way as to
facilitate piecewise access to tiles without downloading the entire image. The GDAL library, a frequently used open
source implementation of GIS data tools, has the ability to read images directly from S3 making use of partial range
reads to only download the part of the overall image necessary to process the region.

### Tile Overlap and Merging Results

Many of the ML algorithms we expect to run will involve object detection or feature extraction. It is possible that
features of interest would fall on the tile boundaries and therefore be missed by the ML models because they are only
seeing a fractional object. This application mitigates that by allowing requests to specify an overlap region size that
should be tuned to the expected size of the objects. Each tile sent to the ML model will be cut from the full image
overlapping the previous by the specified amount. Then the results from each tile are aggregated with the aid of a Non
Maximal Suppression algorithm used to eliminate duplicates in cases where an object in an overlap region was picked up
by multiple model runs.

## Package Layout

* **/src**: This is the Python implementation of this application.
* **/test**: Unit tests have been implemented using [pytest](https://docs.pytest.org).
* **/bin**: The entry point for the containerized application.
* **/configuration**: The Dockerfile template used by the build system to package the application.
* **/scripts**: Utility scripts that are not part of the main application frequently used in development / testing.

## Development Environment


To run the container in a test mode: 

```shell
docker run -it -v/path/to/test:/home/test --entrypoint /bin/bash .
```

```bash
python3 -m pytest test
```

## Linting/Formatting

This package uses a number of tools to enforce formatting, linting, and general best practices:
- [Black](https://github.com/ambv/black) and [isort](https://github.com/timothycrosley/isort) for formatting with a max line length of 100
- [mypy](http://mypy-lang.org/) to enforce static type checking
- [flake8](https://pypi.python.org/pypi/flake8) to check pep8 compliance and logical errors in code

## Building



