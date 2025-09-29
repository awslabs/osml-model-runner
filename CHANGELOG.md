# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.6.0] - 2025-09-29

### Changed

- Pointed GDAL base image to gdal-base:v3-8-5-2 

## [2.5.3] - 2025-09-10

### Changed

- Image updates

## [2.5.2] - 2025-08-25

### Changed

- Run pipeline to auto-update packages

## [2.5.1] - 2025-08-18

### Changed

- Fix for UC pipelines

## [2.5.0] - 2025-08-12

### Added

### Changed

- Refactor to leverage Common/cicd-templates

### Fixed

## [v2.4.1] - 2025-07-31

- Update base path location of GDAL image for UC

## [v2.4.0] - 2025-07-07

- Update base path location of GDAL image
- Bump pytest, mock, moto, boto3, botocore, setuptools, and codeguru-profiler-agent packages 

## [v2.3.1]

- Update root_logger to root instead of __name__. This was causing no logs to be emitted to CW
- Update METRIC to emit the completed duration instead of default

## v2.3.0

- OSML Model Runner release v2.3.0

## v2.2.0

- Modify TTL from 24 hours to 7 days for longer historical storage of state 

## v2.1.4

- Remove usage of root python logger

## v2.1.3

- osml-imagery-toolkit version bump to v1.4.2

## v2.1.2

- Pass in referential S3 file location in sourceMetadata field if not a NITF

## v2.1.1

## v2.1.0

- Upgrade to OSML v2.3.0 to bring to parity. Includes features:
    - Feature Selection: Resolved issues in feature selection, particularly addressing errors related to zero-area features, ensuring accurate and error-free processing
    - Kinesis Integration: Addressed rate exceedance issues with Kinesis by implementing the put_records method, improving data throughput and stability
    - Miscellaneous Fixes: Corrected missing list handling in FeatureCollection dump calls and removed redundant logic in the no-operation model, streamlining the processing workflow
- Update `create_elevation_model` function to enable additional parameters for use with `GenericDEMTileSet`

## v2.0.4

- Add tracking of duplicate detections via logging.warning
- ci: Add review account deployment

## v2.0.3

- ci: Include stable tag in container wormhole
- ci: Require manual job execution to push stable image to OPS
- Fix hash collision by performing a check if hash already in dict

## v2.0.2

- Fix `inferenceTime` KeyError caused by non-unique `feature_hash_id`

## v2.0.1

- Fix stable tag job in UC

## v2.0.0

- Upgrade to OSML v2 beta solution to get two core features:
    - fixed tile size
    - better (~40%) increased write performance
- fork from OSML to ensure we keep a referential S3 file location

## v1.4.2

- revert from v1.5.1 -> v1.4.2
- fix key error caused by giving identical strings to hash function in feature_selection.py
- ci: Add stable release CI Pipeline

## v1.5.1

- Overlap between regions and tiles is now properly accounted for for feature deduplication/nms given the tiling logic changes in 1.5.0.
- Update osml-imagery-toolkit 1.3.1->1.3.2 fix for SICD metadata

## v1.5.0

- Update tile sizes to always be equal to tile_size in the request. Tiles at the edges of regions will now have larger overlaps. Selecting regions now follows that logic as well.

## v1.4.1

- update for create_elevation_model function to create DigitalElevationModel using GenericDEMTileSet

## v1.4.0

- Updating CHANGELOG and release version to match OSML release version
- Updating GitLab CI Pipeline to match release version

## v0.3.2

- Fix CI rebake job
- Update GitLab CI Pipeline to identify package version updates
- Sync Makefile with version number release

## v0.3.1

- Add GitLab CI Pipeline automation

## v0.3.0

- osml-model-runner(`v1.4.0`)
- Add container wormhole trigger CI job

## v0.2.0

- osml-model-runner (`v1.1.0`)
- bug: RSM camera model index array management
- bug: OOM management for GDAL tiles
- fix: version bumps  
- feature: HTTP model invocation option
- feature: Expanded polygon feature support outside of pure bbox returns
- bug: patched and suppressed results of security scans as appropriate (IE-216)
- bug: removed unnecessary (present in InferencePlatformCDK) scripts

## v0.1.0

- Initial release
- osml-model-runner (`v1.0.0`)
