# Model Runner CHANGELOG

This file is used to represent changes made between each tagged version of the Model Runner image. This should be updated as part of each merge request to identify what has changed. If possible, include the story that links to the feature update.

[Reference for OSML Model Runner Release Guide](https://github.com/aws-solutions-library-samples/osml-model-runner/releases)

## v1.5.1
- Overlap between regions and tiles is now properly accounted for for feature deduplication/nms given the tiling logic changes in 1.5.0.

## v1.5.0
- Update tile sizes to always be equal to tile_size in the request. Tiles at the edges of regions will now have larger overlaps. Selecting regions now follows that logic as well.

## v1.4.2
- fix key error caused by giving identical strings to hash function in feature_selection.py

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
