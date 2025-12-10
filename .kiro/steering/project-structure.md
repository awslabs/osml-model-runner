# Project Structure

This document describes the organization of the repository and the purpose of each top-level directory.

## Directory Overview

### `cdk/`
Contains the infrastructure as code necessary to deploy the application. This includes AWS CDK constructs, stacks, and configuration for provisioning cloud resources.

### `docker/`
Contains Dockerfiles used to package the software into container images. These define the runtime environment and dependencies for containerized deployments.

### `src/`
The application source code itself. This is where the core business logic, models, services, and application components live.

### `test/`
The test code including unit tests, integration tests, and load tests. Tests mirror the structure of the `src/` directory for easy navigation.

### `conda/`
Contains the conda environment configurations for this application. These YAML files define the Python dependencies and environment setup used by tox and local development.

### `doc/`
The Sphinx-generated documentation for the project. This includes API documentation, guides, and other technical documentation built from docstrings and markdown files.

### `images/`
Contains documentation images and diagrams referenced in the markdown README and other guides. Used for visual documentation assets.

### `bin/`
Contains entrypoints for the application. These are executable scripts that serve as the main entry points for running the application in different modes or configurations.

### `scripts/`
Utilities and operations tools that might be useful when working with this application. These are helper scripts for development, deployment, or maintenance tasks.

## Configuration Files

- `.pre-commit-config.yaml` - Pre-commit hook configuration for automated code quality checks
- `tox.ini` - Tox configuration for running tests across multiple Python versions
- `pyproject.toml` / `setup.py` / `setup.cfg` - Python package configuration and metadata
- `.flake8` / `.coveragerc` - Linting and coverage configuration

## Working with the Project

- **Application code** goes in `src/`
- **Tests** go in `test/` mirroring the `src/` structure
- **Infrastructure changes** go in `cdk/`
- **Container changes** go in `docker/`
- **Documentation** is generated from `doc/` using Sphinx
