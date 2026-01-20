# OSML Contributing Guidelines

Thank you for your interest in contributing to our project! This guide walks you through the process of setting up your development environment, making changes, and submitting contributions.

## Table of Contents

- [Getting Started](#getting-started)
- [Making Changes](#making-changes)
- [Submitting Contributions](#submitting-contributions)

## Getting Started

### Prerequisites

Before you begin, ensure you have the following tools installed:

- [Python 3.10+](https://www.python.org/downloads/) (3.10, 3.11, 3.12, or 3.13)
- [Docker](https://docs.docker.com/get-docker/) for building and running containers
- [tox](https://tox.wiki/en/latest/installation.html) for running tests across multiple Python versions
- [Conda](https://docs.conda.io/en/latest/miniconda.html) or [Mamba](https://mamba.readthedocs.io/) for managing Python environments
- [Node.js and npm](https://nodejs.org/) (required for CDK infrastructure development)
- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/cli.html) (`npm install -g aws-cdk`) for infrastructure deployment

### Repository Structure

```text
osml-model-runner/
├── src/                 # Application source code (Python)
├── test/                # Unit, integration, and load tests (pytest)
├── bin/                 # Entry points for the containerized application
├── scripts/             # Utility scripts for development and operations
├── cdk/                 # AWS CDK infrastructure as code (TypeScript)
├── conda/               # Conda environment configurations
├── docker/              # Dockerfiles for container builds
├── doc/                 # Sphinx documentation source files
└── images/              # Documentation images and diagrams
```

Key directories:

- **src/**: The Python implementation of the Model Runner application.
- **test/**: Tests organized by type - unit tests mirror the `src/` structure, integration tests in `test/integ/`, and load tests in `test/load/`. See [test/load/README.md](./test/load/README.md) for load testing details.
- **cdk/**: Infrastructure as code for deploying on AWS. See [cdk/README.md](./cdk/README.md) for deployment instructions.
- **conda/**: Environment definitions used by tox to create isolated test environments with GDAL and other dependencies.
- **docker/**: Container definitions using multi-stage builds for different deployment targets.

### Setting Up Your Environment

1. **Clone the repository:**

   ```bash
   git clone git@github.com:awslabs/osml-model-runner.git
   cd osml-model-runner
   ```

2. **Create a development environment using conda:**

   ```bash
   conda env create -f conda/model-runner.yml
   conda activate osml_model_runner
   ```

3. **Install Build Tools:**

   ```bash
   conda install -c conda-forge tox
   conda install -c conda-forge pre-commit
   pre-commit install
   ```

4. **Verify your setup:**

   ```bash
   tox -e lint
   ```

## Making Changes

### Code Style and Linting

This project uses automated tools to enforce consistent code style:

- [black](https://github.com/psf/black) for code formatting (125 character line length)
- [isort](https://github.com/PyCQA/isort) for import organization (black-compatible profile)
- [flake8](https://github.com/PyCQA/flake8) for style guide enforcement
- [autopep8](https://github.com/hhatto/autopep8) for additional PEP 8 formatting

Pre-commit hooks run automatically on `git commit`. To run linters manually:

```bash
# Run all linting checks
tox -e lint
```

Follow the patterns in the existing codebase for naming conventions, module organization, and documentation style.

### Running Tests

This project uses **tox** to run tests across multiple Python versions in isolated environments.

```bash
# Run all tests (excludes integration tests by default)
tox

# Run tests for a specific Python version
tox -e py312-prod

# Run a specific test file or test
tox -- test/aws/osml/model_runner/test_example.py::test_function_name

# Run tests with verbose output
tox -- -v

# Run integration tests (requires deployed infrastructure)
tox -- -m integration
```

For load testing details, see [test/load/README.md](./test/load/README.md).

### Building Documentation

API documentation is generated using Sphinx from docstrings in the source code.

```bash
tox -e docs
```

The generated documentation will be available in `doc/_build/html/`.

## Submitting Contributions

### Branching Strategy

We follow [trunk-based development](https://trunkbaseddevelopment.com/). All work branches off `main` and merges back quickly.

**For internal developers:**

1. Create a feature branch: `git checkout -b feature/<feature-name>`
2. Make commits following our [commit message conventions](#commit-messages)
3. Push and open a PR against `main` early (mark as WIP if needed)
4. Rebase before merging: `git pull --rebase origin main`

**For external contributors:**

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
2. Create a feature branch in your fork
3. Make your changes and push to your fork
4. [Open a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) against the upstream `main` branch

To work from a specific release tag:

```bash
git checkout -b my-branch tags/v1.0.0
```

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for clear, structured commit history:

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Pull Request Guidelines

- Open PRs early to enable discussion and feedback
- Ensure all CI checks pass (tests, linting, security scans)
- PRs must be reviewed by someone who did not contribute to the branch
- Keep PRs focused - one feature or fix per PR when possible

### Issue Tracking

Before starting work, check [existing issues](https://github.com/awslabs/osml-model-runner/issues) to avoid duplication. When opening a new issue, provide:

- Clear description of the problem or feature request
- Steps to reproduce (for bugs)
- Relevant environment information

### Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact opensource-codeofconduct@amazon.com with any additional questions or comments.

### Security Issue Notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

### Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

We appreciate your contributions!
