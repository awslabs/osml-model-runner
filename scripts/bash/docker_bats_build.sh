#!/usr/bin/env bash
# This is a script that uses the bats CLI (see: https://builderhub.corp.amazon.com/docs/bats/cli-guide/index.html) to
# build the model runner's docker image in the same way it would be build by the CI/CD pipeline.
set -xe
export TMPDIR=/local/tmp
#rde :brazil sandbox-build --verbose --target release
bats transform -x DockerImage-1.0 -t AWSOversightMLModelRunner-1.0 -p AWSOversightMLModelRunner-1.0
