#!/usr/bin/env bash
set -xe
export TMPDIR=/local/tmp
#rde :brazil sandbox-build --verbose --target release
bats transform -x DockerImage-1.0 -t AWSOversightMLModelRunner-1.0 -p AWSOversightMLModelRunner-1.0