#!/bin/bash

set -ex

docker build --build-arg destEnv=$destEnv -t cs/drill_dev_unit_test .
docker run \
  --rm \
    -e BUILD_URL \
    -e JENKINS=1 \
    -v `pwd`:`pwd` \
    -w `pwd` \
    -P cs/drill_dev_unit_test \
    sh .ci/run_all.sh
