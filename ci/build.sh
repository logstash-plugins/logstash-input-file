#!/bin/bash
# version: 1
########################################################
#
# AUTOMATICALLY GENERATED! DO NOT EDIT
#
########################################################
set -e

echo "Starting build process in: `pwd`"
source ./ci/setup.sh

if [[ -f "ci/run.sh" ]]; then
    echo "Running custom build script in: `pwd`/ci/run.sh"
    source ./ci/run.sh
else
    echo "Running default build scripts in: `pwd`/ci/build.sh"
    bundle install
    bundle exec rake vendor
    LOG_AT=WARN bundle exec rspec --tag ~rotations --tag ~lsof spec
    LOG_AT=TRACE bundle exec rspec -fd --fail-fast --tag rotations spec
fi
