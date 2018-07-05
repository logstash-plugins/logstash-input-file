#!/bin/bash

# This is intended to be run the plugin's root directory. `ci/unit/docker-test.sh`
# Ensure you have Docker installed locally and set the ELASTIC_STACK_VERSION environment variable.
set -e

docker-compose -f ci/unit/docker-compose.yml run logstash
