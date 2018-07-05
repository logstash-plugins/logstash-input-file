#!/bin/bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

bundle exec rspec -fd --pattern spec/**/*_spec.rb,spec/**/*_specs.rb
