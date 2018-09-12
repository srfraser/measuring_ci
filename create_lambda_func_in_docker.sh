#!/bin/bash

set -e

# The target environment is Linux, so use a container to build
# the python modules, so that mac users don't have to worry about
# the compiled components
docker build -t measuring_ci_lambda_builder .

docker run -v "$(pwd)":/work measuring_ci_lambda_builder /create_lambda_func.sh



