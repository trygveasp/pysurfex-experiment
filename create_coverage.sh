#!/bin/bash

set -x

cd ..
export PYTHONPATH=$PWD/pysurfex-experiment/test:$PWD/pysurfex
coverage run --source=pysurfex-experiment -m unittest discover -s pysurfex-experiment || exit 1
coverage html -d pysurfex-experiment/coverage/html || exit 1
coverage xml -o pysurfex-experiment/coverage/coverage.xml || exit 1
