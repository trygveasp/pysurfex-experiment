#!/bin/bash

set -x

export PATH=$PWD/test/bin:/usr/bin/:$PATH
nosetests --with-coverage --cover-erase --cover-html --cover-html-dir=coverage \
--cover-package=experiment \
test/test_tasks.py \
|| exit 1

