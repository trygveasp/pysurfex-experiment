#!/bin/bash


set -x

export PATH=$PWD/test/bin:/usr/bin/:$PATH
./prepare_testdata.sh 
nosetests --with-timer --with-coverage --cover-erase --cover-html --cover-html-dir=coverage \
--cover-package=experiment \
test/test_submission.py \
test/test_ecflow.py \
test/test_tasks.py \
|| exit 1


