#!/bin/bash

export PYTHONPATH=`~/.local/bin/poetry env info -p`/lib/python3.8/site-packages/:/pysurfex-experiment:`~/.local/bin/poetry env info -p`/src/pysurfex/
make html

