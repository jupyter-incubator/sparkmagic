#!/bin/sh
# set -e
python hdijupyterutils/setup.py sdist upload -r pypitest
echo $?