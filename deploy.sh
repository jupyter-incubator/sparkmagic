#!/bin/sh
set -e

cd hdijupyterutils
python setup.py sdist upload -r pypi
cd ..

cd autovizwidget
python setup.py sdist upload -r pypi
cd ..

cd sparkmagic
python setup.py sdist upload -r pypi
cd ..
