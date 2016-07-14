#!/bin/sh
set -e

cd hdijupyterutils
python setup.py sdist upload -r pypitest
cd ..

cd autovizwidget
python setup.py sdist upload -r pypitest
cd ..

cd sparkmagic
python sparkmagic/setup.py sdist upload -r pypitest
cd ..
