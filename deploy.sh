#!/bin/sh
set -e
python hdijupyterutils/setup.py sdist upload -r pypi
python autovizwidget/setup.py sdist upload -r pypi
python sparkmagic/setup.py sdist upload -r pypi
