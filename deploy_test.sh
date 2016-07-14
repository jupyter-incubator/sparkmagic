#!/bin/sh
set -e
python hdijupyterutils/setup.py sdist upload -r pypitest
python autovizwidget/setup.py sdist upload -r pypitest
python sparkmagic/setup.py sdist upload -r pypitest
