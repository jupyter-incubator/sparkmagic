#!/bin/sh
set -e

cd hdijupyterutils
python setup.py sdist && python twine upload dist/*
cd ..

cd autovizwidget
python setup.py sdist && python twine upload dist/*
cd ..

cd sparkmagic
python setup.py sdist && python twine upload dist/*
cd ..
