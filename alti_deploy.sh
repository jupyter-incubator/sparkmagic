#!/bin/sh
set -e

cd hdijupyterutils
pip install -e .
cd ..

cd autovizwidget
pip install -e .
cd ..

cd sparkmagic
pip install -e .
cd ..

cd remote_hivemetastore
pip install -e .
cd ..

jupyter-kernelspec install ./sparkmagic/sparkmagic/kernels/sqlkernel/
