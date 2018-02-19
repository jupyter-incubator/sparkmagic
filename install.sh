#!/bin/sh
set -e

cd sparkmagic
pip install -e .
jupyter-kernelspec install sparkmagic/kernels/sqlkernel/
cd ..
