#!/bin/bash
#echo "Current working directory = $(pwd)" 
nosetests hdijupyterutils autovizwidget sparkmagic 2>&1 | tee container_alti_test.log
