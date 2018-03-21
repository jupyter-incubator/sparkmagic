#!/bin/bash
nosetests hdijupyterutils autovizwidget sparkmagic
start-notebook.sh --NotebookApp.iopub_data_rate_limit=1000000000
