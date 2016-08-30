# Description

This extension allows to re-connect a notebook to a different cluster passing in the notebook path and cluster information.

Assumes that kernel is already running for a notebook and that cluster information is valid.
Kernel will be restarted and connected to cluster specified.

# Installation

Run:
    1. `pip install -e .`
    2. `jupyter serverextension enable --py sparkmagic_ext`

# API

`/reconnectsparkmagic`:
    * `POST`:
        ```
        {
            'path': 'path.ipynb',
            'username': 'user',
            'password': 'password',
            'endpoint': 'url'
        }
        ```
