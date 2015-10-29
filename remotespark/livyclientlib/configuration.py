"""Utility to read configs for spark magic.
"""
# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json

from .utils import join_paths, get_magics_home_path, ensure_path_exists, ensure_file_exists


first_run = True
config = None


def get_configuration(config_name, default_value):
    global first_run, config

    if first_run:
        _read_config()
        first_run = False

    if config is None:
        raise ValueError("Config has not been read.")

    return config.get(config_name, default_value)


def _read_config():
    global config

    home_path = get_magics_home_path()
    ensure_path_exists(home_path)
    config_file = join_paths(home_path, "config.json")
    ensure_file_exists(config_file)

    with open(config_file, "r+") as f:
        config_text = f.readlines()
        line = "".join(config_text).strip()
        if line == "":
            config = {}
        else:
            config = json.loads(line)
