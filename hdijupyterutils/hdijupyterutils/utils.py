# Distributed under the terms of the Modified BSD License.

import os
import uuid


first_run = True
instance_id = None


def expand_path(path):
    return os.path.expanduser(path)


def join_paths(p1, p2):
    return os.path.join(p1, p2)


def generate_uuid():
    return uuid.uuid4()


def get_instance_id():
    global first_run, instance_id

    if first_run:
        first_run = False
        instance_id = generate_uuid()

    if instance_id is None:
        raise ValueError("Tried to return empty instance ID.")

    return instance_id
