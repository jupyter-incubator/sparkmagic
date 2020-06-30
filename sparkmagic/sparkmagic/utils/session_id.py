# coding: utf-8

import os
import sys
import fcntl
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import HOME_PATH
from sparkmagic.utils.sparklogger import SparkLog


def default_get_username():
    return 'anonymous'

if sys.version_info.major == 3:
    from importlib.machinery import SourceFileLoader

    try:
        get_username = SourceFileLoader("get_username", conf.get_username_filename()).load_module().get_username
    except:
        get_username = default_get_username
else:
    import imp

    try:
        get_username = imp.load_source('get_username', conf.get_username_filename()).get_username
    except:
        get_username = default_get_username


def get_local_profile_name():
    return os.path.expanduser(os.path.join(conf.jupyter_dir(), get_username(), conf.local_profile_name()))


def get_spark_profile_name():
    return os.path.expanduser(os.path.join(conf.jupyter_dir(), get_username(), conf.spark_profile_name()))


class SessionIdFileLock(object):

    def __init__(self):
        self.logger = SparkLog(u"SessionIdStorage")
        self.session_id_filename = os.path.expanduser(os.path.join(HOME_PATH, conf.spark_session_id_dir(), get_username()))
        if not os.path.exists(os.path.dirname(self.session_id_filename)):
            self.logger.info('Session id directory "{}" does not exist.'.format(os.path.dirname(self.session_id_filename)))
            os.makedirs(os.path.dirname(self.session_id_filename))
        if not os.path.exists(self.session_id_filename):
            self.logger.info('Session id file "{}" does not exist.'.format(self.session_id_filename))
            with open(self.session_id_filename, 'w'):
                pass
        self.session_id_file = open(self.session_id_filename)
        fcntl.flock(self.session_id_file.fileno(), fcntl.LOCK_EX)

        try:
            with open(self.session_id_filename) as f:
                self.session_id = int(f.read())
        except ValueError:
            self.session_id = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.write_session_id()
        self.session_id_file.close()

    def get_session_id(self):
        return self.session_id

    def update_session_id(self, session_id):
        self.session_id = session_id

    def write_session_id(self):
        with open(self.session_id_filename, 'w') as f:
            f.write(str(self.session_id))
