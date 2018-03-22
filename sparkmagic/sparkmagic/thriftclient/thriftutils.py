import os
from sparkmagic.utils.constants import THRIFT_LOG_VAR
from time import time

from sparkmagic.thriftclient.thriftexceptions import ThriftConfigurationError
from hdijupyterutils.filesystemreaderwriter import FileSystemReaderWriter

from collections import namedtuple

DefaultVar = namedtuple('DefaultVar', ['varible', 'default'])
NamedVar = namedtuple('NamedVar', ['name', 'value'])

def writeln(querylogs, ipython_display):
    def _writeln(*args, **kwargs):
        querylogs + args
        ipython_display.writeln(*args, **kwargs)
    return _writeln

def send_error(querylogs, ipython_display):
    def _send_error(*args, **kwargs):
        querylogs + args
        ipython_display.writeln(*args, **kwargs)
    return _send_error

def exit_with_none(shell, ipython_display, logs, log_var_name=None):
    _save_logs(shell, ipython_display, logs, log_var_name)
    return None

def exit_with_data(shell, ipython_display, logs, data, log_var_name=None, data_var_name=None):
    _save_data(shell, ipython_display, data, data_var_name)
    _save_logs(shell, ipython_display, logs, log_var_name)
    return data

def _save_logs(shell, ipython_display, logs, log_var_name=None):
    if log_var_name:
        ipython_display.writeln("Stored query data in user variable: {}".format(log_var_name))
        shell.user_ns[log_var_name] = str(logs)
    else:
        ipython_display.writeln("Stored query data in user variable: {}".format(THRIFT_VAR))
        shell.user_ns[THRIFT_LOG_VAR] = str(logs)

def _save_data(shell, ipython_display, data, data_var_name):
    if data_var_name:
        ipython_display.writeln("Stored query data in user variable: {}".format(data_var_name))
        shell.user_ns[data_var_name] = data
    else:
        ipython_display.writeln("Stored query data in user variable: {}".format(THRIFT_VAR))
        shell.user_ns[THRIFT_LOG_VAR] = data

def time_and_write(write_func=None):
    def time_and_write_wrap(f):
        def _time_and_write_wrap(*args):
            t_all = time()
            ret = f(*args)
            if write_func:
                write_func("Total execution time: {:.2f}s".format(time() - t_all))
            else:
                args[0].ipython_display.writeln("Total execution time: {:.2f}s".format(time() - t_all))
            return ret
        return _time_and_write_wrap
    return time_and_write_wrap


def env_alti_local(env=None, alti=None, local=None):
    if env and os.getenv(env):
        return env
    if alti and os.path.isfile(os.path.expanduser(alti)):
        return FileSystemReaderWriter(alti)
    if local and os.path.isfile(os.path.expanduser(local)):
        return FileSystemReaderWriter(local)

    raise ThriftConfigurationError("Could not locate either\n{!r}\nor\n{!r}\n".format(alti, local))
