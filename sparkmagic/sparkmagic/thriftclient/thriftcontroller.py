import os
from time import time
from collections import namedtuple

from pyhive import hive
from thrift.transport.TTransport import TTransportException

from sparkmagic.utils.configuration import thrift_hivetez_conf, thrift_hive_hostname, thrift_hive_port, hive_user

class ThriftController:
    def __init__(self, ipython_display):
        self.ipython_display = ipython_display
        self.hiveconf = {}
        self.connection = ThriftConnection(
            host=thrift_hive_hostname(),
            port=thrift_hive_port(),
            user=hive_user(),
            conf=thrift_hivetez_conf())

        self.cursor = None


    # Set query confs
    def set_conf(conf, replace=True):
        # if not replace -> merge
        self.connection.conf.update(conf)

    # Set connection details
    def set_connection(conf, replace=True):
        for k,v in conf.items():
            self.connection[k] = v

    def connect(self):
        try:
            t_all = time()
            self.cursor = hive.connect(self.connection.host,
                self.connection.port,
                self.connection.user,
                configuration=self.connection.conf).cursor()
            self.ipython_display.writeln("Start-up time: {:.2f}".format(time() - t_all))
        except TTransportException as tte:
            self.ipython_display.send_error(tte.message)
            self.ipython_display.send_error(
                "Possible issues in order: {}".format(''.join(['\n-> {}']*5)).format(
                        "Connection to destination host is down",
                        "Bad/wrong hostname: {!r}".format(self.host),
                        "Bad/wrong port: {!r}".format(self.port),
                        "Destination host is not reachable (ssh and port forward is not set up)",
                        "Thriftserver is not running"
                    )
                )
            return False
        return True

    def execute(self, query, async=False):
        self.cursor.execute(str(query), async=async)

    def reset(self):
        self.cursor.cancel()
        self.connect()


class ThriftConnection(namedtuple('ThriftConnection', 'host port user conf')):
    __slots__ = ()

    def __repr__(self):
        return "\n".join("{}: {}".format(k,v) for k,v in self._asdict().items())
