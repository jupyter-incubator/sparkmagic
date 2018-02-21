import os
from time import time

from pyhive import hive
from thrift.transport.TTransport import TTransportException

from sparkmagic.utils.configuration import thrift_hivetez_conf, thrift_hive_hostname, thrift_hive_port

class ThriftController:
    def __init__(self, ipython_display):
        self.ipython_display = ipython_display
        self.hiveconf = {}
        self.host = thrift_hive_hostname()
        self.port = thrift_hive_port()
        self.host = 'hiveserver-dogfood.s3s.altiscale.com'

        self.user = 'tnystrand' #os.getenv("USER")
        self.cursor = None

        self.conf = thrift_hivetez_conf()
        self.conf = {'hive.execution.engine': 'tez'}

    def set_conf(_conf, replace=True):
        # if not replace -> merge
        self.conf.update(_conf)

    def connect(self):
        try:
            t_all = time()
            self.cursor = hive.connect(self.host, self.port, self.user, configuration=self.conf).cursor()
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
