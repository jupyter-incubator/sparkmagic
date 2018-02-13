import os
from time import time

from pyhive import hive
from thrift.transport.TTransport import TTransportException

class ThriftController:
    def __init__(self, ipython_display):
        self.ipython_display = ipython_display
        self.hiveconf = {}
        self.host = 'localhost'
        self.port = 10000
        self.user = 'tnystrand' #os.getenv("USER")
        self.cursor = None
        # Fix this ->
        self.conf = {'hive.execution.engine': 'tez',
                     'tez.cpu.vcores': '4',
                     'tez.queue.name': 'production',
                     'hive.tez.container.size': '4096',
                     'hive.tez.java.opts': '-Xmx3686m',
                     'hive.exec.max.dynamic.partitions': '5000',
                     'hive.exec.max.dynamic.partitions.pernode': '5000',
                     'hive.exec.dynamic.partition': 'true',
                     'hive.support.sql11.reserved.keywords': 'false',
                     'hive.cbo.enable': 'true',
                     'hive.compute.query.using.stats': 'true',
                     'hive.stats.fetch.column.stats': 'true',
                     'hive.stats.fetch.partition.stats': 'true',
                     'hive.vectorized.execution.enabled': 'true',
                     'hive.vectorized.execution.reduce.enabled': 'true',
                     'hive.vectorized.execution.reduce.groupby.enabled': 'true',
                     'hive.exec.parallel': 'true',
                     'hive.exec.parallel.thread.number': '16',
                     'hive.cli.print.header': 'true'
                     }
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
                        "Bad/wrong hostname",
                        "Bad/wrong port",
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
