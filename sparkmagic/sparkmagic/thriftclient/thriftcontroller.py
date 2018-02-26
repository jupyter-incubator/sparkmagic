import os
import re
from time import time
from collections import namedtuple

from pyhive import hive
from thrift.transport.TTransport import TTransportException

import sparkmagic.utils.configuration as conf
from sparkmagic.thriftclient.thriftexceptions import ThriftConfigurationError
from sparkmagic.thriftclient.thriftutils import alti_or_local

class ThriftController:
    def __init__(self, ipython_display):
        self.ipython_display = ipython_display
        self.hiveconf = {}
        self.cursor = None
        self.connection = None

    def locate_connection_details(self):
        # Try to find altiscale cluster-info-env first - then local
        cluster_info = alti_or_local(conf.alti_cluster_info_env(), conf.local_cluster_info_env())

        matches = re.findall(r'[\t ]*HIVESERVER2_HOST[\t ]*=[\t ]*([a-zA-Z0-9_\-.]+)',
                ''.join(line for line in cluster_info.read_lines()))

        if matches:
            thrift_hive_hostname = matches[0]
        else:
            raise ThriftConfigurationError("Could not locate HIVSERVER2_HOST in {!r}".format(cluster_info.path))

        self.connection = ThriftConnection(
            host=thrift_hive_hostname,
            port=conf.thrift_hive_port(),
            user=conf.hive_user(),
            conf=conf.thrift_hivetez_conf())

    # Set query confs
    def set_conf(conf, replace=True):
        # if not replace -> merge
        self.connection.conf.update(conf)

    # Set connection details
    def set_connection(conf, replace=True):
        for k,v in conf.items():
            self.connection[k] = v

    def connect(self):
        if self.connection is None:
            raise ThriftConfigurationError(
                "Cannot connect without connection details.\n" +
                "Make sure hive-site.xml exists in {!r} or {!r}".format(
                    conf.alti_cluster_info_env(),
                    conf.local_cluster_info_env()
                ))
        try:
            t_all = time()
            self.cursor = hive.connect(self.connection.host,
                self.connection.port,
                self.connection.user,
                configuration=self.connection.conf).cursor()
            self.ipython_display.writeln("Start-up time: {:.2f}".format(time() - t_all))
        except TTransportException as tte:
            err = tte.message
            err += "Possible issues in order: {}".format(''.join(['\n-> {}']*5)).format(
                        "Connection to destination host is down",
                        "Bad/wrong hostname: {!r}".format(self.connection.host),
                        "Bad/wrong port: {!r}".format(self.connection.port),
                        "Destination host is not reachable (ssh and port forward is not set up)",
                        "Thriftserver is not running")
            raise ThriftConfigurationError(err)

    def execute(self, query, async=False):
        self.cursor.execute(str(query), async=async)

    def reset(self):
        self.cursor.cancel()
        self.connect()


class ThriftConnection(namedtuple('ThriftConnection', 'host port user conf')):
    __slots__ = ()

    def __repr__(self):
        return "\n".join("{}: {}".format(k,v) for k,v in self._asdict().items())
