import os
import re
from time import time
from collections import namedtuple

from pyhive import hive
from thrift.transport.TTransport import TTransportException

from sparkmagic.utils.thriftlogger import ThriftLog
import sparkmagic.utils.configuration as conf
from sparkmagic.thriftclient.thriftexceptions import ThriftConfigurationError
from sparkmagic.thriftclient.thriftutils import env_alti_local

from sparkmagic.utils.constants import HIVESERVER2_HOST, HIVE_CONF_RC

class ThriftController:
    def __init__(self, ipython_display):
        self.logger = ThriftLog(self.__class__, conf.logging_config_debug())
        self.ipython_display = ipython_display
        self.hiveconf = {}
        self.cursor = None
        self.connection = None

    def locate_connection_details(self):
        # Try to find altiscale cluster-info-env first - then local

        # hiveserver host name
        if os.getenv(HIVESERVER2_HOST):
            thrift_hive_hostname = os.getenv(HIVESERVER2_HOST)
        else:
            cluster_info = env_alti_local(alti=conf.alti_cluster_info_env(),
                                          local=conf.local_cluster_info_env())

            matches = re.findall(r'[\t ]*HIVESERVER2_HOST[\t ]*=[\t ]*([a-zA-Z0-9_\-.]+)',
                    ''.join(line for line in cluster_info.read_lines()))

            if matches:
                thrift_hive_hostname = matches[0]
            else:
                raise ThriftConfigurationError("Could not locate env {0!r} or {0!r} in file {1!r}".format(HIVSERVER2_HOST, cluster_info.path))


        # Find hive configuration setting
        hivercfile = None
        if os.getenv(HIVESERVER2_HOST):
            hivercfile = os.getenv(HIVE_CONF_RC)
        elif os.path.isfile(conf.local_thrift_hivetez_conf()):
            hivercfile = conf.local_thrift_hivetez_conf()

        hiveconf = {}
        if hivercfile is None:
            self.logger.warn("No hive configuration found at env {!r} or file {!r}".format(HIVE_CONF_RC, conf.local_thrift_hivetez_conf()))
        else:
            with open(hivercfile, 'r') as f:
                hiveconfs = f.readlines()
            for hc in hiveconfs:
                key, val = hc.split('=')
                hiveconf[key.strip()] = val.strip()
        self.logger.info("Saving configuration: {}".format(hiveconf))

        # Find username
        user = conf.hive_user()
        if not user:
            self.logger.error("USER environmental variable was not found - please set user manually with:")
            self.logger.error(r"%%sqlconfig -c")

        self.connection = ThriftConnection(
            host=thrift_hive_hostname,
            port=conf.thrift_hive_port(),
            user=user,
            conf=hiveconf)

    # Update current configu
    def set_conf(self, conf):
        # Do not overwrite old configuration till new configuration is type-tested
        new_connection = ThriftConnection(**self.connection._asdict())

        # Rows on format SET key=var; are added to conf
        extra_confs = {}

        for k,v in conf.items():
            if hasattr(new_connection, k):
                self.logger.debug("Updating connection with {}={}".format(k,v))
                # If user did not put string in quotations, fail...
                try:
                    guess = eval(v)
                except NameError as e:
                    raise ThriftConfigurationError("Configuration types not valid: {}={}\n".format(k,v) +
                                                    "Likely a variable missing quotations. Please make sure it is in python format (and not JSON for conf)\n"+
                                                    "E.g.\nhost: 'localhost'\nconf: {'SET hive.execution.engine': 'tez'}")
                # For user dictionaries try to turn all values into strings
                if type(guess) == dict:
                    guess = {k: str(v) for k,v in guess.items()}
                new_connection = eval(r"new_connection._replace({}={!r})".format(k,guess))
            else:
                raise ThriftConfigurationError("{!r} is not a valid configuration keyword".format(k))
        try:
            new_connection.check_types()
        except AssertionError as ae:
            raise ThriftConfigurationError("{}: {!r}\n{}\n{}".format("Configuration types not valid", ae.message,
                                            "Likely an error in conf. Please make sure it is in python dict format (not JSON)",
                                            "E.g. 'conf: {'SET hive.execution.engine': 'tez'}'"))
        # Only update connection if everything seems ok
        self.connection = new_connection

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
            err += "Possible issues in order: {}\n".format(''.join(['\n-> {}']*5)).format(
                        "Connection to destination host is down",
                        "Bad/wrong hostname: {!r}".format(self.connection.host),
                        "Bad/wrong port: {!r}".format(self.connection.port),
                        "Destination host is not reachable (ssh and port forward is not set up)",
                        "Thriftserver is not running")
            raise ThriftConfigurationError(err)

    def execute(self, query, async=False):
        self.logger.debug("Executing query...")
        self.cursor.execute(str(query), async=async)

    def reset(self):
        self.logger.debug("Stopping current cursor...")
        try:
            self.cursor.cancel()
        except Exception as tco:
            # This seems to be a bug in pyhive, but it still kills the connection...
            self.logger.warn("Pyhive still failing cancel with {} and message {}".format(tco.__class__, tco.message))
        except AttributeError as ae:
            # In case cursor is not created yet
            self.logger.debug("Attempted to cancel noexistant cursor...")
        self.logger.debug("Reconnecting...")
        self.connect()

    def reset_defaults(self):
        self.locate_connection_details()


class ThriftConnection(namedtuple('ThriftConnection', 'host port user conf')):
    __slots__ = ()

    def check_types(self):
        assert(type(self.host) == str)
        assert(type(self.port) == str or type(self.port) == int)
        assert(type(self.user) == str)
        assert(type(self.conf) == dict)
        for k,v in self.conf.items():
            assert(type(v) == str, "{} for {} in {} must be 'str'".format(k, v, "conf"))

    def __repr__(self):
        return "\n".join("{}: {}".format(k,v) for k,v in self._asdict().items())

    __str__ = __repr__
