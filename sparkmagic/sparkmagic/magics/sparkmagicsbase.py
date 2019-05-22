# -*- coding: utf-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from six import string_types

from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME, MIMETYPE_TEXT_HTML, MIMETYPE_TEXT_PLAIN
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand
from sparkmagic.livyclientlib.exceptions import SparkStatementException
from sparkmagic.livyclientlib.sendpandasdftosparkcommand import SendPandasDfToSparkCommand
from sparkmagic.livyclientlib.sendstringtosparkcommand import SendStringToSparkCommand
from sparkmagic.livyclientlib.exceptions import BadUserDataException


from threading import Thread
from tqdm import tqdm_notebook
from hops import constants as hopsconstants
from hops import tls

import socket
import ssl
import struct
import pickle
import time
import os
import json


try:
        import http.client as http
except ImportError:
        import httplib as http

MAX_RETRIES = 3
BUFSIZE = 1024 * 2
DEBUG = False

@magics_class
class SparkMagicBase(Magics):

    _STRING_VAR_TYPE = 'str'
    _PANDAS_DATAFRAME_VAR_TYPE = 'df'
    _ALLOWED_LOCAL_TO_SPARK_TYPES = [_STRING_VAR_TYPE, _PANDAS_DATAFRAME_VAR_TYPE]

    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = SparkLog(u"SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        self.logger.debug(u'Initialized spark magics.')

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def do_send_to_spark(self, cell, input_variable_name, var_type, output_variable_name, max_rows, session_name):
        try:
            input_variable_value = self.shell.user_ns[input_variable_name]
        except KeyError:
            raise BadUserDataException(u'Variable named {} not found.'.format(input_variable_name))
        if input_variable_value is None:
            raise BadUserDataException(u'Value of {} is None!'.format(input_variable_name))

        if not output_variable_name:
            output_variable_name = input_variable_name

        if not max_rows:
            max_rows = conf.default_maxrows()

        input_variable_type = var_type.lower()
        if input_variable_type == self._STRING_VAR_TYPE:
            command = SendStringToSparkCommand(input_variable_name, input_variable_value, output_variable_name)
        elif input_variable_type == self._PANDAS_DATAFRAME_VAR_TYPE:
            command = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, max_rows)
        else:
            raise BadUserDataException(u'Invalid or incorrect -t type. Available are: [{}]'.format(u','.join(self._ALLOWED_LOCAL_TO_SPARK_TYPES)))

        (success, result, mime_type) = self.spark_controller.run_command(command, None)
        if not success:
            self.ipython_display.send_error(result)
        else:
            self.ipython_display.write(u'Successfully passed \'{}\' as \'{}\' to Spark'
                                       u' kernel'.format(input_variable_name, output_variable_name))

    def execute_final(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):
        (success, out, mimetype) = self.spark_controller.run_command(Command(cell), session_name)
        if not success:
            if conf.shutdown_session_on_spark_statement_errors():
                self.spark_controller.cleanup()

            raise SparkStatementException(out)
        else:
            if isinstance(out, string_types):
                if mimetype == MIMETYPE_TEXT_HTML:
                    self.ipython_display.html(out)
                else:
                    self.ipython_display.write(out)
            else:
                self.ipython_display.display(out)
            if output_var is not None:
                spark_store_command = self._spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce)
                df = self.spark_controller.run_command(spark_store_command, session_name)
                self.shell.user_ns[output_var] = df

    def execute_spark(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):

        if "lagom as" in cell:
            self.ipython_display.send_error("You are not allowed to do the following: 'import maggy.experiment.lagom as ...'. Please, just use 'import maggy.experiment as experiment' (or something else)")
            raise
        elif ".lagom" in cell:
            client = Client(self.spark_controller, self.session_name, 5, self.ipython_display)
            try:
                client.start_heartbeat()
                if DEBUG:
                    self.ipython_display.writeln("Started heartbeating...")
                self.execute_final(cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce)
            except:
                raise
            finally:
                # 4. Kill thread before leaving current scope
                client.stop()
                try:
                    client.close()
                except:
                    if DEBUG:
                        print("Socket already closed by maggy server.")
                    pass
        else:
            self.execute_final(cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce)


    @staticmethod
    def _spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce):
        return SparkStoreCommand(output_var, samplemethod, maxrows, samplefraction, coerce=coerce)

    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         session, output_var, quiet, coerce):
        sqlquery = self._sqlquery(cell, samplemethod, maxrows, samplefraction, coerce)
        df = self.spark_controller.run_sqlquery(sqlquery, session)
        if output_var is not None:
            self.shell.user_ns[output_var] = df
        if quiet:
            return None
        else:
            return df

    @staticmethod
    def _sqlquery(cell, samplemethod, maxrows, samplefraction, coerce):
        return SQLQuery(cell, samplemethod, maxrows, samplefraction, coerce=coerce)

    def _print_endpoint_info(self, info_sessions, current_session_id):
        if info_sessions:
            info_sessions = sorted(info_sessions, key=lambda s: s.id)
            html = get_sessions_info_html(info_sessions, current_session_id)
            self.ipython_display.html(html)
        else:
            self.ipython_display.html(u'No active sessions.')



class MessageSocket(object):
    """Abstract class w/ length-prefixed socket send/receive functions."""

    def receive(self, sock):
        """
        Receive a message on ``sock``

        Args:
            sock:

        Returns:

        """
        msg = None
        data = b''
        recv_done = False
        recv_len = -1
        while not recv_done:
            buf = sock.recv(BUFSIZE)
            if buf is None or len(buf) == 0:
                raise Exception("socket closed")
            if recv_len == -1:
                recv_len = struct.unpack('>I', buf[:4])[0]
                data += buf[4:]
                recv_len -= len(data)
            else:
                data += buf
                recv_len -= len(buf)
            recv_done = (recv_len == 0)

        msg = pickle.loads(data)
        return msg

    def send(self, sock, msg):
        """
        Send ``msg`` to destination ``sock``.

        Args:
            sock:
            msg:

        Returns:

        """
        data = pickle.dumps(msg)
        buf = struct.pack('>I', len(data)) + data
        sock.sendall(buf)


class Client(MessageSocket):
    """Client to register and await log events

    Args:

    """
    def __init__(self, spark_controller, session_name, hb_interval, ipython_display):
        # socket for heartbeat thread
        self.hb_sock = None
        self.hb_sock = None
        self.server_addr = None
        self.done = False
        self.hb_interval = hb_interval
        self.ipython_display = ipython_display
        self.spark_controller = spark_controller
        self.session_name = session_name
        self._app_id = None
        self._maggy_ip = None
        self._maggy_port = None
        self._secret = None
        self._num_trials = None
        self._trials_todate = None

    def _request(self, req_sock, msg_data=None):
        """Helper function to wrap msg w/ msg_type."""
        msg = {}
        msg['type'] = "LOG"
        msg['secret'] = self._secret

        if msg_data or ((msg_data == True) or (msg_data == False)):
            msg['data'] = msg_data

        done = False
        tries = 0
        while not done and tries < MAX_RETRIES:
            try:
                MessageSocket.send(self, req_sock, msg)
                done = True
            except socket.error as e:
                tries += 1
                if tries >= MAX_RETRIES:
                    raise
                if DEBUG:
                    print("Socket error: {}".format(e))
                req_sock.close()
                req_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    req_sock.connect(self.server_addr)
                except socket.error as exp:
                    # If we can't connect again, maggy is probably done
                    if DEBUG:
                        print("Maggy server terminated.")
                    tries = MAX_RETRIES
                    self.stop()
                    return

        resp = MessageSocket.receive(self, req_sock)

        return resp

    def close(self):
        """Close the client's sockets."""
        if self.hb_sock != None:
            self.hb_sock.close()
            if DEBUG:
                print("HB socket closed")
        if self.t.isAlive():
            self.t.join()

    def start_heartbeat(self):

        def _heartbeat(self):
            self._app_id = self.spark_controller.get_app_id(self.session_name)

            num_tries = 10
            while num_tries > 0:
                num_tries -= 1
                try:
                    if DEBUG:
                        self.ipython_display.writeln("Looking for the maggy server...")
                    self._get_maggy_driver()
                    num_tries = 0
                    self.server_addr = (self._maggy_ip, self._maggy_port)
                except:
                    time.sleep(self.hb_interval)
                    pass

            if DEBUG:
                self.ipython_display.writeln("Serveraddr: {0}, Secret: {1}"
                    .format(self.server_addr, self._secret))

            # 3. Start thread running polling logs in Maggy.
            self.hb_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.hb_sock.connect(self.server_addr)

            if DEBUG:
                self.ipython_display.writeln("Connected to the maggy server...")

            # get total number of trials for first time to init tqdm
            resp = self._request(self.hb_sock,'LOG')

            if DEBUG:
                self.ipython_display.writeln('First message received: {}'.format(resp))

            self._num_trials = 0
            self._trials_todate = 0
            if resp['num_trials'] != None:
                self._num_trials = resp['num_trials']
            if resp['to_date'] != None:
                self._trials_todate = resp['to_date']

            with tqdm_notebook(range(self._num_trials),
                desc='Maggy experiment',
                unit='trial',
                postfix={'early stopped': resp['stopped'],
                    'best metric': resp['metric']}) as pbar:

                while not self.done:
                    time.sleep(self.hb_interval)
                    resp = self._request(self.hb_sock,'LOG')
                    if DEBUG:
                        self.ipython_display.writeln('Another message received: {}'.format(resp))
                    if resp:
                        _ = self._handle_message(resp, pbar)


        self.t = Thread(target=_heartbeat, args=(self,))
        self.t.daemon = True
        self.t.start()


    def stop(self):
        """Stop the Clients's heartbeat thread."""
        self.done = True

    def _handle_message(self, msg, pbar):
        """
        Handles a  message dictionary. Expects a 'type' attribute in
        the message dictionary.

        {‘type’: 'OK' or 'ERR',
            ‘ex_logs’: string,     # aggregated logs by all executors
            ‘num_trials’: int,     # total number planned trials
            ‘to_date’: int,        # number trials finished to date
            ‘stopped’: int,        # number trials early stopped
            ‘metric’: float        # best metric to date
        }

        Args:
            sock:
            msg:

        Returns:

        """
        if msg['type'] != 'OK':
            self.ipython_display.writeln("FAILURE")
            return

        if msg['to_date'] > self._trials_todate:
            pbar.update(msg['to_date'] - self._trials_todate)
            self._trials_todate = msg['to_date']

        pbar.set_postfix({'early stopped': msg['stopped'],
                    'best metric': msg['metric']})

        if msg['ex_logs']:
            self.ipython_display.write(msg['ex_logs'])

        return

    def _get_maggy_driver(self):
        if DEBUG:
            self.ipython_display.writeln(u"Asking Hopsworks")
        method = hopsconstants.HTTP_CONFIG.HTTP_GET
        resource_url = hopsconstants.DELIMITERS.SLASH_DELIMITER + \
                       hopsconstants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + hopsconstants.DELIMITERS.SLASH_DELIMITER + \
                       "maggy" + hopsconstants.DELIMITERS.SLASH_DELIMITER + "drivers" + \
                       hopsconstants.DELIMITERS.SLASH_DELIMITER + self._app_id
        connection = self._get_http_connection(https=True)
        headers = {}
        jwt_text = self._get_jwt()
        headers[hopsconstants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + jwt_text
        connection.request(method, resource_url, None, headers)
        response = connection.getresponse()

        if response.status == hopsconstants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
            headers[hopsconstants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + jwt_text
            connection.request(method, resource, body, headers)
            response = connection.getresponse()

        # '500' response if maggy has not registered yet
        if response.status != 200:
            raise Exception
        resp_body = response.read()
        resp = json.loads(resp_body)

        # Reset values to 'None' if empty string returned
        self._maggy_ip = resp[u'hostIp']
        self._maggy_port = resp[u'port']
        self._secret = resp[u'secret']
        connection.close()

    def _get_hopsworks_rest_endpoint(self):
        elastic_endpoint = os.environ[hopsconstants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]
        return elastic_endpoint

    def _get_host_port_pair(self):
        endpoint = self._get_hopsworks_rest_endpoint()
        if 'http' in endpoint:
            last_index = endpoint.rfind('/')
            endpoint = endpoint[last_index + 1:]
            host_port_pair = endpoint.split(':')
            return host_port_pair

    def _get_http_connection(self, https=False):
        host_port_pair = self._get_host_port_pair()
        if (https):
            PROTOCOL = ssl.PROTOCOL_TLSv1_2
            ssl_context = ssl.SSLContext(PROTOCOL)
            connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]), context = ssl_context)
        else:
            connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
        return connection

    def _get_jwt(self):
        with open(hopsconstants.REST_CONFIG.JWT_TOKEN, "r") as jwt:
            return jwt.read()

    def _send_request(self, connection, method, resource, body=None):
        headers = {}
        jwt_text = self._get_jwt()
        headers[hopsconstants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + jwt_text
        connection.request(method, resource, body, headers)
        response = connection.getresponse()
        if response.status == hopsconstants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
            headers[hopsconstants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + jwt_text
            connection.request(method, resource, body, headers)
            response = connection.getresponse()
        return response
