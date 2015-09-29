# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import textwrap
from time import sleep

from .log import Log
from .constants import Constants


class LivySession(object):
    """Session that is livy specific."""
    logger = Log()

    # TODO(aggftw): do a pass to remove all strings and consolidate into variables
    _idle_session_state = "idle"
    _possible_session_states = ['not_started', _idle_session_state, 'starting', 'busy', 'error', 'dead']

    def __init__(self, http_client, kind):
        # TODO(aggftw): make threadsafe
        kind = kind.lower()
        if kind not in Constants.session_supported_kinds:
            raise ValueError("Session of kind '{}' not supported. Session must be of kinds {}."
                             .format(kind, ", ".join(Constants.session_supported_kinds)))

        self._state = "not_started"
        self._id = "-1"
        self._http_client = http_client
        self._kind = kind
        self._started_sql_context = False

    def start(self):
        """Start the session against actual livy server."""
        # TODO(aggftw): do a pass to make all contracts variables; i.e. not peppered in code
        self.logger.debug("Starting '{}' session.".format(self._kind))

        r = self._http_client.post("/sessions", [201], {"kind": self._kind})
        self._id = str(r.json()["id"])
        self._state = str(r.json()["state"])

        self.logger.debug("Session '{}' started.".format(self._kind))

    def create_sql_context(self):
        """Create a sqlContext object on the session. Object will be accessible via variable 'sqlContext'."""
        if self._started_sql_context:
            return

        self.logger.debug("Starting '{}' session.".format(self._kind))

        if self._kind == Constants.session_kind_spark:
            sql_context_command = "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n" \
                                  "import sqlContext.implicits._"
        elif self._kind == Constants.session_kind_pyspark:
            sql_context_command = "from pyspark.sql import SQLContext\nfrom pyspark.sql.types import *\n" \
                                  "sqlContext = SQLContext(sc)"
        else:
            raise ValueError("Do not know how to create sqlContext in session of kind {}.".format(self._kind))

        # Wait for session to be idle
        self.wait_for_state(self._idle_session_state)

        self.execute(sql_context_command)

        self._started_sql_context = True

    @property
    def kind(self):
        """One of: 'spark', 'pyspark'"""
        return self._kind

    @property
    def state(self):
        state = self._get_session_state()

        if state in self._possible_session_states:
            self._state = state
        else:
            raise ValueError("State '{}' not supported by session.".format(state))

        return self._state

    @property
    def http_client(self):
        return self._http_client
    
    def execute(self, commands):
        """Executes commands in session."""
        code = textwrap.dedent(commands)

        self.logger.debug("Executing code:\n{}\nFrom commands: {}".format(code, commands))

        data = {"code": code}
        r = self._http_client.post(self._statements_url(), [201], data)
        statement_id = r.json()['id']
        
        return self._get_statement_output(statement_id)

    def delete(self):
        """Deletes the session and releases any resources."""
        self.logger.debug("Deleting session '{}'".format(self._id))

        if self._state != "not_started" and self._state != "dead":
            self._http_client.delete("/sessions/{}".format(self._id), [200, 404])
            self._state = 'dead'
        else:
            raise ValueError("Cannot delete session {} that is in state '{}'."
                             .format(self._id, self._state))

    def wait_for_state(self, state):
        """Wait for session to be in a certain state. Sleep meanwhile."""
        # TODO(aggftw): implement timeout
        while True:
            current_state = self.state
            if current_state == state:
                return
            else:
                self.logger.debug("Session {} in state {}. Sleeping."
                                  .format(self._id, current_state))
                sleep(2)

    def _statements_url(self):
        return "/sessions/{}/statements".format(self._id)

    def _get_session_state(self):
        """Get current session state."""
        r = self._http_client.get("/sessions", [200])
        sessions = r.json()["sessions"]
        filtered_sessions = [s for s in sessions if s["id"] == int(self._id)]
                    
        if len(filtered_sessions) != 1:
            raise AssertionError("Expected one session of id {} but got {} sessions."
                                 .format(self._id, len(filtered_sessions)))
            
        session = filtered_sessions[0]
        return session['state']
    
    def _get_statement_output(self, statement_id):
        statement_running = True
        output = ""
        while statement_running:
            r = self._http_client.get(self._statements_url(), [200])
            statement = [i for i in r.json()["statements"] if i["id"] == statement_id][0]
            state = statement["state"]

            self.logger.debug("State of statement {} is {}.".format(statement_id, state))

            if state == "running":
                sleep(2)
            else:
                statement_running = False
                
                statement_output = statement["output"]
                if statement_output["status"] == "ok":
                    output = statement_output["data"]["text/plain"]
                elif statement_output["status"] == "error":
                    output = statement_output['evalue']

        self.logger.debug("Output of statement {} is {}.".format(statement_id, output))
        return output
