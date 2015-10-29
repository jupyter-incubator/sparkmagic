# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import textwrap
from time import sleep, time

from .log import Log
from .constants import Constants
from .livyclienttimeouterror import LivyClientTimeoutError
from .livysessionstate import LivySessionState
from .configuration import get_configuration


class LivySession(object):
    """Session that is livy specific."""
    # TODO(aggftw): make threadsafe

    def __init__(self, http_client, language, session_id, sql_created):
        status_sleep_seconds = get_configuration(Constants.status_sleep_seconds, 2)
        statement_sleep_seconds = get_configuration(Constants.statement_sleep_seconds, 2)
        create_sql_context_timeout_seconds = get_configuration(Constants.create_sql_context_timeout_seconds, 60)

        assert status_sleep_seconds > 0
        assert statement_sleep_seconds > 0
        assert create_sql_context_timeout_seconds > 0
        if session_id == "-1" and sql_created is True:
            raise ValueError("Cannot indicate sql state without session id.")

        self.logger = Log("LivySession")

        language = language.lower()
        if language not in Constants.lang_supported:
            raise ValueError("Session of language '{}' not supported. Session must be of languages {}."
                             .format(language, ", ".join(Constants.lang_supported)))

        if session_id == "-1":
            self._status = Constants.not_started_session_status
            sql_created = False
        else:
            self._status = Constants.busy_session_status

        self._http_client = http_client
        self._status_sleep_seconds = status_sleep_seconds
        self._statement_sleep_seconds = statement_sleep_seconds
        self._create_sql_context_timeout_seconds = create_sql_context_timeout_seconds

        self._state = LivySessionState(session_id, http_client.connection_string,
                                       language, sql_created)

    def get_state(self):
        return self._state

    def start(self):
        """Start the session against actual livy server."""
        # TODO(aggftw): do a pass to make all contracts variables; i.e. not peppered in code
        self.logger.debug("Starting '{}' session.".format(self.language))

        r = self._http_client.post("/sessions", [201], {"kind": self._get_livy_kind()})
        self._state.session_id = str(r.json()["id"])
        self._status = str(r.json()["state"])

        self.logger.debug("Session '{}' started.".format(self.language))

    def create_sql_context(self):
        """Create a sqlContext object on the session. Object will be accessible via variable 'sqlContext'."""
        if self.started_sql_context:
            return

        self.logger.debug("Starting '{}' sql and hive session.".format(self.language))

        self.wait_for_status(Constants.idle_session_status, self._create_sql_context_timeout_seconds)
        self.execute(self._get_sql_context_creation_command())
        self.logger.debug("Started '{}' sql session.".format(self.language))

        self.wait_for_status(Constants.idle_session_status, self._create_sql_context_timeout_seconds)
        self.execute(self._get_hive_context_creation_command())
        self.logger.debug("Started '{}' hive session.".format(self.language))

        self._state.sql_context_created = True

    @property
    def id(self):
        return self._state.session_id

    @property
    def started_sql_context(self):
        return self._state.sql_context_created

    @property
    def language(self):
        return self._state.language

    @property
    def status(self):
        status = self._get_latest_status()

        if status in Constants.possible_session_status:
            self._status = status
        else:
            raise ValueError("Status '{}' not supported by session.".format(status))

        return self._status

    @property
    def http_client(self):
        return self._http_client

    @staticmethod
    def is_final_status(status):
        return status in Constants.final_status
    
    def execute(self, commands):
        """Executes commands in session."""
        code = textwrap.dedent(commands)

        data = {"code": code}
        r = self._http_client.post(self._statements_url(), [201], data)
        statement_id = r.json()['id']
        
        return self._get_statement_output(statement_id)

    def delete(self):
        """Deletes the session and releases any resources."""
        self.logger.debug("Deleting session '{}'".format(self.id))

        if self._status != Constants.not_started_session_status and self._status != Constants.dead_session_status:
            self._http_client.delete("/sessions/{}".format(self.id), [200, 404])
            self._status = Constants.dead_session_status
            self._state.session_id = "-1"
        else:
            raise ValueError("Cannot delete session {} that is in state '{}'."
                             .format(self.id, self._status))

    def wait_for_status(self, status, seconds_to_wait):
        """Wait for session to be in a certain status. Sleep meanwhile. Calls done every status_sleep_seconds as
        indicated by the constructor."""
        start_time = time()
        current_status = self.status
        if current_status == status:
            return
        elif seconds_to_wait > 0:
            self.logger.debug("Session {} in state {}. Sleeping {} seconds."
                              .format(self.id, current_status, seconds_to_wait))
            sleep(self._status_sleep_seconds)
            elapsed = (time() - start_time)
            return self.wait_for_status(status, seconds_to_wait - elapsed)
        else:
            raise LivyClientTimeoutError("Session {} did not reach {} status in time. Current status is {}."
                                         .format(self.id, status, current_status))

    def _statements_url(self):
        return "/sessions/{}/statements".format(self.id)

    def _get_latest_status(self):
        """Get current session state. Network call."""
        r = self._http_client.get("/sessions", [200])
        sessions = r.json()["sessions"]
        filtered_sessions = [s for s in sessions if s["id"] == int(self.id)]
                    
        if len(filtered_sessions) != 1:
            raise ValueError("Expected one session of id {} and got {} sessions."
                             .format(self.id, len(filtered_sessions)))
            
        session = filtered_sessions[0]
        return session['state']
    
    def _get_statement_output(self, statement_id):
        statement_running = True
        output = ""
        while statement_running:
            r = self._http_client.get(self._statements_url(), [200])
            statement = [i for i in r.json()["statements"] if i["id"] == statement_id][0]
            status = statement["state"]

            self.logger.debug("Status of statement {} is {}.".format(statement_id, status))

            if status == "running":
                sleep(self._statement_sleep_seconds)
            else:
                statement_running = False
                
                statement_output = statement["output"]
                if statement_output["status"] == "ok":
                    output = statement_output["data"]["text/plain"]
                elif statement_output["status"] == "error":
                    output = statement_output['evalue']

        return output

    def _get_livy_kind(self):
        if self.language == Constants.lang_scala:
            return Constants.session_kind_spark
        elif self.language == Constants.lang_python:
            return Constants.session_kind_pyspark
        else:
            raise ValueError("Cannot get session kind for {}.".format(self.language))

    def _get_sql_context_creation_command(self):
        if self.language == Constants.lang_scala:
            sql_context_command = "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n" \
                                  "import sqlContext.implicits._"
        elif self.language == Constants.lang_python:
            sql_context_command = "from pyspark.sql import SQLContext\nfrom pyspark.sql.types import *\n" \
                                  "sqlContext = SQLContext(sc)"
        else:
            raise ValueError("Do not know how to create sqlContext in session of language {}.".format(self.language))

        return sql_context_command

    def _get_hive_context_creation_command(self):
        if self.language == Constants.lang_scala:
            hive_context_command = "val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)"
        elif self.language == Constants.lang_python:
            hive_context_command = "from pyspark.sql import HiveContext\nhiveContext = HiveContext(sc)"
        else:
            raise ValueError("Do not know how to create hiveContext in session of language {}.".format(self.language))

        return hive_context_command
