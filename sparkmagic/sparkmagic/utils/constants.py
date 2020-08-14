# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import os

HOME_PATH = os.environ.get("SPARKMAGIC_CONF_DIR", "~/.sparkmagic")
CONFIG_FILE = os.environ.get("SPARKMAGIC_CONF_FILE", "config.json")

SESSION_KIND_SPARK = "spark"
SESSION_KIND_PYSPARK = "pyspark"
SESSION_KIND_SPARKR = "sparkr"
SESSION_KINDS_SUPPORTED = [SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, SESSION_KIND_SPARKR]

LIBRARY_LOADED_EVENT = "notebookLoaded"
CLUSTER_CHANGE_EVENT = "notebookClusterChange"
SESSION_CREATION_START_EVENT = "notebookSessionCreationStart"
SESSION_CREATION_END_EVENT = "notebookSessionCreationEnd"
SESSION_DELETION_START_EVENT = "notebookSessionDeletionStart"
SESSION_DELETION_END_EVENT = "notebookSessionDeletionEnd"
STATEMENT_EXECUTION_START_EVENT = "notebookStatementExecutionStart"
STATEMENT_EXECUTION_END_EVENT = "notebookStatementExecutionEnd"
SQL_EXECUTION_START_EVENT = "notebookSqlExecutionStart"
SQL_EXECUTION_END_EVENT = "notebookSqlExecutionEnd"
MAGIC_EXECUTION_START_EVENT = "notebookMagicExecutionStart"
MAGIC_EXECUTION_END_EVENT = "notebookMagicExecutionEnd"

CLUSTER_DNS_NAME = "ClusterDnsName"
SESSION_ID = "SessionId"
SESSION_GUID = "SessionGuid"
STATEMENT_ID = "StatementId"
STATEMENT_GUID = "StatementGuid"
SQL_GUID = "SqlGuid"
MAGIC_NAME = "MagicName"
MAGIC_GUID = "MagicGuid"
LIVY_KIND = "LivyKind"
STATUS = "Status"
SUCCESS = "Success"
EXCEPTION_TYPE = "ExceptionType"
EXCEPTION_MESSAGE = "ExceptionMessage"
SAMPLE_METHOD = "SampleMethod"
MAX_ROWS = "MaxRows"
SAMPLE_FRACTION = "SampleFraction"
ERROR_MESSAGE = "ErrorMessage"
STATUS_CODE = "StatusCode"

CONTEXT_NAME_SPARK = "spark"
CONTEXT_NAME_SQL = "sql"

LANG_SCALA = "scala"
LANG_PYTHON = "python"
LANG_R = "r"
LANGS_SUPPORTED = [LANG_SCALA, LANG_PYTHON, LANG_R]

LONG_RANDOM_VARIABLE_NAME = "yQeKOYBsFgLWWGWZJu3y"

WIDGET_WIDTH = "800px"

MAGICS_LOGGER_NAME = "magicsLogger"

# The list here https://livy.incubator.apache.org/docs/latest/rest-api.html
# appears incomplete; full list is in
# https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/org/apache/livy/sessions/SessionState.scala:
IDLE_SESSION_STATUS = "idle"
ERROR_SESSION_STATUS = "error"
DEAD_SESSION_STATUS = "dead"
NOT_STARTED_SESSION_STATUS = "not_started"
STARTING_SESSION_STATUS = "starting"
BUSY_SESSION_STATUS = "busy"
SUCCESS_SESSION_STATUS = "success"
SHUT_DOWN_SESSION_STATUS = "shutting_down"
RUNNING_SESSION_STATUS = "running"
KILLED_SESSION_STATUS = "killed"
RECOVERING_SESSION_STATUS = "recovering"

POSSIBLE_SESSION_STATUS = [NOT_STARTED_SESSION_STATUS, IDLE_SESSION_STATUS, STARTING_SESSION_STATUS,
                           BUSY_SESSION_STATUS, ERROR_SESSION_STATUS, DEAD_SESSION_STATUS,
                           SUCCESS_SESSION_STATUS, SHUT_DOWN_SESSION_STATUS, RUNNING_SESSION_STATUS,
                           KILLED_SESSION_STATUS, RECOVERING_SESSION_STATUS]
FINAL_STATUS = [DEAD_SESSION_STATUS, ERROR_SESSION_STATUS, SUCCESS_SESSION_STATUS,
                KILLED_SESSION_STATUS]

ERROR_STATEMENT_STATUS = "error"
CANCELLED_STATEMENT_STATUS = "cancelled"
AVAILABLE_STATEMENT_STATUS = "available"
FINAL_STATEMENT_STATUS = [ERROR_STATEMENT_STATUS, CANCELLED_STATEMENT_STATUS, AVAILABLE_STATEMENT_STATUS]

DELETE_SESSION_ACTION = "delete"
START_SESSION_ACTION = "start"
DO_NOTHING_ACTION = "nothing"

INTERNAL_ERROR_MSG = "An internal error was encountered.\n" \
                     "Please file an issue at https://github.com/jupyter-incubator/sparkmagic\nError:\n{}"
EXPECTED_ERROR_MSG = "An error was encountered:\n{}"

YARN_RESOURCE_LIMIT_MSG = "Queue's AM resource limit exceeded."
RESOURCE_LIMIT_WARNING = "Warning: The Spark session does not have enough YARN resources to start. {}"

LIVY_HEARTBEAT_TIMEOUT_PARAM = u"heartbeatTimeoutInSecond"
LIVY_KIND_PARAM = u"kind"

NO_AUTH = "None"
AUTH_KERBEROS = "Kerberos"
AUTH_BASIC = "Basic_Access"

CONFIGURABLE_RETRY = "configurable"
LINEAR_RETRY = "linear"

MIMETYPE_IMAGE_PNG = "image/png"
MIMETYPE_TEXT_HTML = "text/html"
MIMETYPE_TEXT_PLAIN = "text/plain"
