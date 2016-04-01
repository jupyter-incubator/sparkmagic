# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


CONFIG_JSON = "config.json"

SESSION_KIND_SPARK = "spark"
SESSION_KIND_PYSPARK = "pyspark"
SESSION_KIND_SPARKR = "sparkr"
SESSION_KINDS_SUPPORTED = [SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, SESSION_KIND_SPARKR]

LIBRARY_LOADED_EVENT = "notebookLoaded"
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
GRAPH_RENDER_EVENT = "notebookGraphRender"

INSTANCE_ID = "InstanceId"
TIMESTAMP = "Timestamp"
EVENT_NAME = "EventName"
SESSION_ID = "SessionId"
SESSION_GUID = "SessionGuid"
STATEMENT_ID = "StatementId"
STATEMENT_GUID = "StatementGuid"
SQL_GUID = "SqlGuid"
MAGIC_NAME = "MagicName"
MAGIC_GUID = "MagicGuid"
LIVY_KIND = "LivyKind"
STATUS = "Status"
GRAPH_TYPE = "GraphType"
SUCCESS = "Success"
EXCEPTION_TYPE = "ExceptionType"
EXCEPTION_MESSAGE = "ExceptionMessage"
SAMPLE_METHOD = "SampleMethod"
MAX_ROWS = "MaxRows"
SAMPLE_FRACTION = "SampleFraction"

CONTEXT_NAME_SPARK = "spark"
CONTEXT_NAME_SQL = "sql"

LANG_SCALA = "scala"
LANG_PYTHON = "python"
LANG_R = "r"
LANGS_SUPPORTED = [LANG_SCALA, LANG_PYTHON, LANG_R]

LONG_RANDOM_VARIABLE_NAME = "_yQeKOYBsFgLWWGWZJu3y"

MAGICS_LOGGER_NAME = "magicsLogger"

IDLE_SESSION_STATUS = "idle"
ERROR_SESSION_STATUS = "error"
DEAD_SESSION_STATUS = "dead"
NOT_STARTED_SESSION_STATUS = "not_started"
STARTING_SESSION_STATUS = "starting"
BUSY_SESSION_STATUS = "busy"

POSSIBLE_SESSION_STATUS = [NOT_STARTED_SESSION_STATUS, IDLE_SESSION_STATUS, STARTING_SESSION_STATUS,
                           BUSY_SESSION_STATUS, ERROR_SESSION_STATUS, DEAD_SESSION_STATUS]
FINAL_STATUS = [DEAD_SESSION_STATUS, ERROR_SESSION_STATUS]

DELETE_SESSION_ACTION = "delete"
START_SESSION_ACTION = "start"
DO_NOTHING_ACTION = "nothing"

INTERNAL_ERROR_MSG = "An internal error was encountered.\n" \
                     "Please file an issue at https://github.com/jupyter-incubator/sparkmagic\nError:\n{}"
EXPECTED_ERROR_MSG = "An error was encountered:\n{}"