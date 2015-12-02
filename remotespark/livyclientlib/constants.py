# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class Constants:
    session_kind_spark = "spark"
    session_kind_pyspark = "pyspark"

    context_name_spark = "spark"
    context_name_sql = "sql"
    context_name_hive = "hive"

    lang_scala = "scala"
    lang_python = "python"
    lang_supported = [lang_scala, lang_python]

    serialize = "serialize"
    serialize_periodically = "serialize_periodically"
    serialize_period_seconds = "serialize_period_seconds"
    display_info = "display_info"

    use_auto_viz = "use_auto_viz"
    default_chart_type = "default_chart_type"

    kernel_python_username = "kernel_python_username"
    kernel_python_password = "kernel_python_password"
    kernel_python_url = "kernel_python_url"

    kernel_scala_username = "kernel_scala_username"
    kernel_scala_password = "kernel_scala_password"
    kernel_scala_url = "kernel_scala_url"

    magics_logger_name = "magicsLogger"
    logging_config = "logging_config"

    execute_timeout_seconds = "execute_timeout_seconds"
    status_sleep_seconds = "status_sleep_seconds"
    statement_sleep_seconds = "statement_sleep_seconds"
    create_sql_context_timeout_seconds = "create_sql_context_timeout_seconds"

    idle_session_status = "idle"
    error_session_status = 'error'
    dead_session_status = 'dead'
    not_started_session_status = 'not_started'
    starting_session_status = 'starting'
    busy_session_status = 'busy'

    possible_session_status = [not_started_session_status, idle_session_status, starting_session_status,
                               busy_session_status, error_session_status, dead_session_status]
    final_status = [dead_session_status, error_session_status]

    ignore_ssl_errors = "ignore_ssl_errors"
