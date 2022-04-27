from hdijupyterutils.constants import INSTANCE_ID, EVENT_NAME, TIMESTAMP
from hdijupyterutils.utils import get_instance_id, generate_uuid
from nose.tools import with_setup, raises
from mock import MagicMock

import sparkmagic.utils.constants as constants
from sparkmagic.utils.sparkevents import SparkEvents


def _setup():
    global spark_events, guid1, guid2, guid3, time_stamp

    spark_events = SparkEvents()
    spark_events.handler = MagicMock()
    spark_events.get_utc_date_time = MagicMock()
    spark_events._verify_language_ok = MagicMock()
    time_stamp = spark_events.get_utc_date_time()
    guid1 = generate_uuid()
    guid2 = generate_uuid()
    guid3 = generate_uuid()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_emit_library_loaded_event():
    event_name = constants.LIBRARY_LOADED_EVENT
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
    ]

    spark_events.emit_library_loaded_event()

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_cluster_change_event():
    status_code = 200

    event_name = constants.CLUSTER_CHANGE_EVENT
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.CLUSTER_DNS_NAME, guid1),
        (constants.STATUS_CODE, status_code),
        (constants.SUCCESS, True),
        (constants.ERROR_MESSAGE, None),
    ]

    spark_events.emit_cluster_change_event(guid1, status_code, True, None)

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_creation_start_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_CREATION_START_EVENT
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
    ]

    spark_events.emit_session_creation_start_event(guid1, language)

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_creation_end_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_CREATION_END_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.STATUS, status),
        (constants.SUCCESS, True),
        (constants.EXCEPTION_TYPE, ""),
        (constants.EXCEPTION_MESSAGE, ""),
    ]

    spark_events.emit_session_creation_end_event(
        guid1, language, session_id, status, True, "", ""
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_deletion_start_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_DELETION_START_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.STATUS, status),
    ]

    spark_events.emit_session_deletion_start_event(guid1, language, session_id, status)

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_deletion_end_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_DELETION_END_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.STATUS, status),
        (constants.SUCCESS, True),
        (constants.EXCEPTION_TYPE, ""),
        (constants.EXCEPTION_MESSAGE, ""),
    ]

    spark_events.emit_session_deletion_end_event(
        guid1, language, session_id, status, True, "", ""
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_statement_execution_start_event():
    language = constants.SESSION_KIND_PYSPARK
    session_id = 7
    event_name = constants.STATEMENT_EXECUTION_START_EVENT

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.STATEMENT_GUID, guid2),
    ]

    spark_events.emit_statement_execution_start_event(
        guid1, language, session_id, guid2
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_statement_execution_end_event():
    language = constants.SESSION_KIND_SPARK
    session_id = 7
    statement_id = 400
    event_name = constants.STATEMENT_EXECUTION_END_EVENT
    success = True
    exception_type = ""
    exception_message = "foo"

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.STATEMENT_GUID, guid2),
        (constants.STATEMENT_ID, statement_id),
        (constants.SUCCESS, success),
        (constants.EXCEPTION_TYPE, exception_type),
        (constants.EXCEPTION_MESSAGE, exception_message),
    ]

    spark_events.emit_statement_execution_end_event(
        guid1,
        language,
        session_id,
        guid2,
        statement_id,
        success,
        exception_type,
        exception_message,
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_sql_execution_start_event():
    event_name = constants.SQL_EXECUTION_START_EVENT
    session_id = 22
    language = constants.SESSION_KIND_SPARK
    samplemethod = "sample"
    maxrows = 12
    samplefraction = 0.5

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.SQL_GUID, guid2),
        (constants.SAMPLE_METHOD, samplemethod),
        (constants.MAX_ROWS, maxrows),
        (constants.SAMPLE_FRACTION, samplefraction),
    ]

    spark_events.emit_sql_execution_start_event(
        guid1, language, session_id, guid2, samplemethod, maxrows, samplefraction
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_sql_execution_end_event():
    event_name = constants.SQL_EXECUTION_END_EVENT
    session_id = 17
    language = constants.SESSION_KIND_SPARK
    success = False
    exception_type = "ValueError"
    exception_message = "You screwed up"

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.SESSION_GUID, guid1),
        (constants.LIVY_KIND, language),
        (constants.SESSION_ID, session_id),
        (constants.SQL_GUID, guid2),
        (constants.STATEMENT_GUID, guid3),
        (constants.SUCCESS, success),
        (constants.EXCEPTION_TYPE, exception_type),
        (constants.EXCEPTION_MESSAGE, exception_message),
    ]

    spark_events.emit_sql_execution_end_event(
        guid1,
        language,
        session_id,
        guid2,
        guid3,
        success,
        exception_type,
        exception_message,
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_magic_execution_start_event():
    event_name = constants.MAGIC_EXECUTION_START_EVENT
    magic_name = "sql"
    language = constants.SESSION_KIND_SPARKR

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.MAGIC_NAME, magic_name),
        (constants.LIVY_KIND, language),
        (constants.MAGIC_GUID, guid1),
    ]

    spark_events.emit_magic_execution_start_event(magic_name, language, guid1)

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_magic_execution_end_event():
    event_name = constants.MAGIC_EXECUTION_END_EVENT
    magic_name = "sql"
    language = constants.SESSION_KIND_SPARKR
    success = True
    exception_type = ""
    exception_message = ""

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (constants.MAGIC_NAME, magic_name),
        (constants.LIVY_KIND, language),
        (constants.MAGIC_GUID, guid1),
        (constants.SUCCESS, success),
        (constants.EXCEPTION_TYPE, exception_type),
        (constants.EXCEPTION_MESSAGE, exception_message),
    ]

    spark_events.emit_magic_execution_end_event(
        magic_name, language, guid1, success, exception_type, exception_message
    )

    spark_events._verify_language_ok.assert_called_once_with(language)
    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


def test_magic_verify_language_ok():
    for language in constants.SESSION_KINDS_SUPPORTED:
        SparkEvents()._verify_language_ok(language)


@raises(AssertionError)
def test_magic_verify_language_ok_error():
    SparkEvents()._verify_language_ok("NYARGLEBARGLE")
