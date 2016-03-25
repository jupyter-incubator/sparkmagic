from remotespark.utils.sparkevents import SparkEvents
import remotespark.utils.constants as constants
from remotespark.utils import utils
from nose.tools import with_setup
from mock import MagicMock


def _setup():
    global spark_events, guid, time_stamp

    spark_events = SparkEvents()
    spark_events.handler = MagicMock()
    SparkEvents.get_utc_date_time = MagicMock()
    time_stamp = spark_events.get_utc_date_time()
    guid = utils.generate_uuid()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_emit_session_creation_start_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_CREATION_START_EVENT
    kwargs_list = [(constants.INSTANCE_ID, utils.get_instance_id()),
                   (constants.EVENT_NAME, event_name),
                   (constants.TIMESTAMP, time_stamp),
                   (constants.SESSION_GUID, guid),
                   (constants.LIVY_KIND, language)]

    spark_events.emit_session_creation_start_event(guid, language)

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_creation_end_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_CREATION_END_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [(constants.INSTANCE_ID, utils.get_instance_id()),
                   (constants.EVENT_NAME, event_name),
                   (constants.TIMESTAMP, time_stamp),
                   (constants.SESSION_GUID, guid),
                   (constants.LIVY_KIND, language),
                   (constants.SESSION_ID, session_id),
                   (constants.STATUS, status),
                   (constants.SUCCESS, True),
                   (constants.EXCEPTION_TYPE, ""),
                   (constants.EXCEPTION_MESSAGE, "")]

    spark_events.emit_session_creation_end_event(guid, language, session_id, status, True, "", "")

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_deletion_start_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_DELETION_START_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [(constants.INSTANCE_ID, utils.get_instance_id()),
                   (constants.EVENT_NAME, event_name),
                   (constants.TIMESTAMP, time_stamp),
                   (constants.SESSION_GUID, guid),
                   (constants.LIVY_KIND, language),
                   (constants.SESSION_ID, session_id),
                   (constants.STATUS, status)]

    spark_events.emit_session_deletion_start_event(guid, language, session_id, status)

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)


@with_setup(_setup, _teardown)
def test_emit_session_deletion_end_event():
    language = constants.SESSION_KIND_SPARK
    event_name = constants.SESSION_DELETION_END_EVENT
    status = constants.BUSY_SESSION_STATUS
    session_id = 0
    kwargs_list = [(constants.INSTANCE_ID, utils.get_instance_id()),
                   (constants.EVENT_NAME, event_name),
                   (constants.TIMESTAMP, time_stamp),
                   (constants.SESSION_GUID, guid),
                   (constants.LIVY_KIND, language),
                   (constants.SESSION_ID, session_id),
                   (constants.STATUS, status),
                   (constants.SUCCESS, True),
                   (constants.EXCEPTION_TYPE, ""),
                   (constants.EXCEPTION_MESSAGE, "")]

    spark_events.emit_session_deletion_end_event(guid, language, session_id, status, True, "", "")

    spark_events.get_utc_date_time.assert_called_with()
    spark_events.handler.handle_event.assert_called_once_with(kwargs_list)
