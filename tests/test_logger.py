from nose.tools import with_setup, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.log import Log


def test_log_init():
    logger = Log()

    print(Log.mode)
    assert logger.mode == logger._mode
    assert logger.mode == "normal"
