from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.constants import Constants


def test_log_init():
    logger = Log("something")

    print(Log.level)
    assert logger.level == logger._level
    assert logger.level == Constants.debug_level
