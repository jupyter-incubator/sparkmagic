from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.constants import Constants
import logging

def test_log_init():
    logger = Log("something")
    assert isinstance(logger.logger, logging.Logger)

