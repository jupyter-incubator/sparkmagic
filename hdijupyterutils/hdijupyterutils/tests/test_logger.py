# coding=utf-8
import logging

from hdijupyterutils.log import Log, logging_config


def get_logging_config():
    return logging_config()


def test_log_init():
    logging_config = get_logging_config()
    logger = Log("name", logging_config, "something")
    assert isinstance(logger.logger, logging.Logger)


# A MockLogger class with debug and error methods that store the most recent level + message in an
# instance variable.
class MockLogger(object):
    def __init__(self):
        self.level = self.message = None

    def debug(self, message):
        self.level, self.message = "DEBUG", message

    def error(self, message):
        self.level, self.message = "ERROR", message

    def info(self, message):
        self.level, self.message = "INFO", message


class MockLog(Log):
    def __init__(self, name):
        logging_config = get_logging_config()
        super(MockLog, self).__init__(name, logging_config, name)

    def _getLogger(self):
        self.logger = MockLogger()


def test_log_returnvalue():
    logger = MockLog("test2")
    assert isinstance(logger.logger, MockLogger)
    mock = logger.logger
    logger.debug("word1")
    assert mock.level == "DEBUG"
    assert mock.message == "test2\tword1"
    logger.error("word2")
    assert mock.level == "ERROR"
    assert mock.message == "test2\tword2"
    logger.info("word3")
    assert mock.level == "INFO"
    assert mock.message == "test2\tword3"


def test_log_unicode():
    logger = MockLog("test2")
    assert isinstance(logger.logger, MockLogger)
    mock = logger.logger
    logger.debug("word1è")
    assert mock.level == "DEBUG"
    assert mock.message == "test2\tword1è"
    logger.error("word2è")
    assert mock.level == "ERROR"
    assert mock.message == "test2\tword2è"
    logger.info("word3è")
    assert mock.level == "INFO"
    assert mock.message == "test2\tword3è"
