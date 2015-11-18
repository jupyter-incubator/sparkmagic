from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.constants import Constants
import logging

def test_log_init():
    logger = Log('something')
    assert isinstance(logger.logger, logging.Logger)

# A MockLogger class with debug and error methods that store the most recent level + message in an
# instance variable.
class MockLogger(object):
    def __init__(self):
        self.level = self.message = None

    def debug(self, message):
        self.level, self.message = 'DEBUG', message
    
    def error(self, message):
        self.level, self.message = 'ERROR', message

class MockLog(Log):
    def _getLogger(self):
       self.logger = MockLogger() 

def test_log_returnvalue():
    logger = MockLog('test2')
    assert isinstance(logger.logger, MockLogger)
    mock = logger.logger
    logger.debug('word1')
    assert mock.level == 'DEBUG'
    assert mock.message == 'test2\tword1'
    logger.error('word2')
    assert mock.level == 'ERROR'
    assert mock.message == 'test2\tword2'
