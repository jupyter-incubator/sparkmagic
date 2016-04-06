from mock import MagicMock
from nose.tools import with_setup, assert_equals, assert_is, raises

from remotespark.livyclientlib.exceptions import *
from remotespark.livyclientlib.exceptions import handle_expected_exceptions, wrap_unexpected_exceptions


self = None
ipython_display = None
logger = None


def _setup():
    global self, ipython_display, logger
    self = MagicMock()
    ipython_display = self.ipython_display
    logger = self.logger


@with_setup(_setup)
def test_handle_expected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = 'MockMethod'
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, 2, 3)
    assert_equals(result, mock_method.return_value)
    assert_equals(ipython_display.send_error.call_count, 0)
    mock_method.assert_called_once_with(self, 1, 2, 3)


@with_setup(_setup)
def test_handle_expected_exceptions_handle():
    mock_method = MagicMock(side_effect=LivyUnexpectedStatusException('ridiculous'))
    mock_method.__name__ = 'MockMethod2'
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, kwarg='foo')
    assert_is(result, None)
    assert_equals(ipython_display.send_error.call_count, 1)
    mock_method.assert_called_once_with(self, 1, kwarg='foo')


@raises(ValueError)
@with_setup(_setup)
def test_handle_expected_exceptions_throw():
    mock_method = MagicMock(side_effect=ValueError('HALP'))
    mock_method.__name__ = 'mock_meth'
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, kwarg='foo')


@with_setup(_setup)
def test_wrap_unexpected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = 'tos'
    decorated = wrap_unexpected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 0.0)
    assert_equals(result, mock_method.return_value)
    assert_equals(ipython_display.send_error.call_count, 0)
    mock_method.assert_called_once_with(self, 0.0)


@with_setup(_setup)
def test_wrap_unexpected_exceptions_handle():
    mock_method = MagicMock(side_effect=ValueError('~~~~~~'))
    mock_method.__name__ = 'tos'
    decorated = wrap_unexpected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 'FOOBAR', FOOBAR='FOOBAR')
    assert_is(result, None)
    assert_equals(ipython_display.send_error.call_count, 1)
    mock_method.assert_called_once_with(self, 'FOOBAR', FOOBAR='FOOBAR')