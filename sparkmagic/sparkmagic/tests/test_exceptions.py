from mock import MagicMock
from nose.tools import with_setup, assert_equals, assert_is, raises

import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import *


self = None
ipython_display = None
logger = None


def _setup():
    global self, ipython_display, logger
    self = MagicMock()
    ipython_display = self.ipython_display
    logger = self.logger
    conf.override_all({})


@with_setup(_setup)
def test_handle_expected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = "MockMethod"
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, 2, 3)
    assert_equals(result, mock_method.return_value)
    assert_equals(ipython_display.send_error.call_count, 0)
    mock_method.assert_called_once_with(self, 1, 2, 3)


@with_setup(_setup)
def test_handle_expected_exceptions_handle():
    conf.override_all({"all_errors_are_fatal": False})
    mock_method = MagicMock(side_effect=LivyUnexpectedStatusException("ridiculous"))
    mock_method.__name__ = "MockMethod2"
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, kwarg="foo")
    assert_is(result, None)
    assert_equals(ipython_display.send_error.call_count, 1)
    mock_method.assert_called_once_with(self, 1, kwarg="foo")


@raises(ValueError)
@with_setup(_setup)
def test_handle_expected_exceptions_throw():
    mock_method = MagicMock(side_effect=ValueError("HALP"))
    mock_method.__name__ = "mock_meth"
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, kwarg="foo")


@raises(LivyUnexpectedStatusException)
@with_setup(_setup)
def test_handle_expected_exceptions_throws_if_all_errors_fatal():
    conf.override_all({"all_errors_are_fatal": True})
    mock_method = MagicMock(side_effect=LivyUnexpectedStatusException("Oh no!"))
    mock_method.__name__ = "mock_meth"
    decorated = handle_expected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 1, kwarg="foo")


# test wrap with unexpected to true


@with_setup(_setup)
def test_wrap_unexpected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = "tos"
    decorated = wrap_unexpected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, 0.0)
    assert_equals(result, mock_method.return_value)
    assert_equals(ipython_display.send_error.call_count, 0)
    mock_method.assert_called_once_with(self, 0.0)


@with_setup(_setup)
def test_wrap_unexpected_exceptions_handle():
    mock_method = MagicMock(side_effect=ValueError("~~~~~~"))
    mock_method.__name__ = "tos"
    decorated = wrap_unexpected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, "FOOBAR", FOOBAR="FOOBAR")
    assert_is(result, None)
    assert_equals(ipython_display.send_error.call_count, 1)
    mock_method.assert_called_once_with(self, "FOOBAR", FOOBAR="FOOBAR")


# test wrap with unexpected to true
# test wrap with all to true
@raises(ValueError)
@with_setup(_setup)
def test_wrap_unexpected_exceptions_throws_if_all_errors_fatal():
    conf.override_all({"all_errors_are_fatal": True})
    mock_method = MagicMock(side_effect=ValueError("~~~~~~"))
    mock_method.__name__ = "tos"
    decorated = wrap_unexpected_exceptions(mock_method)
    assert_equals(decorated.__name__, mock_method.__name__)

    result = decorated(self, "FOOBAR", FOOBAR="FOOBAR")
