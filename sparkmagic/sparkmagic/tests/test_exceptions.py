from mock import MagicMock
import pytest

import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import *


self = None
ipython_display = None
logger = None


def setup_function():
    global self, ipython_display, logger
    self = MagicMock()
    ipython_display = self.ipython_display
    logger = self.logger
    conf.override_all({})


def test_handle_expected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = "MockMethod"
    decorated = handle_expected_exceptions(mock_method)
    assert decorated.__name__ == mock_method.__name__

    result = decorated(self, 1, 2, 3)
    assert result == mock_method.return_value
    assert ipython_display.send_error.call_count == 0
    mock_method.assert_called_once_with(self, 1, 2, 3)


def test_handle_expected_exceptions_handle():
    conf.override_all({"all_errors_are_fatal": False})
    mock_method = MagicMock(side_effect=LivyUnexpectedStatusException("ridiculous"))
    mock_method.__name__ = "MockMethod2"
    decorated = handle_expected_exceptions(mock_method)
    assert decorated.__name__ == mock_method.__name__

    result = decorated(self, 1, kwarg="foo")
    assert result is None
    assert ipython_display.send_error.call_count == 1
    mock_method.assert_called_once_with(self, 1, kwarg="foo")


def test_handle_expected_exceptions_throw():
    with pytest.raises(ValueError):
        mock_method = MagicMock(side_effect=ValueError("HALP"))
        mock_method.__name__ = "mock_meth"
        decorated = handle_expected_exceptions(mock_method)
        assert decorated.__name__ == mock_method.__name__

        _ = decorated(self, 1, kwarg="foo")


def test_handle_expected_exceptions_throws_if_all_errors_fatal():
    with pytest.raises(LivyUnexpectedStatusException):
        conf.override_all({"all_errors_are_fatal": True})
        mock_method = MagicMock(side_effect=LivyUnexpectedStatusException("Oh no!"))
        mock_method.__name__ = "mock_meth"
        decorated = handle_expected_exceptions(mock_method)
        assert decorated.__name__ == mock_method.__name__

        _ = decorated(self, 1, kwarg="foo")


# test wrap with unexpected to true
def test_wrap_unexpected_exceptions():
    mock_method = MagicMock()
    mock_method.__name__ = "tos"
    decorated = wrap_unexpected_exceptions(mock_method)
    assert decorated.__name__ == mock_method.__name__

    result = decorated(self, 0.0)
    assert result == mock_method.return_value
    assert ipython_display.send_error.call_count == 0
    mock_method.assert_called_once_with(self, 0.0)


def test_wrap_unexpected_exceptions_handle():
    mock_method = MagicMock(side_effect=ValueError("~~~~~~"))
    mock_method.__name__ = "tos"
    decorated = wrap_unexpected_exceptions(mock_method)
    assert decorated.__name__ == mock_method.__name__

    result = decorated(self, "FOOBAR", FOOBAR="FOOBAR")
    assert result is None
    assert ipython_display.send_error.call_count == 1
    mock_method.assert_called_once_with(self, "FOOBAR", FOOBAR="FOOBAR")


def test_wrap_unexpected_exceptions_throws_if_all_errors_fatal():
    with pytest.raises(ValueError):
        conf.override_all({"all_errors_are_fatal": True})
        mock_method = MagicMock(side_effect=ValueError("~~~~~~"))
        mock_method.__name__ = "tos"
        decorated = wrap_unexpected_exceptions(mock_method)
        assert decorated.__name__ == mock_method.__name__

        _ = decorated(self, "FOOBAR", FOOBAR="FOOBAR")
