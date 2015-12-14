from mock import MagicMock
from pandas.util.testing import assert_frame_equal
import pandas as pd
import numpy as np

from remotespark.livyclientlib.result import SuccessResult, ErrorResult, DataFrameResult

def test_success_result_tostr():
    success = 'Success!!'
    r = SuccessResult(success)
    assert str(r) == success

def test_error_result_tostr():
    error = 'Error!! :((('
    r = ErrorResult(error)
    assert str(r) == error

def test_dataframe_result_tostr():
    df = pd.DataFrame(np.random.randn(5,5))
    r = DataFrameResult(df)
    assert str(r) == str(df)

def test_success_result_render():
    success = 'SUCCESS_RESULT'
    r = SuccessResult(success)
    mock_shell = MagicMock()
    output = r.render(mock_shell)
    assert output is None
    assert mock_shell.write.called_with(success)
    assert not mock_shell.write_err.called

def test_error_result_render():
    error = 'THERE WAS AN ERROR'
    r = ErrorResult(error)
    mock_shell = MagicMock()
    output = r.render(mock_shell)
    assert output is None
    assert mock_shell.write_err.called_with(error)
    assert not mock_shell.write.called

def test_dataframe_result_render():
    df = pd.DataFrame(np.random.randn(5,5))
    r = DataFrameResult(df)
    mock_shell = MagicMock()
    output = r.render(mock_shell)
    assert output is df
    assert not mock_shell.write.called
    assert not mock_shell.write_err.called
