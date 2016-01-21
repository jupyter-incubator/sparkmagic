from remotespark.utils.ipythondisplay import IpythonDisplay
from mock import MagicMock, patch
import sys

def test_stdout_flush():
    ipython_shell = MagicMock()
    ipython_display = IpythonDisplay()
    ipython_display._ipython_shell = ipython_shell
    sys.stdout = MagicMock()

    ipython_display.write('Testing Stdout Flush')
    assert sys.stdout.flush.call_count == 1

def test_stderr_flush():
    ipython_shell = MagicMock()
    ipython_display = IpythonDisplay()
    ipython_display._ipython_shell = ipython_shell
    sys.stderr = MagicMock()

    ipython_display.send_error('Testing Stderr Flush')
    assert sys.stderr.flush.call_count == 1