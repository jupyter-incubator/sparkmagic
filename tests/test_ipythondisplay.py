# coding=utf-8
from remotesparkcommon.ipythondisplay import IpythonDisplay
from mock import MagicMock
import sys

def test_stdout_flush():
    ipython_shell = MagicMock()
    ipython_display = IpythonDisplay()
    ipython_display._ipython_shell = ipython_shell
    sys.stdout = MagicMock()

    ipython_display.write(u'Testing Stdout Flush è')
    assert sys.stdout.flush.call_count == 1

def test_stderr_flush():
    ipython_shell = MagicMock()
    ipython_display = IpythonDisplay()
    ipython_display._ipython_shell = ipython_shell
    sys.stderr = MagicMock()

    ipython_display.send_error(u'Testing Stderr Flush è')
    assert sys.stderr.flush.call_count == 1
