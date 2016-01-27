from mock import MagicMock
from nose.tools import with_setup

import remotespark.utils.configuration as conf
from remotespark.kernels.kernelmagics import KernelMagics
from remotespark.utils.constants import Constants

magic = None
spark_controller = None
shell = None
ipython_display = None


class TestKernelMagics(KernelMagics):
    def __init__(self):
        super(KernelMagics, self).__init__(shell=None)

    @staticmethod
    def get_configuration(language):
        return "user", "pass", "url"


def _setup():
    global magic, spark_controller, shell, ipython_display

    conf.override_all({})

    magic = TestKernelMagics()
    magic.shell = shell = MagicMock()
    magic.ipython_display = ipython_display = MagicMock()
    magic.spark_controller = spark_controller = MagicMock()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_info():
    magic.print_endpoint_info = print_info_mock = MagicMock()
    line = ""
    session_info = ["1", "2"]
    spark_controller.get_all_sessions_endpoint_info = MagicMock(return_value=session_info)

    magic.info(line)

    print_info_mock.assert_called_once_with(session_info)
