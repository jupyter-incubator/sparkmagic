from __future__ import print_function

from mock import MagicMock, mock_open, patch
from nose.tools import with_setup, raises

from sparkmagic.thriftmagics.magics import ThriftKernelMagics
from sparkmagic.thriftclient.thriftexceptions import ThriftConfigurationError
from sparkmagic.livyclientlib.exceptions import BadUserDataException

from IPython.core.magic import magics_class

shell = None
magics = None
ipython_display = None
thriftcontroller = None
logger = None

@magics_class
class TestThriftKernelMagics(ThriftKernelMagics):
    def __init__(self, shell):
        super(TestThriftKernelMagics, self).__init__(shell)


def _setup():
    global shell, magics, thriftcontroller, ipython_display, logger

    # Set shell to None for ipython
    magics = TestThriftKernelMagics(shell=None)
    magics.shell = MagicMock()
    magics.shell.user_ns = {}
    magics.ipython_display = ipython_display = MagicMock()
    magics.thriftcontroller = thriftcontroller = MagicMock()
    magics.logger = logger = MagicMock()
    magics.execute_sqlquery = MagicMock()

def _setup_no_output():
    _setup()
    global magic_writeln, magic_send_error

    magics.magic_send_error = magic_send_error = MagicMock()
    magics.magic_writeln = magic_writeln = MagicMock()

    magic_send_error.side_effect = lambda x: print(x)
    magic_writeln.side_effect = lambda x: print(x)


def _setup_patch_file():
    _setup()
    global patcher_file

    patcher_file = patch('os.path.isfile', return_value=True)

def _teardown():
    pass


@with_setup(_setup_no_output, _teardown)
def test_sql_direct_config():
    line = '-c'
    cell = 'key1:val1\n\nkey2:val2'
    magics.sqlconfig(line, cell)
    thriftcontroller.set_conf.assert_called_once_with({'key1':'val1', 'key2':'val2'})


@with_setup(_setup_no_output, _teardown)
def test_sql_config_file_not_exists():
    m = mock_open()
    patcher_file = patch('os.path.isfile', return_value=False)

    with patcher_file:
        magics.sqlconfig('-f the_file')
        magic_send_error.assert_called()
        thriftcontroller.set_conf.assert_not_called()


@with_setup(_setup_patch_file, _teardown)
def test_sql_config_file_exists_set_conf():
    line = '-f the_file'
    patcher_open = patch('__builtin__.open', mock_open(read_data='set key=val'))

    with patcher_file, patcher_open:
        magics.sqlconfig('-f the_file')
        thriftcontroller.set_conf.assert_called_once_with({'key':'val'})

@with_setup(_setup_patch_file, _teardown)
def test_sql_config_file_exists_dict_conf():
    patcher_open = patch('__builtin__.open', mock_open(read_data='key:val'))
    with patcher_file, patcher_open:
        magics.sqlconfig('-f the_file')
        thriftcontroller.set_conf.assert_called_once_with({'key':'val'})

@with_setup(_setup_patch_file, _teardown)
def test_sql_config_file_exists_hiveset_conf():
    patcher_open = patch('__builtin__.open', mock_open(read_data='set hiveconf:key=val'))
    with patcher_file, patcher_open:
        magics.sqlconfig('-f the_file')
        thriftcontroller.set_conf.assert_called_once_with({'key':'val'})

@with_setup(_setup_patch_file, _teardown)
def test_sql_config_file_exists_bad_setting():
    m = mock_open()
    patcher_open = patch('__builtin__.open', mock_open(read_data='keyval'))

    with patcher_file, patcher_open:
        magics.sqlconfig('-f the_file')
        magic_send_error.assert_called()

@with_setup(_setup_no_output, _teardown)
def test_sql_config_restore():
    line = '-r'

    magics.sqlconfig(line)
    assert thriftcontroller.reset_defaults.call_count == 1
    assert thriftcontroller.reset.call_count == 1
    thriftcontroller.set_conf.assert_not_called()

@with_setup(_setup_no_output, _teardown)
def test_sql_config_print():
    line = '-p'
    connection = "A string"

    thriftcontroller.connection = connection
    magics.sqlconfig(line)
    magics.magic_writeln.assert_called_once_with(connection)

    thriftcontroller.connection = None
    magics.sqlconfig(line)
    assert magic_send_error.call_count == 1

    thriftcontroller.set_conf.assert_not_called()


@with_setup(_setup, _teardown)
def test_sql_query_no_template():
    cell = 'show databases'

    magics.sql('', cell)
    assert magics.execute_sqlquery.call_count == 1

@with_setup(_setup, _teardown)
def test_sql_query_with_args():
    cell = 'show databases'

    magics.sql('-o alt_var1 -l alt_var2', cell)
    magics.execute_sqlquery.assert_called_once_with(
        cell, None, None, None, 'alt_var1', 'alt_var2', False, None)

@with_setup(_setup, _teardown)
@raises(BadUserDataException)
def test_sql_query_with_bad_args():
    cell = 'show databases'

    magics.sql('-o', cell)

@with_setup(_setup, _teardown)
def test_sql_query_template():
    line = ''
    cell = 'show $<var=name>'

    result = magics.sql(line, cell)
    assert result is None
    magics.execute_sqlquery.assert_not_called()

@with_setup(_setup_no_output, _teardown)
def test_sql_no_args():
    line = ''
    result = magics.sql(line)
    assert result is None
    assert magic_writeln.call_count == 1

@with_setup(_setup_no_output, _teardown)
def test_sql_single_argument():
    line = ''
    cell = 'var_name'
    var = 'var'

    magics.shell.user_ns[cell] = var
    result = magics.sql(line, cell)

    magic_writeln.assert_called_once()
    magic_writeln.assert_called_once_with(var)
    magics.execute_sqlquery.assert_not_called()

@with_setup(_setup_no_output, _teardown)
def test_sqlconnect():
    line = ''

    magics.sqlconnect(line)
    assert magic_writeln.call_count == 1

    thriftcontroller.connect = MagicMock(side_effect = ThriftConfigurationError(""))

    magics.sqlconnect(line)
    assert magic_send_error.call_count == 2

@with_setup(_setup_no_output, _teardown)
def test_sqlrefresh():
    line = ""

    magics.sqlrefresh(line)
    assert magic_writeln.call_count == 1

    thriftcontroller.reset = MagicMock(side_effect = ThriftConfigurationError(""))

    magics.sqlrefresh(line)
    assert magic_send_error.call_count == 2


@with_setup(_setup, _teardown)
def test_magic_send_error():
    msg = "error_msg"
    magics.magic_send_error(msg)
    magics.logger.error.assert_called_once()
    ipython_display.send_error.assert_called_once_with(msg)

@with_setup(_setup, _teardown)
def test_magic_writeln():
    msg = "write_msg"
    magics.magic_writeln(msg)
    magics.logger.info.assert_called_once()
    ipython_display.writeln.assert_called_once_with(msg)
