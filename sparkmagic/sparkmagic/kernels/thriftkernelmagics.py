from __future__ import print_function
from IPython.core.magic import magics_class
from sparkmagic.magics.thriftmagicbase import ThriftMagicBase
from IPython.core.magic import needs_local_scope, cell_magic, line_magic
from IPython.core.magic_arguments import argument, magic_arguments

from sparkmagic.utils.utils import parse_argstring_or_throw, get_coerce_value
from sparkmagic.utils.constants import THRIFT_VAR, THRIFT_LOG_VAR

@magics_class
class ThriftKernelMagics(ThriftMagicBase):
    def __init__(self, shell):
        super(ThriftKernelMagics, self).__init__(shell)

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=THRIFT_VAR, help="If present, query will be stored in variable of this "
                                                             "name.")
    @argument("-l", "--logs", type=str, default=THRIFT_LOG_VAR, help="If present, logs will be stored in variable of this "
                                                             "name.")
    @argument("-q", "--quiet", type=bool, default=False, const=True, nargs="?", help="Return None instead of the dataframe.")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for SQL queries: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the server for SQL queries")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from SQL queries")
    @argument("-c", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) "
                                                                        "of the dataframe or not (pass False)")
    #@wrap_unexpected_exceptions
    #@handle_expected_exceptions
    def sql(self, line, cell="", local_ns=None):
        args = parse_argstring_or_throw(self.sql, line)

        coerce = get_coerce_value(args.coerce)

        return self.execute_sqlquery(cell, args.samplemethod, args.maxrows, args.samplefraction,
                                     args.output, args.logs, args.quiet, coerce)


def load_ipython_extension(ip):
    ip.register_magics(ThriftKernelMagics)
