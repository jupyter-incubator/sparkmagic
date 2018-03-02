# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from sparkmagic.kernels.thriftkernelmagics import ThriftKernelMagics
import re

class UserCodeParser(object):
    # A list of the names of all magics that are cell magics, but which have no cell body input.
    # For example, the %%info magic has no cell body input, i.e. it is incorrect to call
    #    %%info
    #    some_input

    _magics_with_no_cell_body = [i.__name__ for i in [ThriftKernelMagics.sqlrefresh,
                                                      ThriftKernelMagics.sqlconnect,
                                                      ThriftKernelMagics.ls_sqlmagics]]
    _magics_with_maybe_cell_body = [i.__name__ for i in [ThriftKernelMagics.sqlconfig]]

    def get_code_to_run(self, code):
        # Remove comments
        splitcode = re.split(r'[\r\n]', code)
        code = '\n'.join(line for line in splitcode if line and not line.startswith('--'))
        try:
            all_but_first_line = code.split(None, 1)[1]
        except IndexError:
            all_but_first_line = ""

        if code.startswith("%%local") or code.startswith("%local"):
            return all_but_first_line
        elif any(code.startswith("%%" + s) for s in self._magics_with_no_cell_body):
            return u"{}\n ".format(code)
        elif any(code.startswith("%" + s) for s in self._magics_with_no_cell_body):
            return u"%{}\n ".format(code)
        elif any(code.startswith("%%" + s) for s in self._magics_with_maybe_cell_body):
            return "{}\n ".format(code)
        elif any(code.startswith("%" + s) for s in self._magics_with_maybe_cell_body):
            return u"%{}\n ".format(code)
        elif code.startswith("%%") or code.startswith("%"):
            # If they use other line magics:
            #       %autosave
            #       my spark code
            # my spark code would be run locally and there might be an error.
            return code
        elif not code:
            return code
        else:
            return u"%%sql\n{}".format(code)
