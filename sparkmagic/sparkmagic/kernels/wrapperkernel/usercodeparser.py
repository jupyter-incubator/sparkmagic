# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from sparkmagic.kernels.kernelmagics import KernelMagics


class UserCodeParser(object):
    # A list of the names of all magics that are cell magics, but which have no cell body input.
    # For example, the %%info magic has no cell body input, i.e. it is incorrect to call
    #    %%info
    #    some_input
    _magics_with_no_cell_body = [i.__name__ for i in [KernelMagics.info, KernelMagics.logs, KernelMagics.cleanup,
                                                      KernelMagics.delete, KernelMagics.help, KernelMagics.spark,
                                                      KernelMagics.send_to_spark]]

    def get_code_to_run(self, code):
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
        elif code.startswith("%%") or code.startswith("%"):
            # If they use other line magics:
            #       %autosave
            #       my spark code
            # my spark code would be run locally and there might be an error.
            return code
        elif not code:
            return code
        else:
            return u"%%spark\n{}".format(code)
