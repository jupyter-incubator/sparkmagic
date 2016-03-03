# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.kernels.kernelmagics import KernelMagics


class UserCodeParser(object):
    @staticmethod
    def get_code_to_run(code):
        # We assume that every single cell will be either a cell magic or a Spark code cell.
        # Because cell magic bodies need to have *some* content, we append a '\n' at the end.
        lines = code.split("\n")
        all_but_first = "\n".join(lines[1:])

        if code.startswith("%%local"):
            return "{}\n ".format(all_but_first)
        elif code.startswith("%local"):
            return "{}\n ".format(all_but_first)
        elif code.startswith("%%"):
            return "{}\n ".format(code)
        elif code.startswith(UserCodeParser.get_kernel_magic_names_as_line_magics()):
            # We could choose to transform all line magics to cell magics here since we expect that there's no line
            # magics to call, except that there are... for example %autosave and etcetera.
            # So, we'll just transform our magics.
            return "%{}\n ".format(code)
        elif code.startswith("%"):
            # If they use other line magics:
            #       %autosave
            #       my spark code
            # my spark code would be run locally and there would be a syntax error.
            return code
        else:
            return "%%spark\n{}\n ".format(code)

    @staticmethod
    def get_kernel_magic_names_as_line_magics():
        # Do not include the _do_not_call magics and the spark magic, as they should only be called explicitly by us.
        return tuple("%{}".format(i.__name__) for i in [KernelMagics.info, KernelMagics.logs, KernelMagics.configure,
                                                        KernelMagics.sql, KernelMagics.cleanup,
                                                        KernelMagics.delete, KernelMagics.help])
