# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class UserCodeParser(object):
    @staticmethod
    def get_code_to_run(code):
        code_to_run = code

        if not code.strip().startswith("%"):
            code_to_run = "%%spark\n{}".format(code)
        elif code.strip().startswith("%local ") or code.strip().startswith("%local\n"):
            code_to_run = code[6:]
        elif code.strip().startswith("%%local ") or code.strip().startswith("%%local\n"):
            code_to_run = code[7:]

        return code_to_run
