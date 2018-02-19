# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from kernelbase import KernelBase
import re
from 

class ThriftKernelBase(KernelBase):
    def __init__(self, implementation, implementation_version, language, language_version, language_info, user_code_parser=None, **kwargs):
        self.logger = SparkLog(u"{}_jupyter_kernel".format('sqlthrift'))

        super(ThriftKernelBase, self).__init__(
                implementation,
                implementation_version,
                language,
                language_version,
                language_info,
                "sparkmagic.kernels.thriftkernelmagics",
                user_code_parser,
                **kwargs)

    def do_complete(self, code, pos):
        re.finditer(r'(\w+)', code)
        content = {
            # The list of all matches to the completion request, such as
            # ['a.isalnum', 'a.isalpha'] for the above example.
            'matches' : ["show", "bitch"],

            # The range of text that should be replaced by the above matches when a completion is accepted.
            # typically cursor_end is the same as cursor_pos in the request.
            'cursor_start' : pos,
            'cursor_end' : pos,

            # Information that frontend plugins might use for extra display information about completions.
            'metadata' : {},

            # status should be 'ok' unless an exception was raised during the request,
            # in which case it should be 'error', along with the usual error message content
            # in other messages.
            'status' : 'ok'
        }
        return content
