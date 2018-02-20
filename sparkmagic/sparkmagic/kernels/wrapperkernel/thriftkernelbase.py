# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from kernelbase import KernelBase
from sparkmagic.utils.tabcompleter import Completer

class ThriftKernelBase(KernelBase):
    def __init__(self, implementation, implementation_version, language, language_version, language_info, user_code_parser=None, **kwargs):
        self.logger = SparkLog(u"{}_jupyter_kernel".format('sqlthrift'))
        self._completer = Completer()
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
        self._completer.complete(code, pos)
        matches = self._completer.suggestions()
        prefix = self._completer.prefix()
        (start_pos, end_pos) = self._completer.cursorpostitions()

        # Append prefix before match to avoid 'auto-swap' at tab when only having one option
        matches.append(prefix)

        if matches is None:
            conent = {
                'matches' : matches,
                'cursor_start' : start_pos,
                'cursor_end' : end_pos,
                'metadata' : {},
                'status' : 'ok'
            }
        else:
            content = {
                'matches' : matches,
                'cursor_start' : start_pos,
                'cursor_end' : end_pos,
                'metadata' : {},
                'status' : 'ok'
            }
        return content
