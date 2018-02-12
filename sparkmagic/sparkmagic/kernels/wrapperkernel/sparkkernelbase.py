# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from kernelbase import KernelBase

class SparkKernelBase(KernelBase):
    def __init__(self, implementation, implementation_version, language, language_version, language_info,
                 session_language, user_code_parser=None, **kwargs):

        # Override
        self.session_language = session_language
        self.logger = SparkLog(u"{}_jupyter_kernel".format(self.session_language))

        super(SparkKernelBase, self).__init__(
                implementation,
                implementation_version,
                language,
                language_version,
                language_info,
                "sparkmagic.kernels.kernelmagics",
                user_code_parser,
                **kwargs)

        if not kwargs.get("testing", False):
            self._change_language()

    def do_shutdown(self, restart):
        # Cleanup
        self._delete_session()

        return self._do_shutdown_ipykernel(restart)

    def _change_language(self):
        register_magics_code = "%%_do_not_call_change_language -l {}\n ".format(self.session_language)
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to change language to {}.".format(self.session_language))
        self.logger.debug("Changed language.")

    def _delete_session(self):
        code = "%%_do_not_call_delete_session\n "
        self._execute_cell_for_user(code, True, False)
