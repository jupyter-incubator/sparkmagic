# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.constants import LANG_PYTHON
from sparkmagic.kernels.wrapperkernel.thriftkernelbase import ThriftKernelBase
from sparkmagic.kernels.sqlkernel.usercodeparser import UserCodeParser
from sparkmagic.utils.thriftlogger import ThriftLog
import sparkmagic.utils.configuration as conf

class SQLKernel(ThriftKernelBase):
    def __init__(self, **kwargs):
        self.logger = ThriftLog('{}'.format(self.__class__), conf.logging_config_debug())
        implementation = 'SQL'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'sql',
            'mimetype': 'text/x-sql',
            'codemirror_mode': 'text/x-sql',
            'pygments_lexer': 'sql'
        }

        user_code_parser = UserCodeParser()
        self.logger.debug("Initializing KERNEL")
        super(SQLKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, user_code_parser=user_code_parser, **kwargs)

        self.logger.debug("Initialized '{}'".format(self.__class__))

if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SQLKernel)
