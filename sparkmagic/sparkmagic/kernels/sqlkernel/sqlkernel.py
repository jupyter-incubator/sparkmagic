# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.constants import LANG_PYTHON
from sparkmagic.kernels.wrapperkernel.thriftkernelbase import ThriftKernelBase
from usercodeparser import UserCodeParser


class SQLKernel(ThriftKernelBase):
    def __init__(self, **kwargs):
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

        super(SQLKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, user_code_parser=user_code_parser, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SQLKernel)
