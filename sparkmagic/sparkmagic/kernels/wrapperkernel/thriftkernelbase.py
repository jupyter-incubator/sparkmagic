# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from kernelbase import KernelBase

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
