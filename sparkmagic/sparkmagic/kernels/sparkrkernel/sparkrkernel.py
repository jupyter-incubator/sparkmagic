# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.constants import LANG_R
from sparkmagic.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase


class SparkRKernel(SparkKernelBase):
    def __init__(self, **kwargs):
        implementation = 'SparkR'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'sparkR',
            'mimetype': 'text/x-rsrc',
            'codemirror_mode': 'text/x-rsrc',
            'pygments_lexer': 'r'
        }

        session_language = LANG_R

        super(SparkRKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, session_language, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkRKernel)
