# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase
from remotespark.utils.constants import LANG_SCALA


class SparkKernel(SparkKernelBase):
    def __init__(self, **kwargs):
        implementation = 'Spark'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'scala',
            'mimetype': 'text/x-scala',
            'pygments_lexer': 'scala'
        }

        session_language = LANG_SCALA

        super(SparkKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, session_language, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkKernel)
