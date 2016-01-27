# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase
from remotespark.utils.constants import Constants


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

        kernel_conf_name = Constants.lang_scala
        session_language = Constants.lang_scala
        client_name = "scala_jupyter_kernel"

        super(SparkKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, kernel_conf_name, session_language, client_name, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkKernel)
