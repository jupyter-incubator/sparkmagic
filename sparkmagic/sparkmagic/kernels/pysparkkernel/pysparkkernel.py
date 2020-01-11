# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.constants import LANG_PYTHON
from sparkmagic.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase


class PySparkKernel(SparkKernelBase):
    def __init__(self, **kwargs):
        implementation = 'PySpark'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'pyspark',
            'mimetype': 'text/x-python',
            'codemirror_mode': {'name': 'python', 'version': 3},
            'pygments_lexer': 'python3'
        }

        session_language = LANG_PYTHON

        super(PySparkKernel, self).__init__(implementation, implementation_version, language, language_version,
                                            language_info, session_language, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=PySparkKernel)
