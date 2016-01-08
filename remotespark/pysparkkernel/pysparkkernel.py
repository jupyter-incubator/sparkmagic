# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.sparkkernelbase import SparkKernelBase
from remotespark.utils.constants import Constants


class PySparkKernel(SparkKernelBase):
    def __init__(self, **kwargs):
        implementation = 'PySpark'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'pyspark',
            'mimetype': 'text/x-python',
            'codemirror_mode' : { 'name' : 'python' },
            'pygments_lexer': 'python2'
        }

        kernel_conf_name = Constants.lang_python
        session_language = Constants.lang_python
        client_name = "python_jupyter_kernel"

        super(PySparkKernel, self).__init__(implementation, implementation_version, language, language_version,
                                            language_info, kernel_conf_name, session_language, client_name, **kwargs)



if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=PySparkKernel)
