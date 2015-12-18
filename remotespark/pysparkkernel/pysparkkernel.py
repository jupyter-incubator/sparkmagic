# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.sparkkernelbase import SparkKernelBase
from remotespark.utils.constants import Constants


class PySparkKernel(SparkKernelBase):
    # Required by Jupyter - Overridden
    implementation = 'PySpark'
    implementation_version = '1.0'
    language = 'no-op'
    language_version = '0.1'
    language_info = {
        'name': 'pyspark',
        'mimetype': 'text/x-python'
    }
    banner = "PySpark with automatic visualizations"

    # Required by Spark - Overridden
    kernel_conf_name = Constants.lang_python
    session_language = Constants.lang_python
    client_name = "python_jupyter_kernel"


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=PySparkKernel)
