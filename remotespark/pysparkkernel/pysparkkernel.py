# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.sparkkernelbase import SparkKernelBase


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
    username_env_var = "SPARKKERNEL_PYTHON_USERNAME"
    password_env_var = "SPARKKERNEL_PYTHON_PASSWORD"
    url_env_var = "SPARKKERNEL_PYTHON_URL"
    session_language = "python"
    client_name = "python_jupyter_kernel"


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=PySparkKernel)
