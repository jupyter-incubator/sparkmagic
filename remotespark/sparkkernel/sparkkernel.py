# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.sparkkernelbase import SparkKernelBase
from remotespark.livyclientlib.constants import Constants


class SparkKernel(SparkKernelBase):
    # Required by Jupyter - Overridden
    implementation = 'Spark'
    implementation_version = '1.0'
    language = 'no-op'
    language_version = '0.1'
    language_info = {
        'name': 'spark',
        'mimetype': 'text/x-python'
    }
    banner = "Spark with automatic visualizations"

    # Required by Spark - Overridden
    username_conf_name = Constants.kernel_scala_username
    password_conf_name = Constants.kernel_scala_password
    url_conf_name = Constants.kernel_scala_url
    session_language = Constants.lang_scala
    client_name = "scala_jupyter_kernel"


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkKernel)
