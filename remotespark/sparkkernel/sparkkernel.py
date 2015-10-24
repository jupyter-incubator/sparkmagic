# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.sparkkernelbase import SparkKernelBase


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
    use_altair = False
    username_env_var = "SPARKKERNEL_SCALA_USERNAME"
    password_env_var = "SPARKKERNEL_SCALA_PASSWORD"
    url_env_var = "SPARKKERNEL_SCALA_URL"
    session_language = "scala"
    client_name = "scala_jupyter_kernel"


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkKernel)
