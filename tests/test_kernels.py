from remotespark.pysparkkernel.pysparkkernel import PySparkKernel
from remotespark.sparkkernel.sparkkernel import SparkKernel
from remotespark.livyclientlib.constants import Constants


def test_pyspark_kernel_configs():
    kernel = PySparkKernel()

    assert kernel.username_conf_name == Constants.kernel_python_username
    assert kernel.password_conf_name == Constants.kernel_python_password
    assert kernel.url_conf_name == Constants.kernel_python_url
    assert kernel.session_language == Constants.lang_python
    assert kernel.client_name == "python_jupyter_kernel"

    assert kernel.implementation == 'PySpark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'pyspark',
        'mimetype': 'text/x-python'
    }
    assert kernel.banner == "PySpark with automatic visualizations"


def test_spark_kernel_configs():
    kernel = SparkKernel()

    assert kernel.username_conf_name == Constants.kernel_scala_username
    assert kernel.password_conf_name == Constants.kernel_scala_password
    assert kernel.url_conf_name == Constants.kernel_scala_url
    assert kernel.session_language == Constants.lang_scala
    assert kernel.client_name == "scala_jupyter_kernel"

    assert kernel.implementation == 'Spark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'spark',
        'mimetype': 'text/x-python'
    }
    assert kernel.banner == "Spark with automatic visualizations"
