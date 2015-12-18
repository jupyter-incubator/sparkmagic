from remotespark.pysparkkernel.pysparkkernel import PySparkKernel
from remotespark.sparkkernel.sparkkernel import SparkKernel
from remotespark.utils.constants import Constants


def test_pyspark_kernel_configs():
    kernel = PySparkKernel()
    assert kernel.kernel_conf_name == Constants.lang_python
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

    assert kernel.kernel_conf_name == Constants.lang_scala
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
