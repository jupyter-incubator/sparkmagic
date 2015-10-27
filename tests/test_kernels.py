from remotespark.pysparkkernel.pysparkkernel import PySparkKernel
from remotespark.sparkkernel.sparkkernel import SparkKernel


def test_pyspark_kernel_configs():
    kernel = PySparkKernel()

    assert kernel.username_env_var == "SPARKKERNEL_PYTHON_USERNAME"
    assert kernel.password_env_var == "SPARKKERNEL_PYTHON_PASSWORD"
    assert kernel.url_env_var == "SPARKKERNEL_PYTHON_URL"
    assert kernel.session_language == "python"
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

    assert kernel.username_env_var == "SPARKKERNEL_SCALA_USERNAME"
    assert kernel.password_env_var == "SPARKKERNEL_SCALA_PASSWORD"
    assert kernel.url_env_var == "SPARKKERNEL_SCALA_URL"
    assert kernel.session_language == "scala"
    assert kernel.client_name == "scala_jupyter_kernel"

    assert kernel.implementation == 'Spark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'spark',
        'mimetype': 'text/x-python'
    }
    assert kernel.banner == "Spark with automatic visualizations"
