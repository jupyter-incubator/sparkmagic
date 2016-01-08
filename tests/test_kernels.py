from remotespark.pysparkkernel.pysparkkernel import PySparkKernel
from remotespark.sparkkernel.sparkkernel import SparkKernel
from remotespark.utils.constants import Constants


class TestPyparkKernel(PySparkKernel):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestPyparkKernel, self).__init__(**kwargs)


class TestSparkKernel(SparkKernel):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestSparkKernel, self).__init__(**kwargs)


def test_pyspark_kernel_configs():
    kernel = TestPyparkKernel()
    assert kernel.kernel_conf_name == Constants.lang_python
    assert kernel.session_language == Constants.lang_python
    assert kernel.client_name == "python_jupyter_kernel"

    assert kernel.implementation == 'PySpark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'pyspark',
        'mimetype': 'text/x-python',
        'codemirror_mode': {'name': 'python'},
        'pygments_lexer': 'python2'
    }


def test_spark_kernel_configs():
    kernel = TestSparkKernel()

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
