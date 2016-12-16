from sparkmagic.utils.constants import LANG_PYTHON, LANG_PYTHON3, LANG_SCALA
from sparkmagic.kernels.sparkkernel.sparkkernel import SparkKernel
from sparkmagic.kernels.pysparkkernel.pysparkkernel import PySparkKernel
from sparkmagic.kernels.pyspark3kernel.pyspark3kernel import PySpark3Kernel


class TestPyparkKernel(PySparkKernel):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestPyparkKernel, self).__init__(**kwargs)


class TestPypark3Kernel(PySpark3Kernel):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestPypark3Kernel, self).__init__(**kwargs)


class TestSparkKernel(SparkKernel):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestSparkKernel, self).__init__(**kwargs)


def test_pyspark_kernel_configs():
    kernel = TestPyparkKernel()
    assert kernel.session_language == LANG_PYTHON

    assert kernel.implementation == 'PySpark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'pyspark',
        'mimetype': 'text/x-python',
        'codemirror_mode': {'name': 'python', 'version': 2},
        'pygments_lexer': 'python2'
    }


def test_pyspark3_kernel_configs():
    kernel = TestPypark3Kernel()
    assert kernel.session_language == LANG_PYTHON3

    assert kernel.implementation == 'PySpark3'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'pyspark3',
        'mimetype': 'text/x-python',
        'codemirror_mode': {'name': 'python', 'version': 3},
        'pygments_lexer': 'python3'
    }


def test_spark_kernel_configs():
    kernel = TestSparkKernel()

    assert kernel.session_language == LANG_SCALA

    assert kernel.implementation == 'Spark'
    assert kernel.language == 'no-op'
    assert kernel.language_version == '0.1'
    assert kernel.language_info == {
        'name': 'scala',
        'mimetype': 'text/x-scala',
        'pygments_lexer': 'scala',
        'codemirror_mode': 'text/x-scala'
    }
