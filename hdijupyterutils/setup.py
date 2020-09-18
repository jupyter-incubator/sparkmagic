DESCRIPTION         = "HdiJupyterUtils: Utils for Jupyter projects from HDInsight team"
NAME                = "hdijupyterutils"
PACKAGES            = ['hdijupyterutils']
AUTHOR              = "Jupyter Development Team"
AUTHOR_EMAIL        = "jupyter@googlegroups.org"
URL                 = 'https://github.com/jupyter-incubator/sparkmagic/hdijupyterutils'
DOWNLOAD_URL        = 'https://github.com/jupyter-incubator/sparkmagic/hdijupyterutils'
LICENSE             = 'BSD 3-clause'

import io
import os
import re

from distutils.core import setup


def read(path, encoding='utf-8'):
    path = os.path.join(os.path.dirname(__file__), path)
    with io.open(path, encoding=encoding) as fp:
        return fp.read()


def version(path):
    """Obtain the package version from a python file e.g. pkg/__init__.py

    See <https://packaging.python.org/en/latest/single_source_version.html>.
    """
    version_file = read(path)
    version_match = re.search(r"""^__version__ = ['"]([^'"]*)['"]""",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


VERSION = version('hdijupyterutils/__init__.py')



setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      url=URL,
      download_url=DOWNLOAD_URL,
      license=LICENSE,
      packages=PACKAGES,
      classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'],
      install_requires=[
          'ipython>=4.0.2',
          'nose',
          'mock',
          'ipywidgets>5.0.0',
          'ipykernel>=4.2.2',
          'jupyter>=1',
          'pandas>=0.17.1',
          'numpy',
          'notebook>=4.2',
          # Work around broken-on-Python-2 pyrsistent release:
          "pyrsistent < 0.17 ; python_version < '3.0'",
      ])
