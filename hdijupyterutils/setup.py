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
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'],
      install_requires=[
          'ipython>=7.5.0,<8',
          'nose>=1.3.7',
          'mock>=3.0.5',
          'ipywidgets>=7.4.2,<8',
          'ipykernel>=5.1.1,<6',
          'jupyter>=1,<2',
          'pandas==0.24.2',
          'numpy>=1.16.3',
          'notebook>=5.7.8,<6'
      ])

