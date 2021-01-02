DESCRIPTION         = "SparkMagic: Spark execution via Livy"
NAME                = "sparkmagic"
PACKAGES            = ['sparkmagic',
                       'sparkmagic/controllerwidget',
                       'sparkmagic/kernels',
                       'sparkmagic/livyclientlib',
                       'sparkmagic/auth',
                       'sparkmagic/magics',
                       'sparkmagic/kernels/pysparkkernel',
                       'sparkmagic/kernels/sparkkernel',
                       'sparkmagic/kernels/sparkrkernel',
                       'sparkmagic/kernels/wrapperkernel',
                       'sparkmagic/utils',
                       'sparkmagic/serverextension']
AUTHOR              = "Jupyter Development Team"
AUTHOR_EMAIL        = "jupyter@googlegroups.org"
URL                 = 'https://github.com/jupyter-incubator/sparkmagic'
DOWNLOAD_URL        = 'https://github.com/jupyter-incubator/sparkmagic'
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


VERSION = version('sparkmagic/__init__.py')



setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      url=URL,
      download_url=DOWNLOAD_URL,
      license=LICENSE,
      packages=PACKAGES,
      include_package_data=True,
      package_data={'sparkmagic': ['kernels/pysparkkernel/kernel.js',
				   'kernels/sparkkernel/kernel.js',
				   'kernels/sparkrkernel/kernel.js',
                                   'kernels/pysparkkernel/kernel.json',
				   'kernels/sparkkernel/kernel.json',
				   'kernels/sparkrkernel/kernel.json']},
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
          'hdijupyterutils>=0.6',
          'autovizwidget>=0.6',
          'ipython>=4.0.2',
          'nose',
          'mock',
          'pandas>=0.17.1',
          'numpy',
          'requests',
          'ipykernel',  # Python 2 will automatically get 4.10
          'ipywidgets>5.0.0',
          'notebook>=4.2',
          'tornado>=4',
          'requests_kerberos>=0.8.0'
      ])

