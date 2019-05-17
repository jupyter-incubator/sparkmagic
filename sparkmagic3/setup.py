DESCRIPTION         = "SparkMagic: Spark execution via Livy"
NAME                = "sparkmagic3"
PACKAGES            = ['sparkmagic3',
                       'sparkmagic3/controllerwidget',
                       'sparkmagic3/kernels',
                       'sparkmagic3/livyclientlib',
                       'sparkmagic3/magics',
                       'sparkmagic3/kernels/pysparkkernel',
                       'sparkmagic3/kernels/pyspark3kernel',
                       'sparkmagic3/kernels/sparkkernel',
                       'sparkmagic3/kernels/sparkrkernel',
                       'sparkmagic3/kernels/wrapperkernel',
                       'sparkmagic3/utils',
                       'sparkmagic3/serverextension']
AUTHOR              = "Jupyter Development Team"
AUTHOR_EMAIL        = "julius@vonkohout.de"
URL                 = 'https://github.com/juliusvonkohout/sparkmagic'
DOWNLOAD_URL        = 'https://github.com/juliusvonkohout/sparkmagic'
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


VERSION = version('sparkmagic3/__init__.py')



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
				   'kernels/pyspark3kernel/kernel.js',
				   'kernels/sparkkernel/kernel.js',
				   'kernels/sparkrkernel/kernel.js',
           'kernels/pysparkkernel/kernel.json',
				   'kernels/pyspark3kernel/kernel.json',
				   'kernels/sparkkernel/kernel.json',
				   'kernels/sparkrkernel/kernel.json']},
      classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'],
      install_requires=[
	'hdijupyterutils>=0.12.7',
	'autovizwidget>=0.12.7',
	'ipython>=7.5.0,<8',
	'nose>=1.3.7',
	'mock>=3.0.5',
	'pandas==0.24.2',
	'numpy>=1.16.3',
	'requests>=2.22.0',
	'ipykernel>=5.1.1,<6',
	'ipywidgets>7.4.2,<8.0',
	'notebook>=5.7.8,<6.0',
	'tornado>=5.1.1,<6',
	'requests_kerberos>=0.12.0'
      ])

