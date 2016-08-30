import setuptools

setuptools.setup(
    name="sparkmagic_ext",
    version='0.1.0',
    url="https://github.com/jupyter-incubator/sparkmagic",
    author="Jupyter Development Team",
    description="Reconnect sparkmagic kernel to cluster",
    long_description=open('README.md').read(),
    packages=setuptools.find_packages(),
)
