# sparkmagic

Sparkmagic is a set of tools for interactively working with remote Spark clusters through [Livy](https://github.com/cloudera/hue/tree/master/apps/spark/java), a Spark REST server, in [Jupyter](http://jupyter.org) notebooks.
The Sparkmagic project includes a set of IPython magics for interactively running Spark code in multiple languages, as well as some kernels that you can use to turn Jupyter into an integrated Spark environment.

![Automatic SparkContext and SQLContext creation](screenshots/sparkcontext.png)

![Automatic visualization](screenshots/autoviz.png)

## Features

* Run Spark code in multiple languages against any remote Spark cluster through Livy

* Automatic visualization of SQL queries with the `%%sql` magic in the PySpark and Spark kernels; use an easy visual interface to interactively construct visualizations, no code required

* Capture the output of SQL queries as Pandas dataframes to work with them on your local machine

## Examples

Check out the [examples](examples) directory.

## Installation

1. Install the library

        git clone https://github.com/jupyter-incubator/sparkmagic
        cd sparkmagic
        pip install -e .

2. (Optional) Install the wrapper kernels

        jupyter-kernelspec install remotespark/kernels/sparkkernel
        jupyter-kernelspec install remotespark/kernels/pysparkkernel
        
3. (Optional) Copy the example configuration file to your home directory 

        cp remotespark/example_config.json ~/.sparkmagic/config.json
        