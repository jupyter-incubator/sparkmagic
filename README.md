# sparkmagic

Sparkmagic is a set of tools for interactively working with remote Spark clusters through [Livy](https://github.com/cloudera/hue/tree/master/apps/spark/java), a Spark REST server, in [Jupyter](http://jupyter.org) notebooks.
The Sparkmagic project includes a set of magics for interactively running Spark code in multiple languages, as well as some kernels that you can use to turn Jupyter into an integrated Spark environment.

![Automatic SparkContext and SQLContext creation](screenshots/sparkcontext.png)

![Automatic visualization](screenshots/autoviz.png)

![Help](screenshots/help.png)

## Features

* Run Spark code in multiple languages against any remote Spark cluster through Livy
* Automatic SparkContext (`sc`) and HiveContext (`sqlContext`) creation
* Ability to execute SparkSQL queries
* Automatic visualization of SQL queries with the `%%sql` magic in the PySpark and Spark kernels; use an easy visual interface to interactively construct visualizations, no code required
* Easy access to Spark application information and logs (`%%info` magic)
* Ability to capture the output of SQL queries as Pandas dataframes to interact with other Python libraries (e.g. matplotlib)

## Examples

There are two ways to use sparkmagic. Head over to the [examples](examples) section for a demonstration on how to use both models of execution.

### 1. Via the IPython kernel

This model of execution works great for when you are working with data both small and large. The scenario is as follows:

        You are a Data Scientist in the IPython kernel doing data analysis, and you have some Big Data need from time to time. For this, you would like to run some Spark code in your Spark cluster from the convenience of the IPython notebook you always use.

sparkmagic allows you to do so by creating some IPython kernel magics that allow you to execute code in your remote Spark cluster and retrieve the results back to the IPython kernel.

### 2. Via the PySpark and Spark kernels

This model of execution allows you to execute all your code in the Spark cluster. These are, in essence, Python and Scala kernels completely dedicated to Spark code execution.
The scenario is as follows:

        You are a Data Scientist that would like to explore Big-Data-size datasets via a Spark cluster. In order to do so, a dedicated Python or Scala kernel is better suited.

If you want to see how to use all the features in these kernels, simply execute the `%%help` magic in them.

## Installation

1. Install the library

        pip install sparkmagic

2. Make sure that ipywidgets is properly installed by running

        jupyter nbextension enable --py --sys-prefix widgetsnbextension 
        
3. (Optional) Install the wrapper kernels. Do `pip show sparkmagic` and it will show the path where `sparkmagic` is installed at. `cd` to that location and do:

        jupyter-kernelspec install sparkmagic/kernels/sparkkernel
        jupyter-kernelspec install sparkmagic/kernels/pysparkkernel
        
4. (Optional) Modify the configuration file at ~/.sparkmagic/config.json. Look at the [example_config.json](sparkmagic/example_config.json)
        
## Architecture

Sparkmagic uses Livy, a REST server for Spark, to remotely execute all user code. 
The library then automatically collects the output of your code as plain text or a JSON document, displaying the results to you as formatted text or as a Pandas dataframe as appropriate.

![Architecture](screenshots/diagram.png)

This architecture offers us some important advantages:

1. Run Spark code completely remotely; no Spark components need to be installed on the Jupyter server

2. Multi-language support; the Python and Scala kernels are equally feature-rich, and adding support for more languages will be easy

3. Support for multiple endpoints; you can use a single notebook to start multiple Spark jobs in different languages and against different remote clusters

4. Easy integration with any Python library for data science or visualization, like Pandas or [Plotly](https://plot.ly/python/offline)

However, there are some important limitations to note:

1. Some overhead added by sending all code and output through Livy

2. Since all code is run on a remote driver through Livy, all structured data must be serialized to JSON and parsed by the Sparkmagic library so that it can be manipulated and visualized on the client side.
In practice this means that you must use Python for client-side data manipulation in `%%local` mode.

## Contributing

We welcome contributions from everyone. 
If you've made an improvement to our code, please send us a [pull request](https://github.com/jupyter-incubator/sparkmagic/pulls).

To dev install, execute the following:

        git clone https://github.com/jupyter-incubator/sparkmagic
        pip install -e hdijupyterutils 
        pip install -e autovizwidget
        pip install -e sparkmagic
        
and optionally follow steps 3 and 4 above.

To run unit tests, run:

        nosetests hdijupyterutils autovizwidget sparkmagic

If you want to see an enhancement made but don't have time to work on it yourself, feel free to submit an [issue](https://github.com/jupyter-incubator/sparkmagic/issues) for us to deal with.