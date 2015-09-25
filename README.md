# sparkmagic
IPython magics for working with remote Spark clusters

## Development team and Advocate

Subproject development team:

* Alejandro Guerrero Gonzalez, Microsoft (`@aggFTW`)
* Auberon Lopez, Cal Poly (`@alope107`)
* Sangeetha Shekar, Microsoft (`@sangeethashekar`)

Steering Council Advocate

* Brian Granger (`@ellisonbg`)

## Subproject goals, scope and functionality

Goals:
* Provide the ability to connect from any IPython notebook to different remote Spark clusters.
* Frictionless Spark usage from IPython notebook without a local installation of Spark.
* The functionality delivered will be pip-installable or conda-installable.

Scope:
* IPython magics to enable remote Spark code execution through [Livy](https://github.com/cloudera/hue/tree/master/apps/spark/java), a Spark REST endpoint, which allows for Scala, Python, and R support as of September 2015.
* The project will create a Python Livy client that will be used by the magics.
* The project will integrate the output of Livy client (by creating [pandas](https://github.com/pydata/pandas) dataframes from it) with the rich visualization framework that is being proposed [LINK].
* This project takes a dependency on Livy. The crew will work on Livy improvements required to support these scenarios.

Functionality:
* Enable users to connect to a remote Spark cluster running Livy from any IPython notebook to execute Scala, Pyspark, and R code.
* Ability to reference custom libraries pre-installed in the remote Spark cluster.
* Automatic rich visualization of Spark responses when appropriate. Look at [Zeppelin](https://zeppelin.incubator.apache.org/) for a vision of the functionality we want to enable for Spark users.
* Return of pandas dataframes from Spark when appropriate to enable users to transform Spark results with the Python libraries available for pandas dataframes. These transformations will only be available on Python.

Additional notes:
* Livy will be installed on a remote Spark cluster. Livy will create Spark drivers that have full network access to the Spark master and the Spark worker nodes in the cluster.
* Most data transformation will happen on the cluster, by sending Scala, Pyspark, or R code to it, where the Scala, Python, or R execution contexts will live. This will keep compute and data close together.
* Livy JSON responses will be transformed to pandas dataframes when appropriate. These dataframes will be used to generate rich visualizations or to allow users to transform data returned by Spark locally. 
* The functionality enables remote Spark code execution on the cluster (e.g. a Spark cluster on Azure or AWS). Any input/output of data will happen on the filesystem enabled on the cluster and not on the local installation of Jupyter. This enables scenarios like Spark Streaming or ML.

## Audience

Spark users who want to have the best experience possible from a Notebook. These users are familiar with the Jupyter ecosystem and the Python libraries available for IPython.

## Other options

Alternatives we know of:

* Combination of IPython, R kernel (or [rpy2](http://rpy.sourceforge.net/rpy2.html)), and Scala kernel for an in-Spark-cluster Jupyter installation. This does not allow the user to point to different Spark clusters. It might also result in resource contention (CPU or memory) between the Jupyter installation (kernels) and Spark.
* [IBM's Spark kernel](https://github.com/ibm-et/spark-kernel) allows for the execution of Spark code on a local installation of Jupyter in the cluster. Automatic visualizations are not yet supported.
* [sparklingpandas](https://github.com/sparklingpandas/sparklingpandas) builds on Spark's DataFrame class to give users a Pandas-like API. Remote connectivity is not in scope. The project covers Pyspark.

## Integration with Project Jupyter

* These magics will be pip-installable and loadable from any Jupyter/IPython installation.
* By virtue of returning pandas dataframes, the dataframes will be easily visualizable by using the library created by the automatic rich visualizations incubation subproject [LINK].
* There are integration points that a remote Spark job submission story will have to think through regardless of implementation choice. This project will aim to solve them as they arise. Some issues to work through:
	* Figure out the right amount of data from the result set to bring back to the client via the wire. Is it a sample or the top of the result set?
	* Spark Streaming. How do we expose the results endpoint?