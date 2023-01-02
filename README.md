[![Build Status](https://travis-ci.org/jupyter-incubator/sparkmagic.svg?branch=master)](https://travis-ci.org/jupyter-incubator/sparkmagic) [![Join the chat at https://gitter.im/sparkmagic/Lobby](https://badges.gitter.im/sparkmagic/Lobby.svg)](https://gitter.im/sparkmagic/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# sparkmagic

Sparkmagic is a set of tools for interactively working with remote Spark clusters in [Jupyter](http://jupyter.org) notebooks. Sparkmagic interacts with remote Spark clusters through a REST server. Currently there are two server implementations compatible with Spararkmagic: 
* [Livy](https://livy.apache.org) - for running interactive sessions on Yarn
* [Lighter](https://github.com/exacaster/lighter) - for running interactive sessions on Yarn or Kubernetes (only PySpark sessions are supported)

The Sparkmagic project includes a set of magics for interactively running Spark code in multiple languages, as well as some kernels that you can use to turn Jupyter into an integrated Spark environment.

![Automatic SparkContext and SQLContext creation](screenshots/sparkcontext.png)

![Automatic visualization](screenshots/autoviz.png)

![Server-side visualization](screenshots/matplotlib.png)

![Help](screenshots/help.png)

## Features

* Run Spark code in multiple languages against any remote Spark cluster through Livy
* Automatic SparkContext (`sc`) and HiveContext (`sqlContext`) creation
* Easily execute SparkSQL queries with the `%%sql` magic
* Automatic visualization of SQL queries in the PySpark, Spark and SparkR kernels; use an easy visual interface to interactively construct visualizations, no code required
* Easy access to Spark application information and logs (`%%info` magic)
* Ability to capture the output of SQL queries as Pandas dataframes to interact with other Python libraries (e.g. matplotlib)
* Send local files or dataframes to a remote cluster (e.g. sending pretrained local ML model straight to the Spark cluster)
* Authenticate to Livy via Basic Access authentication or via Kerberos

## Examples

There are two ways to use sparkmagic. Head over to the [examples](examples) section for a demonstration on how to use both models of execution.

### 1. Via the IPython kernel

The sparkmagic library provides a %%spark magic that you can use to easily run code against a remote Spark cluster from a normal IPython notebook. See the [Spark Magics on IPython sample notebook](examples/Magics%20in%20IPython%20Kernel.ipynb)

### 2. Via the PySpark and Spark kernels

The sparkmagic library also provides a set of Scala and Python kernels that allow you to automatically connect to a remote Spark cluster, run code and SQL queries, manage your Livy server and Spark job configuration, and generate automatic visualizations.
See [Pyspark](examples/Pyspark%20Kernel.ipynb) and [Spark](examples/Spark%20Kernel.ipynb) sample notebooks.

### 3. Sending local data to Spark Kernel

See the [Sending Local Data to Spark notebook](examples/Send%20local%20data%20to%20Spark.ipynb).

## Installation

1. Install the library

        pip install sparkmagic

2. Make sure that ipywidgets is properly installed by running

        jupyter nbextension enable --py --sys-prefix widgetsnbextension 
 
3. If you're using JupyterLab, you'll need to run another command:

        jupyter labextension install "@jupyter-widgets/jupyterlab-manager"

4. (Optional) Install the wrapper kernels. Do `pip show sparkmagic` and it will show the path where `sparkmagic` is installed at. `cd` to that location and do:

        jupyter-kernelspec install sparkmagic/kernels/sparkkernel
        jupyter-kernelspec install sparkmagic/kernels/pysparkkernel
        jupyter-kernelspec install sparkmagic/kernels/sparkrkernel
        
5. (Optional) Modify the configuration file at ~/.sparkmagic/config.json. Look at the [example_config.json](sparkmagic/example_config.json)

6. (Optional) Enable the server extension so that clusters can be programatically changed:

        jupyter serverextension enable --py sparkmagic

## Authentication Methods
Sparkmagic supports: 
* No auth 
* Basic authentication
* Kerberos

The [Authenticator](sparkmagic/sparkmagic/auth/customauth.py) is the mechanism for authenticating to Livy. The base
Authenticator used by itself supports no auth, but it can be subclassed to enable authentication via other methods. 
Two such examples are the [Basic](sparkmagic/sparkmagic/auth/basic.py) and [Kerberos](sparkmagic/sparkmagic/auth/kerberos.py) Authenticators. 

### Kerberos Authenticator

Kerberos support is implemented via the [requests-kerberos](https://github.com/requests/requests-kerberos) package. Sparkmagic expects a kerberos ticket to be available in the system. Requests-kerberos will pick up the kerberos ticket from a cache file. For the ticket to be available, the user needs to have run [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) to create the kerberos ticket.

#### Kerberos Configuration

By default the `HTTPKerberosAuth` constructor provided by the `requests-kerberos` package will use the following configuration
```python
HTTPKerberosAuth(mutual_authentication=REQUIRED)
```
but this will not be right configuration for every context, so it is able to pass custom arguments for this constructor using the following configuration on the `~/.sparkmagic/config.json`
```json
{
    "kerberos_auth_configuration": {
        "mutual_authentication": 1,
        "service": "HTTP",
        "delegate": false,
        "force_preemptive": false,
        "principal": "principal",
        "hostname_override": "hostname_override",
        "sanitize_mutual_error_response": true,
        "send_cbt": true
    }
}
``` 

### Custom Authenticators

You can write custom Authenticator subclasses to enable authentication via other mechanisms. All Authenticator subclasses 
should override the `Authenticator.__call__(request)` method that attaches HTTP Authentication to the given Request object. 

Authenticator subclasses that add additional class attributes to be used for the authentication, such as the [Basic] (sparkmagic/sparkmagic/auth/basic.py) authenticator which adds `username` and `password` attributes, should override the `__hash__`, `__eq__`, `update_with_widget_values`, and `get_widgets` methods to work with these new attributes. This is necessary in order for the Authenticator to use these attributes in the authentication process.

#### Using a Custom Authenticator with Sparkmagic

If your repository layout is:  

        .
        ├── LICENSE
        ├── README.md
        ├── customauthenticator
        │   ├── __init__.py 
        │   ├── customauthenticator.py 
        └── setup.py

Then to pip install from this repository, run: `pip install git+https://git_repo_url/#egg=customauthenticator`

After installing, you need to register the custom authenticator with Sparkmagic so it can be dynamically imported. This can be done in two different ways:
1.  Edit the configuration file at [`~/.sparkmagic/config.json`](config.json) with the following settings:

       ```json
       {
           "authenticators": {
               "Kerberos": "sparkmagic.auth.kerberos.Kerberos",
               "None": "sparkmagic.auth.customauth.Authenticator",
               "Basic_Access": "sparkmagic.auth.basic.Basic",
               "Custom_Auth": "customauthenticator.customauthenticator.CustomAuthenticator"
         }
       }
       ```

       This adds your `CustomAuthenticator` class in `customauthenticator.py` to Sparkmagic. `Custom_Auth` is the authentication type that will be displayed in the `%manage_spark` widget's Auth type dropdown as well as the Auth type passed as an argument to the -t flag in the `%spark add session` magic.   

2. Modify the `authenticators` method in [`sparkmagic/utils/configuration.py`](sparkmagic/sparkmagic/utils/configuration.py) to return your custom authenticator:

      ```python
      def authenticators():
              return {
                      u"Kerberos": u"sparkmagic.auth.kerberos.Kerberos",
                      u"None": u"sparkmagic.auth.customauth.Authenticator",
                      u"Basic_Access": u"sparkmagic.auth.basic.Basic", 
                      u"Custom_Auth": u"customauthenticator.customauthenticator.CustomAuthenticator"
              }
      ```

## Papermill

If you want Papermill rendering to stop on a Spark error, edit the `~/.sparkmagic/config.json` with the following settings:

```json
{
    "shutdown_session_on_spark_statement_errors": true,
    "all_errors_are_fatal": true
}
```

If you want any registered livy sessions to be cleaned up on exit regardless of whether the process exits gracefully or not, you can set:
 
```json
{
    "cleanup_all_sessions_on_exit": true,
    "all_errors_are_fatal": true
}
```

### Conf overrides in code

In addition to the conf at `~/.sparkmagic/config.json`, sparkmagic conf can be overridden programmatically in a notebook.

For example:
```python
import sparkmagic.utils.configuration as conf
conf.override('cleanup_all_sessions_on_exit', True)
```

Same thing, but referencing the conf member: 

```python
conf.override(conf.cleanup_all_sessions_on_exit.__name__, True)
```

NOTE: override for `cleanup_all_sessions_on_exit` must be set _before_ initializing sparkmagic ie. before this:

    %load_ext sparkmagic.magics

## Docker

The included `docker-compose.yml` file will let you spin up a full
sparkmagic stack that includes a Jupyter notebook with the appropriate
extensions installed, and a Livy server backed by a local-mode Spark instance.
(This is just for testing and developing sparkmagic itself; in reality,
sparkmagic is not very useful if your Spark instance is on the same machine!)

In order to use it, make sure you have [Docker](https://docker.com) and
[Docker Compose](https://docs.docker.com/compose/) both installed, and
then simply run:

    docker compose build
    docker compose up

You will then be able to access the Jupyter notebook in your browser at
http://localhost:8888. Inside this notebook, you can configure a
sparkmagic endpoint at http://spark:8998. This endpoint is able to
launch both Scala and Python sessions. You can also choose to start a
wrapper kernel for Scala, Python, or R from the list of kernels.

To shut down the containers, you can interrupt `docker compose` with
`Ctrl-C`, and optionally remove the containers with `docker compose
down`.

If you are developing sparkmagic and want to test out your changes in
the Docker container without needing to push a version to PyPI, you can
set the `dev_mode` build arg in `docker-compose.yml` to `true`, and then
re-build the container. This will cause the container to install your
local version of autovizwidget, hdijupyterutils, and sparkmagic. The local packages are installed with the editable flag, meaning you can make edits directly to the libraries within the Jupyterlab docker service to debug issues in realtime. To make local changes available in Jupyterlab, make  sure to re-run `docker compose build` before spinning up the services.

## Server extension API

### `/reconnectsparkmagic`:
* `POST`:
Allows to specify Spark cluster connection information to a notebook passing in the notebook path and cluster information.
Kernel will be started/restarted and connected to cluster specified.

Request Body example:
        ```
        {
                'path': 'path.ipynb',
                'username': 'username',
                'password': 'password',
                'endpoint': 'url',
                'auth': 'Kerberos',
                'kernelname': 'pysparkkernel'
        }
        ```

*Note that the auth can be either None, Basic_Access or Kerberos based on the authentication enabled in livy. The kernelname parameter is optional and defaults to the one specified on the config file or pysparkkernel if not on the config file.*
Returns `200` if successful; `400` if body is not JSON string or key is not found; `500` if error is encountered changing clusters.

Reply Body example:
        ```
        {
                'success': true,
                'error': null
        }
        ```

## Architecture

Sparkmagic uses Livy, a REST server for Spark, to remotely execute all user code. 
The library then automatically collects the output of your code as plain text or a JSON document, displaying the results to you as formatted text or as a Pandas dataframe as appropriate.

![Architecture](screenshots/diagram.png)

This architecture offers us some important advantages:

1. Run Spark code completely remotely; no Spark components need to be installed on the Jupyter server

2. Multi-language support; the Python, Python3, Scala and R kernels are equally feature-rich, and adding support for more languages will be easy

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

1. Clone the repo
```bash
git clone https://github.com/jupyter-incubator/sparkmagic
```

2. Install local versions of packages
```bash
pip install -e hdijupyterutils 
pip install -e autovizwidget
pip install -e sparkmagic
```

Alternatively, you can use [Poetry](https://python-poetry.org/docs/) to setup a virtual environment

```bash
poetry install
# If you run into issues install numpy or pandas, run
# poetry run pip install numpy pandas
# then re-run poetry install
```

3. Run unit tests, with `pytest`

```bash
# if you don't have pytest and mock installed, run
# pip install pytest mock
pytest
```

If you installed packages with Poetry, run
```bash 
poetry run pytest 
```

If you want to see an enhancement made but don't have time to work on it yourself, feel free to submit an [issue](https://github.com/jupyter-incubator/sparkmagic/issues) for us to deal with.
