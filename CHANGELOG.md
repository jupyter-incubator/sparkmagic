# Changelog

## 0.16.0

### Bug fixes

* Fix ContextualVersionConflict in Dockerfile.spark. Thanks Linan Zheng, @LinanZheng
* Fix Info Subcommand in RemoteSparkMagic. Thanks Linan Zheng, @LinanZheng
* Fix to ignore unsupported spark session types whilst fetching the session list. Thanks, Murat Burak Migdisoglu, @mmigdiso

## 0.15.0

### Features
* `cleanup_all_sessions_on_exit` configuration to cleanup all registered livy sessions regardless of whether the process exits gracefully. Thanks Juho Autio, @juhoautio
* Add configuration options to for the default `HTTPKerberosAuth` constructor. Thanks Pedro Gonçalves Rossi Rodrigues, @PedroRossi


### Bug fixes

* Respect the `all_errors_are_fatal` flag and raise an exception if the session fails to start. Thanks Devin Stein, @devstein
* Use `requests.Session` to avoid negotiating Kerberos tickets in every request. Thanks Pedro Gonçalves Rossi Rodrigues, @PedroRossi



## 0.14.0

### Bug fixes

* Enabled heartbeat by default, so long-running tasks don't time out. Thanks to John Pugliesi for the bug report.
* The PySpark kernel uses Python 3 lexer, instead of Python 2. Python 2 support is going away in the near future.
* Fixed papermill support; there's a different option now you need to use, `all_errors_are_fatal`. See the README for details. Thanks to Devin Stein for the patch.

## 0.13.1

### Other changes

* Fixed silly mistake in the tests.

## 0.13.0

### Features

* Added two configuration options that make it easier to run Sparkmagic notebooks with Papermill. Thanks to Michael Diolosa for the patch.
* Support `text/html` messages from the Livy server; currently Livy/Spark itself don't really do this, but some experimental tools can benefit from this. Thanks to Steve Suh.

## 0.12.9

### Features

* Support server-side rendering of images, so you don't have to ship all the data to the client to do visualization—see the `%matplot` usage in the example notebook. Thanks to wangqiaoshi for the patch.
* Progress bar for long running queries. Thanks to @juliusvonkohout.

### Bug fixes

* Work correctly with newer versions of the Jupyter notebook. Thanks to Jaipreet Singh for the patch, Eric Dill for testing, and G-Research for sponsoring Itamar Turner-Trauring's time.

### Other changes

* Switch to Plotly 3.

## 0.12.8

### Bug fixes:

* Updated code to work with Livy 0.5 and later, where Python 3 support is not a different kind of session. Thanks to Gianmario Spacagna for contributing some of the code, and G-Research for sponsoring Itamar Turner-Trauring's time.
* Fixed `AttributeError` on `None`, thanks to Eric Dill.
* `recovering` session status won't cause a blow up anymore. Thanks to G-Research for sponsoring Itamar Turner-Trauring's time.

