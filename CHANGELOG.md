# Changelog

## Next Release

## 0.20.5

### Bug Fixes
- Render the output of IPython.display.display_html as HTML
- Pin to pandas<2.0.0

## 0.20.4

### Bug Fixes
- Support non ASCII characters in `%%pretty` output

## 0.20.3

### Bug Fixes
* Fix package install

## 0.20.2

### Updates
* Add official support for Python 3.10 and 3.11
* Migrate from nose to pytest for tests
* Add Poetry as an option for local development

### Bug Fixes
* Support `ipywidgets==8.0.0` 

## 0.20.1

### Bug Fixes

* Fixed compatibility issue with Pandas >=1.5.0 (https://github.com/jupyter-incubator/sparkmagic/issues/776). Thanks @GaspardBT

## 0.20.0

### Updates

* Officially drop support for Python 3.6. Sparkmagic will not guarantee Python 3.6 compatibility moving forward.
* Re-format all code with [`black`](https://black.readthedocs.io/en/stable/index.html) and validate via CI

### Bug Fixes

* Fixed, simplified, and sped up the sparkmagic Docker images for local development and testing
* Adds support for `ipython>=8`
* Adds support for `ipykernel>=6`

## 0.19.2

### Bug Fixes

* Pins `ipython<8.0.0` because of breaking changes. Thanks @utkarshgupta137

## 0.19.1

### Bug Fixes

* Pins `ipykernel<6.0.0` because of breaking changes

### Updates
* Migrate from Travis to Github Actions for CI

## 0.19.0

### Features

* Added one internal magic to enable retry of session creation. Thanks @edwardps
* New `%%pretty` magic for pretty printing a dataframe as an HTML table. Thanks @hegary 
* Update Endpoint widget to shield passwords when entering them in the ipywidget. Thanks @J0rg3M3nd3z @jodom961

## 0.18.0

### Updates

* Officially drop support for Python 2. Sparkmagic will not guarantee Python 2 compatibility moving forward.

### Features

* Update Spark, SparkR, and PySpark kernels to include  language info and file extension for Jupyterlab LSP compatibility. Thanks @skakker
* `KeyboardInterrupt` cancels the Livy statement. Thanks @hanyucui
* Add user to %%info output. Thanks @tprelle

## 0.17.1

### Bug fixes

* Fix missing url passing when using a custom authenticator [#684](https://github.com/jupyter-incubator/sparkmagic/pull/684). Thanks @gthomas-slack

## 0.17.0

### Features

* Customizable Livy authentication methods [#662](https://github.com/jupyter-incubator/sparkmagic/pull/662). Thanks @alexismacaskilll

### Bug fixes

* Fix Dockerfile.jupyter build  [#672](https://github.com/jupyter-incubator/sparkmagic/pull/672)

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

