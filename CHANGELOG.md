# Changelog

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

