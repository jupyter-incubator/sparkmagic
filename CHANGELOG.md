# Changelog

## 0.12.9 (unreleased)

### Features

* Support server-side rendering of images, so you don't have to ship all the data to the client to do visualization—see the `%matplot` usage in the example notebook. Thanks to wangqiaoshi for the patch.

### Bug fixes

* Work with somewhat newer versions of the Jupyter notebook. Thanks to Jaipreet Singh for the patch, Eric Dill for testing, and G-Research for sponsoring Itamar Turner-Trauring's time.

## 0.12.8

### Bug fixes:

* Updated code to work with Livy 0.5 and later, where Python 3 support is not a different kind of session. Thanks to Gianmario Spacagna for contributing some of the code, and G-Research for sponsoring Itamar Turner-Trauring's time.
* Fixed `AttributeError` on `None`, thanks to Eric Dill.
* `recovering` session status won't cause a blow up anymore. Thanks to G-Research for sponsoring Itamar Turner-Trauring's time.

